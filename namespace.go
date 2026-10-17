package proplyd

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/c12s/proplyd/pkg/api"
	"github.com/c12s/proplyd/pkg/backend"
	"github.com/c12s/proplyd/pkg/protocol"
)

// Re-exported so that most users only import this package.
type (
	Spec          = protocol.Spec
	ReadStrategy  = protocol.ReadStrategy
	WriteStrategy = protocol.WriteStrategy
	SyncMode      = protocol.SyncMode
)

// Strategies; see the protocol package for details.
const (
	ReadThrough  = protocol.ReadThrough
	CacheAside   = protocol.CacheAside
	RefreshAhead = protocol.RefreshAhead
	WriteThrough = protocol.WriteThrough
	WriteAround  = protocol.WriteAround
	SyncWatch    = protocol.SyncWatch
	SyncNone     = protocol.SyncNone
)

// NamespaceConfig configures a namespace handle.
type NamespaceConfig struct {
	// Spec is registered with proplyd. Every client of the namespace must
	// use the same spec.
	Spec Spec
	// Backend gives direct access to the backing store, with full keys
	// (the namespace's KeyPrefix is added by the client). It is used when
	// the cache is unreachable and for cache-aside loads. Without it, those
	// calls fail with ErrCacheUnavailable.
	Backend backend.Backend
	// NearCache configures the in-process near-cache.
	NearCache NearCacheConfig
	// RecentWrites is how many of this client's recent writes are
	// remembered to guarantee read-your-writes across the near-cache, the
	// cache and fallbacks. Default 4096.
	RecentWrites int
}

// Source tells where a read was served from.
type Source int

const (
	SourceNear    Source = iota + 1 // the in-process near-cache
	SourceCache                     // proplyd's cache (Redis)
	SourceBackend                   // the backing store (loaded by proplyd or by the client)
)

func (s Source) String() string {
	switch s {
	case SourceNear:
		return "near"
	case SourceCache:
		return "cache"
	case SourceBackend:
		return "backend"
	}
	return fmt.Sprintf("Source(%d)", int(s))
}

// Item is the result of a read.
type Item struct {
	Found bool
	// Value is shared with the near-cache unless CopyOnRead is set; do
	// not modify it.
	Value []byte
	// Revision is the backing store revision of the value (or the revision
	// at which the key was seen missing).
	Revision int64
	Source   Source
}

// Namespace is a handle on a cache namespace. It is safe for concurrent
// use.
type Namespace struct {
	c      *Client
	name   string
	spec   Spec
	direct backend.Backend // nil without a configured Backend
	near   *nearCache      // nil when disabled
	ncfg   NearCacheConfig
	writes *recentWrites

	registered atomic.Bool
	regMu      sync.Mutex

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Invalidation stream state.
	streamUp atomic.Bool
	lastMsg  atomic.Int64 // unix nanos of the last stream message
	epoch    string       // subscription goroutine only
	position string       // subscription goroutine only

	stats Stats
}

// Namespace registers a namespace and returns a handle on it. If proplyd is
// unreachable, the handle is returned anyway and registration is retried in
// the background; a conflicting or invalid spec is an error.
func (c *Client) Namespace(ctx context.Context, name string, cfg NamespaceConfig) (*Namespace, error) {
	if err := protocol.ValidateNamespace(name); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidArgument, err)
	}
	spec, err := cfg.Spec.Normalize()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidArgument, err)
	}
	if spec.Read == CacheAside && cfg.Backend == nil {
		return nil, fmt.Errorf("%w: cache-aside needs a Backend", ErrInvalidArgument)
	}
	nc := cfg.NearCache
	if nc.Capacity <= 0 {
		nc.Capacity = 10000
	}
	if nc.TTL <= 0 {
		nc.TTL = min(spec.TTL, time.Minute)
	}
	if nc.HeartbeatTimeout <= 0 {
		nc.HeartbeatTimeout = 3 * time.Second
	}
	if nc.TrackedInvalidations <= 0 {
		nc.TrackedInvalidations = 4096
	}
	if cfg.RecentWrites <= 0 {
		cfg.RecentWrites = 4096
	}

	c.mu.Lock()
	switch {
	case c.closed:
		c.mu.Unlock()
		return nil, ErrClosed
	case c.namespaces[name] != nil:
		c.mu.Unlock()
		return nil, fmt.Errorf("%w: namespace %q is already open on this client", ErrInvalidArgument, name)
	}
	c.mu.Unlock()

	ctxNs, cancel := context.WithCancel(context.Background())
	ns := &Namespace{
		c:      c,
		name:   name,
		spec:   spec,
		ncfg:   nc,
		writes: newRecentWrites(cfg.RecentWrites, time.Minute),
		ctx:    ctxNs,
		cancel: cancel,
	}
	if cfg.Backend != nil {
		ns.direct = backend.Prefixed(cfg.Backend, spec.KeyPrefix)
	}
	if !nc.Disabled {
		if ns.near, err = newNearCache(nc); err != nil {
			cancel()
			return nil, err
		}
	}

	if err := ns.register(ctx); err != nil {
		if !cacheUnavailable(err) {
			ns.close()
			return nil, mapErr(err)
		}
		c.log.Warn("proplyd unreachable; namespace registration will be retried", "namespace", name, "err", err)
	}

	c.mu.Lock()
	if c.closed || c.namespaces[name] != nil {
		c.mu.Unlock()
		ns.close()
		return nil, fmt.Errorf("%w: client closed or namespace opened concurrently", ErrInvalidArgument)
	}
	c.namespaces[name] = ns
	c.mu.Unlock()

	if ns.near != nil {
		ns.wg.Add(1)
		go ns.subscribe()
	}
	return ns, nil
}

// Name returns the namespace name.
func (n *Namespace) Name() string { return n.name }

// Spec returns the normalized spec.
func (n *Namespace) Spec() Spec { return n.spec }

// Close stops the namespace's background work. The client stays open.
func (n *Namespace) Close() {
	n.c.mu.Lock()
	if n.c.namespaces[n.name] == n {
		delete(n.c.namespaces, n.name)
	}
	n.c.mu.Unlock()
	n.close()
}

func (n *Namespace) close() {
	n.cancel()
	n.wg.Wait()
	if n.near != nil {
		n.near.close()
	}
}

func (n *Namespace) register(ctx context.Context) error {
	n.regMu.Lock()
	defer n.regMu.Unlock()
	if n.registered.Load() {
		return nil
	}
	err := n.c.call(ctx, n.c.cfg.WriteTimeout, func(ctx context.Context) error {
		_, err := n.c.api.Register(ctx, &api.RegisterRequest{Namespace: n.name, Spec: n.spec.ToProto()})
		return err
	})
	if err == nil {
		n.registered.Store(true)
	}
	return err
}

// withRegistration runs a cache call, registering the namespace first if
// needed and again if proplyd lost it (Redis restarted empty).
func (n *Namespace) withRegistration(ctx context.Context, timeout time.Duration, fn func(context.Context) error) error {
	for attempt := 0; ; attempt++ {
		if !n.registered.Load() {
			if err := n.register(ctx); err != nil {
				return err
			}
		}
		err := n.c.call(ctx, timeout, fn)
		if protocol.Reason(err) == protocol.ReasonNamespaceNotFound && attempt == 0 {
			n.registered.Store(false)
			continue
		}
		return err
	}
}

func (n *Namespace) check(ctx context.Context, key string) error {
	if err := protocol.ValidateKey(key); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidArgument, err)
	}
	if n.ctx.Err() != nil {
		return ErrClosed
	}
	return ctx.Err()
}

// Get returns the value of key and whether it exists.
func (n *Namespace) Get(ctx context.Context, key string) ([]byte, bool, error) {
	it, err := n.Lookup(ctx, key)
	return it.Value, it.Found, err
}

// Lookup reads key: from the near-cache if it holds a usable copy, else
// from the cache, else (cache unreachable) from the backing store.
func (n *Namespace) Lookup(ctx context.Context, key string) (Item, error) {
	if err := n.check(ctx, key); err != nil {
		return Item{}, err
	}
	minRev := n.writes.get(key)
	if n.near != nil {
		if n.nearUsable() {
			if it, ok := n.near.get(key); ok && it.Revision >= minRev {
				n.stats.NearHits.Add(1)
				return it, nil
			}
		}
		n.stats.NearMisses.Add(1)
	}

	var start uint64
	if n.near != nil {
		start = n.near.begin()
	}
	var resp *api.GetResponse
	err := n.withRegistration(ctx, n.c.cfg.ReadTimeout, func(ctx context.Context) error {
		var err error
		resp, err = n.c.api.Get(ctx, &api.GetRequest{Namespace: n.name, Key: key, MinRevision: minRev})
		return err
	})
	if err != nil && ctx.Err() != nil {
		return Item{}, ctx.Err()
	}
	switch {
	case err == nil && resp.GetMiss():
		return n.loadAside(ctx, key, start)
	case err == nil:
		it := Item{Found: resp.GetFound(), Value: resp.GetValue(), Revision: resp.GetRevision(), Source: SourceCache}
		if resp.GetSource() == api.Source_SOURCE_BACKEND {
			it.Source = SourceBackend
			n.stats.CacheLoads.Add(1)
		} else {
			n.stats.CacheHits.Add(1)
		}
		n.installNear(key, start, it)
		return it, nil
	case canFallback(err):
		// The cache is down, or it cannot reach the backing store (but this
		// client may).
		return n.fallbackRead(ctx, key, err)
	}
	return Item{}, mapErr(err)
}

// canFallback reports whether a failed cache call may be served from the
// backing store: the cache is down, cannot reach the store itself, or lost
// the namespace again right after it was re-registered (Redis flapping).
func canFallback(err error) bool {
	return cacheUnavailable(err) || backendUnavailable(err) || protocol.Reason(err) == protocol.ReasonNamespaceNotFound
}

// loadAside handles a cache-aside miss: load from the backing store, fill
// the cache in the background.
func (n *Namespace) loadAside(ctx context.Context, key string, start uint64) (Item, error) {
	item, err := n.direct.Get(ctx, key)
	if err != nil {
		return Item{}, fmt.Errorf("%w: %w", ErrBackendUnavailable, err)
	}
	n.stats.AsideLoads.Add(1)
	it := itemFromBackend(item)
	n.installNear(key, start, it)
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		_ = n.c.call(n.ctx, n.c.cfg.WriteTimeout, func(ctx context.Context) error {
			_, err := n.c.api.Fill(ctx, &api.FillRequest{
				Namespace: n.name, Key: key, Found: item.Found, Value: item.Value,
				ModRevision: item.ModRevision, ReadRevision: item.ReadRevision,
			})
			return err
		})
	}()
	return it, nil
}

func itemFromBackend(item backend.Item) Item {
	it := Item{Found: item.Found, Value: item.Value, Revision: item.ModRevision, Source: SourceBackend}
	if !item.Found {
		it.Revision = item.ReadRevision
		it.Value = nil
	}
	return it
}

func (n *Namespace) fallbackRead(ctx context.Context, key string, cause error) (Item, error) {
	if n.direct == nil {
		return Item{}, mapErr(cause)
	}
	item, err := n.direct.Get(ctx, key)
	if err != nil {
		return Item{}, errors.Join(fmt.Errorf("%w: %w", ErrBackendUnavailable, err), mapErr(cause))
	}
	n.stats.FallbackReads.Add(1)
	return itemFromBackend(item), nil
}

func (n *Namespace) installNear(key string, start uint64, it Item) {
	if n.near == nil {
		return
	}
	if !it.Found {
		it.Value = nil
	}
	n.near.install(key, start, it)
}

// Set writes key.
//
// With write-through, the cache writes the backing store and caches the
// value; when the cache is unreachable the write fails with
// ErrCacheUnavailable (it may still have been applied if the cache failed
// after writing). With write-around, the cache writes the backing store and
// invalidates the entry; when the cache is unreachable the client writes the
// backing store directly and the cache catches up through its watch.
func (n *Namespace) Set(ctx context.Context, key string, value []byte) error {
	if err := n.check(ctx, key); err != nil {
		return err
	}
	return n.write(ctx, key, false, value)
}

// Delete removes key. It follows the same rules as Set.
func (n *Namespace) Delete(ctx context.Context, key string) error {
	if err := n.check(ctx, key); err != nil {
		return err
	}
	return n.write(ctx, key, true, nil)
}

func (n *Namespace) write(ctx context.Context, key string, del bool, value []byte) error {
	var start uint64
	if n.near != nil {
		start = n.near.begin()
	}
	var rev int64
	err := n.withRegistration(ctx, n.c.cfg.WriteTimeout, func(ctx context.Context) error {
		if del {
			resp, err := n.c.api.Delete(ctx, &api.DeleteRequest{Namespace: n.name, Key: key})
			rev = resp.GetRevision()
			return err
		}
		resp, err := n.c.api.Set(ctx, &api.SetRequest{Namespace: n.name, Key: key, Value: value})
		rev = resp.GetRevision()
		return err
	})
	if err == nil {
		n.writes.add(key, rev)
		n.stats.Writes.Add(1)
		if n.near != nil {
			if n.spec.Write == WriteThrough {
				n.near.install(key, start, Item{Found: !del, Value: value, Revision: rev})
			} else {
				n.near.invalidate(key, rev)
			}
		}
		return nil
	}

	if ctx.Err() != nil {
		// The caller gave up; the write may or may not have been applied.
		if n.near != nil {
			n.near.invalidate(key, 0)
		}
		return ctx.Err()
	}
	fallback := n.spec.Write == WriteAround && canFallback(err)
	if !fallback {
		// Our own copy may be stale if the write landed before the error.
		if n.near != nil && cacheUnavailable(err) {
			n.near.invalidate(key, 0)
		}
		return mapErr(err)
	}
	if n.direct == nil {
		return mapErr(err)
	}
	if del {
		rev, _, err = n.direct.Delete(ctx, key)
	} else {
		rev, err = n.direct.Put(ctx, key, value)
	}
	if err != nil {
		return fmt.Errorf("%w: %w", ErrBackendUnavailable, err)
	}
	n.stats.FallbackWrites.Add(1)
	n.writes.add(key, rev)
	if n.near != nil {
		n.near.invalidate(key, 0)
	}
	return nil
}

// Invalidate drops keys from the cache and from every near-cache, without
// touching the backing store. With no keys it drops the whole namespace.
func (n *Namespace) Invalidate(ctx context.Context, keys ...string) error {
	for _, k := range keys {
		if err := protocol.ValidateKey(k); err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidArgument, err)
		}
	}
	err := n.withRegistration(ctx, n.c.cfg.WriteTimeout, func(ctx context.Context) error {
		_, err := n.c.api.Invalidate(ctx, &api.InvalidateRequest{Namespace: n.name, Keys: keys, All: len(keys) == 0})
		return err
	})
	if n.near != nil {
		if len(keys) == 0 {
			n.near.reset()
		}
		for _, k := range keys {
			n.near.invalidate(k, 0)
		}
	}
	return mapErr(err)
}

// recentWrites remembers the revisions of this client's recent writes, so
// that reads ask the cache for at least that revision (read-your-writes
// even after a fallback write that the cache has not seen yet). It is
// bounded in size and age.
type recentWrites struct {
	mu   sync.Mutex
	ttl  time.Duration
	revs map[string]recentWrite
	ring []ringWrite // oldest first once full
	pos  int
}

type recentWrite struct {
	rev int64
	at  time.Time
}

type ringWrite struct {
	key string
	rev int64
}

func newRecentWrites(max int, ttl time.Duration) *recentWrites {
	return &recentWrites{ttl: ttl, revs: map[string]recentWrite{}, ring: make([]ringWrite, max)}
}

func (r *recentWrites) add(key string, rev int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if cur, ok := r.revs[key]; ok && cur.rev >= rev {
		return
	}
	if old := r.ring[r.pos]; old.key != "" {
		if w, ok := r.revs[old.key]; ok && w.rev == old.rev {
			delete(r.revs, old.key)
		}
	}
	r.ring[r.pos] = ringWrite{key: key, rev: rev}
	r.pos = (r.pos + 1) % len(r.ring)
	r.revs[key] = recentWrite{rev: rev, at: time.Now()}
}

func (r *recentWrites) get(key string) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	w, ok := r.revs[key]
	if !ok {
		return 0
	}
	if time.Since(w.at) > r.ttl {
		delete(r.revs, key)
		return 0
	}
	return w.rev
}

// seen forgets a write once the cache has published a change at or after
// it.
func (r *recentWrites) seen(key string, rev int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if w, ok := r.revs[key]; ok && rev >= w.rev {
		delete(r.revs, key)
	}
}
