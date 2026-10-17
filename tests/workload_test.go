// Package tests holds the system tests of proplyd and its client: a stress
// test with many edges, bad-network tests and a chaos test. They share the
// workload defined here.
package tests

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	proplyd "github.com/c12s/proplyd-go"
	"github.com/c12s/proplyd-go/internal/testcluster"
	"github.com/c12s/proplyd/proplydtest"
)

// nsSpec is a namespace used by the workload.
type nsSpec struct {
	name   string
	spec   proplyd.Spec
	policy proplyd.Policy
}

// allSpecs covers every read x write strategy, each with a different
// near-cache policy. All follow the store's changes (SyncWatch), which is
// what makes convergence after faults a valid expectation.
func allSpecs() []nsSpec {
	ra := func(w proplyd.WriteStrategy) proplyd.Spec {
		return proplyd.Spec{Read: proplyd.RefreshAhead, Write: w, TTL: 5 * time.Second, RefreshAfter: time.Second}
	}
	return []nsSpec{
		{"rt-wt", proplyd.Spec{Read: proplyd.ReadThrough, Write: proplyd.WriteThrough}, proplyd.SIEVE},
		{"rt-wa", proplyd.Spec{Read: proplyd.ReadThrough, Write: proplyd.WriteAround}, proplyd.LRU},
		{"ca-wt", proplyd.Spec{Read: proplyd.CacheAside, Write: proplyd.WriteThrough}, proplyd.LFU},
		{"ca-wa", proplyd.Spec{Read: proplyd.CacheAside, Write: proplyd.WriteAround}, proplyd.TwoQueue},
		{"ra-wt", ra(proplyd.WriteThrough), proplyd.FIFO},
		{"ra-wa", ra(proplyd.WriteAround), proplyd.WTinyLFU},
	}
}

// keyState records the writes to one key. Only its owner writes it, one
// operation at a time; write number c (from 1) stores "owner:c:..." or, for
// deletes, removes the key. State 0 is the initial absence.
type keyState struct {
	ns, key string
	owner   int

	mu      sync.Mutex
	deletes []bool       // deletes[c-1]: write c was a delete
	acked   int          // last acknowledged write
	unacked map[int]bool // failed writes: maybe applied, maybe not
}

// readState is what a read observed.
type readState struct {
	found   bool
	counter int
}

func (k *keyState) stateOf(c int) readState {
	if c == 0 || k.deletes[c-1] {
		return readState{}
	}
	return readState{found: true, counter: c}
}

// allowed reports whether s is a state the key may be in: the last
// acknowledged write, or any write that failed (and may have been applied,
// possibly late).
func (k *keyState) allowed(s readState) bool {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.stateOf(k.acked) == s {
		return true
	}
	for c := range k.unacked {
		if k.stateOf(c) == s {
			return true
		}
	}
	return false
}

func (k *keyState) issued() int {
	k.mu.Lock()
	defer k.mu.Unlock()
	return len(k.deletes)
}

func (k *keyState) describe() string {
	k.mu.Lock()
	defer k.mu.Unlock()
	var un []string
	for c := range k.unacked {
		un = append(un, strconv.Itoa(c))
	}
	return fmt.Sprintf("%s/%s owner=%d acked=%d (%v) unacked=%v", k.ns, k.key, k.owner, k.acked, k.stateOf(k.acked), un)
}

func encodeValue(owner, counter int) []byte {
	pad := strings.Repeat("x", rand.IntN(256))
	return []byte(fmt.Sprintf("%d:%d:%s", owner, counter, pad))
}

func decodeValue(b []byte) (owner, counter int, err error) {
	parts := strings.SplitN(string(b), ":", 3)
	if len(parts) != 3 {
		return 0, 0, fmt.Errorf("malformed value %q", b)
	}
	if owner, err = strconv.Atoi(parts[0]); err != nil {
		return 0, 0, err
	}
	counter, err = strconv.Atoi(parts[1])
	return owner, counter, err
}

type workloadConfig struct {
	edges      int
	keys       int // per namespace
	duration   time.Duration
	writeRatio float64
	// monotonic: every reader must see each key's revisions in order.
	// Only holds without faults (fallback reads can be newer than the
	// cache).
	monotonic bool
	// allowWriteErr: errors a write may fail with under the test's faults.
	// Such writes count as maybe-applied.
	allowWriteErr func(spec proplyd.Spec, err error) bool
	near          func(edge int) proplyd.NearCacheConfig
	// edgeConfig adjusts an edge's client configuration.
	edgeConfig func(edge int, cfg *proplyd.Config)
	// nodes, if set, are the proplyd replicas edges connect to (round
	// robin). Default: the cluster's node.
	nodes []*proplydtest.Node
}

type workload struct {
	t     *testing.T
	c     *testcluster.Cluster
	cfg   workloadConfig
	specs []nsSpec
	edges []*testcluster.Edge
	nss   [][]*proplyd.Namespace // [edge][spec]
	keys  [][]*keyState          // [spec][key]

	ops, writes, reads, failedWrites atomic.Int64
	unexpected                       atomic.Int64
	errMu                            sync.Mutex
	errs                             []string
}

func newWorkload(t *testing.T, c *testcluster.Cluster, cfg workloadConfig, specs []nsSpec) *workload {
	t.Helper()
	w := &workload{t: t, c: c, cfg: cfg, specs: specs}
	for i := range cfg.edges {
		node := c.Node
		if len(cfg.nodes) > 0 {
			node = cfg.nodes[i%len(cfg.nodes)]
		}
		e := c.EdgeTo(node, func(pc *proplyd.Config) {
			if cfg.edgeConfig != nil {
				cfg.edgeConfig(i, pc)
			}
		})
		w.edges = append(w.edges, e)
		var nss []*proplyd.Namespace
		for _, s := range specs {
			near := testcluster.NearConfig()
			if cfg.near != nil {
				near = cfg.near(i)
			}
			near.Policy = s.policy
			nss = append(nss, e.Namespace(t, s.name, s.spec, near))
		}
		w.nss = append(w.nss, nss)
	}
	for _, s := range specs {
		var ks []*keyState
		for j := range cfg.keys {
			ks = append(ks, &keyState{ns: s.name, key: fmt.Sprint("key-", j), owner: j % cfg.edges, unacked: map[int]bool{}})
		}
		w.keys = append(w.keys, ks)
	}
	return w
}

func (w *workload) fail(format string, args ...any) {
	w.unexpected.Add(1)
	w.errMu.Lock()
	defer w.errMu.Unlock()
	if len(w.errs) < 20 {
		w.errs = append(w.errs, fmt.Sprintf(format, args...))
	}
}

// run drives every edge until the duration passes.
func (w *workload) run() {
	ctx, cancel := context.WithTimeout(context.Background(), w.cfg.duration)
	defer cancel()
	var wg sync.WaitGroup
	for i := range w.edges {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.drive(ctx, i)
		}()
	}
	wg.Wait()
}

func (w *workload) drive(ctx context.Context, edge int) {
	r := rand.New(rand.NewPCG(uint64(edge), uint64(time.Now().UnixNano())))
	lastRev := map[*keyState]int64{}
	// Keys this edge owns, per spec.
	owned := make([][]*keyState, len(w.specs))
	for si := range w.specs {
		for _, k := range w.keys[si] {
			if k.owner == edge {
				owned[si] = append(owned[si], k)
			}
		}
	}
	for ctx.Err() == nil {
		si := r.IntN(len(w.specs))
		ns := w.nss[edge][si]
		w.ops.Add(1)
		if len(owned[si]) > 0 && r.Float64() < w.cfg.writeRatio {
			w.write(ns, w.specs[si].spec, owned[si][r.IntN(len(owned[si]))], r.Float64() < 0.1)
			continue
		}
		k := w.keys[si][r.IntN(len(w.keys[si]))]
		w.read(ns, k, edge, lastRev)
	}
}

func (w *workload) write(ns *proplyd.Namespace, spec proplyd.Spec, k *keyState, del bool) {
	k.mu.Lock()
	k.deletes = append(k.deletes, del)
	c := len(k.deletes)
	k.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var err error
	if del {
		err = ns.Delete(ctx, k.key)
	} else {
		err = ns.Set(ctx, k.key, encodeValue(k.owner, c))
	}
	w.writes.Add(1)
	k.mu.Lock()
	defer k.mu.Unlock()
	if err == nil {
		k.acked = c
		return
	}
	k.unacked[c] = true
	w.failedWrites.Add(1)
	if w.cfg.allowWriteErr == nil || !w.cfg.allowWriteErr(spec, err) {
		w.fail("write %s/%s #%d: %v", k.ns, k.key, c, err)
	}
}

func (w *workload) read(ns *proplyd.Namespace, k *keyState, edge int, lastRev map[*keyState]int64) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	it, err := ns.Lookup(ctx, k.key)
	w.reads.Add(1)
	if err != nil {
		w.fail("read %s/%s by edge %d: %v", k.ns, k.key, edge, err)
		return
	}
	s := readState{found: it.Found}
	if it.Found {
		owner, counter, err := decodeValue(it.Value)
		if err != nil || owner != k.owner || counter < 1 || counter > k.issued() {
			w.fail("read %s/%s: garbage value %q (%v)", k.ns, k.key, it.Value, err)
			return
		}
		s.counter = counter
	}
	if edge == k.owner && !k.allowed(s) {
		w.fail("read-your-writes violated: %s read %+v from %v (rev %d)", k.describe(), s, it.Source, it.Revision)
	}
	if w.cfg.monotonic {
		if it.Revision < lastRev[k] {
			w.fail("non-monotonic read of %s/%s by edge %d: rev %d after %d (%v)", k.ns, k.key, edge, it.Revision, lastRev[k], it.Source)
		}
		lastRev[k] = it.Revision
	}
}

// storeState reads a key's state straight from the store.
func (w *workload) storeState(k *keyState) (readState, int64) {
	w.t.Helper()
	it, err := w.c.Store.Get(context.Background(), "/"+k.ns+"/"+k.key)
	if err != nil {
		w.t.Fatal(err)
	}
	if !it.Found {
		return readState{}, it.ReadRevision
	}
	_, counter, err := decodeValue(it.Value)
	if err != nil {
		w.t.Fatal(err)
	}
	return readState{found: true, counter: counter}, it.ModRevision
}

// verify runs every end-of-run check.
func (w *workload) verify() {
	w.t.Helper()
	w.report()
	if n := w.unexpected.Load(); n > 0 {
		w.t.Errorf("%d unexpected errors or violations, first ones:\n  %s", n, strings.Join(w.errs, "\n  "))
	}
	w.verifyStore()
	w.verifyConvergence(30 * time.Second)
	w.verifyRedis()
}

// verifyStore: no acknowledged write was lost.
func (w *workload) verifyStore() {
	w.t.Helper()
	for _, ks := range w.keys {
		for _, k := range ks {
			if s, _ := w.storeState(k); !k.allowed(s) {
				w.t.Errorf("store lost an acknowledged write: %s, store has %+v", k.describe(), s)
			}
		}
	}
}

// verifyConvergence: once faults are gone, every edge reads what the store
// holds, for every key, through the cache: breakers closed, near-caches
// live, and no read served by the fallback path (which would hide a stale
// or stuck cache).
func (w *workload) verifyConvergence(timeout time.Duration) {
	w.t.Helper()
	began := time.Now()
	deadline := began.Add(timeout)
	for round := 1; ; round++ {
		var stale []string
		for e, edge := range w.edges {
			if st := edge.Client.State(); st != proplyd.BreakerClosed {
				stale = append(stale, fmt.Sprintf("edge %d: breaker %v", e, st))
			}
			for si, ns := range w.nss[e] {
				if !ns.Stats().NearUsable {
					stale = append(stale, fmt.Sprintf("edge %d %s: near-cache not usable", e, w.specs[si].name))
				}
			}
		}
		fallbacks := w.fallbackReads()
		for si, ks := range w.keys {
			for _, k := range ks {
				want, _ := w.storeState(k)
				for e := range w.edges {
					it, err := w.nss[e][si].Lookup(context.Background(), k.key)
					got := readState{found: it.Found}
					if it.Found {
						_, got.counter, _ = decodeValue(it.Value)
					}
					if err != nil || got != want {
						stale = append(stale, fmt.Sprintf("edge %d %s/%s: got %+v from %v (err %v), store %+v", e, k.ns, k.key, got, it.Source, err, want))
					}
				}
			}
		}
		if n := w.fallbackReads() - fallbacks; n > 0 {
			stale = append(stale, fmt.Sprintf("%d reads fell back to the store", n))
		}
		if len(stale) == 0 {
			w.t.Logf("converged after %v (%d rounds)", time.Since(began).Round(time.Millisecond), round)
			return
		}
		if time.Now().After(deadline) {
			w.t.Errorf("not converged after %v (%d problems), e.g.:\n  %s", timeout, len(stale), strings.Join(stale[:min(10, len(stale))], "\n  "))
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func (w *workload) fallbackReads() int64 {
	var n int64
	for _, nss := range w.nss {
		for _, ns := range nss {
			n += ns.Stats().FallbackReads
		}
	}
	return n
}

// verifyRedis inspects proplyd's Redis entries directly: every live cached
// value must be the store's current value. This mirrors the entry layout in
// proplyd/internal/engine/entry.go.
func (w *workload) verifyRedis() {
	w.t.Helper()
	rc := w.c.Redis.Client()
	defer rc.Close()
	ctx := context.Background()
	for si, s := range w.specs {
		epoch, err := rc.HGet(ctx, "pd:{"+s.name+"}:m", "epoch").Result()
		if err != nil {
			w.t.Errorf("namespace %s meta: %v", s.name, err)
			continue
		}
		for _, k := range w.keys[si] {
			b, err := rc.Get(ctx, "pd:{"+s.name+"}:k:"+k.key).Bytes()
			if err != nil || len(b) < 25 || string(b[1:9]) != epoch {
				continue // not cached
			}
			kind, rev := b[0]&0x7f, int64(binary.BigEndian.Uint64(b[9:17])) // bit 0x80: quiet flag
			want, wantRev := w.storeState(k)
			switch kind {
			case 1:
				_, counter, _ := decodeValue(b[25:])
				if !want.found || counter != want.counter || rev != wantRev {
					w.t.Errorf("redis holds %s/%s #%d@%d, store %+v@%d", s.name, k.key, counter, rev, want, wantRev)
				}
			case 2:
				if want.found {
					w.t.Errorf("redis says %s/%s is missing (@%d), store has %+v@%d", s.name, k.key, rev, want, wantRev)
				}
			}
		}
	}
}

func (w *workload) report() {
	var st proplyd.StatsSnapshot
	for _, nss := range w.nss {
		for _, ns := range nss {
			s := ns.Stats()
			st.NearHits += s.NearHits
			st.NearMisses += s.NearMisses
			st.CacheHits += s.CacheHits
			st.CacheLoads += s.CacheLoads
			st.AsideLoads += s.AsideLoads
			st.FallbackReads += s.FallbackReads
			st.FallbackWrites += s.FallbackWrites
			st.Invalidations += s.Invalidations
			st.Resets += s.Resets
		}
	}
	secs := w.cfg.duration.Seconds()
	reads := max(st.NearHits+st.NearMisses, 1)
	w.t.Logf("%d edges x %d namespaces x %d keys, %v: %d ops (%.0f/s), %d reads, %d writes (%d failed)",
		len(w.edges), len(w.specs), w.cfg.keys, w.cfg.duration, w.ops.Load(), float64(w.ops.Load())/secs,
		w.reads.Load(), w.writes.Load(), w.failedWrites.Load())
	w.t.Logf("near hits %.1f%%, cache hits %d, cache loads %d, aside loads %d, fallback reads %d, fallback writes %d, invalidations %d, resets %d",
		100*float64(st.NearHits)/float64(reads), st.CacheHits, st.CacheLoads, st.AsideLoads,
		st.FallbackReads, st.FallbackWrites, st.Invalidations, st.Resets)
	if srv := w.c.Node.Server(); srv != nil {
		s := srv.Stats()
		w.t.Logf("server: hits %d, misses %d, loads %d, fills %d (rejected %d), writes %d, watch events %d, purges %d, redis restarts %d",
			s.Hits.Load(), s.Misses.Load(), s.Loads.Load(), s.Fills.Load(), s.FillsRejected.Load(), s.Writes.Load(),
			s.WatchEvents.Load(), s.Purges.Load(), s.RedisRestarts.Load())
	}
}

// writeThroughMayFail allows the errors write-through writes are specified
// to return while the cache, or its link to the store, is unavailable.
func writeThroughMayFail(spec proplyd.Spec, err error) bool {
	return spec.Write == proplyd.WriteThrough &&
		(errors.Is(err, proplyd.ErrCacheUnavailable) || errors.Is(err, proplyd.ErrBackendUnavailable))
}

func envDuration(t *testing.T, key string, def, short time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			t.Fatalf("%s: %v", key, err)
		}
		return d
	}
	if testing.Short() {
		return short
	}
	return def
}

func envInt(t *testing.T, key string, def, short int) int {
	if v := os.Getenv(key); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			t.Fatalf("%s: %v", key, err)
		}
		return n
	}
	if testing.Short() {
		return short
	}
	return def
}
