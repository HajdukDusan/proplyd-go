package proplyd

import (
	"encoding/binary"
	"fmt"
	"hash/maphash"
	"sync"
	"time"

	"github.com/samber/hot"
)

// Policy is a near-cache eviction policy.
type Policy string

// Eviction policies (implemented by samber/hot).
const (
	SIEVE    Policy = "sieve"
	LRU      Policy = "lru"
	LFU      Policy = "lfu"
	TwoQueue Policy = "2q"
	FIFO     Policy = "fifo"
	// TinyLFU and WTinyLFU filter admissions aggressively: with samber/hot
	// v0.13 they hold noticeably fewer entries than their capacity.
	TinyLFU  Policy = "tinylfu"
	WTinyLFU Policy = "wtinylfu"
)

func (p Policy) algorithm() (hot.EvictionAlgorithm, error) {
	switch p {
	case SIEVE, "":
		return hot.SIEVE, nil
	case LRU:
		return hot.LRU, nil
	case LFU:
		return hot.LFU, nil
	case TwoQueue:
		return hot.TwoQueue, nil
	case FIFO:
		return hot.FIFO, nil
	case TinyLFU:
		return hot.TinyLFU, nil
	case WTinyLFU:
		return hot.WTinyLFU, nil
	}
	// ARC is left out on purpose: samber/hot v0.13 does not enforce its
	// capacity.
	return "", fmt.Errorf("%w: unsupported near-cache policy %q", ErrInvalidArgument, p)
}

// NearCacheConfig configures the in-process near-cache of a namespace.
// Size it for the device it runs on; it does not need to match other
// clients.
type NearCacheConfig struct {
	// Disabled turns the near-cache off: every read goes to the cache.
	Disabled bool
	// Policy is the eviction policy. Default SIEVE.
	Policy Policy
	// Capacity in entries. Default 10000.
	Capacity int
	// TTL of near-cache entries. Default: the namespace TTL, at most 1m.
	TTL time.Duration
	// HeartbeatTimeout: the invalidation stream counts as dead after this
	// long without a message. Default 3s.
	HeartbeatTimeout time.Duration
	// MaxStaleness is how long entries may still be served after the
	// invalidation stream is lost, for devices on flaky networks that
	// prefer slightly stale reads to backing store reads. Default 0: stop
	// using the near-cache at once (entries are kept and become usable
	// again if the stream resumes without gaps).
	MaxStaleness time.Duration
	// CopyOnRead returns copies of cached values. Without it, values
	// returned from the near-cache are shared and must not be modified.
	CopyOnRead bool
	// TrackedInvalidations is how many recent invalidations are remembered
	// to reject stale fills from in-flight reads. Default 4096.
	TrackedInvalidations int
}

// nearCache stores items as []byte (one allocation per entry, no pointers
// inside) to keep GC work low:
//
//	[0]    1 if the key exists, 0 for a cached "not found"
//	[1:9]  revision, big endian
//	[9:]   value
//
// Coherence: every invalidation from the stream bumps seq and is
// remembered per key. A read that started at seq s may only install its
// result if no invalidation newer than its result arrived after s. When
// remembered invalidations are forgotten (or on reset), floor rises and
// older in-flight reads are refused.
type nearCache struct {
	shards     []*hot.HotCache[string, []byte]
	seed       maphash.Seed
	copyOnRead bool

	mu      sync.Mutex
	seq     uint64
	floor   uint64
	recent  map[string]mark
	ring    []ringEntry
	ringPos int
	ringLen int
}

type mark struct {
	seq uint64
	rev int64 // 0: drop unconditionally
}

type ringEntry struct {
	key string
	seq uint64
}

const nearHeader = 9

func newNearCache(cfg NearCacheConfig) (*nearCache, error) {
	algo, err := cfg.Policy.algorithm()
	if err != nil {
		return nil, err
	}
	// samber/hot's own sharding does not lock its shards; shard here.
	shards := 1
	if cfg.Capacity >= 4096 {
		shards = 16
	}
	per := (cfg.Capacity + shards - 1) / shards
	n := &nearCache{
		seed:       maphash.MakeSeed(),
		copyOnRead: cfg.CopyOnRead,
		recent:     map[string]mark{},
		ring:       make([]ringEntry, cfg.TrackedInvalidations),
	}
	for range shards {
		n.shards = append(n.shards, hot.NewHotCache[string, []byte](algo, per).WithTTL(cfg.TTL).WithJanitor().Build())
	}
	return n, nil
}

func (n *nearCache) close() {
	for _, s := range n.shards {
		s.StopJanitor()
	}
}

func (n *nearCache) shard(key string) *hot.HotCache[string, []byte] {
	if len(n.shards) == 1 {
		return n.shards[0]
	}
	return n.shards[maphash.String(n.seed, key)%uint64(len(n.shards))]
}

func encodeNear(it Item) []byte {
	b := make([]byte, nearHeader+len(it.Value))
	if it.Found {
		b[0] = 1
	}
	binary.BigEndian.PutUint64(b[1:9], uint64(it.Revision))
	copy(b[nearHeader:], it.Value)
	return b
}

func decodeNear(b []byte) Item {
	return Item{Found: b[0] == 1, Revision: int64(binary.BigEndian.Uint64(b[1:9])), Value: b[nearHeader:], Source: SourceNear}
}

func (n *nearCache) get(key string) (Item, bool) {
	b, ok, _ := n.shard(key).Get(key)
	if !ok || len(b) < nearHeader {
		return Item{}, false
	}
	it := decodeNear(b)
	if !it.Found {
		it.Value = nil
	} else if n.copyOnRead {
		it.Value = append([]byte(nil), it.Value...)
	}
	return it, true
}

// begin returns the sequence number a read must pass to install.
func (n *nearCache) begin() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.seq
}

// install stores the result of a read that began at start, unless an
// invalidation newer than the result arrived meanwhile.
func (n *nearCache) install(key string, start uint64, it Item) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if start < n.floor {
		return false
	}
	if m, ok := n.recent[key]; ok && m.seq > start && (m.rev == 0 || m.rev > it.Revision) {
		return false
	}
	s := n.shard(key)
	if cur, ok := s.Peek(key); ok && len(cur) >= nearHeader && decodeNear(cur).Revision > it.Revision {
		return false // a concurrent read installed something newer
	}
	s.Set(key, encodeNear(it))
	return true
}

// invalidate drops copies of key older than rev (rev 0: any copy).
func (n *nearCache) invalidate(key string, rev int64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.seq++
	n.recent[key] = mark{seq: n.seq, rev: rev}
	n.remember(key, n.seq)
	s := n.shard(key)
	if cur, ok := s.Peek(key); ok && (rev == 0 || len(cur) < nearHeader || decodeNear(cur).Revision < rev) {
		s.Delete(key)
	}
}

// remember records an invalidation in the bounded ring. Forgetting one
// raises the floor so that reads it could have affected are refused.
func (n *nearCache) remember(key string, seq uint64) {
	if len(n.ring) == 0 {
		n.floor = seq
		return
	}
	if n.ringLen == len(n.ring) {
		old := n.ring[n.ringPos]
		if m, ok := n.recent[old.key]; ok && m.seq == old.seq {
			delete(n.recent, old.key)
		}
		n.floor = max(n.floor, old.seq)
	} else {
		n.ringLen++
	}
	n.ring[n.ringPos] = ringEntry{key: key, seq: seq}
	n.ringPos = (n.ringPos + 1) % len(n.ring)
}

// reset drops everything and refuses every read in flight.
func (n *nearCache) reset() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.seq++
	n.floor = n.seq
	clear(n.recent)
	n.ringLen, n.ringPos = 0, 0
	for _, s := range n.shards {
		s.Purge()
	}
}

func (n *nearCache) len() int {
	total := 0
	for _, s := range n.shards {
		total += s.Len()
	}
	return total
}
