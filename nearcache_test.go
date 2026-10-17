package proplyd

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func testNear(t *testing.T, tweak ...func(*NearCacheConfig)) *nearCache {
	t.Helper()
	cfg := NearCacheConfig{Capacity: 100, TTL: time.Minute, TrackedInvalidations: 8}
	for _, f := range tweak {
		f(&cfg)
	}
	n, err := newNearCache(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(n.close)
	return n
}

func val(v string, rev int64) Item { return Item{Found: true, Value: []byte(v), Revision: rev} }

func TestNearInstallAndGet(t *testing.T) {
	n := testNear(t)
	if !n.install("a", n.begin(), val("1", 5)) {
		t.Fatal("install refused")
	}
	it, ok := n.get("a")
	if !ok || string(it.Value) != "1" || it.Revision != 5 || it.Source != SourceNear {
		t.Fatalf("got %+v %v", it, ok)
	}
	n.install("missing", n.begin(), Item{Revision: 7})
	if it, ok := n.get("missing"); !ok || it.Found || it.Value != nil {
		t.Fatalf("negative entry: %+v %v", it, ok)
	}
}

func TestNearInvalidateByRevision(t *testing.T) {
	n := testNear(t)
	n.install("a", n.begin(), val("1", 5))
	n.invalidate("a", 5) // a change the copy already reflects
	if _, ok := n.get("a"); !ok {
		t.Fatal("copy at the invalidated revision must stay")
	}
	n.invalidate("a", 6)
	if _, ok := n.get("a"); ok {
		t.Fatal("older copy must be dropped")
	}
	n.install("b", n.begin(), val("1", 9))
	n.invalidate("b", 0)
	if _, ok := n.get("b"); ok {
		t.Fatal("rev 0 drops unconditionally")
	}
}

// A read that started before an invalidation must not install a result
// older than that invalidation.
func TestNearRejectsStaleInFlightRead(t *testing.T) {
	n := testNear(t)
	start := n.begin()
	n.invalidate("a", 10)
	if n.install("a", start, val("old", 9)) {
		t.Fatal("stale in-flight read installed")
	}
	if !n.install("a", start, val("new", 10)) {
		t.Fatal("a result that reflects the invalidation must install")
	}
	start = n.begin()
	n.invalidate("a", 0)
	if n.install("a", start, val("x", 99)) {
		t.Fatal("rev 0 invalidation must refuse every older read")
	}
	if !n.install("a", n.begin(), val("x", 99)) {
		t.Fatal("a read started after the invalidation must install")
	}
}

func TestNearKeepsNewerCopy(t *testing.T) {
	n := testNear(t)
	n.install("a", n.begin(), val("new", 10))
	if n.install("a", n.begin(), val("old", 8)) {
		t.Fatal("older result replaced a newer copy")
	}
}

func TestNearForgettingRaisesFloor(t *testing.T) {
	n := testNear(t) // remembers 8 invalidations
	start := n.begin()
	for i := range 9 {
		n.invalidate(fmt.Sprint("k", i), 1)
	}
	// The invalidation of k0 is forgotten, so reads that started before it
	// are refused, even for other keys.
	if n.install("unrelated", start, val("x", 1)) {
		t.Fatal("read older than the floor installed")
	}
	if !n.install("unrelated", n.begin(), val("x", 1)) {
		t.Fatal("fresh read refused")
	}
}

func TestNearReset(t *testing.T) {
	n := testNear(t)
	n.install("a", n.begin(), val("1", 1))
	start := n.begin()
	n.reset()
	if _, ok := n.get("a"); ok {
		t.Fatal("reset kept an entry")
	}
	if n.install("b", start, val("1", 1)) {
		t.Fatal("read from before the reset installed")
	}
}

func TestNearPolicies(t *testing.T) {
	for _, p := range []Policy{SIEVE, LRU, LFU, TwoQueue, FIFO, TinyLFU, WTinyLFU} {
		for _, capacity := range []int{100, 8192} { // unsharded and sharded
			t.Run(fmt.Sprintf("%s/%d", p, capacity), func(t *testing.T) {
				n := testNear(t, func(c *NearCacheConfig) { c.Policy = p; c.Capacity = capacity })
				for i := range 3 * capacity {
					n.install(fmt.Sprint("k", i), n.begin(), val("v", int64(i+1)))
				}
				// Sharding rounds capacity up per shard.
				if got, limit := n.len(), capacity+len(n.shards); got > limit {
					t.Fatalf("%d entries, capacity %d", got, capacity)
				}
			})
		}
	}
	if _, err := newNearCache(NearCacheConfig{Policy: "arc", Capacity: 10, TTL: time.Minute}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("arc: %v", err)
	}
}

func TestNearTTL(t *testing.T) {
	n := testNear(t, func(c *NearCacheConfig) { c.TTL = 50 * time.Millisecond })
	n.install("a", n.begin(), val("1", 1))
	time.Sleep(100 * time.Millisecond)
	if _, ok := n.get("a"); ok {
		t.Fatal("expired entry served")
	}
}

func TestNearCopyOnRead(t *testing.T) {
	n := testNear(t, func(c *NearCacheConfig) { c.CopyOnRead = true })
	n.install("a", n.begin(), val("abc", 1))
	it, _ := n.get("a")
	it.Value[0] = 'X'
	if it, _ := n.get("a"); string(it.Value) != "abc" {
		t.Fatalf("cache corrupted through a returned value: %q", it.Value)
	}
}

func TestRecentWrites(t *testing.T) {
	r := newRecentWrites(2, time.Minute)
	r.add("a", 5)
	r.add("a", 3) // older: ignored
	if got := r.get("a"); got != 5 {
		t.Fatalf("a = %d", got)
	}
	r.seen("a", 4)
	if r.get("a") != 5 {
		t.Fatal("an older change must not clear a recent write")
	}
	r.seen("a", 5)
	if r.get("a") != 0 {
		t.Fatal("seeing the write must clear it")
	}
	r.add("b", 1)
	r.add("c", 1)
	r.add("d", 1) // evicts b
	if r.get("b") != 0 || r.get("d") != 1 {
		t.Fatal("bounded size")
	}
	old := newRecentWrites(4, time.Millisecond)
	old.add("a", 1)
	time.Sleep(5 * time.Millisecond)
	if old.get("a") != 0 {
		t.Fatal("expired write still remembered")
	}
}
