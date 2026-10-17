package proplyd_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	proplyd "github.com/c12s/proplyd-go"
	"github.com/c12s/proplyd-go/internal/testcluster"
	"github.com/c12s/proplyd/proplydtest"
)

func ctxT(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func eventually(t *testing.T, cond func() error) {
	t.Helper()
	proplydtest.Eventually(t, 10*time.Second, cond)
}

func mustGet(t *testing.T, ns *proplyd.Namespace, key string) proplyd.Item {
	t.Helper()
	it, err := ns.Lookup(ctxT(t), key)
	if err != nil {
		t.Fatalf("lookup %q: %v", key, err)
	}
	return it
}

func expect(t *testing.T, it proplyd.Item, want string) {
	t.Helper()
	if want == "" {
		if it.Found {
			t.Fatalf("want missing, got %q (%v)", it.Value, it.Source)
		}
		return
	}
	if !it.Found || string(it.Value) != want {
		t.Fatalf("want %q, got found=%v %q (%v)", want, it.Found, it.Value, it.Source)
	}
}

func valueIs(ns *proplyd.Namespace, key, want string) error {
	it, err := ns.Lookup(context.Background(), key)
	if err != nil {
		return err
	}
	if (want == "") == it.Found || (want != "" && string(it.Value) != want) {
		return fmt.Errorf("%s: got found=%v %q from %v, want %q", key, it.Found, it.Value, it.Source, want)
	}
	return nil
}

func mustSet(t *testing.T, ns *proplyd.Namespace, key, value string) {
	t.Helper()
	if err := ns.Set(ctxT(t), key, []byte(value)); err != nil {
		t.Fatalf("set %q: %v", key, err)
	}
}

// Every read strategy with every write strategy, seen by two edges.
func TestStrategies(t *testing.T) {
	c := testcluster.New(t, testcluster.Options{})
	for _, read := range []proplyd.ReadStrategy{proplyd.ReadThrough, proplyd.CacheAside, proplyd.RefreshAhead} {
		for _, write := range []proplyd.WriteStrategy{proplyd.WriteThrough, proplyd.WriteAround} {
			name := fmt.Sprintf("%s.%s", read, write)
			t.Run(name, func(t *testing.T) {
				spec := proplyd.Spec{Read: read, Write: write, TTL: time.Minute}
				a := c.Edge().Namespace(t, name, spec, testcluster.NearConfig())
				b := c.Edge().Namespace(t, name, spec, testcluster.NearConfig())
				testcluster.WaitNearUsable(t, a)
				testcluster.WaitNearUsable(t, b)

				expect(t, mustGet(t, a, "k"), "")
				mustSet(t, a, "k", "v1")
				expect(t, mustGet(t, a, "k"), "v1") // read-your-writes
				eventually(t, func() error { return valueIs(b, "k", "v1") })

				// b caches k in its near-cache...
				eventually(t, func() error {
					if it := mustGet(t, b, "k"); it.Source != proplyd.SourceNear {
						return fmt.Errorf("not from the near-cache: %v", it.Source)
					}
					return nil
				})
				// ...and sees a's next write through the invalidation stream.
				mustSet(t, a, "k", "v2")
				eventually(t, func() error { return valueIs(b, "k", "v2") })

				if err := a.Delete(ctxT(t), "k"); err != nil {
					t.Fatal(err)
				}
				expect(t, mustGet(t, a, "k"), "")
				eventually(t, func() error { return valueIs(b, "k", "") })
			})
		}
	}
}

func TestCacheAsideFillsTheCache(t *testing.T) {
	c := testcluster.New(t, testcluster.Options{})
	spec := proplyd.Spec{Read: proplyd.CacheAside}
	near := proplyd.NearCacheConfig{Disabled: true}
	a := c.Edge().Namespace(t, "aside", spec, near)
	b := c.Edge().Namespace(t, "aside", spec, near)
	if _, err := c.Store.Put(ctxT(t), "/aside/k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if it := mustGet(t, a, "k"); it.Source != proplyd.SourceBackend {
		t.Fatalf("first read: %v", it.Source)
	}
	eventually(t, func() error {
		if it := mustGet(t, b, "k"); it.Source != proplyd.SourceCache {
			return fmt.Errorf("a's fill not visible to b: %v", it.Source)
		}
		return nil
	})
	if st := a.Stats(); st.AsideLoads != 1 {
		t.Fatalf("aside loads = %d", st.AsideLoads)
	}
}

// A client that starts while proplyd is down works from the store, and
// switches to the cache once proplyd appears.
func TestCacheNeverStarted(t *testing.T) {
	c := testcluster.New(t, testcluster.Options{})
	c.Node.Kill()
	wa := c.Edge().Namespace(t, "wa", proplyd.Spec{Write: proplyd.WriteAround}, testcluster.NearConfig())
	wt := c.Edge().Namespace(t, "wt", proplyd.Spec{Write: proplyd.WriteThrough}, testcluster.NearConfig())

	mustSet(t, wa, "k", "v1")
	if it := mustGet(t, wa, "k"); it.Source != proplyd.SourceBackend || string(it.Value) != "v1" {
		t.Fatalf("fallback read: %+v", it)
	}
	if err := wt.Set(ctxT(t), "k", []byte("v1")); !errors.Is(err, proplyd.ErrCacheUnavailable) {
		t.Fatalf("write-through without cache: %v", err)
	}
	if it, _ := c.Store.Get(ctxT(t), "/wt/k"); it.Found {
		t.Fatal("rejected write-through write reached the store")
	}

	c.Node.Start()
	eventually(t, func() error {
		it, err := wa.Lookup(context.Background(), "k")
		if err != nil || it.Source == proplyd.SourceBackend && wa.Stats().CacheLoads == 0 {
			return fmt.Errorf("still not using the cache: %v %v", it.Source, err)
		}
		return nil
	})
	mustSet(t, wt, "k", "v2")
	testcluster.WaitNearUsable(t, wt)
}

func TestCacheCrashAndRestart(t *testing.T) {
	c := testcluster.New(t, testcluster.Options{})
	e := c.Edge()
	ns := e.Namespace(t, "crash", proplyd.Spec{Write: proplyd.WriteAround}, testcluster.NearConfig())
	testcluster.WaitNearUsable(t, ns)
	mustSet(t, ns, "k", "v1")
	expect(t, mustGet(t, ns, "k"), "v1")

	c.Node.Kill()
	// The breaker opens; reads and write-around writes keep working.
	for i := range 10 {
		mustSet(t, ns, "k", fmt.Sprint("down", i))
		expect(t, mustGet(t, ns, "k"), fmt.Sprint("down", i))
	}
	if st := e.Client.State(); st != proplyd.BreakerOpen {
		t.Fatalf("breaker %v, want open", st)
	}
	if ns.Stats().NearUsable {
		t.Fatal("near-cache must not be trusted without the invalidation stream")
	}

	c.Node.Start()
	eventually(t, func() error {
		if _, err := ns.Lookup(context.Background(), "k"); err != nil {
			return err
		}
		if st := e.Client.State(); st != proplyd.BreakerClosed {
			return fmt.Errorf("breaker %v", st)
		}
		return nil
	})
	testcluster.WaitNearUsable(t, ns)
	expect(t, mustGet(t, ns, "k"), "down9")
}

// The scenario behind write-around's fallback: an edge cut off from the
// cache writes the store directly. The cache must pick the write up (watch)
// and invalidate the other edges' copies.
func TestPartitionedWriteAroundReachesOtherEdges(t *testing.T) {
	c := testcluster.New(t, testcluster.Options{})
	spec := proplyd.Spec{Write: proplyd.WriteAround}
	a := c.Edge()
	nsA := a.Namespace(t, "part", spec, testcluster.NearConfig())
	nsB := c.Edge().Namespace(t, "part", spec, testcluster.NearConfig())
	testcluster.WaitNearUsable(t, nsB)
	mustSet(t, nsA, "k", "v1")
	eventually(t, func() error {
		if it := mustGet(t, nsB, "k"); it.Source != proplyd.SourceNear || string(it.Value) != "v1" {
			return fmt.Errorf("b has no near copy: %+v", it)
		}
		return nil
	})

	a.Link.SetMode(proplydtest.Refuse)
	mustSet(t, nsA, "k", "v2") // straight to the store
	expect(t, mustGet(t, nsA, "k"), "v2")
	if nsA.Stats().FallbackWrites == 0 {
		t.Fatal("expected a fallback write")
	}
	eventually(t, func() error { return valueIs(nsB, "k", "v2") })

	// After the partition heals, a reads its own write from the cache.
	a.Link.SetMode(proplydtest.Forward)
	eventually(t, func() error {
		it, err := nsA.Lookup(context.Background(), "k")
		if err != nil || it.Source == proplyd.SourceBackend && a.Client.State() != proplyd.BreakerClosed {
			return fmt.Errorf("not recovered: %+v %v", it, err)
		}
		if string(it.Value) != "v2" {
			t.Fatalf("read-your-writes broken after partition: %q", it.Value)
		}
		return nil
	})
}

func TestWriteThroughRejectsWhenPartitioned(t *testing.T) {
	c := testcluster.New(t, testcluster.Options{})
	e := c.Edge()
	ns := e.Namespace(t, "wt", proplyd.Spec{Write: proplyd.WriteThrough}, testcluster.NearConfig())
	mustSet(t, ns, "k", "v1")
	e.Link.SetMode(proplydtest.Blackhole)
	start := time.Now()
	if err := ns.Set(ctxT(t), "k", []byte("v2")); !errors.Is(err, proplyd.ErrCacheUnavailable) {
		t.Fatalf("set: %v", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("rejection took %v", d)
	}
	// Reads fall back and see the store.
	expect(t, mustGet(t, ns, "k"), "v1")
	e.Link.SetMode(proplydtest.Forward)
}

func TestSlowCacheFallsBack(t *testing.T) {
	c := testcluster.New(t, testcluster.Options{})
	e := c.Edge()
	ns := e.Namespace(t, "slow", proplyd.Spec{}, proplyd.NearCacheConfig{Disabled: true})
	mustSet(t, ns, "k", "v")
	e.Link.SetLatency(time.Second, 0)
	for range 5 {
		start := time.Now()
		it := mustGet(t, ns, "k")
		if d := time.Since(start); d > 700*time.Millisecond {
			t.Fatalf("read took %v despite the 300ms read timeout", d)
		}
		expect(t, it, "v")
	}
	if e.Client.State() != proplyd.BreakerOpen {
		t.Fatalf("breaker %v, want open", e.Client.State())
	}
	e.Link.SetLatency(0, 0)
	eventually(t, func() error {
		if it := mustGet(t, ns, "k"); it.Source != proplyd.SourceCache {
			return fmt.Errorf("source %v", it.Source)
		}
		return nil
	})
}

// Redis dies: proplyd reports itself unhealthy, clients fall back. Redis
// comes back empty: namespaces are re-registered and near-caches reset.
func TestRedisDiesAndComesBackEmpty(t *testing.T) {
	c := testcluster.New(t, testcluster.Options{})
	e := c.Edge()
	ns := e.Namespace(t, "redis", proplyd.Spec{Write: proplyd.WriteAround}, testcluster.NearConfig())
	testcluster.WaitNearUsable(t, ns)
	mustSet(t, ns, "k", "v1")
	expect(t, mustGet(t, ns, "k"), "v1")

	c.Redis.Kill()
	mustSet(t, ns, "k", "v2")
	expect(t, mustGet(t, ns, "k"), "v2")
	eventually(t, func() error {
		if ns.Stats().NearUsable {
			return errors.New("near-cache still trusted without Redis")
		}
		return nil
	})

	resets := ns.Stats().Resets
	c.Redis.Start()
	testcluster.WaitNearUsable(t, ns)
	eventually(t, func() error {
		it, err := ns.Lookup(context.Background(), "k")
		if err != nil {
			return err
		}
		if string(it.Value) != "v2" {
			t.Fatalf("got %q", it.Value)
		}
		if it.Source == proplyd.SourceBackend && ns.Stats().CacheLoads == 0 {
			return errors.New("not using the cache yet")
		}
		return nil
	})
	if ns.Stats().Resets <= resets {
		t.Fatal("near-cache was not reset after Redis lost its data")
	}
}

// proplyd cannot reach the store but the edge can: reads and write-around
// writes go direct, and the breaker stays closed (the cache is fine).
func TestStoreUnreachableFromCacheOnly(t *testing.T) {
	c := testcluster.New(t, testcluster.Options{})
	e := c.Edge()
	wa := e.Namespace(t, "wa", proplyd.Spec{Write: proplyd.WriteAround}, proplyd.NearCacheConfig{Disabled: true})
	wt := e.Namespace(t, "wt", proplyd.Spec{Write: proplyd.WriteThrough}, proplyd.NearCacheConfig{Disabled: true})
	mustSet(t, wa, "cached", "c")
	expect(t, mustGet(t, wa, "cached"), "c")

	c.ServerBackend.SetDown(true)
	expect(t, mustGet(t, wa, "cached"), "c") // still served from the cache
	if _, err := c.Store.Put(ctxT(t), "/wa/miss", []byte("m")); err != nil {
		t.Fatal(err)
	}
	if it := mustGet(t, wa, "miss"); it.Source != proplyd.SourceBackend || string(it.Value) != "m" {
		t.Fatalf("miss: %+v", it)
	}
	mustSet(t, wa, "k", "direct")
	if err := wt.Set(ctxT(t), "k", []byte("x")); !errors.Is(err, proplyd.ErrBackendUnavailable) {
		t.Fatalf("write-through with the store unreachable from the cache: %v", err)
	}
	if st := e.Client.State(); st != proplyd.BreakerClosed {
		t.Fatalf("breaker %v: store errors must not count against the cache", st)
	}
	c.ServerBackend.SetDown(false)
	eventually(t, func() error { return valueIs(wa, "k", "direct") })
}

func TestStoreDownEverywhere(t *testing.T) {
	c := testcluster.New(t, testcluster.Options{})
	e := c.Edge()
	ns := e.Namespace(t, "down", proplyd.Spec{}, testcluster.NearConfig())
	mustSet(t, ns, "k", "v")
	expect(t, mustGet(t, ns, "k"), "v")
	c.ServerBackend.SetDown(true)
	e.Backend.SetDown(true)

	expect(t, mustGet(t, ns, "k"), "v") // cached data survives
	if _, err := ns.Lookup(ctxT(t), "other"); !errors.Is(err, proplyd.ErrBackendUnavailable) {
		t.Fatalf("miss: %v", err)
	}
	if err := ns.Set(ctxT(t), "k", []byte("x")); !errors.Is(err, proplyd.ErrBackendUnavailable) {
		t.Fatalf("set: %v", err)
	}
}

// A stalled link (no RST, nothing delivered) makes the stream go silent:
// the near-cache stops being trusted, and once the link heals the stream
// resumes without losing invalidations or resetting.
func TestStalledLinkResumesInvalidations(t *testing.T) {
	c := testcluster.New(t, testcluster.Options{})
	a := c.Edge()
	nsA := a.Namespace(t, "stall", proplyd.Spec{Write: proplyd.WriteAround}, testcluster.NearConfig())
	nsB := c.Edge().Namespace(t, "stall", proplyd.Spec{Write: proplyd.WriteAround}, testcluster.NearConfig())
	testcluster.WaitNearUsable(t, nsA)
	for _, k := range []string{"x", "y"} {
		mustSet(t, nsB, k, "1")
		eventually(t, func() error { return valueIs(nsA, k, "1") })
	}
	resets := nsA.Stats().Resets

	a.Link.SetMode(proplydtest.Blackhole)
	eventually(t, func() error {
		if nsA.Stats().NearUsable {
			return errors.New("near-cache still trusted on a silent stream")
		}
		return nil
	})
	mustSet(t, nsB, "x", "2")
	mustSet(t, nsB, "y", "2")
	// Reads during the stall fall back to the store and are fresh.
	expect(t, mustGet(t, nsA, "x"), "2")

	a.Link.SetMode(proplydtest.Forward)
	testcluster.WaitNearUsable(t, nsA)
	eventually(t, func() error { return valueIs(nsA, "y", "2") })
	if nsA.Stats().Resets != resets {
		t.Fatal("a short stall must resume, not reset")
	}
}

func TestMaxStalenessServesStaleDuringOutage(t *testing.T) {
	c := testcluster.New(t, testcluster.Options{})
	a := c.Edge()
	near := testcluster.NearConfig()
	near.MaxStaleness = time.Minute
	ns := a.Namespace(t, "stale", proplyd.Spec{}, near)
	testcluster.WaitNearUsable(t, ns)
	mustSet(t, ns, "k", "v")
	eventually(t, func() error {
		if it := mustGet(t, ns, "k"); it.Source != proplyd.SourceNear {
			return fmt.Errorf("source %v", it.Source)
		}
		return nil
	})
	c.Node.Kill()
	a.Backend.SetDown(true)
	// Neither the cache nor the store is reachable, but the edge keeps
	// serving what it has.
	time.Sleep(1500 * time.Millisecond) // past the heartbeat timeout
	if it := mustGet(t, ns, "k"); it.Source != proplyd.SourceNear || string(it.Value) != "v" {
		t.Fatalf("got %+v", it)
	}
}

func TestNamespaceConflict(t *testing.T) {
	c := testcluster.New(t, testcluster.Options{})
	c.Edge().Namespace(t, "conf", proplyd.Spec{TTL: time.Minute}, testcluster.NearConfig())
	_, err := c.Edge().Client.Namespace(ctxT(t), "conf", proplyd.NamespaceConfig{Spec: proplyd.Spec{KeyPrefix: "/conf/", TTL: time.Hour}})
	if !errors.Is(err, proplyd.ErrNamespaceConflict) {
		t.Fatalf("got %v", err)
	}
}

func TestCallerCancellationDoesNotTripBreaker(t *testing.T) {
	c := testcluster.New(t, testcluster.Options{})
	e := c.Edge()
	ns := e.Namespace(t, "cancel", proplyd.Spec{}, proplyd.NearCacheConfig{Disabled: true})
	e.Link.SetLatency(200*time.Millisecond, 0)
	for range 10 {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		_, _ = ns.Lookup(ctx, "k")
		cancel()
	}
	if st := e.Client.State(); st != proplyd.BreakerClosed {
		t.Fatalf("breaker %v", st)
	}
}

func TestInvalidate(t *testing.T) {
	c := testcluster.New(t, testcluster.Options{})
	spec := proplyd.Spec{Sync: proplyd.SyncNone}
	a := c.Edge().Namespace(t, "inv", spec, testcluster.NearConfig())
	b := c.Edge().Namespace(t, "inv", spec, testcluster.NearConfig())
	testcluster.WaitNearUsable(t, b)
	mustSet(t, a, "k", "v1")
	eventually(t, func() error { return valueIs(b, "k", "v1") })
	// Someone writes the store behind the cache's back (no watch).
	if _, err := c.Store.Put(ctxT(t), "/inv/k", []byte("v2")); err != nil {
		t.Fatal(err)
	}
	expect(t, mustGet(t, b, "k"), "v1")
	if err := a.Invalidate(ctxT(t), "k"); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() error { return valueIs(b, "k", "v2") })
}
