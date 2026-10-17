package tests

import (
	"context"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	proplyd "github.com/c12s/proplyd-go"
	"github.com/c12s/proplyd-go/internal/testcluster"
	"github.com/c12s/proplyd/proplydtest"
)

// networkProfile shapes an edge's link to proplyd.
type networkProfile struct {
	name  string
	setup func(p *proplydtest.Proxy)
	// disturb, if set, runs during the test (flapping, resets).
	disturb func(ctx context.Context, p *proplydtest.Proxy)
}

var profiles = []networkProfile{
	{name: "good", setup: func(*proplydtest.Proxy) {}},
	{name: "lossy", setup: func(p *proplydtest.Proxy) {
		p.SetLatency(20*time.Millisecond, 40*time.Millisecond)
		p.SetDropRate(0.002)
	}},
	{name: "slow", setup: func(p *proplydtest.Proxy) {
		// Often slower than the 300ms read timeout.
		p.SetLatency(150*time.Millisecond, 300*time.Millisecond)
	}},
	{name: "narrow", setup: func(p *proplydtest.Proxy) {
		p.SetLatency(50*time.Millisecond, 0)
		p.SetBandwidth(16 << 10)
	}},
	{name: "flapping", setup: func(*proplydtest.Proxy) {}, disturb: func(ctx context.Context, p *proplydtest.Proxy) {
		for sleep(ctx, jitter(time.Second, 2*time.Second)) {
			p.SetMode(proplydtest.Blackhole)
			sleep(ctx, jitter(300*time.Millisecond, time.Second))
			p.SetMode(proplydtest.Forward)
		}
	}},
	{name: "resets", setup: func(*proplydtest.Proxy) {}, disturb: func(ctx context.Context, p *proplydtest.Proxy) {
		for sleep(ctx, jitter(500*time.Millisecond, 1500*time.Millisecond)) {
			p.ResetAll()
		}
	}},
}

// TestBadNetwork runs the workload with edges on degraded links: lossy,
// slow (beyond the read timeout), bandwidth-starved, flapping (partitioned
// on and off) and resetting connections. Edges on bad links fall back to
// the store; every guarantee of the stress test except monotonic reads
// must still hold, and everything converges once the links heal.
func TestBadNetwork(t *testing.T) {
	c := testcluster.New(t, testcluster.Options{})
	w := newWorkload(t, c, workloadConfig{
		edges:         envInt(t, "PROPLYD_BADNET_EDGES", 2*len(profiles), len(profiles)),
		keys:          32,
		duration:      envDuration(t, "PROPLYD_BADNET_DURATION", 8*time.Second, 3*time.Second),
		writeRatio:    0.3,
		allowWriteErr: writeThroughMayFail,
	}, allSpecs())

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for i, e := range w.edges {
		p := profiles[i%len(profiles)]
		p.setup(e.Link)
		if p.disturb != nil {
			wg.Add(1)
			go func() {
				defer wg.Done()
				p.disturb(ctx, e.Link)
			}()
		}
	}
	w.run()
	cancel()
	wg.Wait()

	for i, e := range w.edges {
		var fr, fw int64
		for _, ns := range w.nss[i] {
			fr += ns.Stats().FallbackReads
			fw += ns.Stats().FallbackWrites
		}
		t.Logf("edge %2d %-9s breaker %-9v fallback reads %4d writes %4d", i, profiles[i%len(profiles)].name, e.Client.State(), fr, fw)
		e.Link.Clear()
	}
	w.verify()
}

// TestBadNetworkNearCacheMaxStaleness: an edge that tolerates stale reads
// keeps serving from its near-cache while its link flaps, instead of
// falling back to the store.
func TestBadNetworkNearCacheMaxStaleness(t *testing.T) {
	c := testcluster.New(t, testcluster.Options{})
	e := c.Edge()
	near := testcluster.NearConfig()
	near.MaxStaleness = time.Minute
	ns := e.Namespace(t, "stale", proplyd.Spec{}, near)
	testcluster.WaitNearUsable(t, ns)
	if err := ns.Set(context.Background(), "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	proplydtest.Eventually(t, 5*time.Second, func() error {
		if it, _ := ns.Lookup(context.Background(), "k"); it.Source != proplyd.SourceNear {
			return proplydtest.Errorf("not in the near-cache yet")
		}
		return nil
	})
	e.Link.SetMode(proplydtest.Blackhole)
	e.Backend.SetLatency(time.Second) // a fallback read would be slow
	before := ns.Stats().FallbackReads
	for range 20 {
		start := time.Now()
		it, err := ns.Lookup(context.Background(), "k")
		if err != nil || it.Source != proplyd.SourceNear || time.Since(start) > 50*time.Millisecond {
			t.Fatalf("read during flap: %+v %v in %v", it, err, time.Since(start))
		}
		time.Sleep(100 * time.Millisecond)
	}
	if ns.Stats().FallbackReads != before {
		t.Fatal("fell back despite MaxStaleness")
	}
	e.Link.SetMode(proplydtest.Forward)
}

func jitter(lo, hi time.Duration) time.Duration {
	return lo + time.Duration(rand.Int64N(int64(hi-lo)))
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
