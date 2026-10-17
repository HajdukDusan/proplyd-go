package tests

import (
	"context"
	"math/rand/v2"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/c12s/proplyd-go/internal/testcluster"
	"github.com/c12s/proplyd/pkg/server"
	"github.com/c12s/proplyd/proplydtest"
)

// fault is a failure the chaos test injects: apply breaks something, and
// the returned function heals it.
type fault struct {
	name  string
	apply func(c *testcluster.Cluster, w *workload, r *rand.Rand) (heal func())
}

func someEdges(w *workload, r *rand.Rand) []*testcluster.Edge {
	var out []*testcluster.Edge
	for _, e := range w.edges {
		if r.IntN(3) == 0 {
			out = append(out, e)
		}
	}
	if len(out) == 0 {
		out = append(out, w.edges[r.IntN(len(w.edges))])
	}
	return out
}

var faults = []fault{
	{"cache crash", func(c *testcluster.Cluster, _ *workload, _ *rand.Rand) func() {
		c.Node.Kill()
		return c.Node.Start
	}},
	{"cache graceful restart", func(c *testcluster.Cluster, _ *workload, _ *rand.Rand) func() {
		c.Node.Shutdown()
		return c.Node.Start
	}},
	{"redis crash (data lost)", func(c *testcluster.Cluster, _ *workload, _ *rand.Rand) func() {
		c.Redis.Kill()
		return c.Redis.Start
	}},
	{"cache-redis partition", func(c *testcluster.Cluster, _ *workload, _ *rand.Rand) func() {
		c.RedisLink.SetMode(proplydtest.Blackhole)
		return func() { c.RedisLink.SetMode(proplydtest.Forward) }
	}},
	{"cache-redis resets", func(c *testcluster.Cluster, _ *workload, _ *rand.Rand) func() {
		c.RedisLink.ResetAll()
		return func() {}
	}},
	{"cache-store outage", func(c *testcluster.Cluster, _ *workload, _ *rand.Rand) func() {
		c.ServerBackend.SetDown(true)
		return func() { c.ServerBackend.SetDown(false) }
	}},
	{"cache-store slow", func(c *testcluster.Cluster, _ *workload, _ *rand.Rand) func() {
		c.ServerBackend.SetLatency(300 * time.Millisecond)
		return func() { c.ServerBackend.SetLatency(0) }
	}},
	{"watch broken", func(c *testcluster.Cluster, _ *workload, _ *rand.Rand) func() {
		c.ServerBackend.BreakWatches()
		return func() {}
	}},
	{"watch lagging", func(c *testcluster.Cluster, _ *workload, _ *rand.Rand) func() {
		c.ServerBackend.PauseWatches(true)
		return func() { c.ServerBackend.PauseWatches(false) }
	}},
	{"watch history compacted", func(c *testcluster.Cluster, _ *workload, _ *rand.Rand) func() {
		// While the watch lags, history it still needs is compacted: the
		// cache must purge and resynchronize.
		c.ServerBackend.PauseWatches(true)
		return func() {
			if rev, err := c.Store.Revision(context.Background()); err == nil {
				c.Store.Compact(rev)
			}
			c.ServerBackend.BreakWatches()
			c.ServerBackend.PauseWatches(false)
		}
	}},
	{"edge partition", func(_ *testcluster.Cluster, w *workload, r *rand.Rand) func() {
		edges := someEdges(w, r)
		mode := proplydtest.Refuse
		if r.IntN(2) == 0 {
			mode = proplydtest.Blackhole
		}
		for _, e := range edges {
			e.Link.SetMode(mode)
		}
		return func() {
			for _, e := range edges {
				e.Link.SetMode(proplydtest.Forward)
			}
		}
	}},
	{"edge latency spike", func(_ *testcluster.Cluster, w *workload, r *rand.Rand) func() {
		edges := someEdges(w, r)
		for _, e := range edges {
			e.Link.SetLatency(200*time.Millisecond, 600*time.Millisecond)
		}
		return func() {
			for _, e := range edges {
				e.Link.SetLatency(0, 0)
			}
		}
	}},
	{"edge connection resets", func(_ *testcluster.Cluster, w *workload, r *rand.Rand) func() {
		for _, e := range someEdges(w, r) {
			e.Link.ResetAll()
		}
		return func() {}
	}},
}

// TestChaos runs the workload while injecting one fault after another:
// cache crashes and restarts, Redis crashes (losing everything), network
// partitions and resets on every link, store outages and slowness for the
// cache, broken, lagging and compacted watches. Afterwards it checks that
// no unexpected error surfaced, no acknowledged write was lost, owners
// always read their own writes, and that every near-cache and Redis
// converge to the store.
//
// PROPLYD_CHAOS_DURATION scales it (default 15s).
func TestChaos(t *testing.T) {
	c := testcluster.New(t, testcluster.Options{KeepHistory: 5000})
	w := newWorkload(t, c, workloadConfig{
		edges:         envInt(t, "PROPLYD_CHAOS_EDGES", 12, 6),
		keys:          32,
		duration:      envDuration(t, "PROPLYD_CHAOS_DURATION", 15*time.Second, 5*time.Second),
		writeRatio:    0.3,
		allowWriteErr: writeThroughMayFail,
	}, allSpecs())

	ctx, cancel := context.WithCancel(context.Background())
	counts := map[string]int{}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		r := rand.New(rand.NewPCG(1, uint64(time.Now().UnixNano())))
		// Let the workload warm up first.
		for sleep(ctx, jitter(300*time.Millisecond, time.Second)) {
			f := faults[r.IntN(len(faults))]
			counts[f.name]++
			rev, _ := c.Store.Revision(context.Background())
			t.Logf("%s fault: %s (store revision %d)", time.Now().Format("15:04:05.000"), f.name, rev)
			heal := f.apply(c, w, r)
			sleep(ctx, jitter(300*time.Millisecond, 1500*time.Millisecond))
			heal()
		}
	}()
	w.run()
	cancel()
	wg.Wait()

	// Make sure everything is healed before checking convergence.
	c.Node.Start()
	c.Redis.Start()
	c.RedisLink.Clear()
	c.ServerBackend.SetDown(false)
	c.ServerBackend.SetLatency(0)
	c.ServerBackend.PauseWatches(false)
	for _, e := range w.edges {
		e.Link.Clear()
	}

	var names []string
	for n, k := range counts {
		names = append(names, n+"="+strconv.Itoa(k))
	}
	sort.Strings(names)
	t.Logf("faults injected: %s", strings.Join(names, ", "))
	w.verify()
}

// TestReplicas runs the workload against two proplyd replicas that share
// Redis, crashing them alternately. Edges of one replica must see the
// writes of the other's edges (invalidations travel through Redis), and the
// watcher lease must move to the surviving replica.
func TestReplicas(t *testing.T) {
	c := testcluster.New(t, testcluster.Options{})
	second := c.AddNode(server.Options{})
	nodes := []*proplydtest.Node{c.Node, second}
	w := newWorkload(t, c, workloadConfig{
		edges:         8,
		keys:          32,
		duration:      envDuration(t, "PROPLYD_REPLICAS_DURATION", 10*time.Second, 4*time.Second),
		writeRatio:    0.3,
		allowWriteErr: writeThroughMayFail,
		nodes:         nodes,
	}, allSpecs())

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; sleep(ctx, jitter(time.Second, 2*time.Second)); i++ {
			n := nodes[i%2]
			n.Kill()
			sleep(ctx, jitter(500*time.Millisecond, 2*time.Second))
			n.Start()
		}
	}()
	w.run()
	cancel()
	wg.Wait()
	for _, n := range nodes {
		n.Start()
	}
	w.verify()
}
