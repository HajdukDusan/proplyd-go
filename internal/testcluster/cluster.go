// Package testcluster assembles a proplyd deployment for tests: a real
// Redis, an in-memory backing store, a proplyd node and any number of edge
// clients, each with its own faulty network link to proplyd and to the
// store.
//
//	edge ─proxy─▶ node proxy ─▶ proplyd ─proxy─▶ Redis
//	  │                            │
//	  └── faulty ──▶ store ◀── faulty
package testcluster

import (
	"context"
	"testing"
	"time"

	proplyd "github.com/c12s/proplyd-go"
	"github.com/c12s/proplyd/pkg/backend/faulty"
	"github.com/c12s/proplyd/pkg/backend/memory"
	"github.com/c12s/proplyd/pkg/server"
	"github.com/c12s/proplyd/proplydtest"
)

// Options configures a Cluster.
type Options struct {
	Persistence proplydtest.Persistence
	// Server options for the proplyd node (Redis and Backend are set).
	Server server.Options
	// KeepHistory bounds the store's watch history (0: unbounded).
	KeepHistory int
}

// Cluster is a proplyd deployment.
type Cluster struct {
	t     testing.TB
	Redis *proplydtest.Redis
	// RedisLink is the network between proplyd and Redis.
	RedisLink *proplydtest.Proxy
	Store     *memory.Store
	// ServerBackend is proplyd's link to the store.
	ServerBackend *faulty.Backend
	Node          *proplydtest.Node
}

// New starts a cluster.
func New(t testing.TB, opts Options) *Cluster {
	t.Helper()
	r := proplydtest.StartRedis(t, opts.Persistence)
	link := proplydtest.NewProxy(t, r.Addr())
	store := memory.New(memory.Options{KeepHistory: opts.KeepHistory, ProgressInterval: 50 * time.Millisecond})
	be := faulty.New(store)
	node := proplydtest.StartNode(t, proplydtest.NodeConfig{RedisAddr: link.Addr(), Backend: be, Options: opts.Server})
	return &Cluster{t: t, Redis: r, RedisLink: link, Store: store, ServerBackend: be, Node: node}
}

// Edge is a client device: a proplyd client behind its own network link,
// with its own (faulty) access to the store.
type Edge struct {
	Client *proplyd.Client
	// Link is the network between this edge and proplyd.
	Link *proplydtest.Proxy
	// Backend is this edge's direct access to the store.
	Backend *faulty.Backend
}

// ClientConfig is the client configuration used by tests: short timeouts
// and a quick breaker, so that faults are detected fast.
func ClientConfig(target string) proplyd.Config {
	return proplyd.Config{
		Target:       target,
		ReadTimeout:  300 * time.Millisecond,
		WriteTimeout: time.Second,
		Breaker: proplyd.BreakerConfig{
			ConsecutiveFailures: 3,
			MinRequests:         10,
			OpenTimeout:         300 * time.Millisecond,
			HalfOpenRequests:    1,
		},
	}
}

// Edge creates an edge client of the cluster's node. tweak may adjust its
// configuration.
func (c *Cluster) Edge(tweak ...func(*proplyd.Config)) *Edge {
	c.t.Helper()
	return c.EdgeTo(c.Node, tweak...)
}

// AddNode starts another proplyd node (a replica) on the same Redis and
// store.
func (c *Cluster) AddNode(opts server.Options) *proplydtest.Node {
	c.t.Helper()
	return proplydtest.StartNode(c.t, proplydtest.NodeConfig{RedisAddr: c.RedisLink.Addr(), Backend: c.ServerBackend, Options: opts})
}

// EdgeTo creates an edge client of the given node.
func (c *Cluster) EdgeTo(node *proplydtest.Node, tweak ...func(*proplyd.Config)) *Edge {
	c.t.Helper()
	link := proplydtest.NewProxy(c.t, node.Addr())
	cfg := ClientConfig(link.Addr())
	for _, f := range tweak {
		f(&cfg)
	}
	cli, err := proplyd.New(cfg)
	if err != nil {
		c.t.Fatal(err)
	}
	c.t.Cleanup(func() { _ = cli.Close() })
	return &Edge{Client: cli, Link: link, Backend: faulty.New(c.Store)}
}

// NearConfig is the near-cache configuration used by tests.
func NearConfig() proplyd.NearCacheConfig {
	return proplyd.NearCacheConfig{Capacity: 1000, TTL: 30 * time.Second, HeartbeatTimeout: time.Second}
}

// Namespace opens a namespace on the edge. Unset spec fields get test
// defaults (prefix "/<name>/").
func (e *Edge) Namespace(t testing.TB, name string, spec proplyd.Spec, near proplyd.NearCacheConfig) *proplyd.Namespace {
	t.Helper()
	if spec.KeyPrefix == "" {
		spec.KeyPrefix = "/" + name + "/"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ns, err := e.Client.Namespace(ctx, name, proplyd.NamespaceConfig{Spec: spec, Backend: e.Backend, NearCache: near})
	if err != nil {
		t.Fatalf("namespace %s: %v", name, err)
	}
	return ns
}

// WaitNearUsable waits until the namespace's invalidation stream is live.
func WaitNearUsable(t testing.TB, ns *proplyd.Namespace) {
	t.Helper()
	proplydtest.Eventually(t, 10*time.Second, func() error {
		if !ns.Stats().NearUsable {
			return proplydtest.Errorf("near-cache of %s not usable", ns.Name())
		}
		return nil
	})
}
