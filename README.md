# proplyd-go

Go client of the [proplyd](https://github.com/c12s/proplyd) cache for the
[c12s](https://github.com/c12s) platform. It adds an in-process
**near-cache** in front of the central cache and keeps working against the
backing store when the cache is unreachable.

```
app ─▶ near-cache (in process) ─▶ proplyd (gRPC → Redis) ─▶ etcd
              └──────── fallback when the cache is down ──────┘
```

- **Near-cache.** samber/hot caches holding `[]byte` (one allocation per
  entry, no pointers inside, little GC work), sharded, with a per-device
  policy and size. It is kept coherent by an invalidation stream from
  proplyd and is only trusted while that stream is alive.
- **Circuit breaker and timeouts.** Calls to proplyd have per-call timeouts
  and go through a circuit breaker. Only failures of the cache itself count
  against it.
- **Fallbacks.** When the cache is down, slow or partitioned away, reads go
  to the store directly, and so do write-around writes. Write-through writes
  fail with `ErrCacheUnavailable`.
- **Read-your-writes.** This holds even across fallbacks and restarts: the
  client remembers the revisions of its recent writes and never serves
  anything older.

## Usage

```go
cli, err := proplyd.New(proplyd.Config{Target: "dns:///proplyd:7070"})
if err != nil { ... }
defer cli.Close()

etcdCli, _ := clientv3.New(clientv3.Config{Endpoints: []string{"etcd:2379"}})

nodes, err := cli.Namespace(ctx, "magnetar-nodes", proplyd.NamespaceConfig{
	Spec: proplyd.Spec{
		Read:      proplyd.ReadThrough,
		Write:     proplyd.WriteAround,
		KeyPrefix: "/magnetar/nodes/",
		TTL:       10 * time.Minute,
	},
	// Direct store access for fallbacks (and cache-aside loads).
	Backend: etcd.New(etcdCli), // github.com/c12s/proplyd/pkg/backend/etcd
	NearCache: proplyd.NearCacheConfig{
		Policy:   proplyd.SIEVE,
		Capacity: 5_000,
		TTL:      time.Minute,
	},
})

err = nodes.Set(ctx, "n1", payload)
value, found, err := nodes.Get(ctx, "n1")
item, err := nodes.Lookup(ctx, "n1") // + Revision and Source (near, cache, backend)
err = nodes.Invalidate(ctx, "n1")    // drop from the cache and every near-cache
```

`Namespace` registers the spec with proplyd. If proplyd is unreachable, the
handle is returned anyway and registration happens later; a conflicting
spec is an error. After a Redis wipe, the namespace is re-registered
transparently.

## Configuration

`Config`:

| Field | Default | |
|---|---|---|
| `Target` | required | gRPC target; several replicas are balanced round robin, and `NOT_SERVING` replicas are skipped |
| `ReadTimeout` | 500ms | per cache read; past it the read falls back to the store |
| `WriteTimeout` | 2s | per cache write and registration |
| `Breaker` | 5 consecutive failures, or 50% of ≥20 calls in 10s; open 2s; 3 half-open probes | |
| `DialOptions` | plaintext, keepalive 10s | appended last, e.g. TLS credentials |

`NearCacheConfig` (per namespace; size it for the device):

| Field | Default | |
|---|---|---|
| `Policy` | `SIEVE` | `LRU`, `LFU`, `TwoQueue`, `FIFO`, `TinyLFU`, `WTinyLFU`. ARC is not offered: samber/hot v0.13 does not enforce its capacity. |
| `Capacity` | 10000 | entries; sharded 16 ways from 4096 up |
| `TTL` | namespace TTL, at most 1m | |
| `HeartbeatTimeout` | 3s | the stream counts as dead after this long without a message |
| `MaxStaleness` | 0 | how long entries may still be served after the stream is lost, for edges that prefer slightly stale reads to store reads on flaky links |
| `CopyOnRead` | false | values are shared with the cache; don't modify them, or set this |
| `Disabled` | false | |

## Guarantees

- **Read-your-writes.** A client always reads its own latest successful
  write, even when that write went straight to the store while the cache
  was unreachable.
- **No stale installs.** A read that started before an invalidation never
  installs a result older than that invalidation into the near-cache.
- **Bounded staleness for other clients' writes.** Normally this is the
  invalidation propagation delay (milliseconds). While the stream is down,
  the near-cache is bypassed, or used for at most `MaxStaleness`. When the
  stream comes back, it resumes from its last position and replays what was
  missed. If that is impossible, the near-cache is reset.
- **Ambiguous writes.** A write that fails with a timeout or
  `ErrCacheUnavailable` may or may not have been applied.

Errors: `ErrCacheUnavailable`, `ErrBackendUnavailable`,
`ErrNamespaceConflict`, `ErrPrefixNotAllowed`, `ErrInvalidArgument`,
`ErrClosed`, plus the caller's context errors. `ns.Stats()` reports near and
cache hits, loads, fallbacks, invalidations and resets; `cli.State()`
reports the breaker state.

## Playground

An interactive local deployment with a live dashboard lives in the separate
`proplyd-playground` module (next to this repo).

## Tests

```
make test     # everything with -race; needs redis-server on PATH
make stress   # 30s, 48 edges
make badnet   # 30s of degraded links
make chaos    # 60s of fault injection, plus the two-replica run
```

Every test runs a real Redis, real proplyd servers and real gRPC over TCP
proxies that inject faults.

- **Unit** (`nearcache_test.go`): admission against in-flight reads,
  revision-aware invalidation, floor after forgotten invalidations, reset,
  every eviction policy (sharded and not), TTL, copy-on-read, the
  recent-writes tracker.
- **Integration** (`client_test.go`):
  - every read × write strategy across two edges;
  - cache-aside fill;
  - cache never started;
  - cache crash and restart (breaker open → closed);
  - partitioned write-around reaching the other edges;
  - write-through rejected when partitioned;
  - slow cache;
  - Redis dying and coming back empty;
  - store unreachable from the cache only, and down everywhere;
  - a stalled link that resumes without a reset;
  - `MaxStaleness`;
  - conflicts, cancellations, explicit invalidation.
- **System** (`tests/`), on one workload where every key has a single owner
  edge:
  - `TestStress`: 24 edges over all six read × write combinations, each
    with a different eviction policy. It checks read-your-writes on every
    owner read, monotonic reads for every reader, and that the store holds
    every acknowledged write. All near-caches and Redis must then converge
    to the store, with reads served by the cache, not by fallbacks.
  - `TestBadNetwork`: edges on lossy, slow (beyond the read timeout),
    bandwidth-starved, flapping and resetting links.
  - `TestChaos`: crashes and graceful restarts of proplyd, Redis crashes
    (data lost), partitions and resets on every link, store outages and
    slowness for the cache, broken, lagging and compacted watches.
  - `TestReplicas`: two proplyd replicas sharing Redis, crashed in turn.

`PROPLYD_STRESS_DURATION`/`_EDGES`/`_KEYS`, `PROPLYD_BADNET_DURATION` and
`PROPLYD_CHAOS_DURATION` scale the system tests; `PROPLYD_TEST_LOG=warn`
(or `debug`) shows server logs.

## Development

Until proplyd is tagged, `go.mod` points at a local checkout:

```
replace github.com/c12s/proplyd => ../proplyd
```

Replace it with a real version before releasing this module.
