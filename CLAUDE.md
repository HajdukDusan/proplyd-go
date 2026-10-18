# CLAUDE.md — proplyd-go

Go client of proplyd (sibling repo `../proplyd`, which also holds the shared
`pkg/api`, `pkg/protocol`, `pkg/backend` and the `proplydtest` kit). Read
`../proplyd/CLAUDE.md` first: it has the user's decisions, the environment
gotchas (GOTOOLCHAIN=local, pins, redis-server) and the server invariants.

`go.mod` has `replace github.com/c12s/proplyd => ../proplyd` until proplyd
is tagged.

## Client invariants

- **Near-cache admission** (`nearcache.go`): a read records `begin()` seq;
  `install` refuses if an invalidation arrived after it that is newer than
  the result (`m.seq > start && (m.rev == 0 || m.rev > it.Revision)`), if
  start < floor, or if a newer copy exists. Forgetting a tracked
  invalidation (bounded ring) raises the floor; reset raises it to seq.
- Invalidation rev semantics: copies with revision ≥ rev stay; rev 0 drops
  unconditionally (explicit invalidate, own fallback writes).
- **Near-cache usable** only while the stream is live (a message within
  HeartbeatTimeout), or within MaxStaleness of the last message after it
  died. Installs are allowed anytime: resume replays missed invalidations,
  and a reset purges.
- **Read-your-writes**: `recentWrites` keeps own write revisions (bounded
  ring, 1 min); Lookup sends them as `min_revision` and ignores older
  near-cache copies; an invalidation at or above the revision clears it.
- **Breaker**: `IsSuccessful = !isCacheFailure`. BACKEND_UNAVAILABLE,
  NOT_FOUND, invalid and conflict are neutral. Caller cancel or deadline is
  excluded via the `callerDone` wrapper. Rejections (`ErrOpenState`,
  `ErrTooManyRequests`) mean "cache unavailable".
- **Fallback rules** (`canFallback`): reads always; write-around writes
  only; write-through returns ErrCacheUnavailable or ErrBackendUnavailable.
  NAMESPACE_NOT_FOUND triggers one re-register and retry; if it repeats,
  that counts as cache unavailable.

- **Write-behind**:
  - success: `near.invalidate(key, 0)`, and no `recentWrites` (there is no
    store revision yet);
  - unreachable cache: rejected;
  - BACKPRESSURE (ResourceExhausted): neutral for the breaker, mapped to
    `ErrBackpressure`.

## System tests (`tests/`)

Single-writer-per-key workload. It covers the namespaces rt-wb and ca-wb
too.
- `verify` waits for every write-behind queue to drain first.
- Owner read-your-writes is not checked on write-behind reads that fell
  back to the store.
- Chaos sets `lossyWriteBehind`, because a Redis wipe loses pending writes. Allowed read states for an owner: the last
acked write, or any failed (maybe applied, maybe late) write. Monotonic
reads are checked only in TestStress (no faults). `verifyConvergence`
requires breakers closed, near-caches usable and *no fallback reads* during
the round; otherwise a stuck cache hides behind fallbacks (this masked a
real problem once). `verifyRedis` decodes proplyd's Redis entries directly
(mirrors `internal/engine/entry.go`).

Fallback writes against the in-memory store are very fast, so chaos runs
create huge write storms. Watchers then lag past `KeepHistory` and purge;
that is expected and exercises the resync path.
