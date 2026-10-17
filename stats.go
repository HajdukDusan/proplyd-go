package proplyd

import "sync/atomic"

// Stats are a namespace's counters.
type Stats struct {
	NearHits, NearMisses atomic.Int64
	// CacheHits were served from Redis; CacheLoads were loaded from the
	// backing store by proplyd.
	CacheHits, CacheLoads atomic.Int64
	// AsideLoads were cache-aside misses loaded by this client.
	AsideLoads atomic.Int64
	Writes     atomic.Int64
	// FallbackReads and FallbackWrites went to the backing store directly
	// because the cache was unavailable.
	FallbackReads, FallbackWrites atomic.Int64
	// Invalidations and Resets received on the invalidation stream.
	Invalidations, Resets atomic.Int64
	// Subscriptions counts invalidation streams that went live.
	Subscriptions atomic.Int64
}

// StatsSnapshot is a point-in-time copy of Stats.
type StatsSnapshot struct {
	NearHits, NearMisses          int64
	CacheHits, CacheLoads         int64
	AsideLoads, Writes            int64
	FallbackReads, FallbackWrites int64
	Invalidations, Resets         int64
	Subscriptions                 int64
	NearEntries                   int
	NearUsable                    bool
}

// Stats returns the namespace counters.
func (n *Namespace) Stats() StatsSnapshot {
	s := &n.stats
	snap := StatsSnapshot{
		NearHits: s.NearHits.Load(), NearMisses: s.NearMisses.Load(),
		CacheHits: s.CacheHits.Load(), CacheLoads: s.CacheLoads.Load(),
		AsideLoads: s.AsideLoads.Load(), Writes: s.Writes.Load(),
		FallbackReads: s.FallbackReads.Load(), FallbackWrites: s.FallbackWrites.Load(),
		Invalidations: s.Invalidations.Load(), Resets: s.Resets.Load(),
		Subscriptions: s.Subscriptions.Load(),
	}
	if n.near != nil {
		snap.NearEntries = n.near.len()
		snap.NearUsable = n.nearUsable()
	}
	return snap
}
