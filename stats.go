package proplyd

import (
	"sync/atomic"
	"time"
)

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
	// SharedLoads counts aside and fallback reads that shared a concurrent
	// read of the same key from the backing store instead of making their
	// own. Store reads made = AsideLoads + FallbackReads - SharedLoads.
	SharedLoads atomic.Int64

	reads  [numReadPaths]histogram
	writes [2]histogram // cache, fallback
}

func (s *Stats) observeRead(p readPath, d time.Duration) { s.reads[p].observe(d) }

func (s *Stats) observeWrite(fallback bool, d time.Duration) {
	if fallback {
		s.writes[1].observe(d)
	} else {
		s.writes[0].observe(d)
	}
}

// StatsSnapshot is a point-in-time copy of Stats.
type StatsSnapshot struct {
	NearHits, NearMisses          int64
	CacheHits, CacheLoads         int64
	AsideLoads, Writes            int64
	FallbackReads, FallbackWrites int64
	Invalidations, Resets         int64
	Subscriptions                 int64
	SharedLoads                   int64
	NearEntries                   int
	NearUsable                    bool
	// ReadLatency holds the latency of successful lookups by read path
	// (PathNear, PathCache, PathLoad, PathAside, PathFallback), WriteLatency
	// that of successful writes (WritePathCache, WritePathFallback).
	ReadLatency  map[string]Histogram
	WriteLatency map[string]Histogram
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
		Subscriptions: s.Subscriptions.Load(), SharedLoads: s.SharedLoads.Load(),
		ReadLatency: make(map[string]Histogram, numReadPaths),
		WriteLatency: map[string]Histogram{
			WritePathCache:    s.writes[0].snapshot(),
			WritePathFallback: s.writes[1].snapshot(),
		},
	}
	for p := range numReadPaths {
		snap.ReadLatency[readPathNames[p]] = s.reads[p].snapshot()
	}
	if n.near != nil {
		snap.NearEntries = n.near.len()
		snap.NearUsable = n.nearUsable()
	}
	return snap
}
