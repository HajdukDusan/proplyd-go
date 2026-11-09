package proplyd

import (
	"sort"
	"sync/atomic"
	"time"
)

// latencyBounds are the upper bounds (seconds) of the latency histograms,
// the same buckets as proplyd's server metrics. Near-cache hits take
// microseconds, so the low end is fine-grained.
var latencyBounds = [...]float64{.0001, .00025, .0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5}

// LatencyBuckets returns the upper bounds (seconds) of the buckets of every
// Histogram.
func LatencyBuckets() []float64 { return append([]float64(nil), latencyBounds[:]...) }

// Histogram is a snapshot of a latency histogram.
type Histogram struct {
	// Buckets[i] counts the observations of at most LatencyBuckets()[i]
	// seconds (cumulative, as in Prometheus).
	Buckets []uint64
	// Count of all observations, including those above the last bound.
	Count uint64
	Sum   time.Duration
}

// histogram is a lock-free latency histogram.
type histogram struct {
	counts [len(latencyBounds) + 1]atomic.Uint64 // the last one is +Inf
	sum    atomic.Int64                          // nanoseconds
}

func (h *histogram) observe(d time.Duration) {
	h.counts[sort.SearchFloat64s(latencyBounds[:], d.Seconds())].Add(1)
	h.sum.Add(int64(d))
}

func (h *histogram) snapshot() Histogram {
	s := Histogram{Buckets: make([]uint64, len(latencyBounds)), Sum: time.Duration(h.sum.Load())}
	for i := range h.counts {
		s.Count += h.counts[i].Load()
		if i < len(latencyBounds) {
			s.Buckets[i] = s.Count
		}
	}
	return s
}

// Read paths of Lookup, the keys of StatsSnapshot.ReadLatency.
const (
	PathNear     = "near"     // served by the near-cache
	PathCache    = "cache"    // served by proplyd from Redis
	PathLoad     = "load"     // a miss proplyd loaded from the backing store
	PathAside    = "aside"    // a cache-aside miss this client loaded
	PathFallback = "fallback" // the cache was unavailable; read from the store directly
)

// Write paths, the keys of StatsSnapshot.WriteLatency: through proplyd, or
// (write-around with the cache unavailable) to the store directly.
const (
	WritePathCache    = "cache"
	WritePathFallback = "fallback"
)

type readPath int

const (
	readNear readPath = iota
	readCache
	readLoad
	readAside
	readFallback
	numReadPaths
)

var readPathNames = [numReadPaths]string{PathNear, PathCache, PathLoad, PathAside, PathFallback}
