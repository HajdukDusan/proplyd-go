// Package promexport exposes the statistics of a proplyd-go client as
// Prometheus metrics. It is a separate package so that clients that do not
// use Prometheus do not depend on it.
//
//	reg.MustRegister(promexport.NewCollector(cli))
package promexport

import (
	"time"

	proplyd "github.com/c12s/proplyd-go"
	"github.com/prometheus/client_golang/prometheus"
)

// Collector reads a client's breaker state and the statistics of all its
// namespaces at scrape time.
type Collector struct {
	cli *proplyd.Client
}

// NewCollector returns a collector for cli.
func NewCollector(cli *proplyd.Client) *Collector { return &Collector{cli: cli} }

func desc(name, help string, labels ...string) *prometheus.Desc {
	return prometheus.NewDesc("proplyd_client_"+name, help, labels, nil)
}

var (
	breakerState       = desc("breaker_state", "Circuit breaker state: 0 closed, 1 half-open, 2 open.")
	breakerTransitions = desc("breaker_transitions_total", "Circuit breaker transitions, by the state entered.", "to")
	breakerLastChange  = desc("breaker_last_change_timestamp_seconds", "Time of the last circuit breaker transition (0 if none).")

	nearEntries   = desc("near_entries", "Entries in the near-cache.", "namespace")
	nearUsable    = desc("near_usable", "Whether the near-cache may be used (the invalidation stream is live).", "namespace")
	readDuration  = desc("read_duration_seconds", "Latency of successful reads, by path (near, cache, load, aside, fallback).", "namespace", "path")
	writeDuration = desc("write_duration_seconds", "Latency of successful writes, by path (cache, fallback).", "namespace", "path")

	counters = []struct {
		desc *prometheus.Desc
		get  func(proplyd.StatsSnapshot) int64
	}{
		{desc("near_hits_total", "Reads served by the near-cache.", "namespace"), func(s proplyd.StatsSnapshot) int64 { return s.NearHits }},
		{desc("near_misses_total", "Reads the near-cache could not serve.", "namespace"), func(s proplyd.StatsSnapshot) int64 { return s.NearMisses }},
		{desc("cache_hits_total", "Reads served by proplyd from Redis.", "namespace"), func(s proplyd.StatsSnapshot) int64 { return s.CacheHits }},
		{desc("cache_loads_total", "Reads proplyd loaded from the backing store.", "namespace"), func(s proplyd.StatsSnapshot) int64 { return s.CacheLoads }},
		{desc("aside_loads_total", "Cache-aside misses this client loaded from the backing store.", "namespace"), func(s proplyd.StatsSnapshot) int64 { return s.AsideLoads }},
		{desc("shared_loads_total", "Aside and fallback reads that shared a concurrent store read.", "namespace"), func(s proplyd.StatsSnapshot) int64 { return s.SharedLoads }},
		{desc("writes_total", "Writes acknowledged by proplyd.", "namespace"), func(s proplyd.StatsSnapshot) int64 { return s.Writes }},
		{desc("fallback_reads_total", "Reads served by the backing store because the cache was unavailable.", "namespace"), func(s proplyd.StatsSnapshot) int64 { return s.FallbackReads }},
		{desc("fallback_writes_total", "Write-around writes made to the backing store because the cache was unavailable.", "namespace"), func(s proplyd.StatsSnapshot) int64 { return s.FallbackWrites }},
		{desc("invalidations_total", "Invalidations received.", "namespace"), func(s proplyd.StatsSnapshot) int64 { return s.Invalidations }},
		{desc("resets_total", "Near-cache resets received.", "namespace"), func(s proplyd.StatsSnapshot) int64 { return s.Resets }},
		{desc("subscriptions_total", "Invalidation streams that went live.", "namespace"), func(s proplyd.StatsSnapshot) int64 { return s.Subscriptions }},
	}
)

// Describe implements prometheus.Collector.
func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{breakerState, breakerTransitions, breakerLastChange, nearEntries, nearUsable, readDuration, writeDuration} {
		ch <- d
	}
	for _, m := range counters {
		ch <- m.desc
	}
}

// Collect implements prometheus.Collector.
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	b := c.cli.BreakerStats()
	ch <- prometheus.MustNewConstMetric(breakerState, prometheus.GaugeValue, float64(b.State))
	ch <- prometheus.MustNewConstMetric(breakerTransitions, prometheus.CounterValue, float64(b.Opened), "open")
	ch <- prometheus.MustNewConstMetric(breakerTransitions, prometheus.CounterValue, float64(b.HalfOpened), "half_open")
	ch <- prometheus.MustNewConstMetric(breakerTransitions, prometheus.CounterValue, float64(b.Closed), "closed")
	last := 0.0
	if !b.LastChange.IsZero() {
		last = float64(b.LastChange.UnixNano()) / float64(time.Second)
	}
	ch <- prometheus.MustNewConstMetric(breakerLastChange, prometheus.GaugeValue, last)

	bounds := proplyd.LatencyBuckets()
	histogram := func(d *prometheus.Desc, h proplyd.Histogram, labels ...string) prometheus.Metric {
		buckets := make(map[float64]uint64, len(bounds))
		for i, ub := range bounds {
			buckets[ub] = h.Buckets[i]
		}
		return prometheus.MustNewConstHistogram(d, h.Count, h.Sum.Seconds(), buckets, labels...)
	}
	for _, ns := range c.cli.Namespaces() {
		name := ns.Name()
		s := ns.Stats()
		for _, m := range counters {
			ch <- prometheus.MustNewConstMetric(m.desc, prometheus.CounterValue, float64(m.get(s)), name)
		}
		usable := 0.0
		if s.NearUsable {
			usable = 1
		}
		ch <- prometheus.MustNewConstMetric(nearEntries, prometheus.GaugeValue, float64(s.NearEntries), name)
		ch <- prometheus.MustNewConstMetric(nearUsable, prometheus.GaugeValue, usable, name)
		for path, h := range s.ReadLatency {
			ch <- histogram(readDuration, h, name, path)
		}
		for path, h := range s.WriteLatency {
			ch <- histogram(writeDuration, h, name, path)
		}
	}
}
