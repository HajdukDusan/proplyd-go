package promexport_test

import (
	"context"
	"testing"
	"time"

	proplyd "github.com/c12s/proplyd-go"
	"github.com/c12s/proplyd-go/internal/testcluster"
	"github.com/c12s/proplyd-go/promexport"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestCollector(t *testing.T) {
	c := testcluster.New(t, testcluster.Options{})
	e := c.Edge()
	ns := e.Namespace(t, "nodes", proplyd.Spec{}, testcluster.NearConfig())
	testcluster.WaitNearUsable(t, ns)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := ns.Set(ctx, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if _, err := ns.Lookup(ctx, "k"); err != nil {
			t.Fatal(err)
		}
	}

	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(promexport.NewCollector(e.Client))
	n, err := testutil.GatherAndCount(reg)
	if err != nil {
		t.Fatal(err)
	}
	if n == 0 {
		t.Fatal("no metrics")
	}
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, mf := range mfs {
		got[mf.GetName()] = true
		if mf.GetName() == "proplyd_client_read_duration_seconds" {
			var reads uint64
			for _, m := range mf.GetMetric() {
				reads += m.GetHistogram().GetSampleCount()
			}
			if reads != 3 {
				t.Errorf("read duration samples = %d, want 3", reads)
			}
		}
	}
	for _, name := range []string{
		"proplyd_client_breaker_state", "proplyd_client_breaker_transitions_total",
		"proplyd_client_near_hits_total", "proplyd_client_cache_loads_total",
		"proplyd_client_read_duration_seconds", "proplyd_client_write_duration_seconds",
		"proplyd_client_near_usable",
	} {
		if !got[name] {
			t.Errorf("%s missing", name)
		}
	}
}
