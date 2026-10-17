package tests

import (
	"testing"
	"time"

	"github.com/c12s/proplyd-go/internal/testcluster"
)

// TestStress runs many edges against one proplyd over every strategy, with
// no faults. Beyond "no errors", it checks read-your-writes on every read of
// an edge's own keys, monotonic reads for every reader, that the store holds
// every acknowledged write, and that all near-caches and Redis converge to
// the store.
//
// PROPLYD_STRESS_DURATION, PROPLYD_STRESS_EDGES and PROPLYD_STRESS_KEYS
// scale it (defaults 5s, 24, 64).
func TestStress(t *testing.T) {
	c := testcluster.New(t, testcluster.Options{})
	w := newWorkload(t, c, workloadConfig{
		edges:      envInt(t, "PROPLYD_STRESS_EDGES", 24, 8),
		keys:       envInt(t, "PROPLYD_STRESS_KEYS", 64, 32),
		duration:   envDuration(t, "PROPLYD_STRESS_DURATION", 5*time.Second, 2*time.Second),
		writeRatio: 0.3,
		monotonic:  true,
	}, allSpecs())
	w.run()
	w.verify()
}
