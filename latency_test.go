package proplyd

import (
	"testing"
	"time"
)

func TestHistogram(t *testing.T) {
	var h histogram
	h.observe(50 * time.Microsecond)  // <= 0.0001
	h.observe(100 * time.Microsecond) // exactly on a bound: counts in it
	h.observe(3 * time.Millisecond)   // <= 0.005
	h.observe(time.Minute)            // above the last bound
	s := h.snapshot()
	if s.Count != 4 || s.Sum != 50*time.Microsecond+100*time.Microsecond+3*time.Millisecond+time.Minute {
		t.Fatalf("count %d sum %v", s.Count, s.Sum)
	}
	bounds := LatencyBuckets()
	for i, ub := range bounds {
		var want uint64
		switch {
		case ub < 0.0001:
		case ub < 0.005:
			want = 2
		default:
			want = 3
		}
		if s.Buckets[i] != want {
			t.Errorf("bucket le=%v: %d, want %d", ub, s.Buckets[i], want)
		}
	}
}
