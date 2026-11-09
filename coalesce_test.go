package proplyd

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/c12s/proplyd/pkg/backend"
)

// gate is a read that blocks until released and counts its calls.
type gate struct {
	calls   atomic.Int32
	release chan struct{}
}

func (g *gate) read(ctx context.Context) (backend.Item, error) {
	n := g.calls.Add(1)
	select {
	case <-g.release:
	case <-ctx.Done():
		return backend.Item{}, ctx.Err()
	}
	return backend.Item{Found: true, Value: []byte{byte(n)}, ModRevision: int64(n), ReadRevision: int64(n)}, nil
}

func TestCoalescerSharesReads(t *testing.T) {
	var c coalescer
	g := &gate{release: make(chan struct{})}
	ctx := context.Background()
	type result struct {
		item   backend.Item
		leader bool
	}
	lookup := func(since uint64) chan result {
		ch := make(chan result, 1)
		go func() {
			it, leader, err := c.do(ctx, "k", since, time.Second, g.read)
			if err != nil {
				t.Error(err)
			}
			ch <- result{it, leader}
		}()
		return ch
	}
	waitCalls := func(n int32) {
		for g.calls.Load() != n {
			time.Sleep(time.Millisecond)
		}
	}

	// A lookup that began before the read started shares it.
	early := c.ticket()
	first := lookup(c.ticket())
	waitCalls(1)
	joined := lookup(early)
	// So does one that began later, while the client has not served or
	// written the key since the read started (a TTL expiry under load).
	late := lookup(c.ticket())
	time.Sleep(20 * time.Millisecond)
	// Once the client served the key, a lookup that began after that may
	// have seen something newer than the read in flight: it reads itself.
	c.observe("k")
	after := lookup(c.ticket())
	waitCalls(2)
	close(g.release)

	leaders := 0
	for _, ch := range []chan result{first, joined, late} {
		r := <-ch
		if r.item.ModRevision != 1 {
			t.Errorf("got read %d, want the shared read 1", r.item.ModRevision)
		}
		if r.leader {
			leaders++
		}
	}
	if leaders != 1 {
		t.Errorf("%d leaders, want 1", leaders)
	}
	if r := <-after; r.item.ModRevision != 2 || !r.leader {
		t.Errorf("lookup after an observation got read %d (leader %v), want its own read 2", r.item.ModRevision, r.leader)
	}
}

func TestCoalescerLeaderGivingUpDoesNotFailOthers(t *testing.T) {
	var c coalescer
	g := &gate{release: make(chan struct{})}
	t1, t2 := c.ticket(), c.ticket()
	lctx, cancel := context.WithCancel(context.Background())
	lerr := make(chan error)
	go func() {
		_, _, err := c.do(lctx, "k", t1, time.Second, g.read)
		lerr <- err
	}()
	for g.calls.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	other := make(chan error)
	go func() {
		_, leader, err := c.do(context.Background(), "k", t2, time.Second, g.read)
		if leader {
			t.Error("second caller started its own read")
		}
		other <- err
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	if err := <-lerr; err != context.Canceled {
		t.Fatalf("leader: %v", err)
	}
	close(g.release)
	if err := <-other; err != nil {
		t.Fatalf("joiner failed with the leader: %v", err)
	}
	if n := g.calls.Load(); n != 1 {
		t.Fatalf("%d reads", n)
	}
}
