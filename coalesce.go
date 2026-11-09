package proplyd

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/c12s/proplyd/pkg/backend"
)

// coalescer shares concurrent reads of a key from the backing store, so
// that many lookups missing the same key at once (a TTL expiry under load)
// cost one store read instead of one each.
//
// Sharing must not return a value older than one the caller has already
// seen (read-your-writes, monotonic reads). A read in flight cannot be
// older than anything the client observed before the read started: the
// store only moves forward. So a lookup may join a read that started after
// the lookup did (it takes a ticket when it starts), and also an older read
// as long as the client has not served or written the key since that read
// started (observe marks the read). Without that, lookups arriving one by
// one during a slow read would each start their own.
type coalescer struct {
	ticks atomic.Uint64
	mu    sync.Mutex
	m     map[string]*flight
}

type flight struct {
	ticket uint64
	done   chan struct{}
	item   backend.Item
	err    error
	// observed: since this read started, the client served the key from
	// the cache, wrote it, or got it from another read; lookups that began
	// after that may have seen something newer and must not join.
	observed bool
}

// ticket marks the start of a lookup.
func (c *coalescer) ticket() uint64 { return c.ticks.Add(1) }

// observe records that the client served or wrote key.
func (c *coalescer) observe(key string) {
	c.mu.Lock()
	if f, ok := c.m[key]; ok {
		f.observed = true
	}
	c.mu.Unlock()
}

// do reads key with read, or waits for a read in flight that the lookup
// holding ticket since may share. leader reports whether this call started
// the read. The read runs on its own so that a caller giving up does not
// fail the others: it keeps the leader's context values and deadline (or
// timeout, without a deadline) but not its cancellation.
func (c *coalescer) do(ctx context.Context, key string, since uint64, timeout time.Duration, read func(context.Context) (backend.Item, error)) (item backend.Item, leader bool, err error) {
	c.mu.Lock()
	f, ok := c.m[key]
	if !ok || f.ticket < since && f.observed {
		leader = true
		f = &flight{ticket: c.ticks.Add(1), done: make(chan struct{})}
		if c.m == nil {
			c.m = map[string]*flight{}
		}
		c.m[key] = f
	}
	c.mu.Unlock()

	if leader {
		rctx := context.WithoutCancel(ctx)
		var cancel context.CancelFunc
		if dl, ok := ctx.Deadline(); ok {
			rctx, cancel = context.WithDeadline(rctx, dl)
		} else {
			rctx, cancel = context.WithTimeout(rctx, timeout)
		}
		go func() {
			defer cancel()
			f.item, f.err = read(rctx)
			c.mu.Lock()
			if cur := c.m[key]; cur == f {
				delete(c.m, key)
			} else if cur != nil && f.err == nil {
				// A newer read replaced this one; this result is about to be
				// returned (observed) after that read started.
				cur.observed = true
			}
			c.mu.Unlock()
			close(f.done)
		}()
	}
	select {
	case <-f.done:
		return f.item, leader, f.err
	case <-ctx.Done():
		return backend.Item{}, leader, ctx.Err()
	}
}
