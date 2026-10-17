package proplyd

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"

	"github.com/c12s/proplyd/pkg/api"
	"github.com/c12s/proplyd/pkg/protocol"
)

const (
	minResubscribe = 100 * time.Millisecond
	maxResubscribe = 5 * time.Second
)

// nearUsable reports whether near-cache entries may be served: the
// invalidation stream is alive, or it was lost less than MaxStaleness ago.
func (n *Namespace) nearUsable() bool {
	last := n.lastMsg.Load()
	if last == 0 {
		return false
	}
	age := time.Since(time.Unix(0, last))
	if n.streamUp.Load() {
		return age < n.ncfg.HeartbeatTimeout
	}
	return age < n.ncfg.MaxStaleness
}

// subscribe keeps the invalidation stream connected. After a disconnect it
// resumes from the last position, so the near-cache stays valid; if the
// server cannot resume (history trimmed, epoch changed, Redis lost data) it
// sends a reset and the near-cache starts over.
func (n *Namespace) subscribe() {
	defer n.wg.Done()
	backoff := minResubscribe
	for n.ctx.Err() == nil {
		live, err := n.subscribeOnce()
		n.streamUp.Store(false)
		if n.ctx.Err() != nil {
			return
		}
		if protocol.Reason(err) == protocol.ReasonNamespaceNotFound {
			n.registered.Store(false)
			if rerr := n.register(n.ctx); rerr == nil {
				continue
			}
		}
		if live {
			backoff = minResubscribe
		}
		n.c.log.Debug("invalidation stream lost", "namespace", n.name, "err", err, "retry_in", backoff)
		// Full jitter, so a fleet of edges does not reconnect in lockstep.
		t := time.NewTimer(time.Duration(rand.Int64N(int64(backoff))) + backoff/2)
		select {
		case <-n.ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}
		backoff = min(2*backoff, maxResubscribe)
	}
}

// subscribeOnce runs one stream until it fails. live reports whether it
// delivered anything.
func (n *Namespace) subscribeOnce() (live bool, err error) {
	if !n.registered.Load() {
		if err := n.register(n.ctx); err != nil {
			return false, err
		}
	}
	ctx, cancel := context.WithCancelCause(n.ctx)
	defer cancel(nil)
	stream, err := n.c.api.Subscribe(ctx, &api.SubscribeRequest{Namespace: n.name, Epoch: n.epoch, Position: n.position})
	if err != nil {
		return false, err
	}

	// A stalled network does not always break the connection; give up on
	// a stream that stays silent past the heartbeat timeout.
	heard := make(chan struct{}, 1)
	go func() {
		timeout := n.ncfg.HeartbeatTimeout
		t := time.NewTimer(timeout)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-heard:
				if !t.Stop() {
					<-t.C
				}
				t.Reset(timeout)
			case <-t.C:
				cancel(errors.New("invalidation stream silent"))
				return
			}
		}
	}()

	for {
		resp, err := stream.Recv()
		if err != nil {
			if cause := context.Cause(ctx); cause != nil && n.ctx.Err() == nil {
				err = cause
			}
			return live, err
		}
		select {
		case heard <- struct{}{}:
		default:
		}
		n.apply(resp)
		if !live {
			live = true
			n.stats.Subscriptions.Add(1)
		}
		n.lastMsg.Store(time.Now().UnixNano())
		n.streamUp.Store(true)
	}
}

func (n *Namespace) apply(resp *api.SubscribeResponse) {
	switch resp.GetKind() {
	case api.SubscribeResponse_KIND_RESET:
		n.near.reset()
		n.stats.Resets.Add(1)
	case api.SubscribeResponse_KIND_INVALIDATIONS:
		for _, inv := range resp.GetInvalidations() {
			n.near.invalidate(inv.GetKey(), inv.GetRevision())
			n.writes.seen(inv.GetKey(), inv.GetRevision())
		}
		n.stats.Invalidations.Add(int64(len(resp.GetInvalidations())))
	}
	n.epoch, n.position = resp.GetEpoch(), resp.GetPosition()
}
