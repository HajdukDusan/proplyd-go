// Package proplyd is the Go client of the proplyd cache for the c12s
// platform. It adds a near-cache in front of the central cache and keeps
// working against the backing store when the cache is unreachable.
//
//	app ─▶ near-cache (in process) ─▶ proplyd (gRPC, Redis) ─▶ backing store
//	                └──────── fallback when the cache is down ───────┘
//
// The near-cache holds []byte values in samber/hot caches and is kept
// coherent by an invalidation stream from proplyd. It is only trusted while
// that stream is alive.
//
// Calls to proplyd go through a circuit breaker with per-call timeouts.
// When the cache is slow or down, reads and write-around writes go to the
// backing store directly; write-through writes fail with
// ErrCacheUnavailable.
package proplyd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/c12s/proplyd/pkg/api"
	"github.com/c12s/proplyd/pkg/protocol"
	"github.com/sony/gobreaker/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	_ "google.golang.org/grpc/health" // client-side health checking
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

// Config configures a Client.
type Config struct {
	// Target is the gRPC target of proplyd, for example
	// "dns:///proplyd:7070". Several proplyd replicas are balanced round
	// robin, and replicas that report NOT_SERVING are skipped.
	Target string
	// DialOptions are applied after the defaults (plaintext, keepalive,
	// health-checked round robin), so they can override them, for example
	// with TLS credentials.
	DialOptions []grpc.DialOption
	// ReadTimeout bounds each cache read. When it passes, the read falls
	// back to the backing store. Default 500ms.
	ReadTimeout time.Duration
	// WriteTimeout bounds each cache write and registration. Default 2s.
	WriteTimeout time.Duration
	Breaker      BreakerConfig
	Logger       *slog.Logger
}

// BreakerConfig configures the circuit breaker in front of the cache.
// Only failures of the cache itself count: timeouts, unreachable cache,
// Redis down. Backing store errors reported by the cache, invalid requests
// and the caller's own cancellations do not.
type BreakerConfig struct {
	// ConsecutiveFailures opens the breaker. Default 5.
	ConsecutiveFailures uint32
	// FailureRatio over Window also opens it, once MinRequests calls were
	// made in the window. Defaults 0.5, 20 and 10s.
	FailureRatio float64
	MinRequests  uint32
	Window       time.Duration
	// OpenTimeout is how long the breaker stays open before letting probe
	// calls through. Default 2s.
	OpenTimeout time.Duration
	// HalfOpenRequests is how many probes may run at once, and how many
	// must succeed to close the breaker. Default 3.
	HalfOpenRequests uint32
}

func (c *Config) setDefaults() error {
	if c.Target == "" {
		return errors.New("proplyd: Config.Target is required")
	}
	if c.ReadTimeout <= 0 {
		c.ReadTimeout = 500 * time.Millisecond
	}
	if c.WriteTimeout <= 0 {
		c.WriteTimeout = 2 * time.Second
	}
	b := &c.Breaker
	if b.ConsecutiveFailures == 0 {
		b.ConsecutiveFailures = 5
	}
	if b.FailureRatio <= 0 {
		b.FailureRatio = 0.5
	}
	if b.MinRequests == 0 {
		b.MinRequests = 20
	}
	if b.Window <= 0 {
		b.Window = 10 * time.Second
	}
	if b.OpenTimeout <= 0 {
		b.OpenTimeout = 2 * time.Second
	}
	if b.HalfOpenRequests == 0 {
		b.HalfOpenRequests = 3
	}
	if c.Logger == nil {
		c.Logger = slog.New(slog.DiscardHandler)
	}
	return nil
}

// BreakerState is the state of the circuit breaker.
type BreakerState = gobreaker.State

// Breaker states.
const (
	BreakerClosed   = gobreaker.StateClosed
	BreakerHalfOpen = gobreaker.StateHalfOpen
	BreakerOpen     = gobreaker.StateOpen
)

// Client is a connection to proplyd shared by any number of namespaces.
// It is safe for concurrent use.
type Client struct {
	cfg     Config
	conn    *grpc.ClientConn
	api     api.CacheServiceClient
	breaker *gobreaker.CircuitBreaker[struct{}]
	log     *slog.Logger

	mu         sync.Mutex
	namespaces map[string]*Namespace
	closed     bool
}

const serviceConfig = `{
  "loadBalancingPolicy": "round_robin",
  "healthCheckConfig": {"serviceName": "proplyd.v1.CacheService"}
}`

// New creates a client. It does not wait for proplyd to be reachable: the
// client works (through its fallbacks) while the cache is down.
func New(cfg Config) (*Client, error) {
	if err := cfg.setDefaults(); err != nil {
		return nil, err
	}
	opts := append([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(serviceConfig),
		// Detect dead connections on lossy networks.
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 10 * time.Second, Timeout: 5 * time.Second, PermitWithoutStream: true}),
	}, cfg.DialOptions...)
	conn, err := grpc.NewClient(cfg.Target, opts...)
	if err != nil {
		return nil, fmt.Errorf("proplyd: %w", err)
	}
	c := &Client{
		cfg:        cfg,
		conn:       conn,
		api:        api.NewCacheServiceClient(conn),
		log:        cfg.Logger,
		namespaces: map[string]*Namespace{},
	}
	b := cfg.Breaker
	c.breaker = gobreaker.NewCircuitBreaker[struct{}](gobreaker.Settings{
		Name:         "proplyd",
		MaxRequests:  b.HalfOpenRequests,
		Interval:     b.Window,
		BucketPeriod: b.Window / 10,
		Timeout:      b.OpenTimeout,
		ReadyToTrip: func(n gobreaker.Counts) bool {
			return n.ConsecutiveFailures >= b.ConsecutiveFailures ||
				(n.Requests >= b.MinRequests && float64(n.TotalFailures)/float64(n.Requests) >= b.FailureRatio)
		},
		IsSuccessful: func(err error) bool { return !isCacheFailure(err) },
		IsExcluded:   isCallerCancel,
		OnStateChange: func(_ string, from, to gobreaker.State) {
			c.log.Warn("proplyd circuit breaker", "from", from.String(), "to", to.String())
		},
	})
	conn.Connect()
	return c, nil
}

// State returns the circuit breaker state.
func (c *Client) State() BreakerState { return c.breaker.State() }

// Close closes every namespace and the connection.
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	nss := c.namespaces
	c.namespaces = nil
	c.mu.Unlock()
	for _, ns := range nss {
		ns.close()
	}
	return c.conn.Close()
}

// call runs fn through the circuit breaker with a per-call timeout.
func (c *Client) call(ctx context.Context, timeout time.Duration, fn func(context.Context) error) error {
	_, err := c.breaker.Execute(func() (struct{}, error) {
		cctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		err := fn(cctx)
		if err != nil && ctx.Err() != nil {
			// The caller gave up (cancel or its own deadline); not the
			// cache's fault.
			return struct{}{}, callerDone{ctx.Err()}
		}
		return struct{}{}, err
	})
	var cd callerDone
	if errors.As(err, &cd) {
		return cd.err
	}
	return err
}

// callerDone marks errors caused by the caller's context, which the breaker
// ignores.
type callerDone struct{ err error }

func (c callerDone) Error() string { return c.err.Error() }
func (c callerDone) Unwrap() error { return c.err }

// isCacheFailure reports whether err means the cache itself is unhealthy.
func isCacheFailure(err error) bool {
	if err == nil || isCallerCancel(err) {
		return false
	}
	st, ok := status.FromError(err)
	if !ok {
		return !errors.Is(err, context.Canceled)
	}
	switch st.Code() {
	case codes.Unavailable:
		return protocol.Reason(err) != protocol.ReasonBackendUnavailable
	case codes.ResourceExhausted:
		// Write-behind backpressure is the cache working as designed.
		return protocol.Reason(err) != protocol.ReasonBackpressure
	case codes.DeadlineExceeded, codes.Internal, codes.Unknown,
		codes.Aborted, codes.DataLoss, codes.Unimplemented:
		return true
	}
	return false
}

func isCallerCancel(err error) bool {
	var cd callerDone
	return errors.As(err, &cd) || errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled
}

// cacheUnavailable reports whether the cache could not serve a call: it
// failed, or the breaker refused to try.
func cacheUnavailable(err error) bool {
	return isCacheFailure(err) || errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests)
}

func backendUnavailable(err error) bool {
	return protocol.Reason(err) == protocol.ReasonBackendUnavailable
}
