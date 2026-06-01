package outbox

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/adapter/outbound/metrics"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/adapter/outbound/outboxstore"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/events"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/pgcommon"
)

const (
	defaultPollInterval  = 5 * time.Second
	defaultBatchSize     = 50
	defaultMaxAttempts   = 5
	defaultDrainTimeout  = 30 * time.Second
	defaultPublishTimeout = 10 * time.Second

	// Poll backoff: doubles on each consecutive failure, capped at maxPollBackoff.
	initPollBackoff = 1 * time.Second
	maxPollBackoff  = 30 * time.Second
)

// Config holds the parameters for constructing a Runner.
type Config struct {
	// Pool is the pgcommon connection pool. Required when Store is nil.
	Pool *pgcommon.Pool

	// Store is an optional OutboxStore implementation. When set, Pool is ignored.
	// Primarily used in unit tests to inject a mock store without a real database.
	Store port.OutboxStore

	// Publisher is the SNS (or other) publisher. Required.
	Publisher events.Publisher

	// Logger is optional. The runner logs per-record results when set.
	Logger port.Logger

	// PollInterval is how often the runner polls for unpublished records.
	// Defaults to 5s.
	PollInterval time.Duration

	// BatchSize is the maximum number of records to publish per poll cycle.
	// Defaults to 50.
	BatchSize int

	// MaxAttempts is the number of publish failures before a record is moved
	// to the dead-letter table. Defaults to 5.
	MaxAttempts int

	// ClaimLeaseDuration overrides how long a claimed record is hidden from other
	// runners before it becomes eligible for re-claim. Defaults to 10 minutes.
	// Should exceed BatchSize × worst-case per-record publish time.
	ClaimLeaseDuration time.Duration

	// DrainTimeout is the maximum time Stop() waits for the current poll cycle
	// to complete before returning an error. Defaults to 30s.
	DrainTimeout time.Duration

	// PublishConcurrency is the number of records published concurrently within
	// a single poll cycle. Defaults to 1 (sequential). Increase for high-throughput
	// workloads where SNS latency is the bottleneck.
	PublishConcurrency int

	// PublishTimeout is the per-record timeout for calls to Publisher.Publish.
	// Defaults to 10s. Prevents a hung SNS client from stalling the entire batch.
	// 0 disables the per-record timeout.
	PublishTimeout time.Duration

	// StartupJitter adds a random delay in [0, StartupJitter) before the first
	// poll. Use when running multiple runner instances to desynchronise their
	// initial polls and avoid a thundering-herd burst on the DB.
	StartupJitter time.Duration
}

// Runner polls the outbox_events table and publishes pending records.
// A Runner is fully restartable: Start() may be called again after Stop().
type Runner struct {
	svc    *service.OutboxService
	cfg    Config
	mu     sync.Mutex
	cancel context.CancelFunc // signals the active Start() goroutine to stop
	doneCh chan struct{}       // closed when the active Start() goroutine has exited
	started atomic.Bool       // guards against concurrent Start() calls
}

// NewRunner constructs a Runner from the provided Config.
// Panics if Publisher is nil, or if both Store and Pool are nil.
// Applies defaults for zero-value fields.
func NewRunner(cfg Config) *Runner {
	if cfg.Publisher == nil {
		panic("outbox: Config.Publisher is required but was nil")
	}
	if cfg.Store == nil && cfg.Pool == nil {
		panic("outbox: one of Config.Store or Config.Pool is required")
	}

	if cfg.PollInterval <= 0 {
		cfg.PollInterval = defaultPollInterval
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = defaultBatchSize
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = defaultMaxAttempts
	}
	if cfg.DrainTimeout <= 0 {
		cfg.DrainTimeout = defaultDrainTimeout
	}
	if cfg.PublishTimeout <= 0 {
		cfg.PublishTimeout = defaultPublishTimeout
	}
	// PublishConcurrency defaults to 1 (sequential); service constructor handles <= 0.

	var store port.OutboxStore
	if cfg.Store != nil {
		store = cfg.Store
	} else {
		store = outboxstore.New(cfg.Pool, cfg.Logger, cfg.ClaimLeaseDuration)
	}

	inner := &publisherBridge{pub: cfg.Publisher}

	svc := service.NewOutboxService(
		store, inner, cfg.Logger, nil,
		cfg.MaxAttempts,
		cfg.PublishConcurrency,
		cfg.PublishTimeout,
	)

	// Pre-closed initial doneCh so Stop() before Start() returns immediately.
	initialDone := make(chan struct{})
	close(initialDone)

	return &Runner{
		svc:    svc,
		cfg:    cfg,
		cancel: func() {}, // noop before first Start()
		doneCh: initialDone,
	}
}

// Start runs the poll loop until ctx is cancelled or Stop is called. Blocks.
// Returns an error if Start has already been called and is still running.
// The runner is fully restartable: Start() may be called again after Stop().
func (r *Runner) Start(ctx context.Context) error {
	if !r.started.CompareAndSwap(false, true) {
		return fmt.Errorf("outbox: runner is already running")
	}

	// Create a fresh stop context and doneCh for this cycle under the mutex so
	// Stop() always captures the channel belonging to the current cycle.
	stopCtx, stopCancel := context.WithCancel(context.Background())
	thisDone := make(chan struct{})
	r.mu.Lock()
	r.cancel = stopCancel
	r.doneCh = thisDone
	r.mu.Unlock()

	defer func() {
		r.started.Store(false)
		close(thisDone)
	}()

	// Optional startup jitter: desynchronises concurrent runner instances so
	// they do not all hammer the DB at the same instant on a rolling restart.
	if r.cfg.StartupJitter > 0 {
		jitter := time.Duration(rand.Int64N(int64(r.cfg.StartupJitter)))
		select {
		case <-time.After(jitter):
		case <-ctx.Done():
			return nil
		case <-stopCtx.Done():
			return nil
		}
	}

	ticker := time.NewTicker(r.cfg.PollInterval)
	defer ticker.Stop()

	pollBackoff := initPollBackoff

	// Run one poll immediately on startup so events that arrived while the runner
	// was stopped are not delayed by a full PollInterval.
	if r.pollOnce(ctx) {
		if !r.sleepBackoff(ctx, stopCtx, &pollBackoff) {
			return nil
		}
	} else {
		pollBackoff = initPollBackoff
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-stopCtx.Done():
			return nil
		case <-ticker.C:
			if r.pollOnce(ctx) {
				if !r.sleepBackoff(ctx, stopCtx, &pollBackoff) {
					return nil
				}
			} else {
				pollBackoff = initPollBackoff
			}
		}
	}
}

// sleepBackoff sleeps for the current backoff duration (interruptible by context
// cancellation or Stop), then doubles the backoff up to maxPollBackoff.
// Returns false if the caller should exit Start immediately.
func (r *Runner) sleepBackoff(ctx, stopCtx context.Context, backoff *time.Duration) bool {
	timer := time.NewTimer(*backoff)
	defer timer.Stop()
	select {
	case <-timer.C:
		*backoff = min(*backoff*2, maxPollBackoff)
		return true
	case <-ctx.Done():
		return false
	case <-stopCtx.Done():
		return false
	}
}

// pollOnce runs a single gauge-update + publish-batch cycle.
// Returns true when the cycle encountered an infrastructure error (triggers
// backoff in Start); returns false on success or context cancellation.
func (r *Runner) pollOnce(ctx context.Context) (hadError bool) {
	if metrics.OutboxPendingTotal != nil {
		gcCtx, gcCancel := context.WithTimeout(ctx, 2*time.Second)
		if n, err := r.svc.PendingCount(gcCtx); err == nil {
			metrics.OutboxPendingTotal.WithLabelValues().Set(float64(n))
		}
		gcCancel()
	}
	if err := r.svc.PublishBatch(ctx, r.cfg.BatchSize); err != nil {
		// Context cancellation is not an infrastructure error — no backoff.
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return false
		}
		if r.cfg.Logger != nil {
			r.cfg.Logger.Error("outbox: poll cycle failed", map[string]any{
				"error": err.Error(),
			})
		}
		return true
	}
	return false
}

// Stop signals the runner to stop and waits up to DrainTimeout for the current
// poll cycle to finish. Returns an error if the drain timeout is exceeded.
// Safe to call multiple times and safe to call before Start.
func (r *Runner) Stop() error {
	r.mu.Lock()
	r.cancel()
	doneCh := r.doneCh
	r.mu.Unlock()

	select {
	case <-doneCh:
		return nil
	case <-time.After(r.cfg.DrainTimeout):
		return fmt.Errorf("outbox: runner did not stop within drain timeout (%s)", r.cfg.DrainTimeout)
	}
}
