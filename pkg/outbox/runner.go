package outbox

import (
	"context"
	"fmt"
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
	defaultPollInterval = 5 * time.Second
	defaultBatchSize    = 50
	defaultMaxAttempts  = 5
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

	var store port.OutboxStore
	if cfg.Store != nil {
		store = cfg.Store
	} else {
		store = outboxstore.New(cfg.Pool, cfg.Logger, cfg.ClaimLeaseDuration)
	}

	// Wrap the public Publisher into the internal port.Publisher interface.
	inner := &publisherBridge{pub: cfg.Publisher}

	svc := service.NewOutboxService(store, inner, cfg.Logger, nil, cfg.MaxAttempts)

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

	ticker := time.NewTicker(r.cfg.PollInterval)
	defer ticker.Stop()

	// Run one poll immediately on startup so events that arrived while the runner
	// was stopped are not delayed by a full PollInterval.
	r.pollOnce(ctx)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-stopCtx.Done():
			return nil
		case <-ticker.C:
			r.pollOnce(ctx)
		}
	}
}

// pollOnce runs a single gauge-update + publish-batch cycle.
func (r *Runner) pollOnce(ctx context.Context) {
	// Update the pending gauge before publishing so it reflects records
	// waiting at the start of this cycle. Use a short bounded context so
	// a slow COUNT(*) query cannot stall the entire poll loop.
	if metrics.OutboxPendingTotal != nil {
		gcCtx, gcCancel := context.WithTimeout(ctx, 2*time.Second)
		if n, err := r.svc.PendingCount(gcCtx); err == nil {
			metrics.OutboxPendingTotal.WithLabelValues().Set(float64(n))
		}
		gcCancel()
	}
	if err := r.svc.PublishBatch(ctx, r.cfg.BatchSize); err != nil {
		if r.cfg.Logger != nil {
			r.cfg.Logger.Error("outbox: poll cycle failed", map[string]any{
				"error": err.Error(),
			})
		}
	}
}

// Stop signals the runner to stop and waits for the current poll cycle to finish.
// Safe to call multiple times and safe to call before Start.
func (r *Runner) Stop() error {
	r.mu.Lock()
	r.cancel()
	doneCh := r.doneCh // capture the current cycle's channel under lock
	r.mu.Unlock()

	<-doneCh // pre-closed before first Start(), fresh channel during/after Start()
	return nil
}
