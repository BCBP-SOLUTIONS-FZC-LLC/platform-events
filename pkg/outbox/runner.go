package outbox

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
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
	defaultPollInterval   = 5 * time.Second
	defaultBatchSize      = 50
	defaultMaxAttempts    = 5
	defaultDrainTimeout   = 30 * time.Second
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
	// Set a negative value (e.g. -1) to disable the per-record timeout.
	PublishTimeout time.Duration

	// StartupJitter adds a random delay in [0, StartupJitter) before the first
	// poll. Use when running multiple runner instances to desynchronise their
	// initial polls and avoid a thundering-herd burst on the DB.
	StartupJitter time.Duration
}

// Runner polls the outbox_events table and publishes pending records.
// A Runner is fully restartable: Start() may be called again after Stop().
type Runner struct {
	svc     *service.OutboxService
	cfg     Config
	mu      sync.Mutex
	cancel  context.CancelFunc // signals the active Start() goroutine to stop
	doneCh  chan struct{}      // closed when the active Start() goroutine has exited
	started atomic.Bool        // guards against concurrent Start() calls

	// readyCh is closed after the first successful poll cycle (or empty poll),
	// signalling that the DB connection and schema are healthy. Expose via Ready().
	readyCh   chan struct{}
	readyOnce sync.Once
}

// outboxMetricsAdapter routes OutboxService metric callbacks to the adapter
// layer without the service layer needing to import the adapter.
type outboxMetricsAdapter struct{}

func (outboxMetricsAdapter) RecordUnmarshalError()     { metrics.RecordOutboxUnmarshalError() }
func (outboxMetricsAdapter) RecordMarkPublishedError() { metrics.RecordOutboxMarkPublishedError() }

// NewRunner constructs a Runner from the provided Config.
// Returns an error if ClaimLeaseDuration is too short to prevent duplicate
// delivery (must be ≥ BatchSize×PublishTimeout+1m when PublishTimeout > 0).
// Panics if Publisher is nil, or if both Store and Pool are nil — those are
// programming errors, not operator configuration mistakes.
func NewRunner(cfg Config) (*Runner, error) {
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
	if cfg.PublishTimeout == 0 {
		cfg.PublishTimeout = defaultPublishTimeout
	}
	// PublishConcurrency defaults to 1 (sequential); service constructor handles <= 0.

	// Validate that ClaimLeaseDuration covers the worst-case publish budget so a
	// second runner cannot re-claim the same record before the first finishes,
	// which would cause silent duplicate delivery. The effective lease when zero
	// is the outboxstore default of 10 minutes.
	effectiveLease := cfg.ClaimLeaseDuration
	if effectiveLease <= 0 {
		effectiveLease = 10 * time.Minute
	}
	const minLeaseFallback = 30 * time.Second
	if cfg.PublishTimeout > 0 {
		minLease := time.Duration(cfg.BatchSize)*cfg.PublishTimeout + time.Minute
		if effectiveLease < minLease {
			return nil, fmt.Errorf(
				"outbox: ClaimLeaseDuration (%s) is too short — must be at least BatchSize×PublishTimeout+1m (%d×%s+1m = %s) to prevent duplicate delivery",
				effectiveLease, cfg.BatchSize, cfg.PublishTimeout, minLease,
			)
		}
	} else if effectiveLease < minLeaseFallback {
		// PublishTimeout is disabled, so we cannot compute a precise minimum lease.
		// Enforce a 30s floor to prevent near-instant lease expiry that would cause
		// concurrent runners to re-claim in-flight records and produce duplicates.
		return nil, fmt.Errorf(
			"outbox: ClaimLeaseDuration (%s) is too short — minimum is %s when PublishTimeout is disabled",
			effectiveLease, minLeaseFallback,
		)
	}

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
	svc.SetOutboxMetrics(outboxMetricsAdapter{})

	// Pre-closed initial doneCh so Stop() before Start() returns immediately.
	initialDone := make(chan struct{})
	close(initialDone)

	return &Runner{
		svc:     svc,
		cfg:     cfg,
		cancel:  func() {}, // noop before first Start()
		doneCh:  initialDone,
		readyCh: make(chan struct{}),
	}, nil
}

// Start runs the poll loop until ctx is cancelled or Stop is called. Blocks.
// Returns an error if Start has already been called and is still running.
// The runner is fully restartable: Start() may be called again after Stop().
func (r *Runner) Start(ctx context.Context) error {
	// Hold the mutex across the CAS and the r.cancel/r.doneCh assignment so that
	// Stop() cannot read the stale pre-closed doneCh in the window between the CAS
	// and the mutex write. Without this guard, Stop() could return immediately
	// while Start() is still initialising (TOCTOU race).
	r.mu.Lock()
	if !r.started.CompareAndSwap(false, true) {
		r.mu.Unlock()
		return fmt.Errorf("outbox: runner is already running")
	}
	stopCtx, stopCancel := context.WithCancel(context.Background())
	thisDone := make(chan struct{})
	r.cancel = stopCancel
	r.doneCh = thisDone
	r.readyCh = make(chan struct{})
	r.readyOnce = sync.Once{}
	r.mu.Unlock()
	defer stopCancel() // always release stopCtx resources when Start() exits

	// close(thisDone) BEFORE Store(false) so a racing Start() cannot succeed its
	// CAS and install a new doneCh before the old one is closed.
	defer func() {
		close(thisDone)
		r.started.Store(false)
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
				ticker.Reset(r.cfg.PollInterval)
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
		*backoff = initPollBackoff // reset so a future Start() cycle begins at base backoff
		return false
	case <-stopCtx.Done():
		*backoff = initPollBackoff
		return false
	}
}

// pollOnce runs a single gauge-update + publish-batch cycle.
// Returns true when the cycle encountered an infrastructure error (triggers
// backoff in Start); returns false on success or context cancellation.
func (r *Runner) pollOnce(ctx context.Context) (hadError bool) {
	if metrics.HasOutboxPendingMetric() {
		gcCtx, gcCancel := context.WithTimeout(ctx, 2*time.Second)
		n, pendErr := r.svc.PendingCount(gcCtx)
		gcCancel()
		if pendErr != nil {
			// Set to -1 so dashboards can distinguish "zero pending" from
			// "reading unavailable" — a stale zero would mask a lagging outbox.
			metrics.SetOutboxPending(-1)
			if r.cfg.Logger != nil {
				r.cfg.Logger.Warn("outbox: failed to query pending count", map[string]any{
					"error": pendErr.Error(),
				})
			}
		} else {
			metrics.SetOutboxPending(float64(n))
		}
	}
	if metrics.HasOutboxLeasedMetric() {
		lcCtx, lcCancel := context.WithTimeout(ctx, 2*time.Second)
		n, leasedErr := r.svc.LeasedCount(lcCtx)
		lcCancel()
		if leasedErr != nil {
			if r.cfg.Logger != nil {
				r.cfg.Logger.Warn("outbox: failed to query leased count", map[string]any{
					"error": leasedErr.Error(),
				})
			}
		} else {
			metrics.SetOutboxLeased(float64(n))
		}
	}
	if err := r.svc.PublishBatch(ctx, r.cfg.BatchSize); err != nil {
		// Context cancellation is not an infrastructure error — no backoff.
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return false
		}
		metrics.RecordOutboxPollError()
		logFields := map[string]any{"error": err.Error()}
		// Surface a clear diagnostic when the outbox schema has not been applied.
		if strings.Contains(err.Error(), `relation "outbox_events" does not exist`) {
			logFields["hint"] = "call outbox.ApplySchema before starting the runner"
		}
		if r.cfg.Logger != nil {
			r.cfg.Logger.Error("outbox: poll cycle failed", logFields)
		}
		return true
	}
	// First successful (or empty) poll: signal readiness for health probes.
	r.readyOnce.Do(func() { close(r.readyCh) })
	return false
}

// Ready returns a channel that is closed after the first successful poll cycle.
// A successful poll means the DB connection is healthy and the outbox schema exists.
// Use this to gate Kubernetes readiness probes:
//
//	select {
//	case <-runner.Ready():
//	    // signal /readyz OK
//	case <-time.After(30 * time.Second):
//	    // signal /readyz not ready
//	}
func (r *Runner) Ready() <-chan struct{} {
	r.mu.Lock()
	ch := r.readyCh
	r.mu.Unlock()
	return ch
}

// ReprocessDeadLetters moves up to limit records from outbox_dead_letters back
// to outbox_events for redelivery, resetting their attempt counters.
// Applies a 30 s internal DB timeout so a saturated database does not block indefinitely.
// Returns the number of records re-queued and any database error.
func (r *Runner) ReprocessDeadLetters(ctx context.Context, limit int) (int, error) {
	return r.svc.ReprocessDeadLetters(ctx, limit)
}

// ListDeadLetters returns up to limit records from outbox_dead_letters that
// match filter, ordered by failed_at ascending (oldest failures first).
// Use this to inspect what is in the DLQ before deciding whether to reprocess
// or discard. Returns an empty slice (not an error) when no records match.
// Applies a 30 s internal DB timeout.
//
// Example — inspect failures for a single tenant:
//
//	records, err := runner.ListDeadLetters(ctx, outbox.DLQFilter{TenantID: "acme"}, 50)
//	for _, r := range records {
//	    log.Printf("id=%s type=%s attempts=%d error=%s", r.ID, r.EventType, r.Attempts, r.LastError)
//	}
func (r *Runner) ListDeadLetters(ctx context.Context, filter DLQFilter, limit int) ([]DeadLetterRecord, error) {
	return r.svc.ListDeadLetters(ctx, filter, limit)
}

// ReprocessDeadLettersWith moves up to limit records that match filter from
// outbox_dead_letters back to outbox_events, resetting their attempt counters
// to zero so they are retried from scratch on the next poll cycle.
// Returns the number of records re-queued and any database error.
// Applies a 30 s internal DB timeout.
//
// Call this after fixing the root cause (SNS permission, invalid payload, etc.)
// to resume delivery for a targeted subset of failures rather than all records.
//
// Example — replay only billing events for tenant "acme" that failed before a
// known incident window:
//
//	n, err := runner.ReprocessDeadLettersWith(ctx, outbox.DLQFilter{
//	    EventType:    "billing.invoice.settled",
//	    TenantID:     "acme",
//	    FailedBefore: incidentEnd,
//	}, 200)
func (r *Runner) ReprocessDeadLettersWith(ctx context.Context, filter DLQFilter, limit int) (int, error) {
	return r.svc.ReprocessDeadLettersWith(ctx, filter, limit)
}

// DiscardDeadLetters permanently deletes up to limit records that match filter
// from outbox_dead_letters. Use for poison-pill records that can never be
// delivered (malformed payload, decommissioned event type, etc.).
// Returns the number of rows deleted and any database error.
// Applies a 30 s internal DB timeout.
//
// ⚠️ Discarded records are unrecoverable. Always call [Runner.ListDeadLetters]
// first to confirm the selection before discarding.
//
// Example — purge an obsolete event type:
//
//	n, err := runner.DiscardDeadLetters(ctx, outbox.DLQFilter{
//	    EventType: "legacy.sync.requested",
//	}, 1000)
func (r *Runner) DiscardDeadLetters(ctx context.Context, filter DLQFilter, limit int) (int64, error) {
	return r.svc.DiscardDeadLetters(ctx, filter, limit)
}

// PrunePublished deletes published records from outbox_events that are older than
// olderThan to prevent unbounded table growth. Batches the delete to at most limit
// rows per call to keep lock hold time bounded.
//
// Call periodically from a scheduled job or maintenance endpoint:
//
//	n, err := runner.PrunePublished(ctx, 7*24*time.Hour, 1000)
//
// Choose olderThan to exceed the longest idempotency deduplication window of any
// downstream consumer. A safe minimum for most workloads is 7 days.
// Returns the number of rows deleted and any database error.
func (r *Runner) PrunePublished(ctx context.Context, olderThan time.Duration, limit int) (int64, error) {
	return r.svc.PrunePublished(ctx, olderThan, limit)
}

// Stop signals the runner to stop and waits up to DrainTimeout for the current
// poll cycle to finish. Returns an error if the drain timeout is exceeded.
// Safe to call multiple times and safe to call before Start.
func (r *Runner) Stop() error {
	r.mu.Lock()
	r.cancel()
	doneCh := r.doneCh
	r.mu.Unlock()

	// Use NewTimer so the timer goroutine is stopped immediately when doneCh fires,
	// preventing a 30s goroutine leak on every normal (fast) Stop() call.
	timer := time.NewTimer(r.cfg.DrainTimeout)
	defer timer.Stop()
	select {
	case <-doneCh:
		return nil
	case <-timer.C:
		return fmt.Errorf("outbox: runner did not stop within drain timeout (%s)", r.cfg.DrainTimeout)
	}
}
