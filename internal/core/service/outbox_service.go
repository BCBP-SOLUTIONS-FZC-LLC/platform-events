package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/port"
)

// panicErr wraps a recovered panic value so publishClaimedSequential can
// distinguish it from a real SNS error without fragile string-prefix matching.
type panicErr struct{ msg string }

func (e *panicErr) Error() string { return e.msg }

const (
	// bookkeepTimeoutPerRecord is the per-record budget for MarkPublished/MarkFailed
	// bookkeeping writes. The total bookkeepCtx timeout = batchSize × this value so
	// that even a full default batch of 50 records has 500 ms each regardless of
	// whether the primary context is cancelled (e.g. graceful shutdown).
	bookkeepTimeoutPerRecord = 500 * time.Millisecond

	// maxLastErrorLen caps the last_error column length stored in outbox_events to
	// prevent unbounded table bloat from verbose AWS SDK or network error messages.
	maxLastErrorLen = 512
)

// OutboxService handles the transactional outbox publish cycle.
type OutboxService struct {
	store              port.OutboxStore
	publisher          port.Publisher
	logger             port.Logger
	clock              port.Clock
	outboxMetrics      port.OutboxMetrics
	maxAttempts        int
	publishConcurrency int
	publishTimeout     time.Duration
}

// NewOutboxService creates an OutboxService with all required dependencies.
// If clock is nil, RealClock is used. publishConcurrency <= 0 defaults to 1
// (sequential). publishTimeout <= 0 disables per-record publish timeouts.
func NewOutboxService(
	store port.OutboxStore,
	publisher port.Publisher,
	logger port.Logger,
	clock port.Clock,
	maxAttempts int,
	publishConcurrency int,
	publishTimeout time.Duration,
) *OutboxService {
	if clock == nil {
		clock = port.RealClock{}
	}
	if publishConcurrency <= 0 {
		publishConcurrency = 1
	}
	return &OutboxService{
		store:              store,
		publisher:          publisher,
		logger:             logger,
		clock:              clock,
		maxAttempts:        maxAttempts,
		publishConcurrency: publishConcurrency,
		publishTimeout:     publishTimeout,
	}
}

// SetOutboxMetrics injects the metrics observer. Call once after construction
// from the adapter layer (e.g. runner.go) so the service layer stays clean.
// Safe to call with nil — metric recording is silently skipped.
func (s *OutboxService) SetOutboxMetrics(m port.OutboxMetrics) {
	s.outboxMetrics = m
}

// Enqueue inserts a serialized envelope into the outbox within the given transaction.
// The caller controls the transaction boundary — this function only does the INSERT.
func (s *OutboxService) Enqueue(ctx context.Context, tx pgx.Tx, env domain.Envelope[json.RawMessage]) error {
	if tx == nil {
		return fmt.Errorf("outbox: transaction must not be nil — use pgcommon.RunInTx to obtain a transaction")
	}
	if env.ID == "" {
		return domain.ErrEnvelopeIDRequired
	}
	if env.Type == "" {
		return domain.ErrEnvelopeTypeRequired
	}
	if env.Source == "" {
		return domain.ErrEnvelopeSourceRequired
	}
	if env.Timestamp.IsZero() {
		return fmt.Errorf("outbox: Enqueue requires a non-zero Timestamp — use events.NewEnvelope to construct envelopes")
	}
	if strings.ContainsRune(env.ID, '\x00') || strings.ContainsRune(env.Type, '\x00') || strings.ContainsRune(env.Source, '\x00') {
		return fmt.Errorf("outbox: envelope fields (ID, Type, Source) must not contain null bytes")
	}
	b, err := json.Marshal(env)
	if err != nil {
		return err
	}
	const maxEnvelopeBytes = 240 * 1024
	if len(b) > maxEnvelopeBytes {
		return fmt.Errorf("outbox: serialised envelope is %d bytes — exceeds safe SNS limit (%d bytes); reduce payload size", len(b), maxEnvelopeBytes)
	}
	record := domain.OutboxRecord{
		ID:          env.ID,
		EventType:   env.Type,
		Payload:     b,
		TenantID:    env.TenantID,
		TraceID:     env.TraceID,
		CreatedAt:   s.clock.Now(),
		ScheduledAt: s.clock.Now(),
	}
	return s.store.Enqueue(ctx, tx, record)
}

// PublishBatch claims up to batchSize unpublished records and publishes them
// with bounded concurrency (publishConcurrency). Per-record errors are logged
// and marked as failed; the batch continues. Returns ctx.Err() if the context
// is cancelled mid-batch so the runner can distinguish shutdown from DB errors.
func (s *OutboxService) PublishBatch(ctx context.Context, batchSize int) error {
	records, err := s.store.ClaimBatch(ctx, batchSize)
	if err != nil {
		return err
	}
	if len(records) == 0 {
		return nil
	}

	if s.publishConcurrency == 1 {
		return s.publishClaimedSequential(ctx, records)
	}

	// bookkeepCtx is used exclusively for MarkPublished and MarkFailed so these
	// critical bookkeeping writes succeed even when ctx is cancelled by graceful
	// shutdown. The timeout must cover the full worst-case publish duration PLUS
	// the per-record bookkeeping write time. Without the publish budget,
	// MarkPublished calls for later records expire while earlier goroutines are
	// still publishing, causing spurious duplicate delivery on the next poll.
	//
	// With publishConcurrency=C and publishTimeout=T per record:
	//   sequential rounds = ceil(len(records) / C)
	//   publish budget    = rounds × T
	//   bookkeep budget   = (len(records)+1) × bookkeepTimeoutPerRecord
	var publishBudget time.Duration
	if s.publishTimeout > 0 {
		rounds := (len(records) + s.publishConcurrency - 1) / s.publishConcurrency
		publishBudget = time.Duration(rounds) * s.publishTimeout
	} else {
		// No per-record timeout; publishing can take arbitrarily long.
		// Use a 5-minute floor so bookkeeping writes succeed even under extreme
		// SNS latency — callers who disable the timeout accept slower shutdowns.
		publishBudget = 5 * time.Minute
	}
	bookkeepTimeout := publishBudget + time.Duration(len(records)+1)*bookkeepTimeoutPerRecord
	bookkeepCtx, bookkeepCancel := context.WithTimeout(context.Background(), bookkeepTimeout)
	defer bookkeepCancel()

	sem := make(chan struct{}, s.publishConcurrency)
	var wg sync.WaitGroup

	for _, rec := range records {
		if ctx.Err() != nil {
			// Context already cancelled — release claim lease without publishing.
			// maxAttempts+1 ensures a routine shutdown never alone triggers
			// dead-lettering for a record that is at maxAttempts-1.
			s.releaseStranded(bookkeepCtx, rec, "batch interrupted by context cancellation")
			continue
		}

		// Interruptible semaphore acquire: a plain channel send would block until a
		// slot frees even if ctx is cancelled, delaying shutdown beyond DrainTimeout.
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			s.releaseStranded(bookkeepCtx, rec, "batch interrupted by context cancellation")
			continue
		}
		wg.Go(func() {
			defer func() { <-sem }()
			defer func() {
				if r := recover(); r != nil {
					if s.logger != nil {
						s.logger.Error("outbox: publish goroutine panic recovered", map[string]any{
							"id":         rec.ID,
							"event_type": rec.EventType,
							"panic":      fmt.Sprintf("%v", r),
							"stack":      string(debug.Stack()),
						})
					}
					// Release the claim lease so the next poll cycle can retry.
					// Use maxAttempts (not +1) — a panic is a programming error that
					// must count toward the retry budget to prevent infinite lease holds.
					if markErr := s.store.MarkFailed(bookkeepCtx, rec, fmt.Sprintf("panic: %v", r), s.maxAttempts); markErr != nil {
						if s.logger != nil {
							s.logger.Error("outbox: failed to mark record failed after panic", map[string]any{
								"id": rec.ID, "error": markErr.Error(),
							})
						}
					}
				}
			}()
			s.publishRecord(ctx, bookkeepCtx, rec)
		})
	}

	wg.Wait()
	return ctx.Err() // nil on success; context error if cancelled during publish
}

// publishClaimedSequential publishes a claimed batch via Publisher.PublishBatch
// (SNS batch API, up to 10 per chunk) when publishConcurrency is 1.
func (s *OutboxService) publishClaimedSequential(ctx context.Context, records []domain.OutboxRecord) error {
	bookkeepCtx, bookkeepCancel := s.newBookkeepCtx(len(records))
	defer bookkeepCancel()

	type item struct {
		rec domain.OutboxRecord
		env domain.Envelope[json.RawMessage]
	}
	items := make([]item, 0, len(records))

	for _, rec := range records {
		if ctx.Err() != nil {
			s.releaseStranded(bookkeepCtx, rec, "batch interrupted by context cancellation")
			continue
		}
		var env domain.Envelope[json.RawMessage]
		if err := json.Unmarshal(rec.Payload, &env); err != nil {
			s.handleUnmarshalError(bookkeepCtx, rec, err)
			continue
		}
		items = append(items, item{rec: rec, env: env})
	}
	if len(items) == 0 {
		return ctx.Err()
	}

	envs := make([]domain.Envelope[json.RawMessage], len(items))
	for i, it := range items {
		envs[i] = it.env
	}

	pubCtx := ctx
	var pubCancel context.CancelFunc
	if s.publishTimeout > 0 {
		pubCtx, pubCancel = context.WithTimeout(ctx, time.Duration(len(items))*s.publishTimeout)
		defer pubCancel()
	}

	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				if s.logger != nil {
					s.logger.Error("outbox: publish batch panic recovered", map[string]any{
						"panic": fmt.Sprintf("%v", r),
						"stack": string(debug.Stack()),
					})
				}
				for _, it := range items {
					s.markFailed(bookkeepCtx, it.rec, fmt.Sprintf("panic: %v", r), s.maxAttempts)
				}
				err = &panicErr{msg: fmt.Sprintf("panic: %v", r)}
			}
		}()
		err = s.publisher.PublishBatch(pubCtx, envs)
	}()
	if err == nil {
		for _, it := range items {
			s.markPublished(bookkeepCtx, it.rec, it.env)
		}
		return ctx.Err()
	}
	var pe *panicErr
	if errors.As(err, &pe) {
		return ctx.Err()
	}

	var batchErr *domain.BatchError
	if errors.As(err, &batchErr) {
		type failInfo struct {
			msg       string
			threshold int
		}
		failed := make(map[string]failInfo, len(batchErr.Failures))
		for _, f := range batchErr.Failures {
			// "TransportError" is the code used by publishChunk for transport-level
			// failures (ThrottlingException, ServiceUnavailable, etc.). Treat it as
			// retryable so it does not consume a retry slot prematurely.
			threshold := s.maxAttempts
			if f.Code == "TransportError" {
				threshold = s.maxAttempts + 1
			}
			failed[f.ID] = failInfo{msg: f.Message, threshold: threshold}
		}
		for _, it := range items {
			if fi, ok := failed[it.rec.ID]; ok {
				s.markFailed(bookkeepCtx, it.rec, fi.msg, fi.threshold)
			} else {
				s.markPublished(bookkeepCtx, it.rec, it.env)
			}
		}
		return ctx.Err()
	}

	threshold := s.failureThreshold(err)
	for _, it := range items {
		s.markFailed(bookkeepCtx, it.rec, truncateError(err.Error()), threshold)
	}
	return ctx.Err()
}

func (s *OutboxService) newBookkeepCtx(recordCount int) (context.Context, context.CancelFunc) {
	var publishBudget time.Duration
	if s.publishTimeout > 0 {
		rounds := (recordCount + s.publishConcurrency - 1) / s.publishConcurrency
		publishBudget = time.Duration(rounds) * s.publishTimeout
	} else {
		publishBudget = 5 * time.Minute
	}
	bookkeepTimeout := publishBudget + time.Duration(recordCount+1)*bookkeepTimeoutPerRecord
	return context.WithTimeout(context.Background(), bookkeepTimeout)
}

func (s *OutboxService) releaseStranded(bookkeepCtx context.Context, rec domain.OutboxRecord, reason string) {
	if markErr := s.store.MarkFailed(bookkeepCtx, rec, reason, s.maxAttempts+1); markErr != nil && s.logger != nil {
		s.logger.Error("outbox: failed to release stranded record lease on shutdown", map[string]any{
			"id": rec.ID, "error": markErr.Error(),
		})
	}
}

func (s *OutboxService) handleUnmarshalError(bookkeepCtx context.Context, rec domain.OutboxRecord, err error) {
	if s.outboxMetrics != nil {
		s.outboxMetrics.RecordUnmarshalError()
	}
	if s.logger != nil {
		s.logger.Error("outbox: failed to unmarshal payload", map[string]any{
			"id": rec.ID, "event_type": rec.EventType, "error": err.Error(),
		})
	}
	if markErr := s.store.MarkFailed(bookkeepCtx, rec, truncateError(err.Error()), s.maxAttempts); markErr != nil && s.logger != nil {
		s.logger.Error("outbox: failed to mark record failed", map[string]any{
			"id": rec.ID, "error": markErr.Error(),
		})
	}
}

func (s *OutboxService) markPublished(bookkeepCtx context.Context, rec domain.OutboxRecord, env domain.Envelope[json.RawMessage]) {
	if err := s.store.MarkPublished(bookkeepCtx, rec.ID); err != nil {
		if s.outboxMetrics != nil {
			s.outboxMetrics.RecordMarkPublishedError()
		}
		if s.logger != nil {
			s.logger.Error("outbox: failed to mark published — event delivered to SNS but may be re-delivered on next poll", map[string]any{
				"id": rec.ID, "event_type": env.Type, "error": err.Error(),
			})
		}
	}
}

func (s *OutboxService) markFailed(bookkeepCtx context.Context, rec domain.OutboxRecord, msg string, threshold int) {
	if markErr := s.store.MarkFailed(bookkeepCtx, rec, msg, threshold); markErr != nil && s.logger != nil {
		s.logger.Error("outbox: failed to mark record failed", map[string]any{
			"id": rec.ID, "error": markErr.Error(),
		})
	}
}

func (s *OutboxService) failureThreshold(err error) int {
	threshold := s.maxAttempts
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, domain.ErrRetryable) {
		threshold = s.maxAttempts + 1
	}
	return threshold
}

// publishRecord handles a single outbox record: unmarshal → publish → mark.
// Called from a bounded goroutine pool inside PublishBatch.
func (s *OutboxService) publishRecord(ctx, bookkeepCtx context.Context, rec domain.OutboxRecord) {
	// Goroutine may have been started just before context cancellation.
	if ctx.Err() != nil {
		s.releaseStranded(bookkeepCtx, rec, "batch interrupted by context cancellation")
		return
	}

	var env domain.Envelope[json.RawMessage]
	if err := json.Unmarshal(rec.Payload, &env); err != nil {
		s.handleUnmarshalError(bookkeepCtx, rec, err)
		return
	}

	// Apply per-record publish timeout so a hung SNS client cannot block the
	// entire batch. The context is still checked via the parent ctx on cancellation.
	pubCtx := ctx
	var pubCancel context.CancelFunc
	if s.publishTimeout > 0 {
		pubCtx, pubCancel = context.WithTimeout(ctx, s.publishTimeout)
		defer pubCancel()
	}

	if err := s.publisher.Publish(pubCtx, env); err != nil {
		if s.logger != nil {
			s.logger.Warn("outbox: publish failed", map[string]any{
				"id": rec.ID, "error": err.Error(),
			})
		}
		// Use maxAttempts+1 when the failure is caused by context cancellation
		// (graceful shutdown) so a record at its last permitted attempt is not
		// prematurely dead-lettered — matching the pre-publish cancellation guard.
		s.markFailed(bookkeepCtx, rec, truncateError(err.Error()), s.failureThreshold(err))
		return
	}

	s.markPublished(bookkeepCtx, rec, env)
}

// PendingCount returns the number of records in outbox_events that have not
// yet been published. Callers use this to update the outbox_pending_total gauge.
func (s *OutboxService) PendingCount(ctx context.Context) (int64, error) {
	return s.store.PendingCount(ctx)
}

// LeasedCount returns the number of records currently claimed (leased) by a runner.
func (s *OutboxService) LeasedCount(ctx context.Context) (int64, error) {
	return s.store.LeasedCount(ctx)
}

// ReprocessDeadLetters moves up to limit records from outbox_dead_letters back
// to outbox_events for redelivery. Returns the number of records re-queued.
func (s *OutboxService) ReprocessDeadLetters(ctx context.Context, limit int) (int, error) {
	return s.store.ReprocessDeadLetters(ctx, limit)
}

// ListDeadLetters returns up to limit dead-letter records matching filter,
// ordered by failed_at ascending (oldest failures first).
func (s *OutboxService) ListDeadLetters(ctx context.Context, filter domain.DLQFilter, limit int) ([]domain.DeadLetterRecord, error) {
	return s.store.ListDeadLetters(ctx, filter, limit)
}

// ReprocessDeadLettersWith moves up to limit filtered records from
// outbox_dead_letters back to outbox_events, resetting their attempt counters.
func (s *OutboxService) ReprocessDeadLettersWith(ctx context.Context, filter domain.DLQFilter, limit int) (int, error) {
	return s.store.ReprocessDeadLettersWith(ctx, filter, limit)
}

// DiscardDeadLetters permanently deletes up to limit filtered records from
// outbox_dead_letters. Returns the number of rows deleted.
func (s *OutboxService) DiscardDeadLetters(ctx context.Context, filter domain.DLQFilter, limit int) (int64, error) {
	return s.store.DiscardDeadLetters(ctx, filter, limit)
}

// PrunePublished deletes published records older than olderThan from outbox_events.
func (s *OutboxService) PrunePublished(ctx context.Context, olderThan time.Duration, limit int) (int64, error) {
	return s.store.PrunePublished(ctx, olderThan, limit)
}

// truncateError caps an error string to maxLastErrorLen runes to prevent
// unbounded growth of the last_error column in outbox_events.
// Uses rune-safe slicing to avoid writing invalid UTF-8 to PostgreSQL.
// The truncation marker ("…[truncated]", 12 runes) is included in the cap so
// the total output never exceeds maxLastErrorLen runes.
func truncateError(msg string) string {
	const marker = "…[truncated]"
	const markerLen = 12 // rune count of marker
	if utf8.RuneCountInString(msg) <= maxLastErrorLen {
		return msg
	}
	return string([]rune(msg)[:maxLastErrorLen-markerLen]) + marker
}
