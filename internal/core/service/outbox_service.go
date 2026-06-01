package service

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/port"
)

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

// Enqueue inserts a serialized envelope into the outbox within the given transaction.
// The caller controls the transaction boundary — this function only does the INSERT.
func (s *OutboxService) Enqueue(ctx context.Context, tx pgx.Tx, env domain.Envelope[json.RawMessage]) error {
	if tx == nil {
		return fmt.Errorf("outbox: transaction must not be nil — use pgcommon.RunInTx to obtain a transaction")
	}
	if env.TenantID == "" && s.logger != nil {
		s.logger.Warn("outbox: enqueueing event with empty TenantID — consumer RLS will not be scoped to a tenant", map[string]any{
			"event_id":   env.ID,
			"event_type": env.Type,
		})
	}
	b, err := json.Marshal(env)
	if err != nil {
		return err
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

	// bookkeepCtx is used exclusively for MarkPublished and MarkFailed so these
	// critical bookkeeping writes succeed even when ctx is cancelled by graceful
	// shutdown. The timeout scales with batchSize so every record gets
	// bookkeepTimeoutPerRecord regardless of batch depth.
	bookkeepTimeout := time.Duration(len(records)+1) * bookkeepTimeoutPerRecord
	bookkeepCtx, bookkeepCancel := context.WithTimeout(context.Background(), bookkeepTimeout)
	defer bookkeepCancel()

	sem := make(chan struct{}, s.publishConcurrency)
	var wg sync.WaitGroup

	for _, rec := range records {
		rec := rec

		if ctx.Err() != nil {
			// Context already cancelled — release claim lease without publishing.
			// maxAttempts+1 ensures a routine shutdown never alone triggers
			// dead-lettering for a record that is at maxAttempts-1.
			if markErr := s.store.MarkFailed(bookkeepCtx, rec.ID, "batch interrupted by context cancellation", s.maxAttempts+1); markErr != nil {
				if s.logger != nil {
					s.logger.Error("outbox: failed to release stranded record lease on shutdown", map[string]any{
						"id":    rec.ID,
						"error": markErr.Error(),
					})
				}
			}
			continue
		}

		sem <- struct{}{} // acquire concurrency slot; goroutines release it quickly on cancellation
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			s.publishRecord(ctx, bookkeepCtx, rec)
		}()
	}

	wg.Wait()
	return ctx.Err() // nil on success; context error if cancelled during publish
}

// publishRecord handles a single outbox record: unmarshal → publish → mark.
// Called from a bounded goroutine pool inside PublishBatch.
func (s *OutboxService) publishRecord(ctx, bookkeepCtx context.Context, rec domain.OutboxRecord) {
	// Goroutine may have been started just before context cancellation.
	if ctx.Err() != nil {
		if markErr := s.store.MarkFailed(bookkeepCtx, rec.ID, "batch interrupted by context cancellation", s.maxAttempts+1); markErr != nil {
			if s.logger != nil {
				s.logger.Error("outbox: failed to release stranded record lease on shutdown", map[string]any{
					"id": rec.ID, "error": markErr.Error(),
				})
			}
		}
		return
	}

	var env domain.Envelope[json.RawMessage]
	if err := json.Unmarshal(rec.Payload, &env); err != nil {
		if s.logger != nil {
			s.logger.Error("outbox: failed to unmarshal payload", map[string]any{
				"id": rec.ID, "error": err.Error(),
			})
		}
		if markErr := s.store.MarkFailed(bookkeepCtx, rec.ID, truncateError(err.Error()), s.maxAttempts); markErr != nil {
			if s.logger != nil {
				s.logger.Error("outbox: failed to mark record failed", map[string]any{
					"id": rec.ID, "error": markErr.Error(),
				})
			}
		}
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
		if markErr := s.store.MarkFailed(bookkeepCtx, rec.ID, truncateError(err.Error()), s.maxAttempts); markErr != nil {
			if s.logger != nil {
				s.logger.Error("outbox: failed to mark record failed", map[string]any{
					"id": rec.ID, "error": markErr.Error(),
				})
			}
		}
		return
	}

	if err := s.store.MarkPublished(bookkeepCtx, rec.ID); err != nil {
		if s.logger != nil {
			s.logger.Error("outbox: failed to mark published", map[string]any{
				"id": rec.ID, "error": err.Error(),
			})
		}
	}
}

// PendingCount returns the number of records in outbox_events that have not
// yet been published. Callers use this to update the outbox_pending_total gauge.
func (s *OutboxService) PendingCount(ctx context.Context) (int64, error) {
	return s.store.PendingCount(ctx)
}

// truncateError caps an error string to maxLastErrorLen characters to prevent
// unbounded growth of the last_error column in outbox_events.
func truncateError(msg string) string {
	if len(msg) <= maxLastErrorLen {
		return msg
	}
	return msg[:maxLastErrorLen] + "…[truncated]"
}
