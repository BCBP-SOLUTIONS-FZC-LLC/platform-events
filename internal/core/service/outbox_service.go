package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/port"
	"github.com/jackc/pgx/v5"
)

// bookkeepTimeoutPerRecord is the per-record budget for MarkPublished/MarkFailed
// bookkeeping writes. The total bookkeepCtx timeout = batchSize × this value so
// that even a full default batch of 50 records has 500 ms each regardless of
// whether the primary context is cancelled (e.g. graceful shutdown).
const bookkeepTimeoutPerRecord = 500 * time.Millisecond

// OutboxService handles the transactional outbox publish cycle.
type OutboxService struct {
	store       port.OutboxStore
	publisher   port.Publisher
	logger      port.Logger
	clock       port.Clock
	maxAttempts int
}

// NewOutboxService creates an OutboxService with all required dependencies.
// If clock is nil, RealClock is used.
func NewOutboxService(
	store port.OutboxStore,
	publisher port.Publisher,
	logger port.Logger,
	clock port.Clock,
	maxAttempts int,
) *OutboxService {
	if clock == nil {
		clock = port.RealClock{}
	}
	return &OutboxService{
		store:       store,
		publisher:   publisher,
		logger:      logger,
		clock:       clock,
		maxAttempts: maxAttempts,
	}
}

// Enqueue inserts a serialized envelope into the outbox within the given transaction.
// The caller controls the transaction boundary — this function only does the INSERT.
func (s *OutboxService) Enqueue(ctx context.Context, tx pgx.Tx, env domain.Envelope[json.RawMessage]) error {
	if tx == nil {
		return fmt.Errorf("outbox: transaction must not be nil — use pgcommon.RunInTx to obtain a transaction")
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

// PublishBatch claims up to batchSize unpublished records and publishes them.
// Per-record errors are logged and marked as failed; the batch continues.
func (s *OutboxService) PublishBatch(ctx context.Context, batchSize int) error {
	records, err := s.store.ClaimBatch(ctx, batchSize)
	if err != nil {
		return err
	}

	// bookkeepCtx is used exclusively for MarkPublished and MarkFailed so these
	// critical bookkeeping writes succeed even when ctx is cancelled by graceful
	// shutdown. The timeout scales with batchSize so every record gets
	// bookkeepTimeoutPerRecord regardless of batch depth.
	bookkeepTimeout := time.Duration(len(records)+1) * bookkeepTimeoutPerRecord
	bookkeepCtx, bookkeepCancel := context.WithTimeout(context.Background(), bookkeepTimeout)
	defer bookkeepCancel()

	for i, rec := range records {
		// If the context is cancelled mid-batch, release the claim lease on all
		// un-iterated records by calling MarkFailed via bookkeepCtx. Without this,
		// those records remain invisible to all runners for claimLeaseDuration
		// (default 10 min) — far beyond the intended PollInterval retry window.
		if ctx.Err() != nil {
			// MarkFailed on stranded (un-iterated) records releases their claim lease
			// immediately so the next runner instance can retry them without waiting
			// claimLeaseDuration (default 10 min). This increments their attempts
			// counter; a record at maxAttempts-1 will be dead-lettered with
			// last_error="batch interrupted by context cancellation" — expected noise
			// during rolling deploys and documented in outbox_dead_letters.
			for _, stranded := range records[i:] {
				if markErr := s.store.MarkFailed(bookkeepCtx, stranded.ID, "batch interrupted by context cancellation", s.maxAttempts); markErr != nil {
					if s.logger != nil {
						s.logger.Error("outbox: failed to release stranded record lease on shutdown", map[string]any{
							"id":    stranded.ID,
							"error": markErr.Error(),
						})
					}
				}
			}
			return ctx.Err()
		}
		var env domain.Envelope[json.RawMessage]
		if err := json.Unmarshal(rec.Payload, &env); err != nil {
			if s.logger != nil {
				s.logger.Error("outbox: failed to unmarshal payload", map[string]any{
					"id":    rec.ID,
					"error": err.Error(),
				})
			}
			if markErr := s.store.MarkFailed(bookkeepCtx, rec.ID, err.Error(), s.maxAttempts); markErr != nil {
				if s.logger != nil {
					s.logger.Error("outbox: failed to mark record failed", map[string]any{
						"id":    rec.ID,
						"error": markErr.Error(),
					})
				}
			}
			continue
		}
		if err := s.publisher.Publish(ctx, env); err != nil {
			if s.logger != nil {
				s.logger.Warn("outbox: publish failed", map[string]any{
					"id":    rec.ID,
					"error": err.Error(),
				})
			}
			if markErr := s.store.MarkFailed(bookkeepCtx, rec.ID, err.Error(), s.maxAttempts); markErr != nil {
				if s.logger != nil {
					s.logger.Error("outbox: failed to mark record failed", map[string]any{
						"id":    rec.ID,
						"error": markErr.Error(),
					})
				}
			}
			continue
		}
		if err := s.store.MarkPublished(bookkeepCtx, rec.ID); err != nil {
			if s.logger != nil {
				s.logger.Error("outbox: failed to mark published", map[string]any{
					"id":    rec.ID,
					"error": err.Error(),
				})
			}
		}
	}
	return nil
}

// PendingCount returns the number of records in outbox_events that have not
// yet been published. Callers use this to update the outbox_pending_total gauge.
func (s *OutboxService) PendingCount(ctx context.Context) (int64, error) {
	return s.store.PendingCount(ctx)
}
