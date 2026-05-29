package port

import (
	"context"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/domain"
	"github.com/jackc/pgx/v5"
)

// OutboxStore persists and retrieves outbox records from durable storage.
type OutboxStore interface {
	// Enqueue inserts a record into the outbox within the caller's transaction.
	Enqueue(ctx context.Context, tx pgx.Tx, record domain.OutboxRecord) error

	// ClaimBatch atomically claims up to batchSize unpublished records.
	// Uses SELECT ... FOR UPDATE SKIP LOCKED to support multiple concurrent runners.
	ClaimBatch(ctx context.Context, batchSize int) ([]domain.OutboxRecord, error)

	// MarkPublished sets published_at = NOW() for the given record ID.
	MarkPublished(ctx context.Context, id string) error

	// MarkFailed increments attempts and sets last_error.
	// If attempts >= maxAttempts, moves the record to the dead-letter table.
	MarkFailed(ctx context.Context, id string, lastError string, maxAttempts int) error

	// PendingCount returns the number of records not yet published.
	// Used to update the outbox_pending_total Prometheus gauge each poll cycle.
	PendingCount(ctx context.Context) (int64, error)
}
