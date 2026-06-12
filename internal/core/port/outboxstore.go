package port

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/domain"
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
	// rec carries the record's original fields for dead-letter insertion, avoiding
	// a second DB read of the payload when moving to outbox_dead_letters.
	MarkFailed(ctx context.Context, rec domain.OutboxRecord, lastError string, maxAttempts int) error

	// PendingCount returns the number of records not yet published.
	// Used to update the outbox_pending_total Prometheus gauge each poll cycle.
	PendingCount(ctx context.Context) (int64, error)

	// LeasedCount returns the number of records currently claimed by a runner
	// (scheduled_at > NOW() and published_at IS NULL). Used for the outbox_leased_total gauge.
	LeasedCount(ctx context.Context) (int64, error)

	// ReprocessDeadLetters moves up to limit records from outbox_dead_letters back
	// to outbox_events for redelivery. Returns the number of records re-queued.
	ReprocessDeadLetters(ctx context.Context, limit int) (int, error)

	// PrunePublished deletes published records older than olderThan from outbox_events
	// to prevent unbounded table growth. The delete is batched to at most limit rows
	// per call. Callers should run this periodically (e.g. daily) with an olderThan
	// window that exceeds the longest idempotency window of any downstream consumer.
	// Returns the number of rows deleted and any database error.
	PrunePublished(ctx context.Context, olderThan time.Duration, limit int) (int64, error)
}

// OutboxMetrics is the observability surface the OutboxService needs.
// Defined in port so the service layer does not import the adapter layer.
// Implementations live in the adapter layer; nil disables all recording.
type OutboxMetrics interface {
	RecordUnmarshalError()
	RecordMarkPublishedError()
}
