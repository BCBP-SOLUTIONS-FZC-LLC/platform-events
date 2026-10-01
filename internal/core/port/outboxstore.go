package port

import (
	"context"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/domain"
)

// OutboxStore persists and retrieves outbox records from durable storage.
type OutboxStore interface {
	// Enqueue inserts a record into the outbox within the caller's transaction.
	Enqueue(ctx context.Context, tx pgcommon.Tx, record domain.OutboxRecord) error

	// ClaimBatch atomically claims up to batchSize unpublished records.
	// Uses SELECT ... FOR UPDATE SKIP LOCKED to support multiple concurrent runners.
	ClaimBatch(ctx context.Context, batchSize int) ([]domain.OutboxRecord, error)

	// MarkPublished sets published_at = NOW() for the given record ID.
	MarkPublished(ctx context.Context, id string) error

	// MarkFailed increments attempts and sets last_error.
	// If attempts >= maxAttempts, moves the record to the dead-letter table;
	// otherwise the record becomes claimable again after retryAfter.
	// rec carries the record's original fields for dead-letter insertion, avoiding
	// a second DB read of the payload when moving to outbox_dead_letters.
	MarkFailed(ctx context.Context, rec domain.OutboxRecord, lastError string, maxAttempts int, retryAfter time.Duration) error

	// ReleaseLease makes a claimed record claimable again after retryAfter and
	// records lastError, without counting an attempt. Used for shutdown and
	// transient failures that say nothing about the record itself.
	ReleaseLease(ctx context.Context, id, lastError string, retryAfter time.Duration) error

	// BlockedCount returns the number of ordered records waiting behind an
	// earlier unpublished record with the same ordering key.
	BlockedCount(ctx context.Context) (int64, error)

	// PromoteWaiting makes due every waiting ordered record whose key has no
	// earlier unpublished record, returning how many it promoted.
	PromoteWaiting(ctx context.Context) (int64, error)

	// OldestPendingAge returns how long the oldest unpublished record has been
	// waiting (now − created_at), or 0 when none is unpublished.
	OldestPendingAge(ctx context.Context) (time.Duration, error)

	// PendingCount returns the number of records not yet published.
	// Used to update the platform_outbox_pending_events gauge each poll cycle.
	PendingCount(ctx context.Context) (int64, error)

	// LeasedCount returns the number of records currently claimed by a runner
	// (scheduled_at > NOW() and published_at IS NULL). Used for the platform_outbox_leased_events gauge.
	LeasedCount(ctx context.Context) (int64, error)

	// ReprocessDeadLetters moves up to limit records from outbox_dead_letters back
	// to outbox_events for redelivery. Returns the number of records re-queued.
	ReprocessDeadLetters(ctx context.Context, limit int) (int, error)

	// ListDeadLetters returns up to limit records from outbox_dead_letters that
	// match filter, ordered by failed_at ascending (oldest failures first).
	// Returns an empty slice (not an error) when no records match.
	ListDeadLetters(ctx context.Context, filter domain.DLQFilter, limit int) ([]domain.DeadLetterRecord, error)

	// ReprocessDeadLettersWith moves up to limit records that match filter from
	// outbox_dead_letters back to outbox_events, resetting attempts to 0, in the
	// same order as ListDeadLetters (failed_at ascending) so a list-then-replay
	// with the same filter and limit replays exactly the inspected records.
	// Returns the number of records re-queued.
	ReprocessDeadLettersWith(ctx context.Context, filter domain.DLQFilter, limit int) (int, error)

	// DiscardDeadLetters permanently deletes up to limit records that match filter
	// from outbox_dead_letters. Use for poison-pill records that will never succeed.
	// Returns the number of rows deleted.
	DiscardDeadLetters(ctx context.Context, filter domain.DLQFilter, limit int) (int64, error)

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
