// Package outboxstore provides a PostgreSQL implementation of port.OutboxStore.
package outboxstore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/adapter/outbound/metrics"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/pgcommon"
)

const defaultClaimLeaseDuration = 10 * time.Minute

// Store implements port.OutboxStore using a platform-pgcommon Pool.
type Store struct {
	pool               *pgcommon.Pool
	logger             port.Logger
	claimLeaseDuration time.Duration
}

// New creates a new Postgres outbox store.
// claimLeaseDuration controls how long a claimed record is hidden from other
// runners. Defaults to 10 minutes when zero; must exceed worst-case batch
// publish time to prevent duplicate delivery.
func New(pool *pgcommon.Pool, logger port.Logger, claimLeaseDuration time.Duration) *Store {
	if claimLeaseDuration <= 0 {
		claimLeaseDuration = defaultClaimLeaseDuration
	}
	return &Store{pool: pool, logger: logger, claimLeaseDuration: claimLeaseDuration}
}

// InsertRecord executes the outbox INSERT within the caller's transaction.
// Shared by Store.Enqueue (service-managed path) and outbox.Enqueue (public API).
// created_at uses DB server NOW() to avoid clock-skew between the Go client
// and Postgres breaking the ClaimBatch scheduled_at <= NOW() predicate.
// scheduled_at is expressed as an offset from NOW() so any ScheduledAt delay
// set by the caller is honoured without trusting the Go clock for absolute times.
func InsertRecord(ctx context.Context, tx pgcommon.Tx, record domain.OutboxRecord) error {
	delaySecs := record.ScheduledAt.Sub(record.CreatedAt).Seconds()
	if delaySecs < 0 {
		delaySecs = 0
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO outbox_events
			(id, event_type, payload, tenant_id, trace_id, created_at, scheduled_at)
		VALUES
			($1, $2, $3, $4, $5, NOW(), NOW() + make_interval(secs => $6))
	`,
		record.ID,
		record.EventType,
		record.Payload,
		record.TenantID,
		record.TraceID,
		delaySecs,
	)
	return err
}

// Enqueue inserts an outbox record within the caller's transaction.
func (s *Store) Enqueue(ctx context.Context, tx pgcommon.Tx, record domain.OutboxRecord) error {
	return InsertRecord(ctx, tx, record)
}

// ClaimBatch atomically claims up to batchSize unpublished records.
// It wraps the SELECT FOR UPDATE SKIP LOCKED and a lease UPDATE in a single
// transaction so the row locks are held until the lease is committed — preventing
// concurrent runners from claiming the same records (TOCTOU race).
// Claimed records have scheduled_at pushed forward by claimLeaseDuration; they
// are made available again immediately by MarkPublished or MarkFailed.
// Returns an error if batchSize <= 0.
func (s *Store) ClaimBatch(ctx context.Context, batchSize int) ([]domain.OutboxRecord, error) {
	if batchSize <= 0 {
		return nil, fmt.Errorf("outboxstore: batchSize must be positive (got %d)", batchSize)
	}
	// Apply a per-call timeout so a hung SELECT FOR UPDATE (e.g. long lock wait
	// under DB saturation) cannot stall the poll loop indefinitely.
	ctx, cancel := context.WithTimeout(ctx, defaultStoreQueryTimeout)
	defer cancel()
	var records []domain.OutboxRecord
	err := pgcommon.RunInTx(ctx, s.pool, pgcommon.TxOptions{}, func(ctx context.Context, tx pgcommon.Tx) error {
		records = nil
		rows, err := tx.Query(ctx, `
			SELECT id, event_type, payload, tenant_id, trace_id,
			       attempts, last_error, created_at, scheduled_at, published_at
			FROM outbox_events
			WHERE published_at IS NULL
			  AND scheduled_at <= NOW()
			ORDER BY scheduled_at, id
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		`, batchSize)
		if err != nil {
			return err
		}
		defer rows.Close() // always release the cursor, even on panic

		var ids []string
		for rows.Next() {
			var rec domain.OutboxRecord
			var publishedAt *time.Time
			if err := rows.Scan(
				&rec.ID,
				&rec.EventType,
				&rec.Payload,
				&rec.TenantID,
				&rec.TraceID,
				&rec.Attempts,
				&rec.LastError,
				&rec.CreatedAt,
				&rec.ScheduledAt,
				&publishedAt,
			); err != nil {
				return err
			}
			rec.PublishedAt = publishedAt
			records = append(records, rec)
			ids = append(ids, rec.ID)
		}
		if err := rows.Err(); err != nil {
			return err
		}

		if len(ids) > 0 {
			// Close rows explicitly before executing the UPDATE on the same
			// transaction. pgx requires no open cursor when issuing the next
			// statement; deferring rows.Close() is not sufficient here because
			// the defer fires at function return, which is after this Exec.
			rows.Close()

			// Push scheduled_at forward to prevent other runners from claiming
			// these records while we are publishing them (lease pattern).
			// Use float64 to preserve sub-second precision — int(0.5s) would
			// truncate to 0, making the lease instant-expire and causing duplicate
			// claims by concurrent runners.
			secs := s.claimLeaseDuration.Seconds()
			_, err = tx.Exec(ctx, `
				UPDATE outbox_events
				SET scheduled_at = NOW() + make_interval(secs => $2)
				WHERE id = ANY($1)
			`, ids, secs)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		// The claim is only in effect once committed: never hand back records
		// whose lease rolled back.
		return nil, err
	}
	return records, nil
}

const defaultStoreQueryTimeout = 5 * time.Second

// PendingCount returns the number of records in outbox_events that are waiting
// to be published (not yet claimed by any runner). Excludes leased records whose
// scheduled_at has been pushed forward by ClaimBatch — those are in-flight, not
// truly pending. Used to populate the outbox_pending_total gauge.
func (s *Store) PendingCount(ctx context.Context) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultStoreQueryTimeout)
	defer cancel()
	var count int64
	err := s.pool.WithConn(ctx, func(ctx context.Context, conn *pgcommon.Conn) error {
		return conn.QueryRow(ctx, `
			SELECT COUNT(*) FROM outbox_events
			WHERE published_at IS NULL
			  AND scheduled_at <= NOW()
		`).Scan(&count)
	})
	return count, err
}

// MarkPublished sets published_at = NOW() for the given record ID.
// Logs a warning if no row was updated (record already published or removed by a concurrent runner).
func (s *Store) MarkPublished(ctx context.Context, id string) error {
	ctx, cancel := context.WithTimeout(ctx, defaultStoreQueryTimeout)
	defer cancel()
	return s.pool.WithConn(ctx, func(ctx context.Context, conn *pgcommon.Conn) error {
		// WHERE published_at IS NULL prevents a concurrent runner from overwriting
		// an already-published timestamp and suppressing the RowsAffected==0 warning.
		tag, err := conn.Exec(ctx,
			`UPDATE outbox_events SET published_at = NOW() WHERE id = $1 AND published_at IS NULL`, id)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			// Record was deleted by a concurrent runner (e.g. moved to dead-letters
			// between ClaimBatch and MarkPublished). Log at warn — not fatal since
			// the event was published; we just can't mark it.
			if s.logger != nil {
				s.logger.Warn("outboxstore: MarkPublished matched no rows — record may have been removed by a concurrent runner", map[string]any{
					"id": id,
				})
			}
		}
		return nil
	})
}

// MarkFailed increments attempts and sets last_error.
// If attempts >= maxAttempts, the record is moved to outbox_dead_letters.
// rec carries the record's original fields so dead-letter insertion does not
// require an additional DB read of the payload.
func (s *Store) MarkFailed(ctx context.Context, rec domain.OutboxRecord, lastError string, maxAttempts int) error {
	ctx, cancel := context.WithTimeout(ctx, defaultStoreQueryTimeout)
	defer cancel()
	deadLettered := false
	err := pgcommon.RunInTx(ctx, s.pool, pgcommon.TxOptions{}, func(ctx context.Context, tx pgcommon.Tx) error {
		deadLettered = false
		// Read only the current attempts count under FOR UPDATE to serialise against
		// a concurrent runner that re-claimed this record after lease expiry. The
		// payload and other fields come from the in-memory rec — they are identical
		// to the DB values because only attempts/last_error/scheduled_at are mutated.
		// SKIP LOCKED is intentionally NOT used: we want the second runner to block
		// briefly and observe the committed attempts, not skip the row.
		var attempts int
		err := tx.QueryRow(ctx, `
			SELECT attempts FROM outbox_events WHERE id = $1 AND published_at IS NULL
			FOR UPDATE
		`, rec.ID).Scan(&attempts)
		if err != nil {
			if errors.Is(err, pgcommon.ErrNoRows) {
				// Record was already published or dead-lettered by a concurrent runner
				// (race after lease expiry). This is expected — not an error.
				if s.logger != nil {
					s.logger.Warn("outboxstore: MarkFailed found no claimable record — already published or removed", map[string]any{
						"id": rec.ID,
					})
				}
				return nil
			}
			return fmt.Errorf("outboxstore: MarkFailed query failed: %w", err)
		}

		newAttempts := attempts + 1
		deadLettered = newAttempts >= maxAttempts

		if newAttempts >= maxAttempts {
			// Move to dead-letter table using in-memory record fields — avoids a
			// second SELECT of the payload. ON CONFLICT DO UPDATE ensures a concurrent
			// runner updates the dead-letter entry with the latest error and attempt
			// count rather than leaving stale data.
			_, err = tx.Exec(ctx, `
				INSERT INTO outbox_dead_letters
					(id, event_type, payload, tenant_id, trace_id, attempts, last_error, created_at)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
				ON CONFLICT (id) DO UPDATE SET
					attempts   = EXCLUDED.attempts,
					last_error = EXCLUDED.last_error,
					failed_at  = NOW()
			`, rec.ID, rec.EventType, rec.Payload, rec.TenantID, rec.TraceID, newAttempts, lastError, rec.CreatedAt)
			if err != nil {
				return err
			}
			_, err = tx.Exec(ctx, `DELETE FROM outbox_events WHERE id = $1`, rec.ID)
			if err != nil {
				return err
			}
		} else {
			// Reset scheduled_at = NOW() to release the claim lease immediately
			// so the next poll cycle can retry this record without delay.
			// WHERE published_at IS NULL prevents a TOCTOU race: if MarkPublished
			// committed between our SELECT and this UPDATE, the already-published
			// row is not overwritten with stale attempt/error data.
			_, err = tx.Exec(ctx, `
				UPDATE outbox_events
				SET attempts = $1, last_error = $2, scheduled_at = NOW()
				WHERE id = $3 AND published_at IS NULL
			`, newAttempts, lastError, rec.ID)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	// Increment the dead-letter counter only after a successful commit so the
	// metric never overcounts on a rolled-back transaction.
	if deadLettered {
		metrics.RecordOutboxDeadLetter(rec.EventType)
	}
	return nil
}

// LeasedCount returns the number of records currently claimed by a runner
// (scheduled_at > NOW() and published_at IS NULL). These are in-flight records
// that may not appear in PendingCount, providing a complete outbox health picture.
func (s *Store) LeasedCount(ctx context.Context) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultStoreQueryTimeout)
	defer cancel()
	var count int64
	err := s.pool.WithConn(ctx, func(ctx context.Context, conn *pgcommon.Conn) error {
		return conn.QueryRow(ctx, `
			SELECT COUNT(*) FROM outbox_events
			WHERE published_at IS NULL
			  AND scheduled_at > NOW()
		`).Scan(&count)
	})
	return count, err
}

// defaultPruneTimeout is a generous timeout for prune deletes, which may scan
// and lock more rows than a typical point-query. Larger than defaultStoreQueryTimeout
// (5 s) because batches of 1000+ rows take more time under normal load.
const defaultPruneTimeout = 30 * time.Second

// PrunePublished deletes published records older than olderThan from outbox_events.
// Batches the delete to at most limit rows so the lock hold time stays bounded.
// Uses the idx_outbox_events_published_at partial index (migration 007) for an
// efficient scan; without that index the query falls back to a sequential scan.
// Applies defaultPruneTimeout (30 s) so a saturated DB does not block indefinitely.
// Returns the number of rows deleted and any database error.
func (s *Store) PrunePublished(ctx context.Context, olderThan time.Duration, limit int) (int64, error) {
	if limit <= 0 {
		return 0, fmt.Errorf("outboxstore: PrunePublished limit must be positive (got %d)", limit)
	}
	if olderThan <= 0 {
		return 0, fmt.Errorf("outboxstore: PrunePublished olderThan must be positive (got %s)", olderThan)
	}
	ctx, cancel := context.WithTimeout(ctx, defaultPruneTimeout)
	defer cancel()
	var deleted int64
	err := s.pool.WithConn(ctx, func(ctx context.Context, conn *pgcommon.Conn) error {
		tag, err := conn.Exec(ctx, `
			DELETE FROM outbox_events
			WHERE id IN (
				SELECT id FROM outbox_events
				WHERE published_at IS NOT NULL
				  AND published_at < NOW() - make_interval(secs => $1)
				LIMIT $2
			)
		`, olderThan.Seconds(), limit)
		if err != nil {
			return err
		}
		deleted = tag.RowsAffected()
		return nil
	})
	return deleted, err
}

// buildDLQWhere constructs the optional WHERE clause and its positional args for
// DLQ filter operations. Returns an empty string when the filter is a zero value
// (matches all rows). argOffset is the $N index to start numbering from.
func buildDLQWhere(f domain.DLQFilter, argOffset int) (string, []any) {
	var parts []string
	var args []any
	n := argOffset
	if f.EventType != "" {
		parts = append(parts, fmt.Sprintf("event_type = $%d", n))
		args = append(args, f.EventType)
		n++
	}
	if f.TenantID != "" {
		parts = append(parts, fmt.Sprintf("tenant_id = $%d", n))
		args = append(args, f.TenantID)
		n++
	}
	if !f.FailedBefore.IsZero() {
		parts = append(parts, fmt.Sprintf("failed_at < $%d", n))
		args = append(args, f.FailedBefore.UTC())
		n++ //nolint:ineffassign // n is unused after the last append, but kept for clarity
	}
	if len(parts) == 0 {
		return "", args
	}
	return " WHERE " + strings.Join(parts, " AND "), args
}

// ListDeadLetters returns up to limit records from outbox_dead_letters that
// match filter, ordered by failed_at ascending (oldest failures first).
func (s *Store) ListDeadLetters(ctx context.Context, filter domain.DLQFilter, limit int) ([]domain.DeadLetterRecord, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("outboxstore: ListDeadLetters limit must be positive (got %d)", limit)
	}
	ctx, cancel := context.WithTimeout(ctx, defaultPruneTimeout)
	defer cancel()

	where, args := buildDLQWhere(filter, 1)
	args = append(args, limit)
	query := fmt.Sprintf(`
		SELECT id, event_type, tenant_id, trace_id, attempts, last_error, created_at, failed_at
		FROM outbox_dead_letters%s
		ORDER BY failed_at ASC
		LIMIT $%d
	`, where, len(args))

	var records []domain.DeadLetterRecord
	err := s.pool.WithConn(ctx, func(ctx context.Context, conn *pgcommon.Conn) error {
		rows, err := conn.Query(ctx, query, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var r domain.DeadLetterRecord
			if err := rows.Scan(&r.ID, &r.EventType, &r.TenantID, &r.TraceID,
				&r.Attempts, &r.LastError, &r.CreatedAt, &r.FailedAt); err != nil {
				return err
			}
			records = append(records, r)
		}
		return rows.Err()
	})
	return records, err
}

// ReprocessDeadLettersWith moves up to limit records that match filter from
// outbox_dead_letters back to outbox_events, resetting attempts to 0.
// Returns the number of records re-queued.
func (s *Store) ReprocessDeadLettersWith(ctx context.Context, filter domain.DLQFilter, limit int) (int, error) {
	if limit <= 0 {
		return 0, fmt.Errorf("outboxstore: ReprocessDeadLettersWith limit must be positive (got %d)", limit)
	}
	ctx, cancel := context.WithTimeout(ctx, defaultPruneTimeout)
	defer cancel()

	where, filterArgs := buildDLQWhere(filter, 1)
	// LIMIT arg comes after filter args in the inner SELECT.
	limitArg := len(filterArgs) + 1
	filterArgs = append(filterArgs, limit)

	query := fmt.Sprintf(`
		WITH moved AS (
			DELETE FROM outbox_dead_letters
			WHERE id IN (
				SELECT id FROM outbox_dead_letters%s
				ORDER BY created_at ASC
				LIMIT $%d
			)
			RETURNING id, event_type, payload, tenant_id, trace_id, created_at
		)
		INSERT INTO outbox_events
			(id, event_type, payload, tenant_id, trace_id, attempts, created_at, scheduled_at)
		SELECT id, event_type, payload, tenant_id, trace_id, 0, created_at, NOW()
		FROM moved
	`, where, limitArg)

	var moved int
	err := s.pool.WithConn(ctx, func(ctx context.Context, conn *pgcommon.Conn) error {
		tag, err := conn.Exec(ctx, query, filterArgs...)
		if err != nil {
			return err
		}
		moved = int(tag.RowsAffected())
		return nil
	})
	if err == nil && moved > 0 {
		metrics.RecordOutboxDeadLettersReprocessed(moved)
	}
	return moved, err
}

// DiscardDeadLetters permanently deletes up to limit records that match filter
// from outbox_dead_letters. Use for poison-pill records that will never succeed.
// Returns the number of rows deleted.
func (s *Store) DiscardDeadLetters(ctx context.Context, filter domain.DLQFilter, limit int) (int64, error) {
	if limit <= 0 {
		return 0, fmt.Errorf("outboxstore: DiscardDeadLetters limit must be positive (got %d)", limit)
	}
	ctx, cancel := context.WithTimeout(ctx, defaultPruneTimeout)
	defer cancel()

	where, filterArgs := buildDLQWhere(filter, 1)
	limitArg := len(filterArgs) + 1
	filterArgs = append(filterArgs, limit)

	query := fmt.Sprintf(`
		DELETE FROM outbox_dead_letters
		WHERE id IN (
			SELECT id FROM outbox_dead_letters%s
			ORDER BY failed_at ASC
			LIMIT $%d
		)
	`, where, limitArg)

	var deleted int64
	err := s.pool.WithConn(ctx, func(ctx context.Context, conn *pgcommon.Conn) error {
		tag, err := conn.Exec(ctx, query, filterArgs...)
		if err != nil {
			return err
		}
		deleted = tag.RowsAffected()
		return nil
	})
	if err == nil && deleted > 0 {
		metrics.RecordOutboxDeadLettersDiscarded(deleted)
	}
	return deleted, err
}

// ReprocessDeadLetters moves up to limit records from outbox_dead_letters back
// to outbox_events, resetting attempts to 0 so they are retried from scratch.
// Returns the number of records re-queued. Returns an error if limit <= 0.
// Applies defaultPruneTimeout (30 s) internally so a saturated DB does not
// block the caller indefinitely.
func (s *Store) ReprocessDeadLetters(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		return 0, fmt.Errorf("outboxstore: limit must be positive (got %d)", limit)
	}
	ctx, cancel := context.WithTimeout(ctx, defaultPruneTimeout)
	defer cancel()
	var moved int
	err := s.pool.WithConn(ctx, func(ctx context.Context, conn *pgcommon.Conn) error {
		tag, err := conn.Exec(ctx, `
			WITH moved AS (
				DELETE FROM outbox_dead_letters
				WHERE id IN (
					SELECT id FROM outbox_dead_letters
					ORDER BY created_at
					LIMIT $1
				)
				RETURNING id, event_type, payload, tenant_id, trace_id, created_at
			)
			INSERT INTO outbox_events
				(id, event_type, payload, tenant_id, trace_id, attempts, created_at, scheduled_at)
			SELECT id, event_type, payload, tenant_id, trace_id, 0, created_at, NOW()
			FROM moved
		`, limit)
		if err != nil {
			return err
		}
		moved = int(tag.RowsAffected())
		return nil
	})
	if err == nil && moved > 0 {
		metrics.RecordOutboxDeadLettersReprocessed(moved)
	}
	return moved, err
}
