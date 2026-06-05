// Package outboxstore provides a PostgreSQL implementation of port.OutboxStore.
package outboxstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

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
func InsertRecord(ctx context.Context, tx pgx.Tx, record domain.OutboxRecord) error {
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
func (s *Store) Enqueue(ctx context.Context, tx pgx.Tx, record domain.OutboxRecord) error {
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
	err := s.pool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		tx, err := conn.Begin(ctx)
		if err != nil {
			return err
		}
		committed := false
		defer func() {
			if !committed {
				_ = tx.Rollback(context.Background())
			}
		}()

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

		if err := tx.Commit(ctx); err != nil {
			return err
		}
		committed = true
		return nil
	})
	return records, err
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
	err := s.pool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		return conn.QueryRow(ctx, `
			SELECT COUNT(*) FROM outbox_events
			WHERE published_at IS NULL
			  AND scheduled_at <= NOW()
		`).Scan(&count)
	})
	return count, err
}

// MarkPublished sets published_at = NOW() for the given record ID.
// Returns an error if no row was updated (record missing or already deleted).
func (s *Store) MarkPublished(ctx context.Context, id string) error {
	return s.pool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
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
	return s.pool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		tx, err := conn.Begin(ctx)
		if err != nil {
			return err
		}
		committed := false
		defer func() {
			if !committed {
				_ = tx.Rollback(context.Background())
			}
		}()

		// Read only the current attempts count under FOR UPDATE to serialise against
		// a concurrent runner that re-claimed this record after lease expiry. The
		// payload and other fields come from the in-memory rec — they are identical
		// to the DB values because only attempts/last_error/scheduled_at are mutated.
		// SKIP LOCKED is intentionally NOT used: we want the second runner to block
		// briefly and observe the committed attempts, not skip the row.
		var attempts int
		err = tx.QueryRow(ctx, `
			SELECT attempts FROM outbox_events WHERE id = $1 AND published_at IS NULL
			FOR UPDATE
		`, rec.ID).Scan(&attempts)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
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
		deadLettered := newAttempts >= maxAttempts

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

		if err := tx.Commit(ctx); err != nil {
			return err
		}
		committed = true

		// Increment the dead-letter counter only after a successful commit so the
		// metric never overcounts on a rolled-back transaction.
		if deadLettered {
			metrics.RecordOutboxDeadLetter(rec.EventType)
		}
		return nil
	})
}

// LeasedCount returns the number of records currently claimed by a runner
// (scheduled_at > NOW() and published_at IS NULL). These are in-flight records
// that may not appear in PendingCount, providing a complete outbox health picture.
func (s *Store) LeasedCount(ctx context.Context) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultStoreQueryTimeout)
	defer cancel()
	var count int64
	err := s.pool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		return conn.QueryRow(ctx, `
			SELECT COUNT(*) FROM outbox_events
			WHERE published_at IS NULL
			  AND scheduled_at > NOW()
		`).Scan(&count)
	})
	return count, err
}

// ReprocessDeadLetters moves up to limit records from outbox_dead_letters back
// to outbox_events, resetting attempts to 0 so they are retried from scratch.
// Returns the number of records re-queued. Returns an error if limit <= 0.
// The caller is responsible for supplying a context with an appropriate deadline —
// this operation runs a CTE delete+insert and may be slow for large limit values.
func (s *Store) ReprocessDeadLetters(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		return 0, fmt.Errorf("outboxstore: limit must be positive (got %d)", limit)
	}
	var moved int
	err := s.pool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
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
