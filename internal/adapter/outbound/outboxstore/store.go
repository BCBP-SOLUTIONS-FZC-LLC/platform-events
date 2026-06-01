// Package outboxstore provides a PostgreSQL implementation of port.OutboxStore.
package outboxstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/adapter/outbound/metrics"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/pgcommon"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
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
// created_at and scheduled_at use the DB server's NOW() to avoid clock-skew
// between the Go client and the Postgres container breaking the ClaimBatch
// scheduled_at <= NOW() predicate.
func InsertRecord(ctx context.Context, tx pgx.Tx, record domain.OutboxRecord) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO outbox_events
			(id, event_type, payload, tenant_id, trace_id, created_at, scheduled_at)
		VALUES
			($1, $2, $3, $4, $5, NOW(), NOW())
	`,
		record.ID,
		record.EventType,
		record.Payload,
		record.TenantID,
		record.TraceID,
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
func (s *Store) ClaimBatch(ctx context.Context, batchSize int) ([]domain.OutboxRecord, error) {
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
			ORDER BY id
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
			// Push scheduled_at forward to prevent other runners from claiming
			// these records while we are publishing them (lease pattern).
			secs := int(s.claimLeaseDuration.Seconds())
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

// PendingCount returns the number of records in outbox_events that are waiting
// to be published (not yet claimed by any runner). Excludes leased records whose
// scheduled_at has been pushed forward by ClaimBatch — those are in-flight, not
// truly pending. Used to populate the outbox_pending_total gauge.
func (s *Store) PendingCount(ctx context.Context) (int64, error) {
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
func (s *Store) MarkFailed(ctx context.Context, id string, lastError string, maxAttempts int) error {
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

		// Read current attempts.
		var attempts int
		var eventType, tenantID, traceID string
		var payload []byte
		var createdAt time.Time
		// FOR UPDATE serializes the attempts read against a concurrent runner that
		// re-claimed this record after the claim lease expired mid-publish. Without
		// it, two runners could read the same attempts value and double-increment or
		// dead-letter one cycle early. SKIP LOCKED is intentionally NOT used here:
		// we want the second runner to block briefly and observe the committed
		// attempts, not skip the row.
		err = tx.QueryRow(ctx, `
			SELECT attempts, event_type, tenant_id, trace_id, payload, created_at
			FROM outbox_events WHERE id = $1 AND published_at IS NULL
			FOR UPDATE
		`, id).Scan(&attempts, &eventType, &tenantID, &traceID, &payload, &createdAt)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				// Record was already published or dead-lettered by a concurrent runner
				// (race after lease expiry). This is expected — not an error.
				if s.logger != nil {
					s.logger.Warn("outboxstore: MarkFailed found no claimable record — already published or removed", map[string]any{
						"id": id,
					})
				}
				return nil
			}
			return fmt.Errorf("outboxstore: MarkFailed query failed: %w", err)
		}

		newAttempts := attempts + 1
		deadLettered := newAttempts >= maxAttempts

		if newAttempts >= maxAttempts {
			// Move to dead-letter table. ON CONFLICT DO UPDATE ensures that a concurrent
			// runner claiming the same record after lease expiry updates the dead-letter
			// entry with the latest error and attempt count rather than leaving stale data.
			_, err = tx.Exec(ctx, `
				INSERT INTO outbox_dead_letters
					(id, event_type, payload, tenant_id, trace_id, attempts, last_error, created_at)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
				ON CONFLICT (id) DO UPDATE SET
					attempts   = EXCLUDED.attempts,
					last_error = EXCLUDED.last_error,
					failed_at  = NOW()
			`, id, eventType, payload, tenantID, traceID, newAttempts, lastError, createdAt)
			if err != nil {
				return err
			}
			_, err = tx.Exec(ctx, `DELETE FROM outbox_events WHERE id = $1`, id)
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
			`, newAttempts, lastError, id)
			if err != nil {
				return err
			}
		}

		if err := tx.Commit(ctx); err != nil {
			return err
		}
		committed = true

		// Increment the dead-letter counter only after a successful commit so the
		// metric never overcounts on a rolled-back transaction. Enables alerting on
		// publish-side failures (rate(outbox_dead_letters_total) > 0).
		if deadLettered && metrics.OutboxDeadLettersTotal != nil {
			metrics.OutboxDeadLettersTotal.WithLabelValues().Inc()
		}
		return nil
	})
}
