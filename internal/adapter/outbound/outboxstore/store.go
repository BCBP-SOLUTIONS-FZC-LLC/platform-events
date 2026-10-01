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
			(id, event_type, payload, tenant_id, trace_id, created_at, scheduled_at, ordering_key, ordering_seq)
		VALUES
			($1, $2, $3, $4, $5, NOW(),
			 CASE WHEN $7 <> '' AND EXISTS (
			     SELECT 1 FROM outbox_events p WHERE p.ordering_key = $7 AND p.published_at IS NULL
			 ) THEN 'infinity'::timestamptz          -- waits behind its key's head
			 ELSE NOW() + make_interval(secs => $6) END,
			 NULLIF($7, ''),
			 CASE WHEN $7 <> '' THEN nextval('outbox_events_ordering_seq') END)
	`,
		record.ID,
		record.EventType,
		record.Payload,
		record.TenantID,
		record.TraceID,
		delaySecs,
		record.OrderingKey,
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
		rows, err := tx.Query(ctx, s.claimQuery(), batchSize)
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
				&rec.OrderingKey,
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

// claimColumns is the column list ClaimBatch scans, in order.
const claimColumns = `id, event_type, payload, tenant_id, trace_id,
	attempts, last_error, created_at, scheduled_at, published_at, COALESCE(ordering_key, '')`

// earlierUnpublished matches an unpublished record with the same ordering key
// as the row aliased o that precedes it in ordering_seq (drawn at INSERT, so
// commit order under the aggregate row lock). Served by idx_outbox_events_ordering.
const earlierUnpublished = `EXISTS (
	SELECT 1 FROM outbox_events prev
	WHERE prev.ordering_key = o.ordering_key
	  AND prev.published_at IS NULL
	  AND prev.ordering_seq < o.ordering_seq
)`

// claimQuery selects the next due records. A keyed record behind an
// unpublished record of its key waits at scheduled_at = 'infinity', so it is
// not due and never scanned; the NOT EXISTS guard (cheap: it only runs for
// due keyed rows, i.e. heads) keeps the order even if two transactions
// enqueued for one key without the documented row lock.
func (s *Store) claimQuery() string {
	return `SELECT ` + claimColumns + `
		FROM outbox_events o
		WHERE published_at IS NULL
		  AND scheduled_at <= NOW()
		  AND (ordering_key IS NULL OR NOT ` + earlierUnpublished + `)
		ORDER BY scheduled_at, id
		LIMIT $1
		FOR UPDATE SKIP LOCKED`
}

// replayScheduledAt / replayOrderingSeq place a replayed record: unkeyed ones
// are due at once; keyed ones join the back of their key (a fresh sequence
// number) and wait until promoteAllSQL finds nothing ahead of them.
const (
	replayScheduledAt = `CASE WHEN ordering_key IS NULL THEN NOW() ELSE 'infinity'::timestamptz END`
	replayOrderingSeq = `CASE WHEN ordering_key IS NOT NULL THEN nextval('outbox_events_ordering_seq') END`
)

// promoteKeySQL makes key $1's waiting record due once nothing precedes it
// (one probe of idx_outbox_events_ordering — it runs on every keyed publish).
// promoteAllSQL does the same for every key (sweep and replay); it walks the
// 'infinity' tail of idx_outbox_events_pending. Two statements rather than one
// with "$1 = ” OR key = $1": a cached generic plan of that cannot use the
// ordering index, turning every keyed publish into a scan of all waiting rows.
const (
	promoteKeySQL = `
	UPDATE outbox_events o SET scheduled_at = NOW()
	WHERE o.ordering_key = $1
	  AND o.published_at IS NULL
	  AND o.scheduled_at = 'infinity'
	  AND NOT ` + earlierUnpublished
	promoteAllSQL = `
	UPDATE outbox_events o SET scheduled_at = NOW()
	WHERE o.published_at IS NULL
	  AND o.scheduled_at = 'infinity'
	  AND o.ordering_key IS NOT NULL
	  AND NOT ` + earlierUnpublished
)

// promoteNext makes the next waiting record of key due, inside tx — called
// when key's head is published or dead-lettered.
func promoteNext(ctx context.Context, tx pgcommon.Tx, key string) error {
	if key == "" {
		return nil
	}
	_, err := tx.Exec(ctx, promoteKeySQL, key)
	return err
}

// PromoteWaiting makes due every waiting keyed record whose key has no
// earlier unpublished record, and returns how many it promoted. Publishing
// or dead-lettering a head promotes its successor directly; this sweep only
// catches a record enqueued while its head was being published (its insert
// saw the head still unpublished) or whose best-effort promotion failed. The
// runner calls it with the gauge refresh: every GaugeInterval, or
// PollInterval if that is longer.
func (s *Store) PromoteWaiting(ctx context.Context) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultStoreQueryTimeout)
	defer cancel()
	var n int64
	err := pgcommon.RunInTx(ctx, s.pool, pgcommon.TxOptions{}, func(ctx context.Context, tx pgcommon.Tx) error {
		tag, err := tx.Exec(ctx, promoteAllSQL)
		n = tag.RowsAffected()
		return err
	})
	return n, err
}

// BlockedCount returns the number of ordered records waiting behind an
// earlier unpublished record with the same ordering key, capped at
// MaxCountedRows.
func (s *Store) BlockedCount(ctx context.Context) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultStoreQueryTimeout)
	defer cancel()
	var count int64
	err := s.pool.WithConn(ctx, func(ctx context.Context, conn *pgcommon.Conn) error {
		return conn.QueryRow(ctx, `
			SELECT COUNT(*) FROM (
				SELECT 1 FROM outbox_events
				WHERE published_at IS NULL
				  AND scheduled_at = 'infinity'
				LIMIT $1
			) capped
		`, MaxCountedRows).Scan(&count)
	})
	return count, err
}

// MaxCountedRows caps PendingCount and LeasedCount. Counting stops there, so
// the gauge query stays an index-range scan of bounded cost even when an SNS
// outage leaves millions of rows pending — an uncapped COUNT(*) would time out
// (blinding the backlog alert) exactly when the backlog matters. A reading of
// MaxCountedRows means "at least this many".
const MaxCountedRows = 100_000

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
			SELECT COUNT(*) FROM (
				SELECT 1 FROM outbox_events
				WHERE published_at IS NULL
				  AND scheduled_at <= NOW()
				LIMIT $1
			) capped
		`, MaxCountedRows).Scan(&count)
	})
	return count, err
}

// MarkPublished sets published_at = NOW() for the given record ID.
// Logs a warning if no row was updated (record already published or removed by a concurrent runner).
func (s *Store) MarkPublished(ctx context.Context, id string) error {
	ctx, cancel := context.WithTimeout(ctx, defaultStoreQueryTimeout)
	defer cancel()
	var key string
	err := s.pool.WithConn(ctx, func(ctx context.Context, conn *pgcommon.Conn) error {
		// WHERE published_at IS NULL prevents a concurrent runner from overwriting
		// an already-published timestamp and suppressing the no-rows warning.
		return conn.QueryRow(ctx,
			`UPDATE outbox_events SET published_at = NOW() WHERE id = $1 AND published_at IS NULL
			 RETURNING COALESCE(ordering_key, '')`, id).Scan(&key)
	})
	if errors.Is(err, pgcommon.ErrNoRows) {
		// Record was deleted by a concurrent runner (e.g. moved to dead-letters
		// between ClaimBatch and MarkPublished). Log at warn — not fatal since
		// the event was published; we just can't mark it.
		if s.logger != nil {
			s.logger.Warn("outboxstore: MarkPublished matched no rows — record may have been removed by a concurrent runner", map[string]any{
				"id": id,
			})
		}
		return nil
	}
	if err != nil || key == "" {
		return err
	}
	// The key's next record is now its head. Promote it best-effort, after the
	// publish is durably marked: a failed promotion must not roll the mark back
	// (that would re-publish the head after its lease). The runner's
	// PromoteWaiting sweep promotes it later.
	if perr := pgcommon.RunInTx(ctx, s.pool, pgcommon.TxOptions{}, func(ctx context.Context, tx pgcommon.Tx) error {
		return promoteNext(ctx, tx, key)
	}); perr != nil && s.logger != nil {
		s.logger.Warn("outboxstore: could not promote the next ordered record — the waiting-record sweep will", map[string]any{
			"id": id, "error": perr.Error(),
		})
	}
	return nil
}

// MarkFailed increments attempts and sets last_error.
// If attempts >= maxAttempts, the record is moved to outbox_dead_letters;
// otherwise it becomes claimable again after retryAfter (the caller's backoff).
// rec carries the record's original fields so dead-letter insertion does not
// require an additional DB read of the payload.
func (s *Store) MarkFailed(ctx context.Context, rec domain.OutboxRecord, lastError string, maxAttempts int, retryAfter time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, defaultStoreQueryTimeout)
	defer cancel()
	deadLettered, found := false, false
	err := pgcommon.RunInTx(ctx, s.pool, pgcommon.TxOptions{}, func(ctx context.Context, tx pgcommon.Tx) error {
		deadLettered, found = false, false
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
		found = true

		newAttempts := attempts + 1
		deadLettered = newAttempts >= maxAttempts

		if newAttempts >= maxAttempts {
			// Move to dead-letter table using in-memory record fields — avoids a
			// second SELECT of the payload. ON CONFLICT DO UPDATE ensures a concurrent
			// runner updates the dead-letter entry with the latest error and attempt
			// count rather than leaving stale data.
			_, err = tx.Exec(ctx, `
				INSERT INTO outbox_dead_letters
					(id, event_type, payload, tenant_id, trace_id, attempts, last_error, created_at, ordering_key)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NULLIF($9, ''))
				ON CONFLICT (id) DO UPDATE SET
					attempts   = EXCLUDED.attempts,
					last_error = EXCLUDED.last_error,
					failed_at  = NOW()
			`, rec.ID, rec.EventType, rec.Payload, rec.TenantID, rec.TraceID, newAttempts, lastError, rec.CreatedAt, rec.OrderingKey)
			if err != nil {
				return err
			}
			_, err = tx.Exec(ctx, `DELETE FROM outbox_events WHERE id = $1`, rec.ID)
			if err != nil {
				return err
			}
			// A dead-lettered head no longer holds its key.
			if err := promoteNext(ctx, tx, rec.OrderingKey); err != nil {
				return err
			}
		} else {
			// Replace the claim lease with the retry backoff: the record is
			// claimable again at NOW() + retryAfter.
			// WHERE published_at IS NULL prevents a TOCTOU race: if MarkPublished
			// committed between our SELECT and this UPDATE, the already-published
			// row is not overwritten with stale attempt/error data.
			_, err = tx.Exec(ctx, `
				UPDATE outbox_events
				SET attempts = $1, last_error = $2, scheduled_at = NOW() + make_interval(secs => $4)
				WHERE id = $3 AND published_at IS NULL
			`, newAttempts, lastError, rec.ID, max(retryAfter, 0).Seconds())
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
	// A record another runner already published or dead-lettered was neither
	// retried nor dead-lettered here.
	switch {
	case deadLettered:
		metrics.RecordOutboxDeadLetter(rec.EventType)
	case found:
		metrics.IncRetry("outbox_publish", rec.EventType)
	}
	return nil
}

// ReleaseLease makes a claimed record claimable again after retryAfter without
// counting an attempt. Used for shutdown and transient (transport, throttling,
// timeout) failures, which say nothing about the record itself and must never
// push it towards the dead-letter table.
func (s *Store) ReleaseLease(ctx context.Context, id, lastError string, retryAfter time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, defaultStoreQueryTimeout)
	defer cancel()
	return pgcommon.RunInTx(ctx, s.pool, pgcommon.TxOptions{}, func(ctx context.Context, tx pgcommon.Tx) error {
		_, err := tx.Exec(ctx, `
			UPDATE outbox_events
			SET last_error = $2, scheduled_at = NOW() + make_interval(secs => $3)
			WHERE id = $1 AND published_at IS NULL
		`, id, lastError, max(retryAfter, 0).Seconds())
		return err
	})
}

// OldestPendingAge returns how long the oldest unpublished record has been
// waiting (database NOW() − created_at), 0 when nothing is unpublished. A
// single probe of idx_outbox_events_unpublished_created (migration 009).
func (s *Store) OldestPendingAge(ctx context.Context) (time.Duration, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultStoreQueryTimeout)
	defer cancel()
	var secs float64
	err := s.pool.WithConn(ctx, func(ctx context.Context, conn *pgcommon.Conn) error {
		return conn.QueryRow(ctx, `
			SELECT COALESCE(EXTRACT(EPOCH FROM NOW() - MIN(created_at)), 0)::float8
			FROM outbox_events
			WHERE published_at IS NULL
		`).Scan(&secs)
	})
	return time.Duration(max(secs, 0) * float64(time.Second)), err
}

// LeasedCount returns the number of unpublished records not yet due
// (scheduled_at > NOW()): claimed by a runner, or waiting out a retry
// backoff. They do not appear in PendingCount; together the two give the
// whole unpublished backlog. Capped at MaxCountedRows.
func (s *Store) LeasedCount(ctx context.Context) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultStoreQueryTimeout)
	defer cancel()
	var count int64
	err := s.pool.WithConn(ctx, func(ctx context.Context, conn *pgcommon.Conn) error {
		return conn.QueryRow(ctx, `
			SELECT COUNT(*) FROM (
				SELECT 1 FROM outbox_events
				WHERE published_at IS NULL
				  AND scheduled_at > NOW()
				  AND scheduled_at <> 'infinity' -- waiting ordered records: BlockedCount
				LIMIT $1
			) capped
		`, MaxCountedRows).Scan(&count)
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
// inOutbox matches dead letters whose ID is (back) in outbox_events.
const (
	inOutbox    = "EXISTS (SELECT 1 FROM outbox_events e WHERE e.id = outbox_dead_letters.id)"
	notInOutbox = "NOT " + inOutbox
)

// appendCondition adds cond to a WHERE clause built by buildDLQWhere.
func appendCondition(where, cond string) string {
	if where == "" {
		return " WHERE " + cond
	}
	return where + " AND " + cond
}

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
		ORDER BY failed_at ASC, id ASC
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
// outbox_dead_letters back to outbox_events, resetting attempts to 0 and
// created_at to NOW() — a replayed record is a fresh outbox entry, so the
// oldest-pending-age gauge measures delivery delay since the replay rather
// than the dead letter's age (the envelope keeps its own time).
// Returns the number of records re-queued.
func (s *Store) ReprocessDeadLettersWith(ctx context.Context, filter domain.DLQFilter, limit int) (int, error) {
	if limit <= 0 {
		return 0, fmt.Errorf("outboxstore: ReprocessDeadLettersWith limit must be positive (got %d)", limit)
	}
	ctx, cancel := context.WithTimeout(ctx, defaultPruneTimeout)
	defer cancel()

	where, filterArgs := buildDLQWhere(filter, 1)
	// A dead letter whose ID is back in outbox_events (re-enqueued by the
	// application, e.g. with a deterministic ID) cannot be re-inserted; it
	// stays in outbox_dead_letters instead of failing the whole replay on the
	// primary key — every later batch would select it again (oldest first).
	where = appendCondition(where, notInOutbox)
	// LIMIT arg comes after filter args in the inner SELECT.
	limitArg := len(filterArgs) + 1
	filterArgs = append(filterArgs, limit)

	query := fmt.Sprintf(`
		WITH moved AS (
			DELETE FROM outbox_dead_letters
			WHERE id IN (
				SELECT id FROM outbox_dead_letters%s
				ORDER BY failed_at ASC, id ASC
				LIMIT $%d
			)
			RETURNING id, event_type, payload, tenant_id, trace_id, created_at, ordering_key
		)
		INSERT INTO outbox_events
			(id, event_type, payload, tenant_id, trace_id, attempts, created_at, scheduled_at, ordering_key, ordering_seq)
		SELECT id, event_type, payload, tenant_id, trace_id, 0, NOW(), `+replayScheduledAt+`, ordering_key, `+replayOrderingSeq+`
		FROM (SELECT * FROM moved ORDER BY id) m
	`, where, limitArg)

	var moved, skipped int
	err := pgcommon.RunInTx(ctx, s.pool, pgcommon.TxOptions{}, func(ctx context.Context, tx pgcommon.Tx) error {
		tag, err := tx.Exec(ctx, query, filterArgs...)
		if err != nil {
			return err
		}
		moved = int(tag.RowsAffected())
		// Replayed keyed records were queued behind their keys; make the heads due.
		if _, err = tx.Exec(ctx, promoteAllSQL); err != nil {
			return err
		}
		skippedWhere, skippedArgs := buildDLQWhere(filter, 1)
		return tx.QueryRow(ctx, `SELECT count(*) FROM (SELECT 1 FROM outbox_dead_letters`+
			appendCondition(skippedWhere, inOutbox)+` LIMIT 1000) x`, skippedArgs...).Scan(&skipped)
	})
	if err == nil && skipped > 0 && s.logger != nil {
		s.logger.Warn("outboxstore: dead letters left in place — their IDs are already in outbox_events (inspect, then discard)", map[string]any{
			"count": skipped,
		})
	}
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
			ORDER BY failed_at ASC, id ASC
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
	return s.ReprocessDeadLettersWith(ctx, domain.DLQFilter{}, limit)
}
