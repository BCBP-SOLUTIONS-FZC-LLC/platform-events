package inbox

import (
	"context"
	"errors"
	"fmt"
	"time"

	"encoding/json"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/events"
)

// Store is the processed_events ledger for one consumer.
type Store struct {
	pool     *pgcommon.Pool
	consumer string
}

// NewStore returns a Store recording under consumer (a stable name, e.g.
// "offboarding_consumer"; distinct consumers sharing a table dedup
// independently). pool must be non-nil and consumer non-empty.
func NewStore(pool *pgcommon.Pool, consumer string) (*Store, error) {
	if pool == nil {
		return nil, errors.New("inbox: NewStore requires a non-nil pool")
	}
	if consumer == "" {
		return nil, errors.New("inbox: NewStore requires a non-empty consumer name")
	}
	return &Store{pool: pool, consumer: consumer}, nil
}

// Consumer returns the consumer name this Store records under.
func (s *Store) Consumer() string { return s.consumer }

// IsProcessed reports whether eventID was already recorded for this consumer.
func (s *Store) IsProcessed(ctx context.Context, eventID uuid.UUID) (bool, error) {
	var exists bool
	err := pgcommon.RunInTx(ctx, s.pool, pgcommon.TxOptions{}, func(ctx context.Context, tx pgcommon.Tx) error {
		return tx.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM processed_events WHERE event_id = $1 AND consumer = $2)`,
			eventID.String(), s.consumer).Scan(&exists)
	})
	if err != nil {
		return false, fmt.Errorf("inbox: check processed %s: %w", eventID, err)
	}
	return exists, nil
}

// MarkProcessed records eventID for this consumer. Recording an ID twice is
// a no-op.
func (s *Store) MarkProcessed(ctx context.Context, eventID uuid.UUID) error {
	err := pgcommon.RunInTx(ctx, s.pool, pgcommon.TxOptions{}, func(ctx context.Context, tx pgcommon.Tx) error {
		_, execErr := tx.Exec(ctx,
			`INSERT INTO processed_events (event_id, consumer) VALUES ($1, $2) ON CONFLICT (event_id, consumer) DO NOTHING`,
			eventID.String(), s.consumer)
		return execErr
	})
	if err != nil {
		return fmt.Errorf("inbox: mark processed %s: %w", eventID, err)
	}
	return nil
}

// errDeadLettered rolls back a Process transaction whose fn dead-lettered the
// message, so the claim is not kept.
var errDeadLettered = errors.New("inbox: message dead-lettered")

// Process runs fn exactly once per envelope ID for this consumer, inside one
// transaction on the Store's pool: it claims the ID (INSERT … ON CONFLICT DO
// NOTHING), runs fn with that transaction, and commits both together. A
// duplicate — already processed, or being processed concurrently, which the
// claim's row lock serialises — returns nil without calling fn and is counted
// like [Handler]'s duplicates. If fn fails, the claim rolls back with fn's
// writes and the message is retried; if fn dead-letters the message
// (SendToDLQ) and returns nil, the transaction is rolled back too, so a
// redrive is processed. fn must do its database work through tx.
//
//	consumer := events.NewSQSConsumer(cfg, func(ctx context.Context, env events.Envelope[json.RawMessage]) error {
//	    return store.Process(ctx, env, func(ctx context.Context, tx pgcommon.Tx) error {
//	        return repo.ApplyUserCreated(ctx, tx, env)
//	    })
//	}, ...)
func (s *Store) Process(ctx context.Context, env events.Envelope[json.RawMessage], fn func(ctx context.Context, tx pgcommon.Tx) error) error {
	id, err := parseEventID(env.ID)
	if err != nil {
		return err
	}
	duplicate := false
	err = pgcommon.RunInTx(ctx, s.pool, pgcommon.TxOptions{}, func(ctx context.Context, tx pgcommon.Tx) error {
		duplicate = false
		tag, err := tx.Exec(ctx,
			`INSERT INTO processed_events (event_id, consumer) VALUES ($1, $2) ON CONFLICT (event_id, consumer) DO NOTHING`,
			id.String(), s.consumer)
		if err != nil {
			return fmt.Errorf("inbox: claim %s: %w", id, err)
		}
		if tag.RowsAffected() == 0 {
			duplicate = true
			return nil
		}
		if err := fn(ctx, tx); err != nil {
			return err
		}
		if port.DLQAttributionFromContext(ctx).Recorded() {
			return errDeadLettered
		}
		return nil
	})
	if errors.Is(err, errDeadLettered) {
		return nil
	}
	if err == nil && duplicate {
		recordDuplicate(ctx, s.consumer, env.Type)
	}
	return err
}

// DefaultPruneBatch bounds each Prune delete so a large backlog never holds
// one long transaction.
const DefaultPruneBatch = 5000

// Prune deletes this consumer's records older than retention, in batches of
// batch rows (DefaultPruneBatch when <= 0), and returns how many it deleted.
// retention must be positive. Keep it at least as long as a duplicate can
// still arrive — the source queue's message retention, and any outbox replay
// window. Dead-lettered messages are never recorded, so a DLQ redrive is
// processed regardless of retention.
func (s *Store) Prune(ctx context.Context, retention time.Duration, batch int) (int64, error) {
	if retention <= 0 {
		return 0, errors.New("inbox: Prune requires a positive retention")
	}
	if batch <= 0 {
		batch = DefaultPruneBatch
	}
	var total int64
	for {
		var n int64
		err := pgcommon.RunInTx(ctx, s.pool, pgcommon.TxOptions{}, func(ctx context.Context, tx pgcommon.Tx) error {
			tag, execErr := tx.Exec(ctx, `
				DELETE FROM processed_events
				WHERE ctid IN (
					SELECT ctid FROM processed_events
					WHERE consumer = $1 AND processed_at < now() - make_interval(secs => $2)
					LIMIT $3
				)`, s.consumer, retention.Seconds(), batch)
			n = tag.RowsAffected()
			return execErr
		})
		if err != nil {
			return total, fmt.Errorf("inbox: prune: %w", err)
		}
		total += n
		if n < int64(batch) {
			return total, nil
		}
	}
}
