package inbox

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/pgcommon"
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

// DefaultPruneBatch bounds each Prune delete so a large backlog never holds
// one long transaction.
const DefaultPruneBatch = 5000

// Prune deletes this consumer's records older than retention, in batches of
// batch rows (DefaultPruneBatch when <= 0), and returns how many it deleted.
// retention must be positive. Keep it at least the DLQ's message retention so
// a redriven message is still recognised.
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
