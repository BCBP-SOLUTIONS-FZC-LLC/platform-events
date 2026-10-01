//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/events"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/inbox"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/test/fixtures"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/pgcommon"
)

// Store.Process claims the ID in the handler's transaction: writes happen
// exactly once, roll back with a failure, and concurrent copies serialise.
func TestInboxStore_Process_ExactlyOnce(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := fixtures.NewTestDB(ctx, t)
	defer cleanup()
	require.NoError(t, pgcommon.RunInTx(ctx, pool, pgcommon.TxOptions{}, func(ctx context.Context, tx pgcommon.Tx) error {
		_, err := tx.Exec(ctx, `CREATE TABLE effects (event_id TEXT NOT NULL)`)
		return err
	}))
	store, err := inbox.NewStore(pool, "process_consumer")
	require.NoError(t, err)
	effects := func(id string) int {
		var n int
		require.NoError(t, pool.WithConn(ctx, func(ctx context.Context, conn *pgcommon.Conn) error {
			return conn.QueryRow(ctx, `SELECT count(*) FROM effects WHERE event_id = $1`, id).Scan(&n)
		}))
		return n
	}
	write := func(env events.Envelope[json.RawMessage]) func(context.Context, pgcommon.Tx) error {
		return func(ctx context.Context, tx pgcommon.Tx) error {
			_, err := tx.Exec(ctx, `INSERT INTO effects (event_id) VALUES ($1)`, env.ID)
			return err
		}
	}
	newEnv := func() events.Envelope[json.RawMessage] {
		return events.Envelope[json.RawMessage]{ID: uuid.NewString(), Type: "T"}
	}

	t.Run("duplicate skipped", func(t *testing.T) {
		e := newEnv()
		require.NoError(t, store.Process(ctx, e, write(e)))
		require.NoError(t, store.Process(ctx, e, write(e)))
		assert.Equal(t, 1, effects(e.ID))
	})

	t.Run("failure rolls back claim and writes", func(t *testing.T) {
		e := newEnv()
		boom := errors.New("boom")
		err := store.Process(ctx, e, func(ctx context.Context, tx pgcommon.Tx) error {
			require.NoError(t, write(e)(ctx, tx))
			return boom
		})
		require.ErrorIs(t, err, boom)
		assert.Equal(t, 0, effects(e.ID))
		require.NoError(t, store.Process(ctx, e, write(e)), "retry processes it")
		assert.Equal(t, 1, effects(e.ID))
	})

	t.Run("dead-lettered not recorded", func(t *testing.T) {
		e := newEnv()
		dctx, _ := port.WithDLQAttribution(ctx, "explicit")
		require.NoError(t, store.Process(dctx, e, func(ctx context.Context, _ pgcommon.Tx) error {
			port.DLQAttributionFromContext(ctx).MarkRecorded()
			return nil
		}))
		seen, err := store.IsProcessed(ctx, uuid.MustParse(e.ID))
		require.NoError(t, err)
		assert.False(t, seen, "a redrive must be processed")
	})

	t.Run("concurrent copies run once", func(t *testing.T) {
		e := newEnv()
		var ran atomic.Int32
		var wg sync.WaitGroup
		for range 5 {
			wg.Go(func() {
				assert.NoError(t, store.Process(ctx, e, func(ctx context.Context, tx pgcommon.Tx) error {
					ran.Add(1)
					time.Sleep(50 * time.Millisecond) // hold the claim while the others arrive
					return write(e)(ctx, tx)
				}))
			})
		}
		wg.Wait()
		assert.Equal(t, int32(1), ran.Load())
		assert.Equal(t, 1, effects(e.ID))
	})

	t.Run("invalid id", func(t *testing.T) {
		assert.Error(t, store.Process(ctx, events.Envelope[json.RawMessage]{ID: "nope"}, write(newEnv())))
	})
}
