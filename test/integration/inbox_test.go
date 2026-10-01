//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/events"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/inbox"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/test/fixtures"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"
)

func TestInboxStore_EndToEnd(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := fixtures.NewTestDB(ctx, t)
	defer cleanup()

	a, err := inbox.NewStore(pool, "consumer_a")
	require.NoError(t, err)
	b, err := inbox.NewStore(pool, "consumer_b")
	require.NoError(t, err)
	id := uuid.New()

	seen, err := a.IsProcessed(ctx, id)
	require.NoError(t, err)
	assert.False(t, seen)
	require.NoError(t, a.MarkProcessed(ctx, id))
	require.NoError(t, a.MarkProcessed(ctx, id), "marking twice is a no-op")
	seen, _ = a.IsProcessed(ctx, id)
	assert.True(t, seen)
	seen, _ = b.IsProcessed(ctx, id)
	assert.False(t, seen, "consumers dedup independently")

	// Handler against the real store: second delivery skipped.
	calls := 0
	h := inbox.Handler(b, func(context.Context, events.Envelope[json.RawMessage]) error { calls++; return nil })
	envl := events.Envelope[json.RawMessage]{ID: uuid.NewString(), Type: "T"}
	require.NoError(t, h(ctx, envl))
	require.NoError(t, h(ctx, envl))
	assert.Equal(t, 1, calls)

	// Prune: age consumer_a's row, prune in batches of 1, consumer_b untouched.
	require.NoError(t, pgcommon.RunInTx(ctx, pool, pgcommon.TxOptions{}, func(ctx context.Context, tx pgcommon.Tx) error {
		_, e := tx.Exec(ctx, `INSERT INTO processed_events (event_id, consumer, processed_at) VALUES ($1, 'consumer_a', now() - interval '10 days')`, uuid.NewString())
		if e != nil {
			return e
		}
		_, e = tx.Exec(ctx, `UPDATE processed_events SET processed_at = now() - interval '10 days' WHERE consumer = 'consumer_a'`)
		return e
	}))
	n, err := a.Prune(ctx, 8*24*time.Hour, 1)
	require.NoError(t, err)
	assert.Equal(t, int64(2), n)
	seen, _ = b.IsProcessed(ctx, uuid.MustParse(envl.ID))
	assert.True(t, seen, "prune is per consumer")
	_, err = a.Prune(ctx, 0, 0)
	assert.Error(t, err)
}
