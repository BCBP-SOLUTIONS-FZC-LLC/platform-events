//go:build integration

package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/internal/adapter/outbound/outboxstore"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/inbox"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/test/fixtures"
)

// Every NewTestDB call owns a fresh database, so these tests can drop a table
// to make one statement fail and assert the store surfaces the error instead
// of swallowing it or reporting success.

func dropTable(ctx context.Context, t *testing.T, pool *pgcommon.Pool, table string) {
	t.Helper()
	require.NoError(t, pool.WithConn(ctx, func(ctx context.Context, conn *pgcommon.Conn) error {
		_, err := conn.Exec(ctx, "DROP TABLE "+table+" CASCADE")
		return err
	}))
}

func TestOutboxStore_MissingOutboxTable_SurfacesErrors(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := fixtures.NewTestDB(ctx, t)
	defer cleanup()
	store := outboxstore.New(pool, &fixtures.MockLogger{}, 0)
	dropTable(ctx, t, pool, "outbox_events")

	_, err := store.ClaimBatch(ctx, 10)
	assert.Error(t, err, "ClaimBatch")
	assert.Error(t, store.MarkPublished(ctx, uuid.NewString()), "MarkPublished")
	err = store.MarkFailed(ctx, makeRecord("missing.table"), "boom", 5, 0)
	require.Error(t, err, "MarkFailed")
	assert.Contains(t, err.Error(), "outboxstore: MarkFailed query failed")
	_, err = store.PrunePublished(ctx, time.Hour, 100)
	assert.Error(t, err, "PrunePublished")
}

func TestOutboxStore_MissingDeadLetterTable_SurfacesErrors(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := fixtures.NewTestDB(ctx, t)
	defer cleanup()
	store := outboxstore.New(pool, &fixtures.MockLogger{}, 0)

	// A record on its last attempt: MarkFailed must move it to
	// outbox_dead_letters, which no longer exists — the whole transaction
	// fails and the record stays in outbox_events.
	rec := makeRecord("missing.dlq")
	require.NoError(t, pgcommon.RunInTx(ctx, pool, pgcommon.TxOptions{}, func(ctx context.Context, tx pgcommon.Tx) error {
		return store.Enqueue(ctx, tx, rec)
	}))
	dropTable(ctx, t, pool, "outbox_dead_letters")

	assert.Error(t, store.MarkFailed(ctx, rec, "boom", 1, 0), "MarkFailed → dead-letter insert")
	pending, err := store.PendingCount(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), pending, "the failed dead-lettering rolled back")

	_, err = store.ListDeadLetters(ctx, domain.DLQFilter{}, 10)
	assert.Error(t, err, "ListDeadLetters")
	_, err = store.ReprocessDeadLettersWith(ctx, domain.DLQFilter{EventType: "missing.dlq"}, 10)
	assert.Error(t, err, "ReprocessDeadLettersWith")
	_, err = store.DiscardDeadLetters(ctx, domain.DLQFilter{EventType: "missing.dlq"}, 10)
	assert.Error(t, err, "DiscardDeadLetters")
	_, err = store.ReprocessDeadLetters(ctx, 10)
	assert.Error(t, err, "ReprocessDeadLetters")
}

func TestInboxStore_ValidationAndErrors(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := fixtures.NewTestDB(ctx, t)
	defer cleanup()

	_, err := inbox.NewStore(pool, "")
	require.Error(t, err, "a consumer name is required")

	store, err := inbox.NewStore(pool, "orders-consumer")
	require.NoError(t, err)
	id := uuid.New()
	require.NoError(t, store.MarkProcessed(ctx, id))
	_, err = store.Prune(ctx, -time.Hour, 10)
	require.ErrorContains(t, err, "positive retention")
	time.Sleep(20 * time.Millisecond)
	n, err := store.Prune(ctx, time.Millisecond, 0) // batch <= 0 → DefaultPruneBatch
	require.NoError(t, err)
	assert.Equal(t, int64(1), n, "rows older than the retention are pruned, using the default batch")

	dropTable(ctx, t, pool, "processed_events")
	_, err = store.IsProcessed(ctx, id)
	assert.ErrorContains(t, err, "inbox: check processed")
	assert.ErrorContains(t, store.MarkProcessed(ctx, id), "inbox: mark processed")
	_, err = store.Prune(ctx, time.Hour, 10)
	assert.ErrorContains(t, err, "inbox: prune")
}
