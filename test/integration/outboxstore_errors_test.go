//go:build integration

package integration_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/internal/adapter/outbound/outboxstore"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/test/fixtures"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"
)

// closedPoolStore returns a store backed by a pool that has already been closed,
// exercising DB error paths without mocking pgx internals.
func closedPoolStore(t *testing.T) (*outboxstore.Store, context.Context) {
	t.Helper()
	ctx := context.Background()
	pool, cleanup := fixtures.NewTestDB(ctx, t)
	t.Cleanup(cleanup)
	store := outboxstore.New(pool, nil, 0)
	pool.Close()
	return store, ctx
}

func enqueueRecord(t *testing.T, store *outboxstore.Store, pool *pgcommon.Pool) domain.OutboxRecord {
	t.Helper()
	rec := makeRecord("closed.pool.event")
	err := pgcommon.RunInTx(context.Background(), pool, pgcommon.TxOptions{}, func(ctx context.Context, tx pgcommon.Tx) error {
		return store.Enqueue(ctx, tx, rec)
	})
	require.NoError(t, err)
	return rec
}

func TestOutboxStore_ClaimBatch_ClosedPool_Error(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, ctx := closedPoolStore(t)

	records, err := store.ClaimBatch(ctx, 10)
	require.Error(t, err)
	assert.Nil(t, records)
}

func TestOutboxStore_ClaimBatch_ClosedPool_AfterEnqueue_Error(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	pool, cleanup := fixtures.NewTestDB(ctx, t)
	t.Cleanup(cleanup)
	store := outboxstore.New(pool, nil, 0)
	_ = enqueueRecord(t, store, pool)
	pool.Close()

	records, err := store.ClaimBatch(ctx, 10)
	require.Error(t, err)
	assert.Nil(t, records)
}

func TestOutboxStore_MarkPublished_ClosedPool_Error(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, ctx := closedPoolStore(t)

	err := store.MarkPublished(ctx, "01926e4f-dead-7000-beef-000000000001")
	require.Error(t, err)
}

func TestOutboxStore_MarkPublished_ClosedPool_AfterEnqueue_Error(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	pool, cleanup := fixtures.NewTestDB(ctx, t)
	t.Cleanup(cleanup)
	store := outboxstore.New(pool, nil, 0)
	rec := enqueueRecord(t, store, pool)
	pool.Close()

	err := store.MarkPublished(ctx, rec.ID)
	require.Error(t, err)
}

func TestOutboxStore_MarkFailed_ClosedPool_Error(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, ctx := closedPoolStore(t)

	err := store.MarkFailed(ctx, domain.OutboxRecord{ID: "01926e4f-dead-7000-beef-000000000002"}, "error", 5, 0)
	require.Error(t, err)
}

func TestOutboxStore_MarkFailed_ClosedPool_AfterEnqueue_Error(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	pool, cleanup := fixtures.NewTestDB(ctx, t)
	t.Cleanup(cleanup)
	store := outboxstore.New(pool, nil, 0)
	rec := enqueueRecord(t, store, pool)
	pool.Close()

	err := store.MarkFailed(ctx, rec, "publish failed", 5, 0)
	require.Error(t, err)
}

func TestOutboxStore_ReprocessDeadLetters_ClosedPool_Error(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	store, ctx := closedPoolStore(t)

	n, err := store.ReprocessDeadLetters(ctx, 10)
	require.Error(t, err)
	assert.Equal(t, 0, n)
}
