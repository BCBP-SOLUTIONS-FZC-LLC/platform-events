//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/adapter/outbound/outboxstore"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/test/fixtures"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/pgcommon"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupOutboxTest(ctx context.Context, t *testing.T) (*outboxstore.Store, *pgcommon.Pool, func()) {
	t.Helper()
	pool, cleanup := fixtures.NewTestDB(ctx, t)
	store := outboxstore.New(pool, nil, 0)
	return store, pool, cleanup
}

func makeRecord(eventType string) domain.OutboxRecord {
	env := domain.NewEnvelope(eventType, "test-svc", json.RawMessage(`{"test":true}`))
	payload, _ := json.Marshal(env)
	now := time.Now().UTC()
	return domain.OutboxRecord{
		ID:          env.ID,
		EventType:   eventType,
		Payload:     payload,
		TenantID:    "acme",
		TraceID:     "trace-001",
		CreatedAt:   now,
		ScheduledAt: now,
	}
}

// ----------------------------
// Enqueue + ClaimBatch round trip
// ----------------------------

func TestOutboxStore_EnqueueAndClaimBatch(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	store, pool, cleanup := setupOutboxTest(ctx, t)
	defer cleanup()

	rec := makeRecord("order.created")

	// Enqueue within a transaction.
	err := pgcommon.RunInTx(ctx, pool, pgx.TxOptions{}, func(ctx context.Context, tx pgx.Tx) error {
		return store.Enqueue(ctx, tx, rec)
	})
	require.NoError(t, err)

	// ClaimBatch should return the record.
	records, err := store.ClaimBatch(ctx, 10)
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, rec.ID, records[0].ID)
	assert.Equal(t, "order.created", records[0].EventType)
	assert.Equal(t, "acme", records[0].TenantID)
	assert.Equal(t, "trace-001", records[0].TraceID)
}

// ----------------------------
// MarkPublished sets published_at
// ----------------------------

func TestOutboxStore_MarkPublished(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	store, pool, cleanup := setupOutboxTest(ctx, t)
	defer cleanup()

	rec := makeRecord("payment.processed")
	err := pgcommon.RunInTx(ctx, pool, pgx.TxOptions{}, func(ctx context.Context, tx pgx.Tx) error {
		return store.Enqueue(ctx, tx, rec)
	})
	require.NoError(t, err)

	// Mark as published.
	err = store.MarkPublished(ctx, rec.ID)
	require.NoError(t, err)

	// After MarkPublished, ClaimBatch should return nothing (published_at is set).
	records, err := store.ClaimBatch(ctx, 10)
	require.NoError(t, err)
	assert.Empty(t, records)
}

// ----------------------------
// MarkFailed increments attempts
// ----------------------------

func TestOutboxStore_MarkFailed_IncrementsAttempts(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	store, pool, cleanup := setupOutboxTest(ctx, t)
	defer cleanup()

	rec := makeRecord("user.registered")
	err := pgcommon.RunInTx(ctx, pool, pgx.TxOptions{}, func(ctx context.Context, tx pgx.Tx) error {
		return store.Enqueue(ctx, tx, rec)
	})
	require.NoError(t, err)

	// Mark as failed (maxAttempts = 5, so we won't move to dead letter yet).
	err = store.MarkFailed(ctx, rec.ID, "publish error", 5)
	require.NoError(t, err)

	// Record should still be in outbox_events with attempts=1.
	var attempts int
	var lastError string
	err = pool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		return conn.QueryRow(ctx,
			"SELECT attempts, last_error FROM outbox_events WHERE id = $1", rec.ID,
		).Scan(&attempts, &lastError)
	})
	require.NoError(t, err)
	assert.Equal(t, 1, attempts)
	assert.Equal(t, "publish error", lastError)
}

// ----------------------------
// MarkFailed at maxAttempts moves to dead_letters
// ----------------------------

func TestOutboxStore_MarkFailed_MovesToDeadLetter(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	store, pool, cleanup := setupOutboxTest(ctx, t)
	defer cleanup()

	rec := makeRecord("invoice.settled")
	err := pgcommon.RunInTx(ctx, pool, pgx.TxOptions{}, func(ctx context.Context, tx pgx.Tx) error {
		return store.Enqueue(ctx, tx, rec)
	})
	require.NoError(t, err)

	// Fail it maxAttempts times (maxAttempts=1 so it moves immediately).
	err = store.MarkFailed(ctx, rec.ID, "fatal error", 1)
	require.NoError(t, err)

	// Record should NOT be in outbox_events.
	var count int
	err = pool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		return conn.QueryRow(ctx,
			"SELECT COUNT(*) FROM outbox_events WHERE id = $1", rec.ID,
		).Scan(&count)
	})
	require.NoError(t, err)
	assert.Equal(t, 0, count, "record should be deleted from outbox_events")

	// Record should be in outbox_dead_letters.
	var dlCount int
	err = pool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		return conn.QueryRow(ctx,
			"SELECT COUNT(*) FROM outbox_dead_letters WHERE id = $1", rec.ID,
		).Scan(&dlCount)
	})
	require.NoError(t, err)
	assert.Equal(t, 1, dlCount, "record should be in outbox_dead_letters")
}

// ----------------------------
// ClaimBatch respects batchSize limit
// ----------------------------

func TestOutboxStore_ClaimBatch_RespectsLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	store, pool, cleanup := setupOutboxTest(ctx, t)
	defer cleanup()

	// Enqueue 5 records.
	for i := 0; i < 5; i++ {
		rec := makeRecord("batch.event")
		err := pgcommon.RunInTx(ctx, pool, pgx.TxOptions{}, func(ctx context.Context, tx pgx.Tx) error {
			return store.Enqueue(ctx, tx, rec)
		})
		require.NoError(t, err)
	}

	// ClaimBatch with limit 3.
	records, err := store.ClaimBatch(ctx, 3)
	require.NoError(t, err)
	assert.Len(t, records, 3)
}

// ----------------------------
// MarkFailed idempotent for dead letter (ON CONFLICT DO NOTHING)
// ----------------------------

func TestOutboxStore_MarkFailed_DeadLetter_Idempotent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	store, pool, cleanup := setupOutboxTest(ctx, t)
	defer cleanup()

	rec := makeRecord("idempotent.test")
	err := pgcommon.RunInTx(ctx, pool, pgx.TxOptions{}, func(ctx context.Context, tx pgx.Tx) error {
		return store.Enqueue(ctx, tx, rec)
	})
	require.NoError(t, err)

	// Move to dead letter.
	err = store.MarkFailed(ctx, rec.ID, "error1", 1)
	require.NoError(t, err)

	// Re-enqueue to test re-insertion to dead letter (ON CONFLICT DO NOTHING should not error).
	rec2 := makeRecord("idempotent.test2")
	err = pgcommon.RunInTx(ctx, pool, pgx.TxOptions{}, func(ctx context.Context, tx pgx.Tx) error {
		return store.Enqueue(ctx, tx, rec2)
	})
	require.NoError(t, err)
	err = store.MarkFailed(ctx, rec2.ID, "error2", 1)
	require.NoError(t, err)

	// Both records should be in dead letters.
	var count int
	err = pool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		return conn.QueryRow(ctx,
			"SELECT COUNT(*) FROM outbox_dead_letters",
		).Scan(&count)
	})
	require.NoError(t, err)
	assert.Equal(t, 2, count)
}

// ----------------------------
// MarkFailed: record not found returns error
// ----------------------------

func TestOutboxStore_MarkFailed_RecordNotFound(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	store, _, cleanup := setupOutboxTest(ctx, t)
	defer cleanup()

	// A valid UUID that does not exist in the table. MarkFailed now returns nil
	// when the record is absent (it was already published or removed by a
	// concurrent runner) — this is logged at WARN, not propagated as an error.
	err := store.MarkFailed(ctx, "01926e4f-dead-7000-beef-000000000001", "error", 5)
	require.NoError(t, err, "non-existent record should return nil — treated as already-handled")
}

func TestOutboxStore_PendingCount(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	store, pool, cleanup := setupOutboxTest(ctx, t)
	defer cleanup()

	// Initially no pending records.
	n, err := store.PendingCount(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)

	// Enqueue one record.
	rec := makeRecord("pending.count")
	err = pgcommon.RunInTx(ctx, pool, pgx.TxOptions{}, func(ctx context.Context, tx pgx.Tx) error {
		return store.Enqueue(ctx, tx, rec)
	})
	require.NoError(t, err)

	n, err = store.PendingCount(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)

	// Mark published — should not appear in pending count.
	require.NoError(t, store.MarkPublished(ctx, rec.ID))
	n, err = store.PendingCount(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)
}

// ----------------------------
// ClaimBatch: empty result (no records)
// ----------------------------

func TestOutboxStore_ClaimBatch_Empty(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	store, _, cleanup := setupOutboxTest(ctx, t)
	defer cleanup()

	records, err := store.ClaimBatch(ctx, 10)
	require.NoError(t, err)
	assert.Empty(t, records)
}

// ----------------------------
// schema.ApplySchema tested via NewTestDB fixture
// ----------------------------

func TestApplySchema_CreatesOutboxTables(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	// NewTestDB already calls ApplySchema — verify the tables exist.
	pool, cleanup := fixtures.NewTestDB(ctx, t)
	defer cleanup()

	var outboxTableExists bool
	err := pool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		return conn.QueryRow(ctx, `
			SELECT EXISTS(
				SELECT FROM information_schema.tables
				WHERE table_name = 'outbox_events'
			)`).Scan(&outboxTableExists)
	})
	require.NoError(t, err)
	assert.True(t, outboxTableExists, "outbox_events table should exist")

	var dlTableExists bool
	err = pool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		return conn.QueryRow(ctx, `
			SELECT EXISTS(
				SELECT FROM information_schema.tables
				WHERE table_name = 'outbox_dead_letters'
			)`).Scan(&dlTableExists)
	})
	require.NoError(t, err)
	assert.True(t, dlTableExists, "outbox_dead_letters table should exist")
}
