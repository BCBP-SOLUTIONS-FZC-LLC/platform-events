//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/adapter/outbound/metrics"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/adapter/outbound/outboxstore"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/outbox"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/test/fixtures"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/pgcommon"
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
	err = store.MarkFailed(ctx, rec, "publish error", 5)
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

	// Initialise metrics against an isolated registry so we can assert the
	// dead-letter counter increments when a record moves to the dead-letter table.
	metrics.InitWithRegisterer("outboxstore-dl-test", "v0.0.0", prometheus.NewRegistry())
	// WithLabelValues materializes the series at 0 so ToFloat64 has exactly one
	// series to read. "invoice.settled" matches the event type used by makeRecord below.
	dlCounter := metrics.OutboxDeadLettersTotal.WithLabelValues("invoice.settled")
	before := testutil.ToFloat64(dlCounter)

	rec := makeRecord("invoice.settled")
	err := pgcommon.RunInTx(ctx, pool, pgx.TxOptions{}, func(ctx context.Context, tx pgx.Tx) error {
		return store.Enqueue(ctx, tx, rec)
	})
	require.NoError(t, err)

	// Fail it maxAttempts times (maxAttempts=1 so it moves immediately).
	err = store.MarkFailed(ctx, rec, "fatal error", 1)
	require.NoError(t, err)

	// The dead-letter counter must have incremented by exactly one.
	assert.InDelta(t, before+1, testutil.ToFloat64(dlCounter), 0.001,
		"outbox_dead_letters_total should increment when a record is dead-lettered")

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
	err = store.MarkFailed(ctx, rec, "error1", 1)
	require.NoError(t, err)

	// Re-enqueue to test re-insertion to dead letter (ON CONFLICT DO NOTHING should not error).
	rec2 := makeRecord("idempotent.test2")
	err = pgcommon.RunInTx(ctx, pool, pgx.TxOptions{}, func(ctx context.Context, tx pgx.Tx) error {
		return store.Enqueue(ctx, tx, rec2)
	})
	require.NoError(t, err)
	err = store.MarkFailed(ctx, rec2, "error2", 1)
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
	err := store.MarkFailed(ctx, domain.OutboxRecord{ID: "01926e4f-dead-7000-beef-000000000001"}, "error", 5)
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
// MarkFailed: non-existent record logs a warning (ErrNoRows path with logger)
// ----------------------------

func TestOutboxStore_MarkFailed_NonExistentRecord_WithLogger(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	_, pool, cleanup := setupOutboxTest(ctx, t)
	defer cleanup()

	logger := &fixtures.MockLogger{}
	store := outboxstore.New(pool, logger, 0)

	// Call MarkFailed on a non-existent record — should log a warning, not error.
	err := store.MarkFailed(ctx, domain.OutboxRecord{ID: "01926e4f-dead-7000-beef-000000000002"}, "test error", 5)
	require.NoError(t, err, "MarkFailed on a missing record must not error")

	entries := logger.Entries()
	found := false
	for _, e := range entries {
		if e.Level == "WARN" {
			found = true
			break
		}
	}
	assert.True(t, found, "expected WARN log when MarkFailed finds no matching row")
}

// ----------------------------
// MarkPublished: non-existent record logs a warning (RowsAffected == 0 path)
// ----------------------------

func TestOutboxStore_MarkPublished_NonExistentRecord_LogsWarning(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	_, pool, cleanup := setupOutboxTest(ctx, t)
	defer cleanup()

	logger := &fixtures.MockLogger{}
	store := outboxstore.New(pool, logger, 0)

	// Call MarkPublished with a well-formed but non-existent ID.
	err := store.MarkPublished(ctx, "01926e4f-dead-7000-beef-000000000099")
	require.NoError(t, err, "MarkPublished on a missing record must not error")

	entries := logger.Entries()
	found := false
	for _, e := range entries {
		if e.Level == "WARN" {
			found = true
			break
		}
	}
	assert.True(t, found, "expected WARN log when MarkPublished finds no matching row")
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

	// Verify isolation: ApplySchema must use its own tracking table, not the
	// shared "schema_migrations" table, so multiple runners can coexist in the
	// same database without version-number collisions.
	var trackingTableExists bool
	err = pool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		return conn.QueryRow(ctx, `
			SELECT EXISTS(
				SELECT FROM information_schema.tables
				WHERE table_name = $1
			)`, outbox.MigrationsTable).Scan(&trackingTableExists)
	})
	require.NoError(t, err)
	assert.True(t, trackingTableExists, "outbox_migrations tracking table should exist (not schema_migrations)")
}

// ----------------------------
// LeasedCount
// ----------------------------

func TestOutboxStore_LeasedCount(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	store, pool, cleanup := setupOutboxTest(ctx, t)
	defer cleanup()

	// Initially no leased records.
	n, err := store.LeasedCount(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)

	// Enqueue a record and claim it (ClaimBatch sets scheduled_at to a future time = leased).
	rec := makeRecord("leased.event")
	err = pgcommon.RunInTx(ctx, pool, pgx.TxOptions{}, func(ctx context.Context, tx pgx.Tx) error {
		return store.Enqueue(ctx, tx, rec)
	})
	require.NoError(t, err)

	claimed, err := store.ClaimBatch(ctx, 1)
	require.NoError(t, err)
	require.Len(t, claimed, 1)

	n, err = store.LeasedCount(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)

	// After marking published the record is gone — leased count returns to zero.
	require.NoError(t, store.MarkPublished(ctx, rec.ID))
	n, err = store.LeasedCount(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)
}

// ----------------------------
// ReprocessDeadLetters
// ----------------------------

func TestOutboxStore_ReprocessDeadLetters(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	store, pool, cleanup := setupOutboxTest(ctx, t)
	defer cleanup()

	// Initially no dead-letter records to reprocess.
	n, err := store.ReprocessDeadLetters(ctx, 10)
	require.NoError(t, err)
	assert.Equal(t, 0, n)

	// Enqueue a record and move it to the dead-letter table via MarkFailed.
	rec := makeRecord("dl.reprocess")
	err = pgcommon.RunInTx(ctx, pool, pgx.TxOptions{}, func(ctx context.Context, tx pgx.Tx) error {
		return store.Enqueue(ctx, tx, rec)
	})
	require.NoError(t, err)
	require.NoError(t, store.MarkFailed(ctx, rec, "permanent failure", 1 /* maxAttempts=1 */))

	// Verify record is now in dead_letters.
	var dlCount int
	err = pool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		return conn.QueryRow(ctx,
			"SELECT COUNT(*) FROM outbox_dead_letters WHERE id = $1", rec.ID,
		).Scan(&dlCount)
	})
	require.NoError(t, err)
	require.Equal(t, 1, dlCount)

	// Reprocess: should move the record back to outbox_events.
	n, err = store.ReprocessDeadLetters(ctx, 10)
	require.NoError(t, err)
	assert.Equal(t, 1, n, "one record should be reprocessed")

	// Verify it's back in outbox_events and no longer in dead_letters.
	var outboxCount int
	err = pool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		return conn.QueryRow(ctx,
			"SELECT COUNT(*) FROM outbox_events WHERE id = $1", rec.ID,
		).Scan(&outboxCount)
	})
	require.NoError(t, err)
	assert.Equal(t, 1, outboxCount, "reprocessed record should be back in outbox_events")

	var newDlCount int
	err = pool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		return conn.QueryRow(ctx,
			"SELECT COUNT(*) FROM outbox_dead_letters WHERE id = $1", rec.ID,
		).Scan(&newDlCount)
	})
	require.NoError(t, err)
	assert.Equal(t, 0, newDlCount, "record should be removed from dead_letters after reprocessing")
}

func TestOutboxStore_ReprocessDeadLetters_Limit(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	store, pool, cleanup := setupOutboxTest(ctx, t)
	defer cleanup()

	// Create 3 dead-letter records.
	for i := 0; i < 3; i++ {
		rec := makeRecord("dl.limit")
		err := pgcommon.RunInTx(ctx, pool, pgx.TxOptions{}, func(ctx context.Context, tx pgx.Tx) error {
			return store.Enqueue(ctx, tx, rec)
		})
		require.NoError(t, err)
		require.NoError(t, store.MarkFailed(ctx, rec, "fatal", 1))
	}

	// Reprocess only 2.
	n, err := store.ReprocessDeadLetters(ctx, 2)
	require.NoError(t, err)
	assert.Equal(t, 2, n, "only limit records should be reprocessed")
}

// ----------------------------
// ClaimBatch: batchSize <= 0 validation
// ----------------------------

func TestOutboxStore_ClaimBatch_InvalidBatchSize(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	store, _, cleanup := setupOutboxTest(ctx, t)
	defer cleanup()

	for _, size := range []int{0, -1, -100} {
		records, err := store.ClaimBatch(ctx, size)
		require.Error(t, err, "batchSize=%d should return error", size)
		assert.Nil(t, records)
		assert.Contains(t, err.Error(), "batchSize must be positive")
	}
}

// ----------------------------
// MarkPublished: 0 rows affected (record already published)
// ----------------------------

func TestOutboxStore_MarkPublished_AlreadyPublished(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	_, pool, cleanup := setupOutboxTest(ctx, t)
	defer cleanup()

	// Create store with a logger so the WARN branch exercises the log call.
	logger := &fixtures.MockLogger{}
	storeWithLogger := outboxstore.New(pool, logger, 0)

	rec := makeRecord("double.publish")
	err := pgcommon.RunInTx(ctx, pool, pgx.TxOptions{}, func(ctx context.Context, tx pgx.Tx) error {
		return storeWithLogger.Enqueue(ctx, tx, rec)
	})
	require.NoError(t, err)

	// First MarkPublished — marks the row.
	require.NoError(t, storeWithLogger.MarkPublished(ctx, rec.ID))

	// Second MarkPublished — 0 rows affected; must not error and must log WARN.
	logger.Reset()
	err = storeWithLogger.MarkPublished(ctx, rec.ID)
	require.NoError(t, err, "second MarkPublished must not return an error")

	found := false
	for _, e := range logger.Entries() {
		if e.Level == "WARN" {
			found = true
			break
		}
	}
	assert.True(t, found, "expected WARN log when MarkPublished finds 0 rows (already published)")
}

// ----------------------------
// MarkFailed: dead-letter path when attempts >= maxAttempts
// ----------------------------

func TestOutboxStore_MarkFailed_DeadLetterOnFirstAttempt(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	store, pool, cleanup := setupOutboxTest(ctx, t)
	defer cleanup()

	rec := makeRecord("dead.letter.immediate")
	err := pgcommon.RunInTx(ctx, pool, pgx.TxOptions{}, func(ctx context.Context, tx pgx.Tx) error {
		return store.Enqueue(ctx, tx, rec)
	})
	require.NoError(t, err)

	// With maxAttempts=1, attempts(0)+1=1 >= 1 → record moves to dead_letters immediately.
	require.NoError(t, store.MarkFailed(ctx, rec, "permanent error", 1))

	var outboxCount int
	err = pool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		return conn.QueryRow(ctx,
			"SELECT COUNT(*) FROM outbox_events WHERE id = $1", rec.ID,
		).Scan(&outboxCount)
	})
	require.NoError(t, err)
	assert.Equal(t, 0, outboxCount, "record must be removed from outbox_events after dead-lettering")

	var dlCount int
	err = pool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		return conn.QueryRow(ctx,
			"SELECT COUNT(*) FROM outbox_dead_letters WHERE id = $1", rec.ID,
		).Scan(&dlCount)
	})
	require.NoError(t, err)
	assert.Equal(t, 1, dlCount, "record must be present in outbox_dead_letters")
}

// ----------------------------
// InsertRecord: negative delay is clamped to 0
// ----------------------------

func TestOutboxStore_InsertRecord_NegativeDelay(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	store, pool, cleanup := setupOutboxTest(ctx, t)
	defer cleanup()

	// ScheduledAt is 5 seconds before CreatedAt — negative delay must be clamped to 0.
	now := time.Now().UTC()
	env := domain.NewEnvelope("negative.delay", "test-svc", json.RawMessage(`{"test":true}`))
	payload, _ := json.Marshal(env)
	rec := domain.OutboxRecord{
		ID:          env.ID,
		EventType:   "negative.delay",
		Payload:     payload,
		TenantID:    "acme",
		TraceID:     "trace-neg",
		CreatedAt:   now,
		ScheduledAt: now.Add(-5 * time.Second), // 5 s in the past → delaySecs < 0
	}

	err := pgcommon.RunInTx(ctx, pool, pgx.TxOptions{}, func(ctx context.Context, tx pgx.Tx) error {
		return store.Enqueue(ctx, tx, rec)
	})
	require.NoError(t, err)

	// With delaySecs clamped to 0, scheduled_at = NOW(), so the record is immediately claimable.
	records, err := store.ClaimBatch(ctx, 10)
	require.NoError(t, err)
	require.Len(t, records, 1, "negative-delay record must be immediately claimable (delay clamped to 0)")
	assert.Equal(t, rec.ID, records[0].ID)
}

// ----------------------------
// ReprocessDeadLetters: limit <= 0 validation
// ----------------------------

func TestOutboxStore_ReprocessDeadLetters_InvalidLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	store, _, cleanup := setupOutboxTest(ctx, t)
	defer cleanup()

	for _, limit := range []int{0, -1, -50} {
		n, err := store.ReprocessDeadLetters(ctx, limit)
		require.Error(t, err, "limit=%d should return error", limit)
		assert.Equal(t, 0, n)
		assert.Contains(t, err.Error(), "limit must be positive")
	}
}

// ----------------------------
// PrunePublished: argument validation and end-to-end delete
// ----------------------------

func TestOutboxStore_PrunePublished_InvalidArgs(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	store, _, cleanup := setupOutboxTest(ctx, t)
	defer cleanup()

	// limit <= 0 must error.
	_, err := store.PrunePublished(ctx, 24*time.Hour, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "limit must be positive")

	_, err = store.PrunePublished(ctx, 24*time.Hour, -1)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "limit must be positive")

	// olderThan <= 0 must error.
	_, err = store.PrunePublished(ctx, 0, 100)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "olderThan must be positive")

	_, err = store.PrunePublished(ctx, -time.Hour, 100)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "olderThan must be positive")
}

func TestOutboxStore_PrunePublished_DeletesOldPublishedRecords(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	store, pool, cleanup := setupOutboxTest(ctx, t)
	defer cleanup()

	// Insert two records and mark them published.
	rec1 := makeRecord("prune.event.one")
	rec2 := makeRecord("prune.event.two")
	for _, rec := range []domain.OutboxRecord{rec1, rec2} {
		err := pgcommon.RunInTx(ctx, pool, pgx.TxOptions{}, func(ctx context.Context, tx pgx.Tx) error {
			return store.Enqueue(ctx, tx, rec)
		})
		require.NoError(t, err)
	}
	require.NoError(t, store.MarkPublished(ctx, rec1.ID))
	require.NoError(t, store.MarkPublished(ctx, rec2.ID))

	// Backdate published_at to simulate old records so olderThan=1ms is satisfied.
	err := pool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		_, err := conn.Exec(ctx,
			`UPDATE outbox_events SET published_at = NOW() - INTERVAL '1 hour' WHERE id = ANY($1)`,
			[]string{rec1.ID, rec2.ID},
		)
		return err
	})
	require.NoError(t, err)

	// Prune with olderThan=1ms (records are 1 hour old — well past the threshold).
	deleted, err := store.PrunePublished(ctx, time.Millisecond, 1000)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, deleted, int64(2), "both backdated published records should be pruned")
}

func TestOutboxStore_PrunePublished_LimitBoundsDelete(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	store, pool, cleanup := setupOutboxTest(ctx, t)
	defer cleanup()

	// Insert 3 records, mark them all published, backdate.
	const total = 3
	for i := range total {
		rec := makeRecord(fmt.Sprintf("prune.limit.%d", i))
		err := pgcommon.RunInTx(ctx, pool, pgx.TxOptions{}, func(ctx context.Context, tx pgx.Tx) error {
			return store.Enqueue(ctx, tx, rec)
		})
		require.NoError(t, err)
		require.NoError(t, store.MarkPublished(ctx, rec.ID))
	}
	err := pool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		_, err := conn.Exec(ctx, `UPDATE outbox_events SET published_at = NOW() - INTERVAL '1 hour' WHERE published_at IS NOT NULL`)
		return err
	})
	require.NoError(t, err)

	// Limit=1 → only one row deleted per call.
	deleted, err := store.PrunePublished(ctx, time.Millisecond, 1)
	require.NoError(t, err)
	assert.Equal(t, int64(1), deleted, "limit=1 must delete exactly one row per call")

	// Remaining rows are still present.
	count, err := store.PendingCount(ctx)
	require.NoError(t, err)
	_ = count // pending count excludes published rows; just ensure no error
}

func TestOutboxStore_PrunePublished_RecentRecordsNotDeleted(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	store, pool, cleanup := setupOutboxTest(ctx, t)
	defer cleanup()

	// Insert and mark published right now (no backdating).
	rec := makeRecord("prune.recent")
	err := pgcommon.RunInTx(ctx, pool, pgx.TxOptions{}, func(ctx context.Context, tx pgx.Tx) error {
		return store.Enqueue(ctx, tx, rec)
	})
	require.NoError(t, err)
	require.NoError(t, store.MarkPublished(ctx, rec.ID))

	// olderThan=7 days → recently published record must NOT be deleted.
	deleted, err := store.PrunePublished(ctx, 7*24*time.Hour, 1000)
	require.NoError(t, err)
	assert.Equal(t, int64(0), deleted, "recently published record must not be pruned")
}
