//go:build integration

package integration_test

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/migrate"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/adapter/outbound/metrics"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/adapter/outbound/outboxstore"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/events"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/outbox"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/test/fixtures"
)

const outboxMigrationsDir = "../../pkg/outbox/migrations"

// migratedDB returns a pool on a fresh database with the outbox schema and a
// runner that steps the outbox migrations (same tracking table as ApplySchema).
func migratedDB(ctx context.Context, t *testing.T) (*pgcommon.Pool, *migrate.Runner) {
	t.Helper()
	dsn, drop := fixtures.NewEmptyTestDB(ctx, t)
	t.Cleanup(drop)
	require.NoError(t, outbox.ApplySchema(ctx, &migrate.Runner{DSN: dsn}))
	pool, err := pgcommon.NewPool(ctx, pgcommon.Config{DSN: dsn})
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return pool, &migrate.Runner{FS: os.DirFS(outboxMigrationsDir), DSN: dsn + "&x-migrations-table=" + outbox.MigrationsTable}
}

func queryInt(ctx context.Context, t *testing.T, pool *pgcommon.Pool, sql string, args ...any) int64 {
	t.Helper()
	var n int64
	require.NoError(t, pool.WithConn(ctx, func(ctx context.Context, conn *pgcommon.Conn) error {
		return conn.QueryRow(ctx, sql, args...).Scan(&n)
	}))
	return n
}

// Rolling migration 010 back must not strand ordered records waiting at
// 'infinity' — the pre-010 claim query would never select them.
func TestMigration010Down_ReleasesWaitingOrderedRecords(t *testing.T) {
	ctx := context.Background()
	pool, steps := migratedDB(ctx, t)
	store := outboxstore.New(pool, nil, 0)

	head, next := makeRecord("ordered.down"), makeRecord("ordered.down")
	head.OrderingKey, next.OrderingKey = "user/1", "user/1"
	require.NoError(t, pgcommon.RunInTx(ctx, pool, pgcommon.TxOptions{}, func(ctx context.Context, tx pgcommon.Tx) error {
		if err := store.Enqueue(ctx, tx, head); err != nil {
			return err
		}
		return store.Enqueue(ctx, tx, next)
	}))
	require.EqualValues(t, 1, queryInt(ctx, t, pool, `SELECT count(*) FROM outbox_events WHERE scheduled_at = 'infinity'`),
		"the second record waits behind its key's head")

	require.NoError(t, steps.Down(ctx, 2)) // 011, then 010
	assert.EqualValues(t, 0, queryInt(ctx, t, pool, `SELECT count(*) FROM outbox_events WHERE scheduled_at = 'infinity'`),
		"no record may be left at 'infinity' after the rollback")
	assert.EqualValues(t, 2, queryInt(ctx, t, pool, `SELECT count(*) FROM outbox_events WHERE published_at IS NULL AND scheduled_at <= NOW()`),
		"both records are claimable by the pre-010 claim query")

	require.NoError(t, steps.Up(ctx), "the schema migrates up again")
}

// Re-running 003 (e.g. after moving the tracking table to outbox_migrations)
// leaves an index of the target shape alone instead of dropping and
// rebuilding it under an ACCESS EXCLUSIVE lock; an old-shape index is rebuilt.
func TestMigration003_RebuildsOnlyWhenShapeDiffers(t *testing.T) {
	ctx := context.Background()
	pool, _ := migratedDB(ctx, t)
	sql, err := os.ReadFile(outboxMigrationsDir + "/003_optimize_outbox_index.up.sql")
	require.NoError(t, err)
	exec := func(stmt string) {
		require.NoError(t, pool.WithConn(ctx, func(ctx context.Context, conn *pgcommon.Conn) error {
			_, err := conn.Exec(ctx, stmt)
			return err
		}))
	}
	oid := func() int64 {
		return queryInt(ctx, t, pool, `SELECT 'idx_outbox_events_pending'::regclass::oid::bigint`)
	}
	before := oid()
	exec(string(sql))
	assert.Equal(t, before, oid(), "an index already of the target shape must not be rebuilt")

	exec(`DROP INDEX idx_outbox_events_pending; CREATE INDEX idx_outbox_events_pending ON outbox_events (id) WHERE published_at IS NULL`)
	exec(string(sql))
	var def string
	require.NoError(t, pool.WithConn(ctx, func(ctx context.Context, conn *pgcommon.Conn) error {
		return conn.QueryRow(ctx, `SELECT pg_get_indexdef('idx_outbox_events_pending'::regclass)`).Scan(&def)
	}))
	assert.Contains(t, def, "(scheduled_at, id) WHERE (published_at IS NULL)")

	// An INVALID index of the right shape (a failed CREATE INDEX
	// CONCURRENTLY) serves no queries and is rebuilt.
	exec(`UPDATE pg_index SET indisvalid = false WHERE indexrelid = 'idx_outbox_events_pending'::regclass`)
	invalid := oid()
	exec(string(sql))
	assert.NotEqual(t, invalid, oid(), "an INVALID index must be rebuilt")
	assert.EqualValues(t, 1, queryInt(ctx, t, pool, `SELECT count(*) FROM pg_index WHERE indexrelid = 'idx_outbox_events_pending'::regclass AND indisvalid`))
}

// Migration 011 replaces the failed_at-only dead-letter index with
// (failed_at, id), the list / replay / discard sort order.
func TestMigration011_DeadLetterIndex(t *testing.T) {
	ctx := context.Background()
	pool, _ := migratedDB(ctx, t)
	assert.EqualValues(t, 1, queryInt(ctx, t, pool, `SELECT count(*) FROM pg_indexes WHERE indexname = 'idx_outbox_dead_letters_failed_at_id'`))
	assert.EqualValues(t, 0, queryInt(ctx, t, pool, `SELECT count(*) FROM pg_indexes WHERE indexname = 'idx_outbox_dead_letters_failed_at'`))
}

// A dead letter whose ID is back in outbox_events (re-enqueued by the
// application) no longer fails every replay on the primary key: it stays in
// outbox_dead_letters and the other dead letters are replayed.
func TestReprocessDeadLetters_SkipsIDAlreadyInOutbox(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := fixtures.NewTestDB(ctx, t)
	defer cleanup()
	logger := &fixtures.MockLogger{}
	store := outboxstore.New(pool, logger, 0)

	colliding, other := makeRecord("dl.collide"), makeRecord("dl.collide")
	enqueueAndDeadLetter(ctx, t, store, pool, colliding) // oldest failure: selected first
	enqueueAndDeadLetter(ctx, t, store, pool, other)
	require.NoError(t, pgcommon.RunInTx(ctx, pool, pgcommon.TxOptions{}, func(ctx context.Context, tx pgcommon.Tx) error {
		return store.Enqueue(ctx, tx, colliding) // the application re-enqueued the same ID
	}))

	for _, replay := range []func() (int, error){
		func() (int, error) { return store.ReprocessDeadLetters(ctx, 1) },
		func() (int, error) { return store.ReprocessDeadLetters(ctx, 10) },
	} {
		_, err := replay()
		require.NoError(t, err, "a colliding ID must not fail the replay")
	}
	assert.EqualValues(t, 1, queryInt(ctx, t, pool, `SELECT count(*) FROM outbox_dead_letters WHERE id = $1`, colliding.ID),
		"the colliding dead letter stays for inspection")
	assert.EqualValues(t, 0, queryInt(ctx, t, pool, `SELECT count(*) FROM outbox_dead_letters WHERE id = $1`, other.ID),
		"the other dead letter was replayed")
	assert.EqualValues(t, 1, queryInt(ctx, t, pool, `SELECT count(*) FROM outbox_events WHERE id = $1`, other.ID))

	warned := false
	for _, e := range logger.Entries() {
		if e.Level == "WARN" && strings.Contains(e.Message, "dead letters left in place") {
			warned = true
		}
	}
	assert.True(t, warned, "the left-behind dead letter is reported")
}

// MarkFailed on a record another runner already published (lease expired
// mid-publish) neither retries nor dead-letters it, so it counts no retry.
func TestMarkFailed_AlreadyPublished_CountsNoRetry(t *testing.T) {
	ctx := context.Background()
	store, pool, cleanup := setupOutboxTest(ctx, t)
	defer cleanup()
	prev := metrics.CurrentPlatform()
	t.Cleanup(func() { metrics.ReplacePlatform(prev) })
	_, err := events.InitMetrics(events.MetricsIdentity{Domain: "iam", Service: "it", Environment: "test"}, prometheus.NewRegistry())
	require.NoError(t, err)
	retries := metrics.CurrentPlatform().Retries.WithLabelValues("outbox_publish", "mark.failed.gone")

	rec := makeRecord("mark.failed.gone")
	require.NoError(t, pgcommon.RunInTx(ctx, pool, pgcommon.TxOptions{}, func(ctx context.Context, tx pgcommon.Tx) error {
		return store.Enqueue(ctx, tx, rec)
	}))
	require.NoError(t, store.MarkPublished(ctx, rec.ID))
	require.NoError(t, store.MarkFailed(ctx, rec, "late failure", 5, 0))
	assert.Zero(t, testutil.ToFloat64(retries))

	live := makeRecord("mark.failed.gone")
	require.NoError(t, pgcommon.RunInTx(ctx, pool, pgcommon.TxOptions{}, func(ctx context.Context, tx pgcommon.Tx) error {
		return store.Enqueue(ctx, tx, live)
	}))
	require.NoError(t, store.MarkFailed(ctx, live, "real failure", 5, 0))
	assert.Equal(t, 1.0, testutil.ToFloat64(retries), "a real retry is still counted")
}
