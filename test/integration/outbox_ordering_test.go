//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/adapter/outbound/outboxstore"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/events"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/outbox"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/test/fixtures"
)

// Per-key ordering: each key's records are claimable one at a time, oldest
// first; a failing head holds its key until published or dead-lettered;
// unkeyed records and other keys are unaffected.
func TestOutboxStore_Ordering(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := fixtures.NewTestDB(ctx, t)
	defer cleanup()
	store := outboxstore.New(pool, nil, 0)

	enqueue := func(eventType, key string) string {
		env := events.NewEnvelope(eventType, "svc", json.RawMessage(`{}`))
		require.NoError(t, pgcommon.RunInTx(ctx, pool, pgcommon.TxOptions{}, func(ctx context.Context, tx pgcommon.Tx) error {
			if key == "" {
				return outbox.Enqueue(ctx, tx, env)
			}
			return outbox.EnqueueOrdered(ctx, tx, env, key)
		}))
		return env.ID
	}
	a1 := enqueue("user.created", "user/a")
	a2 := enqueue("user.updated", "user/a")
	a3 := enqueue("user.deleted", "user/a")
	b1 := enqueue("user.created", "user/b")
	free := enqueue("audit.logged", "")

	ids := func() []string {
		recs, err := store.ClaimBatch(ctx, 50)
		require.NoError(t, err)
		var out []string
		for _, r := range recs {
			out = append(out, r.ID)
		}
		sort.Strings(out)
		return out
	}
	sorted := func(v ...string) []string { sort.Strings(v); return v }
	blocked := func() int64 {
		n, err := store.BlockedCount(ctx)
		require.NoError(t, err)
		return n
	}

	assert.EqualValues(t, 2, blocked(), "a2 and a3 wait behind a1")
	assert.Equal(t, sorted(a1, b1, free), ids(), "only each key's head, plus unkeyed records")
	assert.Empty(t, ids(), "heads are leased; their successors still wait")

	// a1 fails and is released: still the head, a2 still waiting.
	require.NoError(t, store.ReleaseLease(ctx, a1, "throttled", 0))
	assert.Equal(t, []string{a1}, ids())

	// a1 published → a2 promoted in the same transaction.
	require.NoError(t, store.MarkPublished(ctx, a1))
	assert.EqualValues(t, 1, blocked())
	assert.Equal(t, []string{a2}, ids())

	// a2 dead-letters → a3 promoted; the dead letter keeps its key.
	rec2 := makeRecord("user.updated")
	rec2.ID, rec2.OrderingKey = a2, "user/a"
	require.NoError(t, store.MarkFailed(ctx, rec2, "permanent", 1, 0))
	assert.Equal(t, []string{a3}, ids())
	var key string
	require.NoError(t, pool.WithConn(ctx, func(ctx context.Context, conn *pgcommon.Conn) error {
		return conn.QueryRow(ctx, `SELECT ordering_key FROM outbox_dead_letters WHERE id = $1`, a2).Scan(&key)
	}))
	assert.Equal(t, "user/a", key)

	// Replay: a2 keeps its key and joins the back — it waits behind a3.
	n, err := store.ReprocessDeadLetters(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	assert.EqualValues(t, 1, blocked(), "replayed a2 waits behind a3")
	require.NoError(t, store.MarkPublished(ctx, a3))
	assert.Equal(t, []string{a2}, ids())
}

// Order is INSERT order (a sequence), not transaction start: a transaction
// that started first but enqueued after another committed comes second.
func TestOutboxStore_Ordering_CommitOrderNotTxStart(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := fixtures.NewTestDB(ctx, t)
	defer cleanup()
	store := outboxstore.New(pool, nil, 0)

	first := events.NewEnvelope("user.updated", "svc", json.RawMessage(`{}`))
	second := events.NewEnvelope("user.updated", "svc", json.RawMessage(`{}`))
	t2Started, t1Done := make(chan struct{}), make(chan struct{})
	errs := make(chan error, 1)
	go func() { // T2: starts first (fixing its NOW()), enqueues last.
		errs <- pgcommon.RunInTx(ctx, pool, pgcommon.TxOptions{}, func(ctx context.Context, tx pgcommon.Tx) error {
			if _, err := tx.Exec(ctx, `SELECT NOW()`); err != nil {
				return err
			}
			close(t2Started)
			<-t1Done
			return outbox.EnqueueOrdered(ctx, tx, second, "user/a")
		})
	}()
	<-t2Started
	time.Sleep(20 * time.Millisecond) // T1's NOW() is later than T2's
	require.NoError(t, pgcommon.RunInTx(ctx, pool, pgcommon.TxOptions{}, func(ctx context.Context, tx pgcommon.Tx) error {
		return outbox.EnqueueOrdered(ctx, tx, first, "user/a")
	}))
	close(t1Done)
	require.NoError(t, <-errs)

	recs, err := store.ClaimBatch(ctx, 50)
	require.NoError(t, err)
	require.Len(t, recs, 1)
	assert.Equal(t, first.ID, recs[0].ID, "the record committed first is the head")
}

// A record enqueued while its head was being published (its insert saw the
// head unpublished, but the head's promotion ran before it committed) is
// promoted by the PromoteWaiting sweep.
func TestOutboxStore_PromoteWaiting(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := fixtures.NewTestDB(ctx, t)
	defer cleanup()
	store := outboxstore.New(pool, nil, 0)
	var ids []string
	for range 2 {
		env := events.NewEnvelope("user.updated", "svc", json.RawMessage(`{}`))
		require.NoError(t, pgcommon.RunInTx(ctx, pool, pgcommon.TxOptions{}, func(ctx context.Context, tx pgcommon.Tx) error {
			return outbox.EnqueueOrdered(ctx, tx, env, "user/a")
		}))
		ids = append(ids, env.ID)
	}
	// Publish the head behind the store's back (no promotion), as in the race.
	require.NoError(t, pool.WithConn(ctx, func(ctx context.Context, conn *pgcommon.Conn) error {
		_, err := conn.Exec(ctx, `UPDATE outbox_events SET published_at = NOW() WHERE id = $1`, ids[0])
		return err
	}))
	recs, err := store.ClaimBatch(ctx, 50)
	require.NoError(t, err)
	assert.Empty(t, recs, "still waiting")
	n, err := store.PromoteWaiting(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 1, n)
	recs, err = store.ClaimBatch(ctx, 50)
	require.NoError(t, err)
	require.Len(t, recs, 1)
	assert.Equal(t, ids[1], recs[0].ID)
}

// orderRecorder is an events.Publisher that records publish order and fails
// the first attempt of one event (a transient error), so it must be retried
// before the key's later events go out.
type orderRecorder struct {
	mu       sync.Mutex
	order    []string
	failOnce string
	failed   bool
}

func (p *orderRecorder) Publish(ctx context.Context, env events.Envelope[json.RawMessage]) error {
	return p.PublishBatch(ctx, []events.Envelope[json.RawMessage]{env})
}

func (p *orderRecorder) PublishBatch(_ context.Context, envs []events.Envelope[json.RawMessage]) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	var be events.BatchError
	for _, e := range envs {
		if e.ID == p.failOnce && !p.failed {
			p.failed = true
			be.Failures = append(be.Failures, events.BatchFailure{ID: e.ID, Code: "Throttled", Retryable: true})
			continue
		}
		p.order = append(p.order, e.ID)
	}
	if len(be.Failures) > 0 {
		return &be
	}
	return nil
}

// End to end: a key's events are published in enqueue order even when the
// head fails once.
func TestRunner_Ordering_EndToEnd(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := fixtures.NewTestDB(ctx, t)
	defer cleanup()
	var want []string
	for range 5 {
		env := events.NewEnvelope("user.updated", "svc", json.RawMessage(`{}`))
		require.NoError(t, pgcommon.RunInTx(ctx, pool, pgcommon.TxOptions{}, func(ctx context.Context, tx pgcommon.Tx) error {
			return outbox.EnqueueOrdered(ctx, tx, env, "user/a")
		}))
		want = append(want, env.ID)
	}
	pub := &orderRecorder{failOnce: want[0]}
	r, err := outbox.NewRunner(outbox.Config{Pool: pool, Publisher: pub,
		PollInterval: 50 * time.Millisecond, RetryBackoff: 10 * time.Millisecond, MaxRetryBackoff: 20 * time.Millisecond})
	require.NoError(t, err)
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { defer close(done); _ = r.Start(runCtx) }()
	require.Eventually(t, func() bool {
		pub.mu.Lock()
		defer pub.mu.Unlock()
		return len(pub.order) == 5
	}, 20*time.Second, 20*time.Millisecond)
	cancel()
	<-done
	assert.Equal(t, want, pub.order, "published in enqueue order despite the head's failed first attempt")
}
