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

// Strict ordering: each key's records are claimed one at a time, oldest
// first; a failing head blocks its key until published or dead-lettered;
// unkeyed records and other keys are unaffected.
func TestOutboxStore_StrictOrdering(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := fixtures.NewTestDB(ctx, t)
	defer cleanup()
	store := outboxstore.New(pool, nil, 0)
	store.SetStrictOrdering(true)

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

	blocked, err := store.BlockedCount(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 2, blocked, "a2 and a3 wait behind a1")

	assert.Equal(t, sorted(a1, b1, free), ids(), "only each key's head, plus unkeyed records")
	assert.Empty(t, ids(), "heads are leased; their successors stay blocked")

	// a1 fails and is released: still the head, a2 still blocked.
	require.NoError(t, store.ReleaseLease(ctx, a1, "throttled", 0))
	assert.Equal(t, []string{a1}, ids())

	// a1 published → a2 is next.
	require.NoError(t, store.MarkPublished(ctx, a1))
	assert.Equal(t, []string{a2}, ids())

	// a2 dead-letters → it no longer blocks a3; the dead letter keeps its key.
	rec2 := makeRecord("user.updated")
	rec2.ID, rec2.OrderingKey = a2, "user/a"
	require.NoError(t, store.MarkFailed(ctx, rec2, "permanent", 1, 0))
	assert.Equal(t, []string{a3}, ids())
	var key string
	require.NoError(t, pool.WithConn(ctx, func(ctx context.Context, conn *pgcommon.Conn) error {
		return conn.QueryRow(ctx, `SELECT ordering_key FROM outbox_dead_letters WHERE id = $1`, a2).Scan(&key)
	}))
	assert.Equal(t, "user/a", key)

	// Replay restores the key; it goes behind the key's newer records.
	n, err := store.ReprocessDeadLetters(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.NoError(t, pool.WithConn(ctx, func(ctx context.Context, conn *pgcommon.Conn) error {
		return conn.QueryRow(ctx, `SELECT ordering_key FROM outbox_events WHERE id = $1`, a2).Scan(&key)
	}))
	assert.Equal(t, "user/a", key)
	blocked, err = store.BlockedCount(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 1, blocked, "the replayed a2 waits behind a3 (leased, unpublished)")
}

// Without strict ordering the key is stored and ignored.
func TestOutboxStore_OrderingKeyIgnoredWhenNotStrict(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := fixtures.NewTestDB(ctx, t)
	defer cleanup()
	store := outboxstore.New(pool, nil, 0)
	for range 3 {
		env := events.NewEnvelope("user.updated", "svc", json.RawMessage(`{}`))
		require.NoError(t, pgcommon.RunInTx(ctx, pool, pgcommon.TxOptions{}, func(ctx context.Context, tx pgcommon.Tx) error {
			return outbox.EnqueueOrdered(ctx, tx, env, "user/a")
		}))
	}
	recs, err := store.ClaimBatch(ctx, 50)
	require.NoError(t, err)
	assert.Len(t, recs, 3)
	for _, r := range recs {
		assert.Equal(t, "user/a", r.OrderingKey)
	}
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

// End to end: with strict ordering, a key's events are published in enqueue
// order even when the head fails once.
func TestRunner_StrictOrdering_EndToEnd(t *testing.T) {
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
	r, err := outbox.NewRunner(outbox.Config{Pool: pool, Publisher: pub, StrictOrdering: true,
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
