//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/events"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/outbox"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/test/fixtures"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"
)

// TestOutboxRunner_EndToEnd tests the full outbox flow:
// Enqueue inside a transaction → Runner polls → Publisher receives the message.
func TestOutboxRunner_EndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool, cleanup := fixtures.NewTestDB(ctx, t)
	defer cleanup()

	mockPub := &fixtures.MockPublisher{}
	pub := events.NewPublisherFromPort(mockPub)

	runner, err := outbox.NewRunner(outbox.Config{
		Pool:         pool,
		Publisher:    pub,
		PollInterval: 100 * time.Millisecond,
		BatchSize:    10,
		MaxAttempts:  3,
	})
	require.NoError(t, err)

	runnerCtx, runnerCancel := context.WithCancel(ctx)
	defer func() {
		runnerCancel()
		_ = runner.Stop()
	}()
	go func() { _ = runner.Start(runnerCtx) }()

	// Enqueue an event inside a transaction.
	env := events.NewEnvelope("order.completed", "billing", json.RawMessage(`{"amount":99}`),
		events.WithTenantID("acme"),
		events.WithTraceID("trace-e2e"),
	)

	err = pgcommon.RunInTx(ctx, pool, pgcommon.TxOptions{}, func(ctx context.Context, tx pgcommon.Tx) error {
		return outbox.Enqueue(ctx, tx, env)
	})
	require.NoError(t, err)

	// Wait for runner to publish the event.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if len(mockPub.Published()) >= 1 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	published := mockPub.Published()
	require.NotEmpty(t, published, "publisher should have been called")
	assert.Equal(t, env.ID, published[0].ID)
	assert.Equal(t, "order.completed", published[0].Type)
	assert.Equal(t, "acme", published[0].TenantID)
}

// TestOutboxEnqueue_WithTransaction verifies that Enqueue is transactional:
// rolling back the transaction prevents the record from appearing.
func TestOutboxEnqueue_TransactionRollback(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	pool, cleanup := fixtures.NewTestDB(ctx, t)
	defer cleanup()

	env := events.NewEnvelope("rollback.test", "svc", json.RawMessage(`{}`))

	// Enqueue and rollback.
	err := pgcommon.RunInTx(ctx, pool, pgcommon.TxOptions{}, func(ctx context.Context, tx pgcommon.Tx) error {
		if err := outbox.Enqueue(ctx, tx, env); err != nil {
			return err
		}
		return errors.New("force rollback")
	})
	// RunInTx may or may not return error on forced rollback — just check nothing published.
	_ = err

	mockPub := &fixtures.MockPublisher{}
	pub := events.NewPublisherFromPort(mockPub)

	runner, err := outbox.NewRunner(outbox.Config{
		Pool:         pool,
		Publisher:    pub,
		PollInterval: 50 * time.Millisecond,
	})
	require.NoError(t, err)

	runnerCtx, runnerCancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer runnerCancel()
	_ = runner.Start(runnerCtx)

	// Nothing should have been published (the enqueue was rolled back).
	assert.Empty(t, mockPub.Published())
}
