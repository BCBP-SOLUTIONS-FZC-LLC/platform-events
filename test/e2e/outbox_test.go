//go:build e2e

package e2e_test

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/events"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/outbox"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/test/fixtures"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/pgcommon"
)

// TestOutbox_EnqueueAndDeliver_EndToEnd verifies the full transactional outbox flow:
//  1. An event is enqueued inside a Postgres transaction via outbox.Enqueue.
//  2. The outbox runner polls the DB and publishes to SNS.
//  3. The SQS consumer receives the event.
func TestOutbox_EnqueueAndDeliver_EndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test")
	}
	setAWSEnv(t)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// Start infrastructure.
	ls := fixtures.StartLocalStack(ctx, t)
	pool, cleanupDB := fixtures.NewTestDB(ctx, t)
	defer cleanupDB()

	topicARN := ls.CreateTopic(ctx, t, "outbox-e2e-topic")
	queueURL := ls.CreateQueue(ctx, t, "outbox-e2e-queue")
	ls.SubscribeQueueToTopic(ctx, t, topicARN, queueURL)

	// Create the SNS publisher the runner will use.
	snsPub, err := events.NewSNSPublisher(events.SNSConfig{
		TopicARN:    topicARN,
		Region:      "us-east-1",
		EndpointURL: ls.EndpointURL,
	})
	require.NoError(t, err)

	// Start the outbox runner.
	runner, err := outbox.NewRunner(outbox.Config{
		Pool:         pool,
		Publisher:    snsPub,
		PollInterval: 500 * time.Millisecond,
		BatchSize:    10,
		MaxAttempts:  3,
	})
	require.NoError(t, err)
	runnerCtx, runnerCancel := context.WithCancel(ctx)
	defer runnerCancel()
	go func() { _ = runner.Start(runnerCtx) }()

	// Start the SQS consumer.
	received := make(chan events.Envelope[json.RawMessage], 10)
	handler := func(_ context.Context, env events.Envelope[json.RawMessage]) error {
		received <- env
		return nil
	}
	consumer, err := events.NewSQSConsumer(events.SQSConfig{
		QueueURL:    queueURL,
		Region:      "us-east-1",
		EndpointURL: ls.EndpointURL,
		WaitSeconds: 1,
		MaxMessages: 1,
	}, handler)
	require.NoError(t, err)
	consumerCtx, consumerCancel := context.WithCancel(ctx)
	defer func() {
		consumerCancel()
		_ = consumer.Stop()
	}()
	go func() { _ = consumer.Start(consumerCtx) }()

	// Enqueue the event inside a transaction.
	env := events.NewEnvelope("order.placed", "billing-svc",
		json.RawMessage(`{"amount":99}`),
		events.WithTenantID("acme"),
		events.WithTraceID("trace-outbox-e2e-001"),
	)
	err = pgcommon.RunInTx(ctx, pool, pgx.TxOptions{}, func(ctx context.Context, tx pgx.Tx) error {
		return outbox.Enqueue(ctx, tx, env)
	})
	require.NoError(t, err, "Enqueue should succeed within a transaction")

	// The runner picks it up and publishes to SNS → SQS → consumer.
	select {
	case got := <-received:
		assert.Equal(t, env.ID, got.ID, "event ID must be preserved end-to-end")
		assert.Equal(t, "order.placed", got.Type)
		assert.Equal(t, "acme", got.TenantID)
		assert.Equal(t, "trace-outbox-e2e-001", got.TraceID)
	case <-time.After(60 * time.Second):
		t.Fatal("timed out waiting for outbox event to be delivered")
	}
}

// TestOutbox_MultipleEvents verifies that all enqueued events are eventually
// delivered when the runner processes a batch.
func TestOutbox_MultipleEvents(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test")
	}
	setAWSEnv(t)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	ls := fixtures.StartLocalStack(ctx, t)
	pool, cleanupDB := fixtures.NewTestDB(ctx, t)
	defer cleanupDB()

	topicARN := ls.CreateTopic(ctx, t, "outbox-multi-topic")
	queueURL := ls.CreateQueue(ctx, t, "outbox-multi-queue")
	ls.SubscribeQueueToTopic(ctx, t, topicARN, queueURL)

	snsPub, err := events.NewSNSPublisher(events.SNSConfig{
		TopicARN:    topicARN,
		Region:      "us-east-1",
		EndpointURL: ls.EndpointURL,
	})
	require.NoError(t, err)

	runner, err := outbox.NewRunner(outbox.Config{
		Pool:         pool,
		Publisher:    snsPub,
		PollInterval: 500 * time.Millisecond,
		BatchSize:    10,
		MaxAttempts:  3,
	})
	require.NoError(t, err)
	runnerCtx, runnerCancel := context.WithCancel(ctx)
	defer runnerCancel()
	go func() { _ = runner.Start(runnerCtx) }()

	var receivedCount atomic.Int32
	handler := func(_ context.Context, _ events.Envelope[json.RawMessage]) error {
		receivedCount.Add(1)
		return nil
	}
	consumer, err := events.NewSQSConsumer(events.SQSConfig{
		QueueURL:    queueURL,
		Region:      "us-east-1",
		EndpointURL: ls.EndpointURL,
		WaitSeconds: 1,
		MaxMessages: 10,
	}, handler, events.WithConcurrency(3))
	require.NoError(t, err)
	consumerCtx, consumerCancel := context.WithCancel(ctx)
	defer func() {
		consumerCancel()
		_ = consumer.Stop()
	}()
	go func() { _ = consumer.Start(consumerCtx) }()

	// Enqueue 5 events, each in its own transaction (simulates separate service calls).
	const numEvents = 5
	for i := 0; i < numEvents; i++ {
		env := events.NewEnvelope("item.shipped", "warehouse-svc",
			json.RawMessage(`{"item":"widget"}`),
			events.WithTenantID("tenant-a"),
		)
		err := pgcommon.RunInTx(ctx, pool, pgx.TxOptions{}, func(ctx context.Context, tx pgx.Tx) error {
			return outbox.Enqueue(ctx, tx, env)
		})
		require.NoError(t, err)
	}

	// Wait for all events to be delivered.
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if receivedCount.Load() >= numEvents {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	assert.GreaterOrEqual(t, receivedCount.Load(), int32(numEvents),
		"all %d enqueued events should be delivered", numEvents)
}

// TestOutbox_RollbackDoesNotPublish verifies that an event enqueued in a rolled-back
// transaction is never delivered — the transactional guarantee of the outbox pattern.
func TestOutbox_RollbackDoesNotPublish(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test")
	}
	setAWSEnv(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	ls := fixtures.StartLocalStack(ctx, t)
	pool, cleanupDB := fixtures.NewTestDB(ctx, t)
	defer cleanupDB()

	topicARN := ls.CreateTopic(ctx, t, "outbox-rollback-topic")
	queueURL := ls.CreateQueue(ctx, t, "outbox-rollback-queue")
	ls.SubscribeQueueToTopic(ctx, t, topicARN, queueURL)

	snsPub, err := events.NewSNSPublisher(events.SNSConfig{
		TopicARN:    topicARN,
		Region:      "us-east-1",
		EndpointURL: ls.EndpointURL,
	})
	require.NoError(t, err)

	runner, err := outbox.NewRunner(outbox.Config{
		Pool:         pool,
		Publisher:    snsPub,
		PollInterval: 500 * time.Millisecond,
		BatchSize:    10,
		MaxAttempts:  3,
	})
	require.NoError(t, err)
	runnerCtx, runnerCancel := context.WithCancel(ctx)
	defer runnerCancel()
	go func() { _ = runner.Start(runnerCtx) }()

	var receivedCount atomic.Int32
	handler := func(_ context.Context, _ events.Envelope[json.RawMessage]) error {
		receivedCount.Add(1)
		return nil
	}
	consumer, err := events.NewSQSConsumer(events.SQSConfig{
		QueueURL:    queueURL,
		Region:      "us-east-1",
		EndpointURL: ls.EndpointURL,
		WaitSeconds: 1,
		MaxMessages: 1,
	}, handler)
	require.NoError(t, err)
	consumerCtx, consumerCancel := context.WithCancel(ctx)
	defer func() {
		consumerCancel()
		_ = consumer.Stop()
	}()
	go func() { _ = consumer.Start(consumerCtx) }()

	// Enqueue inside a transaction that we force to roll back by returning an error.
	rolledBackEnv := events.NewEnvelope("payment.failed", "billing-svc", json.RawMessage(`{}`),
		events.WithTenantID("test-tenant"))
	err = pgcommon.RunInTx(ctx, pool, pgx.TxOptions{}, func(ctx context.Context, tx pgx.Tx) error {
		_ = outbox.Enqueue(ctx, tx, rolledBackEnv)
		return errIntentionalRollback // non-nil error causes RunInTx to rollback
	})
	require.ErrorIs(t, err, errIntentionalRollback)

	// Enqueue a sentinel event in a committed transaction so we know the runner
	// has processed at least one poll cycle after the rollback.
	sentinelEnv := events.NewEnvelope("heartbeat.ping", "billing-svc", json.RawMessage(`{}`),
		events.WithTenantID("test-tenant"))
	err = pgcommon.RunInTx(ctx, pool, pgx.TxOptions{}, func(ctx context.Context, tx pgx.Tx) error {
		return outbox.Enqueue(ctx, tx, sentinelEnv)
	})
	require.NoError(t, err)

	// Wait for the sentinel to arrive; it confirms the runner polled at least once.
	sentinelReceived := make(chan struct{}, 1)
	consumerCancel()
	_ = consumer.Stop()

	var sentinelCount atomic.Int32
	sentinelHandler := func(_ context.Context, env events.Envelope[json.RawMessage]) error {
		switch env.ID {
		case sentinelEnv.ID:
			sentinelReceived <- struct{}{}
		case rolledBackEnv.ID:
			sentinelCount.Add(1) // should never happen
		}
		return nil
	}
	sentinel, err := events.NewSQSConsumer(events.SQSConfig{
		QueueURL:    queueURL,
		Region:      "us-east-1",
		EndpointURL: ls.EndpointURL,
		WaitSeconds: 1,
		MaxMessages: 5,
	}, sentinelHandler)
	require.NoError(t, err)
	sentinelCtx, sentinelCancel := context.WithCancel(ctx)
	defer sentinelCancel()
	go func() { _ = sentinel.Start(sentinelCtx) }()

	select {
	case <-sentinelReceived:
		// Good — sentinel arrived, runner has processed the DB.
	case <-time.After(60 * time.Second):
		t.Fatal("timed out waiting for sentinel event")
	}

	// The rolled-back event must never have been published.
	assert.Equal(t, int32(0), sentinelCount.Load(),
		"rolled-back event must never reach the consumer")
}

// errIntentionalRollback is a sentinel error used to force a transaction rollback
// in TestOutbox_RollbackDoesNotPublish.
var errIntentionalRollback = &testError{"intentional rollback"}

type testError struct{ msg string }

func (e *testError) Error() string { return e.msg }
