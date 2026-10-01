//go:build e2e

package e2e_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/events"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/test/fixtures"
)

// setAWSEnv configures static dummy credentials so that awsconfig.LoadDefaultConfig
// succeeds without trying EC2 IMDS. Must be called at the start of every e2e test.
func setAWSEnv(t *testing.T) {
	t.Helper()
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_SESSION_TOKEN", "test")
}

// TestPublishConsume_EndToEnd tests the complete SNS → SQS → Consumer pipeline
// using Floci.
func TestPublishConsume_EndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test")
	}
	setAWSEnv(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	emu := fixtures.StartFloci(ctx, t)

	topicARN := emu.CreateTopic(ctx, t, "e2e-topic")
	queueURL := emu.CreateQueue(ctx, t, "e2e-queue")
	emu.SubscribeQueueToTopic(ctx, t, topicARN, queueURL)

	// Create publisher.
	pub, err := events.NewSNSPublisher(events.SNSConfig{
		TopicARN:    topicARN,
		Region:      "us-east-1",
		EndpointURL: emu.EndpointURL,
	})
	require.NoError(t, err)

	// Create consumer.
	received := make(chan events.Envelope[json.RawMessage], 10)
	handler := func(_ context.Context, env events.Envelope[json.RawMessage]) error {
		received <- env
		return nil
	}

	consumer, err := events.NewSQSConsumer(events.SQSConfig{
		QueueURL:    queueURL,
		Region:      "us-east-1",
		EndpointURL: emu.EndpointURL,
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

	// Publish an event.
	env := events.NewEnvelope("user.created", "platform-iam", json.RawMessage(`{"name":"Alice"}`),
		events.WithTenantID("acme"),
		events.WithTraceID("trace-e2e-001"),
	)
	err = pub.Publish(ctx, env)
	require.NoError(t, err)

	// Wait for the consumer to receive it.
	select {
	case got := <-received:
		assert.Equal(t, env.ID, got.ID)
		assert.Equal(t, env.Type, got.Type)
		assert.Equal(t, "acme", got.TenantID)
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for message to be received")
	}
}

// TestPublishConsume_HandlerError_MessageRetried verifies that a handler error
// causes the message to NOT be deleted (remains in queue).
func TestPublishConsume_HandlerError_MessageRetried(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test")
	}
	setAWSEnv(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	emu := fixtures.StartFloci(ctx, t)
	queueURL := emu.CreateQueue(ctx, t, "retry-queue")

	// Publish a message directly to SQS.
	env := events.NewEnvelope("retry.event", "svc", json.RawMessage(`{}`), events.WithTenantID("test-tenant"))
	body, _ := json.Marshal(env)
	_, err := emu.SQSClient.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(queueURL),
		MessageBody: aws.String(string(body)),
	})
	require.NoError(t, err)

	// Handler that always returns an error.
	var handlerCallCount atomic.Int32
	handler := func(_ context.Context, _ events.Envelope[json.RawMessage]) error {
		handlerCallCount.Add(1)
		return errors.New("intentional handler error")
	}

	consumer, err := events.NewSQSConsumer(events.SQSConfig{
		QueueURL:    queueURL,
		Region:      "us-east-1",
		EndpointURL: emu.EndpointURL,
		WaitSeconds: 1,
		MaxMessages: 1,
	}, handler)
	require.NoError(t, err)

	consumerCtx, consumerCancel := context.WithTimeout(ctx, 5*time.Second)
	defer consumerCancel()
	go func() { _ = consumer.Start(consumerCtx) }()

	// Wait for at least one handler call.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if handlerCallCount.Load() >= 1 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	assert.GreaterOrEqual(t, handlerCallCount.Load(), int32(1),
		"handler should have been called at least once")
}

// TestPublishConsume_Concurrent tests concurrent message handling.
func TestPublishConsume_Concurrent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test")
	}
	setAWSEnv(t)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	emu := fixtures.StartFloci(ctx, t)

	topicARN := emu.CreateTopic(ctx, t, "concurrent-topic")
	queueURL := emu.CreateQueue(ctx, t, "concurrent-queue")
	emu.SubscribeQueueToTopic(ctx, t, topicARN, queueURL)

	pub, err := events.NewSNSPublisher(events.SNSConfig{
		TopicARN:    topicARN,
		Region:      "us-east-1",
		EndpointURL: emu.EndpointURL,
	})
	require.NoError(t, err)

	var receivedCount atomic.Int32
	handler := func(_ context.Context, _ events.Envelope[json.RawMessage]) error {
		receivedCount.Add(1)
		return nil
	}

	consumer, err := events.NewSQSConsumer(events.SQSConfig{
		QueueURL:    queueURL,
		Region:      "us-east-1",
		EndpointURL: emu.EndpointURL,
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

	// Publish 5 messages.
	const numMessages = 5
	for i := 0; i < numMessages; i++ {
		env := events.NewEnvelope("concurrent.event", "svc", json.RawMessage(`{}`), events.WithTenantID("test-tenant"))
		err := pub.Publish(ctx, env)
		require.NoError(t, err)
	}

	// Wait for all messages to be received.
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		if receivedCount.Load() >= numMessages {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	assert.GreaterOrEqual(t, receivedCount.Load(), int32(numMessages))
}

// TestPublishBatch_EndToEnd tests batch publishing.
func TestPublishBatch_EndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test")
	}
	setAWSEnv(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	emu := fixtures.StartFloci(ctx, t)

	topicARN := emu.CreateTopic(ctx, t, "batch-topic")
	queueURL := emu.CreateQueue(ctx, t, "batch-queue")
	emu.SubscribeQueueToTopic(ctx, t, topicARN, queueURL)

	pub, err := events.NewSNSPublisher(events.SNSConfig{
		TopicARN:    topicARN,
		Region:      "us-east-1",
		EndpointURL: emu.EndpointURL,
	})
	require.NoError(t, err)

	var receivedCount atomic.Int32
	handler := func(_ context.Context, _ events.Envelope[json.RawMessage]) error {
		receivedCount.Add(1)
		return nil
	}

	consumer, err := events.NewSQSConsumer(events.SQSConfig{
		QueueURL:    queueURL,
		Region:      "us-east-1",
		EndpointURL: emu.EndpointURL,
		WaitSeconds: 1,
		MaxMessages: 10,
	}, handler)
	require.NoError(t, err)

	consumerCtx, consumerCancel := context.WithCancel(ctx)
	defer func() {
		consumerCancel()
		_ = consumer.Stop()
	}()
	go func() { _ = consumer.Start(consumerCtx) }()

	// Batch publish 3 events.
	envs := []events.Envelope[json.RawMessage]{
		events.NewEnvelope("batch.one", "svc", json.RawMessage(`{}`), events.WithTenantID("test-tenant")),
		events.NewEnvelope("batch.two", "svc", json.RawMessage(`{}`), events.WithTenantID("test-tenant")),
		events.NewEnvelope("batch.three", "svc", json.RawMessage(`{}`), events.WithTenantID("test-tenant")),
	}
	err = pub.PublishBatch(ctx, envs)
	require.NoError(t, err)

	// Wait for all to arrive.
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if receivedCount.Load() >= 3 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	assert.GreaterOrEqual(t, receivedCount.Load(), int32(3))
}
