//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/events"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/test/fixtures"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQSConsumeLoop(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	// Set dummy credentials so AWS SDK doesn't try to fetch from IMDS.
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	ls := fixtures.StartLocalStack(ctx, t)
	queueURL := ls.CreateQueue(ctx, t, "consumer-test-queue")

	// Pre-load a message into the queue.
	env := events.NewEnvelope("test.consume", "svc", json.RawMessage(`{"x":42}`),
		events.WithTenantID("acme"),
	)
	body, err := json.Marshal(env)
	require.NoError(t, err)

	_, err = ls.SQSClient.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(queueURL),
		MessageBody: aws.String(string(body)),
	})
	require.NoError(t, err)

	var received atomic.Int32
	handler := func(_ context.Context, e events.Envelope[json.RawMessage]) error {
		received.Add(1)
		return nil
	}

	consumer, err := events.NewSQSConsumer(events.SQSConfig{
		QueueURL:    queueURL,
		Region:      "us-east-1",
		EndpointURL: ls.EndpointURL,
		MaxMessages: 1,
		WaitSeconds: 1,
	}, handler)
	require.NoError(t, err)

	consumerCtx, consumerCancel := context.WithCancel(ctx)

	errCh := make(chan error, 1)
	go func() {
		errCh <- consumer.Start(consumerCtx)
	}()

	// Wait for the message to be received.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if received.Load() >= 1 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	consumerCancel()
	_ = consumer.Stop()

	assert.GreaterOrEqual(t, received.Load(), int32(1))
}
