//go:build integration

package integration_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/events"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/test/fixtures"
)

// fakeReversingCodec is a deliberately non-Glue events.Codec used to prove the
// WithCodec/WithConsumerCodec wiring works end-to-end through real SNS/SQS —
// this library does not implement AWS Glue Schema Registry itself.
type fakeReversingCodec struct{}

func (fakeReversingCodec) Encode(_ context.Context, _ string, payload json.RawMessage) ([]byte, string, error) {
	return reverseBytes(payload), "fake-schema-v1", nil
}

func (fakeReversingCodec) Decode(_ context.Context, _ string, encoded []byte) (json.RawMessage, error) {
	return reverseBytes(encoded), nil
}

func reverseBytes(b []byte) []byte {
	out := make([]byte, len(b))
	for i, c := range b {
		out[len(b)-1-i] = c
	}
	return out
}

func TestCodecRoundTrip_SNSPublish_SQSConsume(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	emu := fixtures.StartFloci(ctx, t)

	// Two queues subscribed to the same topic: inspectQueueURL is read directly
	// with the raw SQS client to assert the wire format; consumeQueueURL is
	// driven by a real events.Consumer to assert decoded handler behaviour.
	// Using separate queues avoids a manual ReceiveMessage and the Consumer's
	// own poll loop racing for the same single message.
	topicARN := emu.CreateTopic(ctx, t, "codec-test-topic")
	inspectQueueURL := emu.CreateQueue(ctx, t, "codec-test-inspect-queue")
	consumeQueueURL := emu.CreateQueue(ctx, t, "codec-test-consume-queue")
	emu.SubscribeQueueToTopic(ctx, t, topicARN, inspectQueueURL)
	emu.SubscribeQueueToTopic(ctx, t, topicARN, consumeQueueURL)

	pub, err := events.NewSNSPublisher(events.SNSConfig{
		TopicARN:    topicARN,
		Region:      "us-east-1",
		EndpointURL: emu.EndpointURL,
	}, events.WithCodec(fakeReversingCodec{}))
	require.NoError(t, err)

	originalPayload := json.RawMessage(`{"name":"Alice"}`)
	env := events.NewEnvelope("user.created", "platform-iam", originalPayload,
		events.WithTenantID("acme"),
	)

	err = pub.Publish(ctx, env)
	require.NoError(t, err)

	// Inspect the raw SQS message: the wire format must reflect the codec —
	// dataschema set, data a base64 JSON string.
	out, err := emu.SQSClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(inspectQueueURL),
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     5,
	})
	require.NoError(t, err)
	require.Len(t, out.Messages, 1)

	var wire struct {
		Data     json.RawMessage `json:"data"`
		SchemaID string          `json:"dataschema"`
	}
	require.NoError(t, json.Unmarshal([]byte(aws.ToString(out.Messages[0].Body)), &wire))
	assert.Equal(t, "fake-schema-v1", wire.SchemaID)

	var b64 string
	require.NoError(t, json.Unmarshal(wire.Data, &b64), "data should be a base64 JSON string when a codec is configured")
	raw, err := base64.StdEncoding.DecodeString(b64)
	require.NoError(t, err)
	assert.Equal(t, string(originalPayload), string(reverseBytes(raw)))

	// Now consume the other subscribed queue with WithConsumerCodec and
	// assert the handler receives the original, decoded payload — not the
	// raw base64 string.
	var receivedPayload atomic.Value
	handler := func(_ context.Context, e events.Envelope[json.RawMessage]) error {
		receivedPayload.Store(string(e.Payload))
		return nil
	}

	consumer, err := events.NewSQSConsumer(events.SQSConfig{
		QueueURL:    consumeQueueURL,
		Region:      "us-east-1",
		EndpointURL: emu.EndpointURL,
		MaxMessages: 1,
		WaitSeconds: 1,
	}, handler, events.WithConsumerCodec(fakeReversingCodec{}))
	require.NoError(t, err)

	consumerCtx, consumerCancel := context.WithCancel(ctx)
	go func() { _ = consumer.Start(consumerCtx) }()

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if receivedPayload.Load() != nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	consumerCancel()
	_ = consumer.Stop()

	got, _ := receivedPayload.Load().(string)
	assert.Equal(t, string(originalPayload), got, "handler should receive the original payload bytes exactly")
}
