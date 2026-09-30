package sqs_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	internalsqs "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/adapter/outbound/sqs"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/port"
)

// Tier 1 consumer semantics (Enterprise Platform Observability Standard):
// every delivery is received once; it ends processed, failed (and retried),
// or dead-lettered — and a dead-lettered message is counted exactly once,
// whoever forwarded it.

const testQueue = "test-queue" // QueueName(testQueueURL)

func eventually(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	require.Eventually(t, cond, 5*time.Second, 10*time.Millisecond, msg)
}

func dlqCount(t *testing.T, reg *prometheus.Registry, eventType, reason string) float64 {
	return counterValue(t, reg, "platform_dlq_messages_total", map[string]string{"operation": "consume", "event_type": eventType, "reason": reason})
}

func TestConsumerPlatformMetrics_ProcessedDelivery(t *testing.T) {
	reg := initPlatformMetrics(t)
	env := domain.NewEnvelope("iam.user.created", "svc", json.RawMessage(`{}`))
	deletes, _ := consumeOnce(t, makeSQSMessage(env), nil)

	eventually(t, func() bool { return deletes() == 1 }, "message deleted")
	eventually(t, func() bool {
		return counterValue(t, reg, "platform_messages_processed_total", map[string]string{"queue": testQueue, "event_type": "iam.user.created"}) == 1
	}, "processed counted")
	assert.InDelta(t, 1, counterValue(t, reg, "platform_messages_received_total", map[string]string{"queue": testQueue}), 0)
	assert.Equal(t, uint64(1), histogramCount(t, reg, "platform_message_processing_duration_seconds", map[string]string{"queue": testQueue}))
	assert.Equal(t, uint64(1), histogramCount(t, reg, "platform_event_propagation_seconds", map[string]string{"queue": testQueue, "event_type": "iam.user.created"}))
	assert.GreaterOrEqual(t, histogramCount(t, reg, "platform_dependency_request_seconds", map[string]string{"dependency": "sqs", "operation": "receive_message", "outcome": "success"}), uint64(1))
	eventually(t, func() bool {
		return histogramCount(t, reg, "platform_dependency_request_seconds", map[string]string{"dependency": "sqs", "operation": "delete_message", "outcome": "success"}) == 1
	}, "delete observed")
	assert.Zero(t, counterValue(t, reg, "platform_messages_failed_total", map[string]string{"queue": testQueue, "event_type": "iam.user.created"}))
}

func TestConsumerPlatformMetrics_HandlerFailures(t *testing.T) {
	for _, tc := range []struct {
		name    string
		handler port.Handler
		reason  string
	}{
		{"error", func(context.Context, domain.Envelope[json.RawMessage]) error { return errors.New("boom") }, "handler_error"},
		{"panic", func(context.Context, domain.Envelope[json.RawMessage]) error { panic("kaboom") }, "handler_panic"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reg := initPlatformMetrics(t)
			env := domain.NewEnvelope("iam.user.created", "svc", json.RawMessage(`{}`))
			deletes, _ := consumeOnce(t, makeSQSMessage(env), tc.handler)

			eventually(t, func() bool {
				return counterValue(t, reg, "platform_messages_failed_total", map[string]string{"event_type": "iam.user.created", "reason": tc.reason}) == 1
			}, "failure counted")
			assert.InDelta(t, 1, counterValue(t, reg, "platform_retry_total", map[string]string{"operation": "consume", "event_type": "iam.user.created"}), 0)
			assert.Zero(t, counterValue(t, reg, "platform_messages_processed_total", map[string]string{"event_type": "iam.user.created"}))
			assert.Equal(t, int32(0), deletes(), "left visible for retry")
		})
	}
}

// A handler that dead-letters explicitly and returns nil is counted as
// dead-lettered (reason=explicit) by the DLQ publisher — not as processed.
func TestConsumerPlatformMetrics_ExplicitDeadLetterFromHandler(t *testing.T) {
	reg := initPlatformMetrics(t)
	pub := newTestDLQPublisher(t, &mockDLQClient{}, "")
	handler := func(ctx context.Context, _ domain.Envelope[json.RawMessage]) error {
		src, _ := port.SourceMessageFromContext(ctx)
		return pub.SendToDLQ(ctx, src.QueueURL, src.Body, src.Attributes, "unsupported tenant")
	}
	env := domain.NewEnvelope("iam.user.created", "svc", json.RawMessage(`{}`))
	env.Source = "svc"
	deletes, _ := consumeOnce(t, makeSQSMessage(env), handler)

	eventually(t, func() bool { return deletes() == 1 }, "original deleted")
	eventually(t, func() bool { return dlqCount(t, reg, "iam.user.created", "explicit") == 1 }, "dead-letter counted once")
	assert.Zero(t, counterValue(t, reg, "platform_messages_processed_total", map[string]string{"event_type": "iam.user.created"}),
		"an explicitly dead-lettered message is not processed")
}

// Malformed bodies forwarded through a DLQPublisher that does not count
// (a mock / custom implementation) are counted by the consumer; through the
// SQS publisher they are counted by the publisher — once either way.
func TestConsumerPlatformMetrics_MalformedForwardCountedOnce(t *testing.T) {
	malformed := sqstypes.Message{MessageId: aws.String("bad"), Body: aws.String("not json"), ReceiptHandle: aws.String("rh")}

	t.Run("non-counting publisher", func(t *testing.T) {
		reg := initPlatformMetrics(t)
		dlq := newSignallingDLQ()
		deletes, _ := consumeOnce(t, malformed, nil, internalsqs.WithDLQPublisher(dlq))
		eventually(t, func() bool { return deletes() == 1 }, "deleted after forward")
		assert.InDelta(t, 1, dlqCount(t, reg, "unknown", "malformed"), 0)
		assert.InDelta(t, 1, counterValue(t, reg, "platform_messages_failed_total", map[string]string{"queue": testQueue, "event_type": "unknown", "reason": "malformed"}), 0)
	})

	t.Run("SQS publisher", func(t *testing.T) {
		reg := initPlatformMetrics(t)
		pub := newTestDLQPublisher(t, &mockDLQClient{}, "")
		deletes, _ := consumeOnce(t, malformed, nil, internalsqs.WithDLQPublisher(pub))
		eventually(t, func() bool { return deletes() == 1 }, "deleted after forward")
		assert.InDelta(t, 1, dlqCount(t, reg, "unknown", "malformed"), 0, "counted by the publisher only")
	})

	t.Run("forward fails → retried", func(t *testing.T) {
		reg := initPlatformMetrics(t)
		dlq := newSignallingDLQ()
		dlq.SetError(errors.New("sqs down"))
		consumeOnce(t, malformed, nil, internalsqs.WithDLQPublisher(dlq))
		waitCalled(t, dlq.called, "SendToDLQ")
		eventually(t, func() bool {
			return counterValue(t, reg, "platform_retry_total", map[string]string{"operation": "consume", "event_type": "unknown"}) == 1
		}, "retry counted")
		assert.Zero(t, dlqCount(t, reg, "unknown", "malformed"))
	})
}

func TestConsumerPlatformMetrics_DeadLetterHandler(t *testing.T) {
	env := domain.NewEnvelope("iam.user.created", "svc", json.RawMessage(`{}`))
	msg := withReceiveCount(makeSQSMessage(env), "6")

	t.Run("handler alone dead-letters", func(t *testing.T) {
		reg := initPlatformMetrics(t)
		dlh := func(context.Context, domain.Envelope[json.RawMessage]) error { return nil }
		deletes, _ := consumeOnce(t, msg, nil, internalsqs.WithDeadLetterHandler(dlh), internalsqs.WithMaxReceiveCount(5))
		eventually(t, func() bool { return deletes() == 1 }, "deleted")
		eventually(t, func() bool { return dlqCount(t, reg, "iam.user.created", "max_receive_count") == 1 }, "dead-letter counted")
		assert.Equal(t, uint64(1), histogramCount(t, reg, "platform_message_processing_duration_seconds", map[string]string{"queue": testQueue}))
	})

	t.Run("handler forwards itself", func(t *testing.T) {
		reg := initPlatformMetrics(t)
		pub := newTestDLQPublisher(t, &mockDLQClient{}, "")
		dlh := func(ctx context.Context, _ domain.Envelope[json.RawMessage]) error {
			src, _ := port.SourceMessageFromContext(ctx)
			return pub.SendToDLQ(ctx, src.QueueURL, src.Body, src.Attributes, "gave up")
		}
		deletes, _ := consumeOnce(t, msg, nil, internalsqs.WithDeadLetterHandler(dlh), internalsqs.WithMaxReceiveCount(5))
		eventually(t, func() bool { return deletes() == 1 }, "deleted")
		time.Sleep(50 * time.Millisecond)
		assert.InDelta(t, 1, dlqCount(t, reg, "iam.user.created", "max_receive_count"), 0, "attributed to max_receive_count and counted once")
	})

	t.Run("handler fails", func(t *testing.T) {
		reg := initPlatformMetrics(t)
		dlh := func(context.Context, domain.Envelope[json.RawMessage]) error { return errors.New("dlh down") }
		consumeOnce(t, msg, nil, internalsqs.WithDeadLetterHandler(dlh), internalsqs.WithMaxReceiveCount(5))
		eventually(t, func() bool {
			return counterValue(t, reg, "platform_messages_failed_total", map[string]string{"event_type": "iam.user.created", "reason": "dead_letter_error"}) == 1
		}, "failure counted")
		assert.InDelta(t, 1, counterValue(t, reg, "platform_retry_total", map[string]string{"operation": "consume", "event_type": "iam.user.created"}), 0)
		assert.Zero(t, dlqCount(t, reg, "iam.user.created", "max_receive_count"))
	})
}

func TestConsumerPlatformMetrics_DecodeFailure(t *testing.T) {
	reg := initPlatformMetrics(t)
	env := makeCodecEncodedEnvelope(t, "iam.user.created", "schema-1", json.RawMessage(`{"a":1}`))
	failing := &fakeCodec{decodeFn: func(context.Context, string, []byte) (json.RawMessage, error) {
		return nil, errors.New("registry down")
	}}
	consumeOnce(t, withReceiveCount(makeSQSMessage(env), "1"), nil, internalsqs.WithCodec(failing))

	eventually(t, func() bool {
		return counterValue(t, reg, "platform_messages_failed_total", map[string]string{"event_type": "iam.user.created", "reason": "decode_error"}) == 1
	}, "decode failure counted")
	assert.InDelta(t, 1, counterValue(t, reg, "platform_retry_total", map[string]string{"operation": "consume", "event_type": "iam.user.created"}), 0)
	assert.Equal(t, uint64(1), histogramCount(t, reg, "platform_dependency_request_seconds", map[string]string{"dependency": "codec", "operation": "decode", "outcome": "error"}))
}

// Propagation is creation → first receipt: a redelivery is not observed.
func TestConsumerPlatformMetrics_PropagationFirstReceiptOnly(t *testing.T) {
	reg := initPlatformMetrics(t)
	env := domain.NewEnvelope("iam.user.created", "svc", json.RawMessage(`{}`))
	deletes, _ := consumeOnce(t, withReceiveCount(makeSQSMessage(env), "2"), nil)
	eventually(t, func() bool { return deletes() == 1 }, "processed")
	assert.Zero(t, histogramCount(t, reg, "platform_event_propagation_seconds", map[string]string{"queue": testQueue}))
}
