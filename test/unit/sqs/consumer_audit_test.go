package sqs_test

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	internalsqs "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/adapter/outbound/sqs"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/test/fixtures"
)

// The consumer never receives more messages than it has free workers: a
// received message's visibility clock starts at once, so messages queued
// behind busy workers would reappear and be processed twice.
func TestReceive_NeverMoreThanFreeWorkers(t *testing.T) {
	var mu sync.Mutex
	var requested []int32
	release := make(chan struct{})
	client := &mockSQSClient{
		receiveMessageFn: func(ctx context.Context, in *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
			mu.Lock()
			requested = append(requested, in.MaxNumberOfMessages)
			mu.Unlock()
			var msgs []sqstypes.Message
			for range in.MaxNumberOfMessages {
				msgs = append(msgs, makeSQSMessage(domain.NewEnvelope("a.b.c", "svc", json.RawMessage(`{}`))))
			}
			return &sqs.ReceiveMessageOutput{Messages: msgs}, nil
		},
		deleteMessageFn: func(context.Context, *sqs.DeleteMessageInput, ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
			return &sqs.DeleteMessageOutput{}, nil
		},
	}
	var started atomic.Int32
	c, err := internalsqs.NewWithClient(internalsqs.Config{QueueURL: testQueueURL, WaitSeconds: 1}, client,
		func(context.Context, domain.Envelope[json.RawMessage]) error {
			started.Add(1)
			<-release
			return nil
		}, internalsqs.WithConcurrency(3), internalsqs.WithDrainTimeout(2*time.Second))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = c.Start(ctx) }()

	eventually(t, func() bool { return started.Load() == 3 }, "all three workers busy")
	time.Sleep(100 * time.Millisecond) // no further receive while every worker is busy
	mu.Lock()
	assert.Equal(t, []int32{3}, requested, "asked for 3 (free workers), not MaxMessages=10, and nothing while busy")
	mu.Unlock()
	close(release)
	cancel()
	<-done
}

// An SNS notification wrapper (subscription without RawMessageDelivery) is
// valid JSON but not an envelope: it is malformed, never handed to the
// handler (where "Type":"Notification" would look like an event type).
func TestDispatch_SNSWrapper_TreatedAsMalformed(t *testing.T) {
	reg := initPlatformMetrics(t)
	wrapper := `{"Type":"Notification","MessageId":"m-1","TopicArn":"arn:aws:sns:us-east-1:1:t","Message":"{\"id\":\"x\"}","Timestamp":"2026-10-01T00:00:00Z"}`
	var called atomic.Bool
	deletes, logger := consumeOnce(t, sqstypes.Message{MessageId: aws.String("w"), Body: aws.String(wrapper), ReceiptHandle: aws.String("rh")},
		func(context.Context, domain.Envelope[json.RawMessage]) error { called.Store(true); return nil })
	eventually(t, func() bool { return deletes() == 1 }, "deleted as malformed")
	assert.False(t, called.Load(), "handler must not see the wrapper")
	assert.InDelta(t, 1, counterValue(t, reg, "platform_messages_failed_total", map[string]string{"queue": testQueue, "event_type": "unknown", "reason": "malformed"}), 0)

	// The body is logged by size and hash only (it may carry PII).
	var fields map[string]any
	for _, e := range logger.Entries() {
		if e.Level == "ERROR" {
			fields = e.Fields
		}
	}
	require.NotNil(t, fields)
	assert.NotContains(t, fields, "body")
	assert.Equal(t, len(wrapper), fields["body_bytes"])
	assert.Len(t, fields["body_sha256"], 64)
	assert.Contains(t, fields["error"], "RawMessageDelivery")
}

// WithHandlerTimeout cancels the handler's context and stops extending the
// message's visibility, so a hung handler cannot hold it forever.
func TestHandlerTimeout_CancelsAndStopsExtending(t *testing.T) {
	var extends atomic.Int32
	var once atomic.Bool
	msg := makeSQSMessage(domain.NewEnvelope("a.b.c", "svc", json.RawMessage(`{}`)))
	client := &mockSQSClient{
		receiveMessageFn: func(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
			if once.CompareAndSwap(false, true) {
				return &sqs.ReceiveMessageOutput{Messages: []sqstypes.Message{msg}}, nil
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
		changeMessageVisibilityFn: func(context.Context, *sqs.ChangeMessageVisibilityInput, ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
			extends.Add(1)
			return &sqs.ChangeMessageVisibilityOutput{}, nil
		},
	}
	handlerErr := make(chan error, 1)
	logger := &fixtures.MockLogger{}
	c, err := internalsqs.NewWithClient(internalsqs.Config{QueueURL: testQueueURL, WaitSeconds: 1, Logger: logger}, client,
		func(ctx context.Context, _ domain.Envelope[json.RawMessage]) error {
			<-ctx.Done()                        // honours the timeout…
			time.Sleep(1500 * time.Millisecond) // …but keeps running past it
			handlerErr <- ctx.Err()
			return ctx.Err()
		},
		internalsqs.WithHandlerTimeout(200*time.Millisecond), internalsqs.WithVisibilityTimeout(time.Second),
		internalsqs.WithDrainTimeout(3*time.Second))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = c.Start(ctx) }()

	select {
	case err := <-handlerErr:
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(5 * time.Second):
		t.Fatal("handler never finished")
	}
	assert.Zero(t, extends.Load(), "no extension after the handler timeout passed")
	found := false
	for _, e := range logger.Entries() {
		found = found || e.Message == "sqs: handler timeout passed — no longer extending visibility; the message will be redelivered"
	}
	assert.True(t, found)
	cancel()
	<-done
}

func TestInFlightGauge(t *testing.T) {
	reg := initPlatformMetrics(t)
	release := make(chan struct{})
	consumeOnce(t, makeSQSMessage(domain.NewEnvelope("a.b.c", "svc", json.RawMessage(`{}`))),
		func(context.Context, domain.Envelope[json.RawMessage]) error { <-release; return nil })
	eventually(t, func() bool {
		v, ok := gaugeValue(t, reg, "platform_messages_in_flight", map[string]string{"queue": testQueue})
		return ok && v == 1
	}, "one message in flight")
	close(release)
	eventually(t, func() bool {
		v, ok := gaugeValue(t, reg, "platform_messages_in_flight", map[string]string{"queue": testQueue})
		return ok && v == 0
	}, "back to zero")
}

// JSON with an id but no type or source is not an envelope either.
func TestDispatch_MissingTypeAndSource_Malformed(t *testing.T) {
	var called atomic.Bool
	deletes, logger := consumeOnce(t, sqstypes.Message{MessageId: aws.String("x"), Body: aws.String(`{"id":"0192d3c4-0000-7000-8000-000000000001"}`), ReceiptHandle: aws.String("rh")},
		func(context.Context, domain.Envelope[json.RawMessage]) error { called.Store(true); return nil })
	eventually(t, func() bool { return deletes() == 1 }, "deleted as malformed")
	assert.False(t, called.Load())
	var errText string
	for _, e := range logger.Entries() {
		if e.Level == "ERROR" {
			errText, _ = e.Fields["error"].(string)
		}
	}
	assert.Contains(t, errText, "missing type, source")
}
