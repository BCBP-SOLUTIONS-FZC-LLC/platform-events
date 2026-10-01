package sqs_test

import (
	"context"
	"encoding/json"
	"errors"
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
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/events/mock"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/test/fixtures"
)

// A dead-letter handler that forwards the message itself, with DLQ forwarding
// also enabled, must not be forwarded a second time (or counted twice).
func TestDeadLetterHandler_ForwardsItself_WithDLQForwarding_NoDoubleForward(t *testing.T) {
	reg := initPlatformMetrics(t)
	client := &mockDLQClient{}
	pub := newTestDLQPublisher(t, client, "")
	dlh := func(ctx context.Context, _ domain.Envelope[json.RawMessage]) error {
		src, _ := port.SourceMessageFromContext(ctx)
		return pub.SendToDLQ(ctx, src.QueueURL, src.Body, src.Attributes, "gave up")
	}
	env := domain.NewEnvelope("iam.user.created", "svc", json.RawMessage(`{}`))
	deletes, _ := consumeOnce(t, withReceiveCount(makeSQSMessage(env), "6"), nil,
		internalsqs.WithDeadLetterHandler(dlh), internalsqs.WithMaxReceiveCount(5), internalsqs.WithDLQPublisher(pub))

	eventually(t, func() bool { return deletes() == 1 }, "deleted")
	time.Sleep(50 * time.Millisecond)
	client.mu.Lock()
	sent := len(client.sent)
	client.mu.Unlock()
	assert.Equal(t, 1, sent, "exactly one copy in the DLQ")
	assert.InDelta(t, 1, dlqCount(t, reg, "iam.user.created", "max_receive_count"), 0, "counted once")
}

// A panicking dead-letter handler is counted as failed and retried, like an
// erroring one, instead of leaving the delivery without an outcome.
func TestDeadLetterHandler_Panic_CountedAsFailure(t *testing.T) {
	reg := initPlatformMetrics(t)
	dlh := func(context.Context, domain.Envelope[json.RawMessage]) error { panic("dlh exploded") }
	env := domain.NewEnvelope("iam.user.created", "svc", json.RawMessage(`{}`))
	deletes, logger := consumeOnce(t, withReceiveCount(makeSQSMessage(env), "6"), nil,
		internalsqs.WithDeadLetterHandler(dlh), internalsqs.WithMaxReceiveCount(5))

	eventually(t, func() bool {
		return counterValue(t, reg, "platform_messages_failed_total", map[string]string{"event_type": "iam.user.created", "reason": "dead_letter_error"}) == 1
	}, "failure counted")
	assert.InDelta(t, 1, counterValue(t, reg, "platform_retry_total", map[string]string{"operation": "consume", "event_type": "iam.user.created"}), 0)
	assert.Equal(t, int32(0), deletes(), "left visible for retry")
	found := false
	for _, e := range logger.Entries() {
		found = found || e.Message == "sqs: dead-letter handler panic recovered"
	}
	assert.True(t, found, "panic logged")
}

// A panicking codec is a decode failure: counted, retried, not lost.
func TestDecode_Panic_CountedAsDecodeError(t *testing.T) {
	reg := initPlatformMetrics(t)
	env := makeCodecEncodedEnvelope(t, "iam.user.created", "schema-1", json.RawMessage(`{"a":1}`))
	panicking := &fakeCodec{decodeFn: func(context.Context, string, []byte) (json.RawMessage, error) { panic("codec bug") }}
	deletes, _ := consumeOnce(t, withReceiveCount(makeSQSMessage(env), "1"), nil, internalsqs.WithCodec(panicking))

	eventually(t, func() bool {
		return counterValue(t, reg, "platform_messages_failed_total", map[string]string{"event_type": "iam.user.created", "reason": "decode_error"}) == 1
	}, "decode failure counted")
	assert.InDelta(t, 1, counterValue(t, reg, "platform_retry_total", map[string]string{"operation": "consume", "event_type": "iam.user.created"}), 0)
	assert.Equal(t, int32(0), deletes())
}

// An in-flight decode is not cancelled by Stop(): it completes and the
// message is processed, like an in-flight handler.
func TestDecode_SurvivesStop(t *testing.T) {
	env := makeCodecEncodedEnvelope(t, "iam.user.created", "schema-1", json.RawMessage(`{"a":1}`))
	started := make(chan struct{})
	release := make(chan struct{})
	codec := &fakeCodec{decodeFn: func(ctx context.Context, _ string, _ []byte) (json.RawMessage, error) {
		close(started)
		<-release
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return json.RawMessage(`{"a":1}`), nil
	}}
	var handled atomic.Bool
	var deletes atomic.Int32
	var once atomic.Bool
	msg := withReceiveCount(makeSQSMessage(env), "1")
	client := &mockSQSClient{
		receiveMessageFn: func(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
			if once.CompareAndSwap(false, true) {
				return &sqs.ReceiveMessageOutput{Messages: []sqstypes.Message{msg}}, nil
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
		deleteMessageFn: func(context.Context, *sqs.DeleteMessageInput, ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
			deletes.Add(1)
			return &sqs.DeleteMessageOutput{}, nil
		},
	}
	c, err := internalsqs.NewWithClient(internalsqs.Config{QueueURL: testQueueURL, WaitSeconds: 1}, client,
		func(context.Context, domain.Envelope[json.RawMessage]) error { handled.Store(true); return nil },
		internalsqs.WithCodec(codec), internalsqs.WithDrainTimeout(5*time.Second))
	require.NoError(t, err)
	done := make(chan struct{})
	go func() { defer close(done); _ = c.Start(context.Background()) }()

	<-started
	stopped := make(chan error, 1)
	go func() { stopped <- c.Stop() }()
	time.Sleep(50 * time.Millisecond) // Stop has cancelled the loop
	close(release)
	require.NoError(t, <-stopped)
	<-done
	assert.True(t, handled.Load(), "decode completed and the handler ran")
	assert.Equal(t, int32(1), deletes.Load())
}

// Stop() while Start is still resolving the DLQ cancels Start instead of
// being lost.
func TestStart_StopDuringDLQCheck_IsNotLost(t *testing.T) {
	resolving := make(chan struct{})
	dlq := &blockingResolveDLQ{resolving: resolving}
	client := &mockSQSClient{
		receiveMessageFn: func(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	c, err := internalsqs.NewWithClient(internalsqs.Config{QueueURL: testQueueURL, WaitSeconds: 1, Logger: &fixtures.MockLogger{}}, client,
		func(context.Context, domain.Envelope[json.RawMessage]) error { return nil }, internalsqs.WithDLQPublisher(dlq))
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- c.Start(context.Background()) }()
	<-resolving
	require.NoError(t, c.Stop())
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Start kept running after Stop during the DLQ check")
	}
}

type blockingResolveDLQ struct {
	mock.DLQPublisher
	resolving chan struct{}
}

func (b *blockingResolveDLQ) ResolveDLQ(ctx context.Context, _ string) (string, error) {
	close(b.resolving)
	<-ctx.Done()
	return "", ctx.Err()
}

// The visibility extension also covers the dead-letter handler.
func TestDeadLetterHandler_VisibilityExtended(t *testing.T) {
	var extends atomic.Int32
	var once atomic.Bool
	env := domain.NewEnvelope("iam.user.created", "svc", json.RawMessage(`{}`))
	msg := withReceiveCount(makeSQSMessage(env), "6")
	client := &mockSQSClient{
		receiveMessageFn: func(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
			if once.CompareAndSwap(false, true) {
				return &sqs.ReceiveMessageOutput{Messages: []sqstypes.Message{msg}}, nil
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
		changeMessageVisibilityFn: func(_ context.Context, in *sqs.ChangeMessageVisibilityInput, _ ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
			if aws.ToString(in.ReceiptHandle) != "" {
				extends.Add(1)
			}
			return &sqs.ChangeMessageVisibilityOutput{}, nil
		},
	}
	dlh := func(context.Context, domain.Envelope[json.RawMessage]) error {
		time.Sleep(1300 * time.Millisecond) // > one extension tick (max(1s/2, 1s))
		return nil
	}
	c, err := internalsqs.NewWithClient(internalsqs.Config{QueueURL: testQueueURL, WaitSeconds: 1}, client,
		func(context.Context, domain.Envelope[json.RawMessage]) error { return errors.New("unused") },
		internalsqs.WithDeadLetterHandler(dlh), internalsqs.WithMaxReceiveCount(5), internalsqs.WithVisibilityTimeout(time.Second))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = c.Start(ctx) }()
	eventually(t, func() bool { return extends.Load() >= 1 }, "visibility extended during the dead-letter handler")
	cancel()
	<-done
}
