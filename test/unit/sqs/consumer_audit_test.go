package sqs_test

import (
	"context"
	"encoding/json"
	"errors"
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

// Batches are received whole (one receive per MaxMessages, not per free
// worker), and every message's visibility is extended from receipt: a message
// waiting behind a busy worker must not reappear and be processed twice.
func TestReceive_QueuedMessagesExtendedWhileWaiting(t *testing.T) {
	var mu sync.Mutex
	var requested []int32
	extended := map[string]int{}
	var once atomic.Bool
	var batch []sqstypes.Message
	for i := range 3 {
		m := makeSQSMessage(domain.NewEnvelope("a.b.c", "svc", json.RawMessage(`{}`)))
		m.ReceiptHandle = aws.String("rh-" + string(rune('a'+i)))
		batch = append(batch, m)
	}
	client := &mockSQSClient{
		receiveMessageFn: func(ctx context.Context, in *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
			mu.Lock()
			requested = append(requested, in.MaxNumberOfMessages)
			mu.Unlock()
			if once.CompareAndSwap(false, true) {
				return &sqs.ReceiveMessageOutput{Messages: batch}, nil
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
		changeMessageVisibilityFn: func(_ context.Context, in *sqs.ChangeMessageVisibilityInput, _ ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
			mu.Lock()
			extended[aws.ToString(in.ReceiptHandle)]++
			mu.Unlock()
			return &sqs.ChangeMessageVisibilityOutput{}, nil
		},
		deleteMessageFn: func(context.Context, *sqs.DeleteMessageInput, ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
			return &sqs.DeleteMessageOutput{}, nil
		},
	}
	release := make(chan struct{})
	var handled atomic.Int32
	c, err := internalsqs.NewWithClient(internalsqs.Config{QueueURL: testQueueURL, WaitSeconds: 1}, client,
		func(context.Context, domain.Envelope[json.RawMessage]) error {
			handled.Add(1)
			<-release
			return nil
		}, internalsqs.WithVisibilityTimeout(time.Second), internalsqs.WithDrainTimeout(3*time.Second)) // concurrency 1
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = c.Start(ctx) }()

	eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return extended["rh-b"] >= 1 && extended["rh-c"] >= 1
	}, "messages waiting for the busy worker are extended")
	assert.Equal(t, int32(1), handled.Load(), "only one worker, the others are still queued")
	mu.Lock()
	assert.Equal(t, int32(10), requested[0], "the batch is received whole")
	mu.Unlock()
	close(release)
	eventually(t, func() bool { return handled.Load() == 3 }, "all processed")
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

// WithHandlerTimeout is one budget for the whole message: time spent in codec
// decode is taken from the handler's deadline, matching when the visibility
// extension stops.
func TestHandlerTimeout_OneDeadlineAcrossDecodeAndHandler(t *testing.T) {
	env := makeCodecEncodedEnvelope(t, "a.b.c", "schema-1", json.RawMessage(`{"a":1}`))
	codec := &fakeCodec{decodeFn: func(context.Context, string, []byte) (json.RawMessage, error) {
		time.Sleep(300 * time.Millisecond)
		return json.RawMessage(`{"a":1}`), nil
	}}
	remaining := make(chan time.Duration, 1)
	consumeOnce(t, withReceiveCount(makeSQSMessage(env), "1"), func(ctx context.Context, _ domain.Envelope[json.RawMessage]) error {
		d, ok := ctx.Deadline()
		require.True(t, ok)
		remaining <- time.Until(d)
		return nil
	}, internalsqs.WithCodec(codec), internalsqs.WithHandlerTimeout(time.Second))
	select {
	case r := <-remaining:
		assert.LessOrEqual(t, r, 750*time.Millisecond, "decode time is part of the budget")
		assert.Greater(t, r, time.Duration(0))
	case <-time.After(5 * time.Second):
		t.Fatal("handler not called")
	}
}

// Stop while received messages still wait for a worker: their visibility
// extension stops (they become visible to another consumer), and Start
// returns once the in-flight handler drains.
func TestStop_QueuedMessagesReleased(t *testing.T) {
	var mu sync.Mutex
	extended := map[string]int{}
	var released []string
	var once atomic.Bool
	var batch []sqstypes.Message
	for i := range 3 {
		m := makeSQSMessage(domain.NewEnvelope("a.b.c", "svc", json.RawMessage(`{}`)))
		m.ReceiptHandle = aws.String("rh-" + string(rune('a'+i)))
		batch = append(batch, m)
	}
	client := &mockSQSClient{
		receiveMessageFn: func(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
			if once.CompareAndSwap(false, true) {
				return &sqs.ReceiveMessageOutput{Messages: batch}, nil
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
		changeMessageVisibilityFn: func(_ context.Context, in *sqs.ChangeMessageVisibilityInput, _ ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
			mu.Lock()
			if in.VisibilityTimeout == 0 {
				released = append(released, aws.ToString(in.ReceiptHandle))
			} else {
				extended[aws.ToString(in.ReceiptHandle)]++
			}
			mu.Unlock()
			return &sqs.ChangeMessageVisibilityOutput{}, nil
		},
		deleteMessageFn: func(context.Context, *sqs.DeleteMessageInput, ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
			return &sqs.DeleteMessageOutput{}, nil
		},
	}
	started := make(chan struct{})
	var handled atomic.Int32
	c, err := internalsqs.NewWithClient(internalsqs.Config{QueueURL: testQueueURL, WaitSeconds: 1}, client,
		func(ctx context.Context, _ domain.Envelope[json.RawMessage]) error {
			if handled.Add(1) == 1 {
				close(started)
			}
			time.Sleep(300 * time.Millisecond)
			return nil
		}, internalsqs.WithVisibilityTimeout(time.Second), internalsqs.WithDrainTimeout(3*time.Second))
	require.NoError(t, err)
	done := make(chan struct{})
	go func() { defer close(done); _ = c.Start(context.Background()) }()
	<-started
	require.NoError(t, c.Stop())
	<-done
	mu.Lock()
	before := extended["rh-c"]
	mu.Unlock()
	time.Sleep(1200 * time.Millisecond) // past an extension tick
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, before, extended["rh-c"], "no extension for an undispatched message after Stop")
	assert.Equal(t, int32(1), handled.Load(), "queued messages are not processed after Stop")
	assert.Equal(t, []string{"rh-b", "rh-c"}, released, "undispatched messages handed back with visibility 0")
}

// With WithHandlerTimeout, a message waiting behind a stuck worker is handed
// back to the queue once the timeout passes, and is never processed here.
func TestHandlerTimeout_WaitingMessageReleased(t *testing.T) {
	var mu sync.Mutex
	var released []string
	var once atomic.Bool
	first := makeSQSMessage(domain.NewEnvelope("a.b.c", "svc", json.RawMessage(`{}`)))
	first.ReceiptHandle = aws.String("rh-stuck")
	second := makeSQSMessage(domain.NewEnvelope("a.b.c", "svc", json.RawMessage(`{}`)))
	second.ReceiptHandle = aws.String("rh-waiting")
	client := &mockSQSClient{
		receiveMessageFn: func(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
			if once.CompareAndSwap(false, true) {
				return &sqs.ReceiveMessageOutput{Messages: []sqstypes.Message{first, second}}, nil
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
		changeMessageVisibilityFn: func(_ context.Context, in *sqs.ChangeMessageVisibilityInput, _ ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
			if in.VisibilityTimeout == 0 {
				mu.Lock()
				released = append(released, aws.ToString(in.ReceiptHandle))
				mu.Unlock()
				return nil, errors.New("throttled") // a failed hand-back is logged, not fatal
			}
			return &sqs.ChangeMessageVisibilityOutput{}, nil
		},
		deleteMessageFn: func(context.Context, *sqs.DeleteMessageInput, ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
			return &sqs.DeleteMessageOutput{}, nil
		},
	}
	unblock := make(chan struct{})
	var handled atomic.Int32
	logger := &fixtures.MockLogger{}
	c, err := internalsqs.NewWithClient(internalsqs.Config{QueueURL: testQueueURL, WaitSeconds: 1, Logger: logger}, client,
		func(context.Context, domain.Envelope[json.RawMessage]) error {
			handled.Add(1)
			<-unblock // ignores its context: the worker is stuck
			return nil
		}, internalsqs.WithHandlerTimeout(500*time.Millisecond), internalsqs.WithVisibilityTimeout(time.Second),
		internalsqs.WithDrainTimeout(3*time.Second))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = c.Start(ctx) }()

	eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(released) == 1 && released[0] == "rh-waiting"
	}, "the waiting message is handed back after the handler timeout")
	close(unblock)
	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, int32(1), handled.Load(), "the released message is not processed by this consumer")
	var msgs []string
	for _, e := range logger.Entries() {
		msgs = append(msgs, e.Message)
	}
	assert.Contains(t, msgs, "sqs: message waited past the handler timeout without a free worker — released back to the queue")
	assert.Contains(t, msgs, "sqs: could not release message back to the queue; it reappears after its visibility timeout")
	cancel()
	<-done
}

// Without a visibility timeout there is no extension, so the consumer asks
// only for as many messages as it has free workers.
func TestReceive_NoVisibilityTimeout_SizedToFreeWorkers(t *testing.T) {
	var mu sync.Mutex
	var requested []int32
	client := &mockSQSClient{
		receiveMessageFn: func(ctx context.Context, in *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
			mu.Lock()
			requested = append(requested, in.MaxNumberOfMessages)
			mu.Unlock()
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	c, err := internalsqs.NewWithClient(internalsqs.Config{QueueURL: testQueueURL, WaitSeconds: 1}, client,
		func(context.Context, domain.Envelope[json.RawMessage]) error { return nil },
		internalsqs.WithConcurrency(3), internalsqs.WithDrainTimeout(time.Second))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = c.Start(ctx) }()
	eventually(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(requested) > 0 }, "received")
	cancel()
	<-done
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, int32(3), requested[0])
}
