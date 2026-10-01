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

	internalsqs "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/internal/adapter/outbound/sqs"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/events"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/events/mock"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/test/fixtures"
)

// signallingDLQ wraps mock.DLQPublisher and signals every SendToDLQ call.
type signallingDLQ struct {
	mock.DLQPublisher
	called chan struct{}
}

func newSignallingDLQ() *signallingDLQ {
	return &signallingDLQ{called: make(chan struct{}, 10)}
}

func (s *signallingDLQ) SendToDLQ(ctx context.Context, sourceQueueURL string, body []byte, attrs map[string]string, reason string) error {
	defer func() { s.called <- struct{}{} }()
	return s.DLQPublisher.SendToDLQ(ctx, sourceQueueURL, body, attrs, reason)
}

var _ port.DLQPublisher = (*signallingDLQ)(nil)

// consumeOnce runs a consumer that receives msg once. deletes reports the
// DeleteMessage calls observed so far; the consumer is stopped on test cleanup.
func consumeOnce(t *testing.T, msg sqstypes.Message, handler port.Handler, opts ...internalsqs.ConsumerOption) (deletes func() int32, logger *fixtures.MockLogger) {
	t.Helper()
	var deleteCount atomic.Int32
	var once sync.Once
	client := &mockSQSClient{
		receiveMessageFn: func(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
			first := false
			once.Do(func() { first = true })
			if first {
				return &sqs.ReceiveMessageOutput{Messages: []sqstypes.Message{msg}}, nil
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
		deleteMessageFn: func(context.Context, *sqs.DeleteMessageInput, ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
			deleteCount.Add(1)
			return &sqs.DeleteMessageOutput{}, nil
		},
	}
	if handler == nil {
		handler = func(context.Context, domain.Envelope[json.RawMessage]) error { return nil }
	}
	logger = &fixtures.MockLogger{}
	c, err := internalsqs.NewWithClient(internalsqs.Config{QueueURL: testQueueURL, WaitSeconds: 1, Logger: logger}, client, handler,
		append([]internalsqs.ConsumerOption{internalsqs.WithDrainTimeout(2 * time.Second)}, opts...)...)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = c.Start(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return deleteCount.Load, logger
}

func waitCalled(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s was not called", what)
	}
}

func withReceiveCount(msg sqstypes.Message, n string) sqstypes.Message {
	msg.Attributes = map[string]string{string(sqstypes.MessageSystemAttributeNameApproximateReceiveCount): n}
	return msg
}

func sourceAttrs() map[string]sqstypes.MessageAttributeValue {
	return map[string]sqstypes.MessageAttributeValue{
		"TenantID": {DataType: aws.String("String"), StringValue: aws.String("acme")},
		"Priority": {DataType: aws.String("Number"), StringValue: aws.String("3")},
		"Blob":     {DataType: aws.String("Binary"), BinaryValue: []byte{1, 2}},
	}
}

// ----------------------------
// Malformed bodies
// ----------------------------

func TestDispatch_Malformed_WithDLQ_ForwardsRawThenDeletes(t *testing.T) {
	dlq := newSignallingDLQ()
	msg := sqstypes.Message{
		MessageId:         aws.String("bad-1"),
		Body:              aws.String("not json {"),
		ReceiptHandle:     aws.String("rh-bad-1"),
		MessageAttributes: sourceAttrs(),
	}
	deletes, _ := consumeOnce(t, msg, nil, internalsqs.WithDLQPublisher(dlq))

	waitCalled(t, dlq.called, "SendToDLQ")
	assert.Eventually(t, func() bool { return deletes() == 1 }, 2*time.Second, 10*time.Millisecond)

	sent := dlq.Sent()
	require.Len(t, sent, 1)
	assert.Equal(t, testQueueURL, sent[0].SourceQueueURL)
	assert.Equal(t, "not json {", string(sent[0].Body))
	assert.Equal(t, map[string]string{"TenantID": "acme", "Priority": "3"}, sent[0].Attrs, "binary attributes omitted")
	assert.Contains(t, sent[0].Reason, "malformed message body: ")
}

func TestDispatch_Malformed_DLQForwardFails_LeftVisible(t *testing.T) {
	dlq := newSignallingDLQ()
	dlq.SetError(errors.New("sqs down"))
	msg := sqstypes.Message{MessageId: aws.String("bad-2"), Body: aws.String("{"), ReceiptHandle: aws.String("rh")}
	deletes, logger := consumeOnce(t, msg, nil, internalsqs.WithDLQPublisher(dlq))

	waitCalled(t, dlq.called, "SendToDLQ")
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int32(0), deletes(), "must not delete when the forward failed")
	assert.Eventually(t, func() bool {
		for _, e := range logger.Entries() {
			if e.Level == "ERROR" && e.Message == "sqs: DLQ forward failed — message left visible for retry" {
				return e.Fields["message_id"] == "bad-2" && e.Fields["error"] == "sqs down"
			}
		}
		return false
	}, time.Second, 10*time.Millisecond)
}

// ----------------------------
// Receive-count threshold
// ----------------------------

func TestDispatch_OverThreshold_DLQOnly_ForwardsAndDeletes(t *testing.T) {
	env := domain.NewEnvelope("order.created", "svc", json.RawMessage(`{}`))
	msg := withReceiveCount(makeSQSMessage(env), "4")
	msg.MessageAttributes = sourceAttrs()
	dlq := newSignallingDLQ()
	var handlerCalled atomic.Bool
	handler := func(context.Context, domain.Envelope[json.RawMessage]) error {
		handlerCalled.Store(true)
		return nil
	}
	deletes, logger := consumeOnce(t, msg, handler, internalsqs.WithDLQPublisher(dlq), internalsqs.WithMaxReceiveCount(3))

	waitCalled(t, dlq.called, "SendToDLQ")
	assert.Eventually(t, func() bool { return deletes() == 1 }, 2*time.Second, 10*time.Millisecond)
	assert.False(t, handlerCalled.Load(), "normal handler must not run for a dead-lettered message")

	sent := dlq.Sent()
	require.Len(t, sent, 1)
	assert.Equal(t, aws.ToString(msg.Body), string(sent[0].Body), "original body forwarded verbatim")
	assert.Equal(t, "acme", sent[0].Attrs["TenantID"])
	assert.Equal(t, "receive count 4 exceeded consumer max receive count 3", sent[0].Reason)

	var warned bool
	for _, e := range logger.Entries() {
		if e.Message == "sqs: dead-letter handler invoked" {
			warned = e.Fields["forwarded_to_dlq"] == true
		}
	}
	assert.True(t, warned)
}

func TestDispatch_DLQOnly_DefaultsMaxReceiveCount(t *testing.T) {
	env := domain.NewEnvelope("order.created", "svc", json.RawMessage(`{}`))
	dlq := newSignallingDLQ()
	deletes, logger := consumeOnce(t, withReceiveCount(makeSQSMessage(env), "6"), nil, internalsqs.WithDLQPublisher(dlq))

	waitCalled(t, dlq.called, "SendToDLQ")
	assert.Eventually(t, func() bool { return deletes() == 1 }, 2*time.Second, 10*time.Millisecond)
	assert.Contains(t, warnMessages(logger), "sqs: WithDeadLetterHandler/WithDLQPublisher set without WithMaxReceiveCount; defaulting maxReceiveCount to 5")
}

func TestDispatch_AtThreshold_NotForwarded(t *testing.T) {
	env := domain.NewEnvelope("order.created", "svc", json.RawMessage(`{}`))
	dlq := newSignallingDLQ()
	handled := make(chan struct{}, 1)
	handler := func(context.Context, domain.Envelope[json.RawMessage]) error {
		handled <- struct{}{}
		return nil
	}
	consumeOnce(t, withReceiveCount(makeSQSMessage(env), "3"), handler, internalsqs.WithDLQPublisher(dlq), internalsqs.WithMaxReceiveCount(3))
	waitCalled(t, handled, "handler")
	assert.Empty(t, dlq.Sent())
}

func TestDispatch_OverThreshold_DLHThenForward(t *testing.T) {
	env := domain.NewEnvelope("order.created", "svc", json.RawMessage(`{}`))
	msg := withReceiveCount(makeSQSMessage(env), "6")

	t.Run("DLH error skips forward", func(t *testing.T) {
		dlq := newSignallingDLQ()
		dlhCalled := make(chan struct{}, 1)
		dlh := func(context.Context, domain.Envelope[json.RawMessage]) error {
			dlhCalled <- struct{}{}
			return errors.New("dlh failed")
		}
		deletes, _ := consumeOnce(t, msg, nil, internalsqs.WithDeadLetterHandler(dlh), internalsqs.WithDLQPublisher(dlq), internalsqs.WithMaxReceiveCount(5))
		waitCalled(t, dlhCalled, "dead-letter handler")
		time.Sleep(100 * time.Millisecond)
		assert.Empty(t, dlq.Sent())
		assert.Equal(t, int32(0), deletes())
	})

	t.Run("DLH ok then forward ok deletes", func(t *testing.T) {
		dlq := newSignallingDLQ()
		var order []string
		var mu sync.Mutex
		dlh := func(ctx context.Context, _ domain.Envelope[json.RawMessage]) error {
			mu.Lock()
			order = append(order, "dlh")
			mu.Unlock()
			src, ok := port.SourceMessageFromContext(ctx)
			assert.True(t, ok)
			assert.Equal(t, 6, src.ReceiveCount)
			return nil
		}
		deletes, _ := consumeOnce(t, msg, nil, internalsqs.WithDeadLetterHandler(dlh), internalsqs.WithDLQPublisher(dlq), internalsqs.WithMaxReceiveCount(5))
		waitCalled(t, dlq.called, "SendToDLQ")
		assert.Eventually(t, func() bool { return deletes() == 1 }, 2*time.Second, 10*time.Millisecond)
		mu.Lock()
		assert.Equal(t, []string{"dlh"}, order)
		mu.Unlock()
	})

	t.Run("DLH ok then forward fails leaves visible", func(t *testing.T) {
		dlq := newSignallingDLQ()
		dlq.SetError(errors.New("sqs down"))
		dlh := func(context.Context, domain.Envelope[json.RawMessage]) error { return nil }
		deletes, logger := consumeOnce(t, msg, nil, internalsqs.WithDeadLetterHandler(dlh), internalsqs.WithDLQPublisher(dlq), internalsqs.WithMaxReceiveCount(5))
		waitCalled(t, dlq.called, "SendToDLQ")
		time.Sleep(100 * time.Millisecond)
		assert.Equal(t, int32(0), deletes())
		for _, e := range logger.Entries() {
			assert.NotEqual(t, "sqs: dead-letter handler failed — message left visible for retry", e.Message,
				"a failed forward is logged once, by the forward path")
		}
	})
}

// ----------------------------
// Codec decode failures past the threshold
// ----------------------------

func TestDispatch_DecodeFailure_OverThreshold_ForwardsUndecoded(t *testing.T) {
	env := makeCodecEncodedEnvelope(t, "order.created", "schema-1", json.RawMessage(`{"a":1}`))
	msg := withReceiveCount(makeSQSMessage(env), "6")
	failing := &fakeCodec{decodeFn: func(context.Context, string, []byte) (json.RawMessage, error) {
		return nil, errors.New("unknown schema")
	}}
	dlq := newSignallingDLQ()
	deletes, _ := consumeOnce(t, msg, nil, internalsqs.WithCodec(failing), internalsqs.WithDLQPublisher(dlq), internalsqs.WithMaxReceiveCount(5))

	waitCalled(t, dlq.called, "SendToDLQ")
	assert.Eventually(t, func() bool { return deletes() == 1 }, 2*time.Second, 10*time.Millisecond)
	sent := dlq.Sent()
	require.Len(t, sent, 1)
	assert.Equal(t, aws.ToString(msg.Body), string(sent[0].Body), "encoded payload forwarded as received")
	assert.Contains(t, sent[0].Reason, "codec decode failed: ")
	assert.Contains(t, sent[0].Reason, "unknown schema")
}

func TestDispatch_DecodeFailure_UnderThreshold_NotForwarded(t *testing.T) {
	env := makeCodecEncodedEnvelope(t, "order.created", "schema-1", json.RawMessage(`{"a":1}`))
	decodeCalled := make(chan struct{}, 1)
	failing := &fakeCodec{decodeFn: func(context.Context, string, []byte) (json.RawMessage, error) {
		decodeCalled <- struct{}{}
		return nil, errors.New("registry down")
	}}
	dlq := newSignallingDLQ()
	deletes, _ := consumeOnce(t, withReceiveCount(makeSQSMessage(env), "2"), nil, internalsqs.WithCodec(failing), internalsqs.WithDLQPublisher(dlq), internalsqs.WithMaxReceiveCount(5))
	waitCalled(t, decodeCalled, "Decode")
	time.Sleep(100 * time.Millisecond)
	assert.Empty(t, dlq.Sent())
	assert.Equal(t, int32(0), deletes())
}

// ----------------------------
// SourceMessage on the handler context
// ----------------------------

func TestDispatch_HandlerContextCarriesSourceMessage(t *testing.T) {
	env := makeCodecEncodedEnvelope(t, "order.created", "schema-1", json.RawMessage(`{"a":1}`))
	msg := withReceiveCount(makeSQSMessage(env), "2")
	msg.MessageAttributes = sourceAttrs()

	got := make(chan port.SourceMessage, 1)
	handler := func(ctx context.Context, e domain.Envelope[json.RawMessage]) error {
		src, ok := events.SourceMessageFromContext(ctx)
		assert.True(t, ok)
		assert.JSONEq(t, `{"a":1}`, string(e.Payload), "handler sees the decoded payload")
		// Each retrieval is an independent copy.
		src.Body[0] = 'X'
		src.Attributes["TenantID"] = "changed"
		again, _ := events.SourceMessageFromContext(ctx)
		got <- again
		return nil
	}
	consumeOnce(t, msg, handler, internalsqs.WithCodec(reversingDecodeCodec()))

	select {
	case src := <-got:
		assert.Equal(t, testQueueURL, src.QueueURL)
		assert.Equal(t, aws.ToString(msg.MessageId), src.MessageID)
		assert.Equal(t, aws.ToString(msg.Body), string(src.Body), "body is the undecoded original")
		assert.Equal(t, map[string]string{"TenantID": "acme", "Priority": "3"}, src.Attributes)
		assert.Equal(t, 2, src.ReceiveCount)
	case <-time.After(5 * time.Second):
		t.Fatal("handler was not called")
	}
}

func TestSourceMessageFromContext_Absent(t *testing.T) {
	_, ok := events.SourceMessageFromContext(context.Background())
	assert.False(t, ok)
	_, ok = events.SourceMessageFromContext(port.WithSourceMessage(context.Background(), nil))
	assert.False(t, ok, "a nil loader is treated as absent")
}

func TestSourceMessageFromContext_LoadsLazily(t *testing.T) {
	var loads atomic.Int32
	ctx := port.WithSourceMessage(context.Background(), func() port.SourceMessage {
		loads.Add(1)
		return port.SourceMessage{Body: []byte("abc")}
	})
	assert.Equal(t, int32(0), loads.Load(), "nothing is built until requested")
	src, ok := events.SourceMessageFromContext(ctx)
	require.True(t, ok)
	assert.Equal(t, "abc", string(src.Body))
	assert.Equal(t, int32(1), loads.Load())
}

// ----------------------------
// Startup DLQ check
// ----------------------------

func startConsumerWithDLQ(t *testing.T, dlq port.DLQPublisher) (*fixtures.MockLogger, error) {
	t.Helper()
	client := &mockSQSClient{receiveMessageFn: func(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	logger := &fixtures.MockLogger{}
	c, err := internalsqs.NewWithClient(internalsqs.Config{QueueURL: testQueueURL, WaitSeconds: 1, Logger: logger}, client,
		func(context.Context, domain.Envelope[json.RawMessage]) error { return nil },
		internalsqs.WithDLQPublisher(dlq), internalsqs.WithDrainTimeout(time.Second))
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	return logger, c.Start(ctx)
}

func TestStart_DLQForwarding_PermanentConfigErrorFailsStart(t *testing.T) {
	for _, kind := range []error{events.ErrDLQNotConfigured, events.ErrDLQInvalidRedrivePolicy} {
		t.Run(kind.Error(), func(t *testing.T) {
			dlq := &mock.DLQPublisher{}
			dlq.SetError(&events.DLQError{Kind: kind, SourceQueue: testQueueURL})
			_, err := startConsumerWithDLQ(t, dlq)
			require.ErrorIs(t, err, kind)
			assert.Contains(t, err.Error(), "no usable RedrivePolicy")
		})
	}
}

func TestStart_DLQForwarding_TransientErrorWarnsAndStarts(t *testing.T) {
	dlq := &mock.DLQPublisher{}
	dlq.SetError(&events.DLQError{Kind: events.ErrDLQUnresolved, Cause: errors.New("throttled")})
	logger, err := startConsumerWithDLQ(t, dlq)
	require.NoError(t, err, "Start runs until its context ends")
	assert.Contains(t, warnMessages(logger), "sqs: could not resolve DLQ at startup; forwards will retry resolution")
}

func TestStart_DLQForwarding_ResolvedLogsInfo(t *testing.T) {
	logger, err := startConsumerWithDLQ(t, &mock.DLQPublisher{DLQURL: "https://sqs/dlq"})
	require.NoError(t, err)
	var found bool
	for _, e := range logger.Entries() {
		if e.Level == "INFO" && e.Message == "sqs: DLQ forwarding enabled" {
			found = e.Fields["dlq_url"] == "https://sqs/dlq"
		}
	}
	assert.True(t, found)
}

// ----------------------------
// Forward deadline vs visibility timeout
// ----------------------------

// deadlineDLQ records the time remaining on each SendToDLQ context.
type deadlineDLQ struct {
	mock.DLQPublisher
	remaining chan time.Duration
}

func (d *deadlineDLQ) SendToDLQ(ctx context.Context, _ string, _ []byte, _ map[string]string, _ string) error {
	dl, ok := ctx.Deadline()
	if !ok {
		d.remaining <- -1
		return nil
	}
	d.remaining <- time.Until(dl)
	return nil
}

func TestDispatch_DLQForwardTimeout_BoundedByVisibility(t *testing.T) {
	cases := map[string]struct {
		opts []internalsqs.ConsumerOption
		want time.Duration
	}{
		"no visibility timeout": {nil, 30 * time.Second},
		"visibility 10s → 5s":   {[]internalsqs.ConsumerOption{internalsqs.WithVisibilityTimeout(10 * time.Second)}, 5 * time.Second},
		"visibility 1s → 1s":    {[]internalsqs.ConsumerOption{internalsqs.WithVisibilityTimeout(time.Second)}, time.Second},
		"visibility 5m → 30s":   {[]internalsqs.ConsumerOption{internalsqs.WithVisibilityTimeout(5 * time.Minute)}, 30 * time.Second},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			dlq := &deadlineDLQ{remaining: make(chan time.Duration, 1)}
			msg := sqstypes.Message{MessageId: aws.String("m"), Body: aws.String("{"), ReceiptHandle: aws.String("rh")}
			consumeOnce(t, msg, nil, append(tc.opts, internalsqs.WithDLQPublisher(dlq))...)
			select {
			case got := <-dlq.remaining:
				assert.InDelta(t, tc.want.Seconds(), got.Seconds(), 0.5)
			case <-time.After(5 * time.Second):
				t.Fatal("SendToDLQ was not called")
			}
		})
	}
}

func TestPublicWithDLQForwarding_WiresPublisher(t *testing.T) {
	dlq := newSignallingDLQ()
	client := &mockSQSClient{}
	var once sync.Once
	client.receiveMessageFn = func(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
		first := false
		once.Do(func() { first = true })
		if first {
			return &sqs.ReceiveMessageOutput{Messages: []sqstypes.Message{{MessageId: aws.String("m"), Body: aws.String("{"), ReceiptHandle: aws.String("rh")}}}, nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	c, err := events.NewSQSConsumerWithClient(events.SQSConfig{QueueURL: testQueueURL, WaitSeconds: 1}, client,
		func(context.Context, events.Envelope[json.RawMessage]) error { return nil },
		events.WithDLQForwarding(dlq), events.WithDrainTimeout(time.Second))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = c.Start(ctx) }()
	defer func() { cancel(); _ = c.Stop() }()
	waitCalled(t, dlq.called, "SendToDLQ")
	require.Len(t, dlq.Sent(), 1)
}
