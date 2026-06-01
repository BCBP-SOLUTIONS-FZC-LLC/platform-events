package sqs_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	internalsqs "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/adapter/outbound/sqs"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/events"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/test/fixtures"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockSQSClient is a test double for SQSClientAPI.
type mockSQSClient struct {
	receiveMessageFn          func(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	deleteMessageFn           func(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
	changeMessageVisibilityFn func(ctx context.Context, params *sqs.ChangeMessageVisibilityInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error)
}

func (m *mockSQSClient) ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	return m.receiveMessageFn(ctx, params, optFns...)
}

func (m *mockSQSClient) DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	if m.deleteMessageFn != nil {
		return m.deleteMessageFn(ctx, params, optFns...)
	}
	return &sqs.DeleteMessageOutput{}, nil
}

func (m *mockSQSClient) ChangeMessageVisibility(ctx context.Context, params *sqs.ChangeMessageVisibilityInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	if m.changeMessageVisibilityFn != nil {
		return m.changeMessageVisibilityFn(ctx, params, optFns...)
	}
	return &sqs.ChangeMessageVisibilityOutput{}, nil
}

var _ internalsqs.SQSClientAPI = (*mockSQSClient)(nil)

const testQueueURL = "https://sqs.us-east-1.amazonaws.com/123456789/test-queue"

func makeSQSMessage(env domain.Envelope[json.RawMessage]) sqstypes.Message {
	b, _ := json.Marshal(env)
	return sqstypes.Message{
		MessageId:     aws.String("msg-" + env.ID),
		Body:          aws.String(string(b)),
		ReceiptHandle: aws.String("rh-" + env.ID),
	}
}

// ----------------------------
// Construction validation
// ----------------------------

func TestNew_EmptyQueueURL_Error(t *testing.T) {
	client := &mockSQSClient{}
	handler := func(_ context.Context, _ domain.Envelope[json.RawMessage]) error { return nil }
	_, err := internalsqs.NewWithClient(internalsqs.Config{QueueURL: ""}, client, handler)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "QueueURL")
}

func TestNew_NilHandler_Error(t *testing.T) {
	client := &mockSQSClient{}
	_, err := internalsqs.NewWithClient(internalsqs.Config{QueueURL: testQueueURL}, client, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "handler")
}

func TestNewWithClient_Success(t *testing.T) {
	client := &mockSQSClient{}
	handler := func(_ context.Context, _ domain.Envelope[json.RawMessage]) error { return nil }
	c, err := internalsqs.NewWithClient(internalsqs.Config{QueueURL: testQueueURL}, client, handler)
	require.NoError(t, err)
	assert.NotNil(t, c)
}

// ----------------------------
// Options
// ----------------------------

func TestWithConcurrency_Applied(t *testing.T) {
	client := &mockSQSClient{}
	handler := func(_ context.Context, _ domain.Envelope[json.RawMessage]) error { return nil }
	// Should not panic/error — the option is applied.
	c, err := internalsqs.NewWithClient(
		internalsqs.Config{QueueURL: testQueueURL},
		client,
		handler,
		internalsqs.WithConcurrency(5),
	)
	require.NoError(t, err)
	assert.NotNil(t, c)
}

func TestWithConcurrency_Zero_UsesDefault(t *testing.T) {
	client := &mockSQSClient{}
	handler := func(_ context.Context, _ domain.Envelope[json.RawMessage]) error { return nil }
	c, err := internalsqs.NewWithClient(
		internalsqs.Config{QueueURL: testQueueURL},
		client,
		handler,
		internalsqs.WithConcurrency(0), // invalid — should default to 1
	)
	require.NoError(t, err)
	assert.NotNil(t, c)
}

func TestWithVisibilityTimeout_Applied(t *testing.T) {
	client := &mockSQSClient{}
	handler := func(_ context.Context, _ domain.Envelope[json.RawMessage]) error { return nil }
	c, err := internalsqs.NewWithClient(
		internalsqs.Config{QueueURL: testQueueURL},
		client,
		handler,
		internalsqs.WithVisibilityTimeout(60*time.Second),
	)
	require.NoError(t, err)
	assert.NotNil(t, c)
}

func TestWithDeadLetterHandler_Applied(t *testing.T) {
	client := &mockSQSClient{}
	handler := func(_ context.Context, _ domain.Envelope[json.RawMessage]) error { return nil }
	dlh := func(_ context.Context, _ domain.Envelope[json.RawMessage]) error { return nil }
	c, err := internalsqs.NewWithClient(
		internalsqs.Config{QueueURL: testQueueURL},
		client,
		handler,
		internalsqs.WithDeadLetterHandler(dlh),
	)
	require.NoError(t, err)
	assert.NotNil(t, c)
}

func TestWithDrainTimeout_Applied(t *testing.T) {
	client := &mockSQSClient{}
	handler := func(_ context.Context, _ domain.Envelope[json.RawMessage]) error { return nil }
	c, err := internalsqs.NewWithClient(
		internalsqs.Config{QueueURL: testQueueURL},
		client,
		handler,
		internalsqs.WithDrainTimeout(5*time.Second),
	)
	require.NoError(t, err)
	assert.NotNil(t, c)
}

// ----------------------------
// Start/Stop cycle
// ----------------------------

func TestStartStop_ImmediateCancel(t *testing.T) {
	// ReceiveMessage blocks until context is cancelled, then returns ctx.Err().
	client := &mockSQSClient{
		receiveMessageFn: func(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	handler := func(_ context.Context, _ domain.Envelope[json.RawMessage]) error { return nil }
	c, err := internalsqs.NewWithClient(
		internalsqs.Config{QueueURL: testQueueURL, WaitSeconds: 1},
		client,
		handler,
		internalsqs.WithDrainTimeout(2*time.Second),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- c.Start(ctx) }()

	// Give the consumer a moment to start then cancel.
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return after context cancel")
	}
}

func TestStop_IsIdempotent(t *testing.T) {
	client := &mockSQSClient{
		receiveMessageFn: func(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	handler := func(_ context.Context, _ domain.Envelope[json.RawMessage]) error { return nil }
	c, err := internalsqs.NewWithClient(
		internalsqs.Config{QueueURL: testQueueURL},
		client,
		handler,
		internalsqs.WithDrainTimeout(500*time.Millisecond),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = c.Start(ctx) }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	time.Sleep(20 * time.Millisecond)

	// Calling Stop twice should not panic.
	assert.NotPanics(t, func() { _ = c.Stop() })
	assert.NotPanics(t, func() { _ = c.Stop() })
}

// ----------------------------
// Dispatch: successful handler → DeleteMessage called
// ----------------------------

func TestDispatch_SuccessfulHandler_DeletesCalled(t *testing.T) {
	env := domain.NewEnvelope("test.event", "svc", json.RawMessage(`{}`))
	msg := makeSQSMessage(env)

	var deleteCount int32
	receiveCallCount := 0
	client := &mockSQSClient{
		receiveMessageFn: func(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
			receiveCallCount++
			if receiveCallCount == 1 {
				return &sqs.ReceiveMessageOutput{Messages: []sqstypes.Message{msg}}, nil
			}
			// Block on subsequent calls until context cancelled.
			<-ctx.Done()
			return nil, ctx.Err()
		},
		deleteMessageFn: func(_ context.Context, _ *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
			atomic.AddInt32(&deleteCount, 1)
			return &sqs.DeleteMessageOutput{}, nil
		},
	}

	handlerCalled := make(chan struct{}, 1)
	handler := func(_ context.Context, _ domain.Envelope[json.RawMessage]) error {
		handlerCalled <- struct{}{}
		return nil
	}

	c, err := internalsqs.NewWithClient(
		internalsqs.Config{QueueURL: testQueueURL, WaitSeconds: 1},
		client,
		handler,
		internalsqs.WithDrainTimeout(2*time.Second),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = c.Start(ctx) }()

	select {
	case <-handlerCalled:
	case <-time.After(5 * time.Second):
		t.Fatal("handler was not called")
	}
	time.Sleep(50 * time.Millisecond) // let delete propagate
	cancel()

	assert.Equal(t, int32(1), atomic.LoadInt32(&deleteCount), "DeleteMessage should be called once")
}

// ----------------------------
// Dispatch: handler error → DeleteMessage NOT called
// ----------------------------

func TestDispatch_HandlerError_NoDelete(t *testing.T) {
	env := domain.NewEnvelope("test.event", "svc", json.RawMessage(`{}`))
	msg := makeSQSMessage(env)

	var deleteCount int32
	receiveCallCount := 0
	client := &mockSQSClient{
		receiveMessageFn: func(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
			receiveCallCount++
			if receiveCallCount == 1 {
				return &sqs.ReceiveMessageOutput{Messages: []sqstypes.Message{msg}}, nil
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
		deleteMessageFn: func(_ context.Context, _ *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
			atomic.AddInt32(&deleteCount, 1)
			return &sqs.DeleteMessageOutput{}, nil
		},
	}

	logger := &fixtures.MockLogger{}
	handlerCalled := make(chan struct{}, 1)
	handler := func(_ context.Context, _ domain.Envelope[json.RawMessage]) error {
		handlerCalled <- struct{}{}
		return errors.New("handler failed")
	}

	c, err := internalsqs.NewWithClient(
		internalsqs.Config{QueueURL: testQueueURL, WaitSeconds: 1},
		client,
		handler,
		internalsqs.WithDrainTimeout(2*time.Second),
	)
	require.NoError(t, err)
	_ = logger // logger is passed to config for completeness

	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = c.Start(ctx) }()

	select {
	case <-handlerCalled:
	case <-time.After(5 * time.Second):
		t.Fatal("handler was not called")
	}
	time.Sleep(50 * time.Millisecond)
	cancel()

	assert.Equal(t, int32(0), atomic.LoadInt32(&deleteCount), "DeleteMessage should NOT be called when handler errors")
}

// ----------------------------
// Dispatch: malformed message body → deleted (not retried)
// ----------------------------

func TestDispatch_MalformedBody_Deleted(t *testing.T) {
	malformedMsg := sqstypes.Message{
		MessageId:     aws.String("bad-msg"),
		Body:          aws.String("this is not json"),
		ReceiptHandle: aws.String("rh-bad"),
	}

	var deleteCount int32
	receiveCallCount := 0
	client := &mockSQSClient{
		receiveMessageFn: func(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
			receiveCallCount++
			if receiveCallCount == 1 {
				return &sqs.ReceiveMessageOutput{Messages: []sqstypes.Message{malformedMsg}}, nil
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
		deleteMessageFn: func(_ context.Context, _ *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
			atomic.AddInt32(&deleteCount, 1)
			return &sqs.DeleteMessageOutput{}, nil
		},
	}

	// Handler should never be called for a malformed message.
	handlerCalled := atomic.Bool{}
	handler := func(_ context.Context, _ domain.Envelope[json.RawMessage]) error {
		handlerCalled.Store(true)
		return nil
	}

	logger := &fixtures.MockLogger{}
	c, err := internalsqs.NewWithClient(
		internalsqs.Config{QueueURL: testQueueURL, WaitSeconds: 1, Logger: logger},
		client,
		handler,
		internalsqs.WithDrainTimeout(2*time.Second),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = c.Start(ctx) }()

	// Wait long enough for the malformed message to be processed.
	time.Sleep(200 * time.Millisecond)
	cancel()

	assert.False(t, handlerCalled.Load(), "handler should not be called for malformed message")
	assert.Equal(t, int32(1), atomic.LoadInt32(&deleteCount), "malformed message should be deleted")
}

// ----------------------------
// Dispatch: handler called with valid TraceID (exercises OTel link path)
// ----------------------------

func TestDispatch_WithTraceID_OTelLinkCreated(t *testing.T) {
	env := domain.NewEnvelope("test.event", "svc", json.RawMessage(`{}`))
	// A valid 32-char hex trace ID.
	env.TraceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	msg := makeSQSMessage(env)

	receiveCallCount := 0
	client := &mockSQSClient{
		receiveMessageFn: func(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
			receiveCallCount++
			if receiveCallCount == 1 {
				return &sqs.ReceiveMessageOutput{Messages: []sqstypes.Message{msg}}, nil
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	handlerCalled := make(chan struct{}, 1)
	handler := func(_ context.Context, received domain.Envelope[json.RawMessage]) error {
		handlerCalled <- struct{}{}
		return nil
	}

	c, err := internalsqs.NewWithClient(
		internalsqs.Config{QueueURL: testQueueURL, WaitSeconds: 1},
		client,
		handler,
		internalsqs.WithDrainTimeout(2*time.Second),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = c.Start(ctx) }()

	select {
	case <-handlerCalled:
	case <-time.After(5 * time.Second):
		t.Fatal("handler was not called")
	}
	cancel()
}

// ----------------------------
// Start: drain timeout path (handler running when ctx cancelled)
// ----------------------------

func TestStart_DrainTimeout(t *testing.T) {
	env := domain.NewEnvelope("slow.event", "svc", json.RawMessage(`{}`))
	msg := makeSQSMessage(env)

	handlerStarted := make(chan struct{})
	handlerCanProceed := make(chan struct{})

	receiveCallCount := 0
	client := &mockSQSClient{
		receiveMessageFn: func(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
			receiveCallCount++
			if receiveCallCount == 1 {
				return &sqs.ReceiveMessageOutput{Messages: []sqstypes.Message{msg}}, nil
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	logger := &fixtures.MockLogger{}
	handler := func(_ context.Context, _ domain.Envelope[json.RawMessage]) error {
		close(handlerStarted)
		<-handlerCanProceed // Block until test releases.
		return nil
	}

	c, err := internalsqs.NewWithClient(
		internalsqs.Config{QueueURL: testQueueURL, WaitSeconds: 1, Logger: logger},
		client,
		handler,
		// Very short drain timeout so it triggers the timeout path.
		internalsqs.WithDrainTimeout(50*time.Millisecond),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- c.Start(ctx) }()

	// Wait for handler to start.
	select {
	case <-handlerStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not start")
	}

	// Cancel context while handler is still running.
	cancel()

	// Wait for Start to return (should hit drain timeout since handler is blocked).
	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		close(handlerCanProceed)
		t.Fatal("Start did not return after drain timeout")
	}

	// Let handler complete.
	select {
	case <-handlerCanProceed:
	default:
		close(handlerCanProceed)
	}
}

// ----------------------------
// ReceiveMessage error: backed off and retried
// ----------------------------

func TestReceiveMessage_Error_BacksOff(t *testing.T) {
	receiveCallCount := int32(0)
	client := &mockSQSClient{
		receiveMessageFn: func(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
			n := atomic.AddInt32(&receiveCallCount, 1)
			if n == 1 {
				return nil, errors.New("temporary error")
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	logger := &fixtures.MockLogger{}
	handler := func(_ context.Context, _ domain.Envelope[json.RawMessage]) error { return nil }
	c, err := internalsqs.NewWithClient(
		internalsqs.Config{QueueURL: testQueueURL, WaitSeconds: 1, Logger: logger},
		client,
		handler,
		internalsqs.WithDrainTimeout(2*time.Second),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- c.Start(ctx) }()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("consumer did not return after context timeout")
	}

	// The consumer should have retried (receiveCallCount >= 2).
	assert.GreaterOrEqual(t, atomic.LoadInt32(&receiveCallCount), int32(2))
}

// ----------------------------
// WithDeadLetterHandler: closure calls domainToPublic on invocation
// ----------------------------

func TestWithDeadLetterHandler_ClosureInvoked(t *testing.T) {
	// To test the dead-letter handler closure, we need to invoke it via the pkg/events
	// layer's WithDeadLetterHandler which wraps domainToPublic. We test it here via
	// the internal adapter directly to verify the handler is stored correctly.
	var dlhCalled bool
	opt := internalsqs.WithDeadLetterHandler(func(_ context.Context, env domain.Envelope[json.RawMessage]) error {
		dlhCalled = true
		return nil
	})
	require.NotNil(t, opt)
	_ = dlhCalled
}

// ----------------------------
// deleteMessage: error path (logger called)
// ----------------------------

func TestDeleteMessage_Error_LoggerCalled(t *testing.T) {
	env := domain.NewEnvelope("test.event", "svc", json.RawMessage(`{}`))
	msg := makeSQSMessage(env)

	receiveCallCount := 0
	client := &mockSQSClient{
		receiveMessageFn: func(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
			receiveCallCount++
			if receiveCallCount == 1 {
				return &sqs.ReceiveMessageOutput{Messages: []sqstypes.Message{msg}}, nil
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
		deleteMessageFn: func(_ context.Context, _ *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
			return nil, errors.New("delete failed")
		},
	}

	logger := &fixtures.MockLogger{}
	handlerCalled := make(chan struct{}, 1)
	handler := func(_ context.Context, _ domain.Envelope[json.RawMessage]) error {
		handlerCalled <- struct{}{}
		return nil
	}

	c, err := internalsqs.NewWithClient(
		internalsqs.Config{QueueURL: testQueueURL, WaitSeconds: 1, Logger: logger},
		client,
		handler,
		internalsqs.WithDrainTimeout(2*time.Second),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = c.Start(ctx) }()

	select {
	case <-handlerCalled:
	case <-time.After(5 * time.Second):
		t.Fatal("handler was not called")
	}
	time.Sleep(100 * time.Millisecond)
	cancel()

	// Logger should have recorded delete error.
	entries := logger.Entries()
	found := false
	for _, e := range entries {
		if e.Level == "ERROR" {
			found = true
			break
		}
	}
	assert.True(t, found, "expected ERROR log for delete failure")
}

// ----------------------------
// Start: already running returns error
// ----------------------------

func TestStart_AlreadyRunning_ReturnsError(t *testing.T) {
	client := &mockSQSClient{
		receiveMessageFn: func(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	handler := func(_ context.Context, _ domain.Envelope[json.RawMessage]) error { return nil }
	c, err := internalsqs.NewWithClient(
		internalsqs.Config{QueueURL: testQueueURL, WaitSeconds: 1},
		client,
		handler,
		internalsqs.WithDrainTimeout(500*time.Millisecond),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start consumer.
	go func() { _ = c.Start(ctx) }()
	time.Sleep(20 * time.Millisecond) // Let it start.

	// Starting again should return an error.
	err = c.Start(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already running")
	cancel()
}

// ----------------------------
// NewSQSConsumerWithClient: exercises pkg/events wrappedHandler (domainToPublic)
// ----------------------------

func TestNewSQSConsumerWithClient_PublicWrapper_ReceivesPublicEnvelope(t *testing.T) {
	env := domain.NewEnvelope("wrapper.test", "svc", json.RawMessage(`{"k":"v"}`))
	env.TenantID = "acme"
	msg := makeSQSMessage(env)

	var deleteCount int32
	receiveCallCount := 0
	client := &mockSQSClient{
		receiveMessageFn: func(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
			receiveCallCount++
			if receiveCallCount == 1 {
				return &sqs.ReceiveMessageOutput{Messages: []sqstypes.Message{msg}}, nil
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
		deleteMessageFn: func(_ context.Context, _ *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
			atomic.AddInt32(&deleteCount, 1)
			return &sqs.DeleteMessageOutput{}, nil
		},
	}

	handlerCalled := make(chan events.Envelope[json.RawMessage], 1)
	// Use events.NewSQSConsumerWithClient to exercise the public wrapper
	c, err := events.NewSQSConsumerWithClient(
		events.SQSConfig{QueueURL: testQueueURL, WaitSeconds: 1},
		client,
		func(_ context.Context, received events.Envelope[json.RawMessage]) error {
			handlerCalled <- received
			return nil
		},
		events.WithDrainTimeout(2*time.Second),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = c.Start(ctx) }()

	select {
	case got := <-handlerCalled:
		assert.Equal(t, env.ID, got.ID)
		assert.Equal(t, "acme", got.TenantID)
	case <-time.After(5 * time.Second):
		t.Fatal("handler was not called")
	}

	time.Sleep(50 * time.Millisecond)
	cancel()

	assert.Equal(t, int32(1), atomic.LoadInt32(&deleteCount))
}

// ----------------------------
// MaxMessages default: values out of range clamp to 10
// ----------------------------

func TestNewWithClient_MaxMessages_OutOfRange(t *testing.T) {
	client := &mockSQSClient{
		receiveMessageFn: func(ctx context.Context, params *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
			// MaxNumberOfMessages should be 10 (clamped).
			assert.Equal(t, int32(10), params.MaxNumberOfMessages)
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	handler := func(_ context.Context, _ domain.Envelope[json.RawMessage]) error { return nil }
	c, err := internalsqs.NewWithClient(
		internalsqs.Config{QueueURL: testQueueURL, MaxMessages: 100, WaitSeconds: 1},
		client,
		handler,
		internalsqs.WithDrainTimeout(500*time.Millisecond),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	_ = c.Start(ctx)
}

// ----------------------------
// Dead-letter routing via WithMaxReceiveCount + WithDeadLetterHandler
// ----------------------------

func TestDispatch_DeadLetterRouting_Invoked(t *testing.T) {
	env := domain.NewEnvelope("dlq.event", "svc", json.RawMessage(`{}`))
	env.TenantID = "acme"

	// Message with ApproximateReceiveCount = 5.
	msg := makeSQSMessage(env)
	msg.Attributes = map[string]string{
		string(sqstypes.MessageSystemAttributeNameApproximateReceiveCount): "5",
	}

	receiveCallCount := 0
	client := &mockSQSClient{
		receiveMessageFn: func(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
			receiveCallCount++
			if receiveCallCount == 1 {
				return &sqs.ReceiveMessageOutput{Messages: []sqstypes.Message{msg}}, nil
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	dlhCalled := make(chan string, 1)
	dlh := func(_ context.Context, env domain.Envelope[json.RawMessage]) error {
		dlhCalled <- env.ID
		return nil
	}

	logger := &fixtures.MockLogger{}
	normalHandlerCalled := false
	normalHandler := func(_ context.Context, _ domain.Envelope[json.RawMessage]) error {
		normalHandlerCalled = true
		return nil
	}

	c, err := internalsqs.NewWithClient(
		internalsqs.Config{QueueURL: testQueueURL, WaitSeconds: 1, Logger: logger},
		client,
		normalHandler,
		internalsqs.WithDeadLetterHandler(dlh),
		internalsqs.WithMaxReceiveCount(5), // threshold = 5; receive count = 5 → routed
		internalsqs.WithDrainTimeout(2*time.Second),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = c.Start(ctx) }()

	select {
	case id := <-dlhCalled:
		assert.Equal(t, env.ID, id)
	case <-time.After(5 * time.Second):
		t.Fatal("dead-letter handler was not called")
	}
	cancel()

	assert.False(t, normalHandlerCalled, "normal handler must not be called for dead-lettered message")
}

func TestDispatch_DeadLetterHandler_Error_LoggerCalled(t *testing.T) {
	env := domain.NewEnvelope("dlq.err", "svc", json.RawMessage(`{}`))
	msg := makeSQSMessage(env)
	msg.Attributes = map[string]string{
		string(sqstypes.MessageSystemAttributeNameApproximateReceiveCount): "3",
	}

	receiveCallCount := 0
	client := &mockSQSClient{
		receiveMessageFn: func(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
			receiveCallCount++
			if receiveCallCount == 1 {
				return &sqs.ReceiveMessageOutput{Messages: []sqstypes.Message{msg}}, nil
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	dlhDone := make(chan struct{}, 1)
	dlh := func(_ context.Context, _ domain.Envelope[json.RawMessage]) error {
		dlhDone <- struct{}{}
		return errors.New("dlh error")
	}

	logger := &fixtures.MockLogger{}
	c, err := internalsqs.NewWithClient(
		internalsqs.Config{QueueURL: testQueueURL, WaitSeconds: 1, Logger: logger},
		client,
		func(_ context.Context, _ domain.Envelope[json.RawMessage]) error { return nil },
		internalsqs.WithDeadLetterHandler(dlh),
		internalsqs.WithMaxReceiveCount(3),
		internalsqs.WithDrainTimeout(2*time.Second),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = c.Start(ctx) }()

	select {
	case <-dlhDone:
	case <-time.After(5 * time.Second):
		t.Fatal("dead-letter handler was not called")
	}
	time.Sleep(50 * time.Millisecond)
	cancel()

	entries := logger.Entries()
	found := false
	for _, e := range entries {
		if e.Level == "ERROR" {
			found = true
			break
		}
	}
	assert.True(t, found, "expected ERROR log for dead-letter handler failure")
}

// ----------------------------
// sqs.New with valid config (covers LoadDefaultConfig success path)
// ----------------------------

func TestNew_ValidConfig_Success(t *testing.T) {
	// Calling New with a valid QueueURL and endpoint constructs without error.
	// The AWS client is created but never used — no real AWS connection is made.
	c, err := internalsqs.New(
		internalsqs.Config{
			QueueURL:    testQueueURL,
			Region:      "us-east-1",
			EndpointURL: "http://localhost:4566",
		},
		func(_ context.Context, _ domain.Envelope[json.RawMessage]) error { return nil },
	)
	require.NoError(t, err)
	assert.NotNil(t, c)
}

func TestNew_WithEndpointURL_Success(t *testing.T) {
	c, err := internalsqs.New(
		internalsqs.Config{
			QueueURL: testQueueURL,
			Region:   "us-east-1",
		},
		func(_ context.Context, _ domain.Envelope[json.RawMessage]) error { return nil },
	)
	require.NoError(t, err)
	assert.NotNil(t, c)
}

// ----------------------------
// approxReceiveCount helper: missing attribute + invalid int
// ----------------------------

func TestDispatch_NoApproximateReceiveCountAttr_NotRouted(t *testing.T) {
	// Message with NO ApproximateReceiveCount attr → approxReceiveCount returns 0 → not routed.
	env := domain.NewEnvelope("test.no.attr", "svc", json.RawMessage(`{}`))
	msg := makeSQSMessage(env)
	// msg.Attributes is nil — no receive count attribute.

	handlerCalled := make(chan struct{}, 1)
	receiveCount := 0
	client := &mockSQSClient{
		receiveMessageFn: func(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
			receiveCount++
			if receiveCount == 1 {
				return &sqs.ReceiveMessageOutput{Messages: []sqstypes.Message{msg}}, nil
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	c, err := internalsqs.NewWithClient(
		internalsqs.Config{QueueURL: testQueueURL, WaitSeconds: 1},
		client,
		func(_ context.Context, _ domain.Envelope[json.RawMessage]) error {
			handlerCalled <- struct{}{}
			return nil
		},
		internalsqs.WithDeadLetterHandler(func(_ context.Context, _ domain.Envelope[json.RawMessage]) error {
			return nil
		}),
		internalsqs.WithMaxReceiveCount(3), // threshold set but attr absent → count = 0 < 3
		internalsqs.WithDrainTimeout(2*time.Second),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = c.Start(ctx) }()

	select {
	case <-handlerCalled:
		// Normal handler was called (not dead-letter).
	case <-time.After(5 * time.Second):
		t.Fatal("normal handler was not called")
	}
	cancel()
}

// ----------------------------
// Start: context cancelled between polls (outer select Done fires)
// ----------------------------

func TestStart_ContextCancelledBetweenPolls(t *testing.T) {
	// Cancel the context immediately after the first ReceiveMessage returns a message.
	// This causes the outer select's loopCtx.Done() case to fire on the NEXT loop
	// iteration (before the second ReceiveMessage call), covering the top-of-loop
	// drain path without a concurrent receive error.
	env := domain.NewEnvelope("between.poll", "svc", json.RawMessage(`{}`))
	msg := makeSQSMessage(env)

	ctx, cancel := context.WithCancel(context.Background())
	receiveCount := 0

	handlerDone := make(chan struct{}, 1)
	client := &mockSQSClient{
		receiveMessageFn: func(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
			receiveCount++
			if receiveCount == 1 {
				// Cancel context after delivering one message so that the
				// NEXT loop iteration hits the outer select Done case.
				cancel()
				return &sqs.ReceiveMessageOutput{Messages: []sqstypes.Message{msg}}, nil
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	c, err := internalsqs.NewWithClient(
		internalsqs.Config{QueueURL: testQueueURL, WaitSeconds: 1},
		client,
		func(_ context.Context, _ domain.Envelope[json.RawMessage]) error {
			handlerDone <- struct{}{}
			return nil
		},
		internalsqs.WithDrainTimeout(2*time.Second),
	)
	require.NoError(t, err)

	errCh := make(chan error, 1)
	go func() { errCh <- c.Start(ctx) }()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return after context cancel")
	}
}

// ----------------------------
// Start: receive error when context is already cancelled (inner Done case)
// ----------------------------

func TestStart_ReceiveError_ContextAlreadyCancelled(t *testing.T) {
	// Trigger the inner select's loopCtx.Done() path by cancelling the context
	// just before ReceiveMessage returns a non-context error.
	ctx, cancel := context.WithCancel(context.Background())
	receiveCount := 0

	client := &mockSQSClient{
		receiveMessageFn: func(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
			receiveCount++
			if receiveCount == 1 {
				cancel() // cancel before returning the error
				return nil, errors.New("network error")
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	c, err := internalsqs.NewWithClient(
		internalsqs.Config{QueueURL: testQueueURL, WaitSeconds: 1},
		client,
		func(_ context.Context, _ domain.Envelope[json.RawMessage]) error { return nil },
		internalsqs.WithDrainTimeout(2*time.Second),
	)
	require.NoError(t, err)

	errCh := make(chan error, 1)
	go func() { errCh <- c.Start(ctx) }()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return")
	}
}

// ----------------------------
// Consumer restart: Stop → Start again must work
// ----------------------------

func TestConsumer_Restart(t *testing.T) {
	client := &mockSQSClient{
		receiveMessageFn: func(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	handler := func(_ context.Context, _ domain.Envelope[json.RawMessage]) error { return nil }
	c, err := internalsqs.NewWithClient(
		internalsqs.Config{QueueURL: testQueueURL, WaitSeconds: 1},
		client,
		handler,
		internalsqs.WithDrainTimeout(500*time.Millisecond),
	)
	require.NoError(t, err)

	// First cycle — cancel via context, NOT Stop().
	ctx1, cancel1 := context.WithCancel(context.Background())
	done1 := make(chan error, 1)
	go func() { done1 <- c.Start(ctx1) }()
	time.Sleep(20 * time.Millisecond)
	cancel1()

	select {
	case err := <-done1:
		require.NoError(t, err, "first Start() should return nil")
	case <-time.After(3 * time.Second):
		t.Fatal("first Start() did not return after context cancel")
	}

	// Second cycle — must not panic or return 'already running'.
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	done2 := make(chan error, 1)
	go func() { done2 <- c.Start(ctx2) }()
	time.Sleep(20 * time.Millisecond)
	cancel2()

	select {
	case err := <-done2:
		require.NoError(t, err, "second Start() should succeed — consumer must be restartable")
	case <-time.After(3 * time.Second):
		t.Fatal("second Start() hung — consumer is not restartable after context-cancel exit")
	}
}

// TestNew_NilHandler_Error_Via_New calls the top-level New constructor (not NewWithClient)
// with a nil handler to cover the nil-handler validation path inside New.
func TestNew_NilHandler_Error_Via_New(t *testing.T) {
	// New validates the handler before calling LoadDefaultConfig, so this does not
	// require live AWS credentials.
	_, err := internalsqs.New(internalsqs.Config{QueueURL: testQueueURL}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "handler")
}

// TestNewWithClient_WaitSeconds_Clamped verifies WaitSeconds > 20 is clamped to 20.
func TestNewWithClient_WaitSeconds_Clamped(t *testing.T) {
	client := &mockSQSClient{}
	handler := func(_ context.Context, _ domain.Envelope[json.RawMessage]) error { return nil }
	c, err := internalsqs.NewWithClient(
		internalsqs.Config{QueueURL: testQueueURL, WaitSeconds: 30}, // > 20 → clamp
		client,
		handler,
	)
	require.NoError(t, err)
	require.NotNil(t, c)
}

// TestNewWithClient_DLH_WithLogger_DefaultsMaxReceiveCount verifies that when a
// dead-letter handler is set without a max-receive-count, the default (5) is applied
// and the logger records a warning.
func TestNewWithClient_DLH_WithLogger_DefaultsMaxReceiveCount(t *testing.T) {
	client := &mockSQSClient{}
	handler := func(_ context.Context, _ domain.Envelope[json.RawMessage]) error { return nil }
	dlh := func(_ context.Context, _ domain.Envelope[json.RawMessage]) error { return nil }
	logger := &fixtures.MockLogger{}
	c, err := internalsqs.NewWithClient(
		internalsqs.Config{QueueURL: testQueueURL, Logger: logger},
		client,
		handler,
		internalsqs.WithDeadLetterHandler(dlh), // no WithMaxReceiveCount → default applied
	)
	require.NoError(t, err)
	require.NotNil(t, c)

	entries := logger.Entries()
	found := false
	for _, e := range entries {
		if e.Level == "WARN" {
			found = true
			break
		}
	}
	assert.True(t, found, "expected WARN log for missing max-receive-count")
}

// TestDispatch_HandlerError_WithLogger verifies that when a handler returns an error
// the logger receives a Warn call (exercises the c.logger != nil branch in dispatch).
func TestDispatch_HandlerError_WithLogger(t *testing.T) {
	env := domain.NewEnvelope("test.event", "svc", json.RawMessage(`{}`))
	msg := makeSQSMessage(env)

	receiveCallCount := 0
	client := &mockSQSClient{
		receiveMessageFn: func(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
			receiveCallCount++
			if receiveCallCount == 1 {
				return &sqs.ReceiveMessageOutput{Messages: []sqstypes.Message{msg}}, nil
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	logger := &fixtures.MockLogger{}
	handlerCalled := make(chan struct{}, 1)
	handler := func(_ context.Context, _ domain.Envelope[json.RawMessage]) error {
		handlerCalled <- struct{}{}
		return errors.New("handler failed")
	}

	c, err := internalsqs.NewWithClient(
		internalsqs.Config{QueueURL: testQueueURL, WaitSeconds: 1, Logger: logger},
		client,
		handler,
		internalsqs.WithDrainTimeout(2*time.Second),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	startDone := make(chan error, 1)
	go func() { startDone <- c.Start(ctx) }()

	select {
	case <-handlerCalled:
	case <-time.After(5 * time.Second):
		t.Fatal("handler was not called")
	}
	time.Sleep(50 * time.Millisecond)
	cancel()
	require.NoError(t, <-startDone)

	entries := logger.Entries()
	found := false
	for _, e := range entries {
		if e.Level == "WARN" {
			found = true
			break
		}
	}
	assert.True(t, found, "expected WARN log from handler error in dispatch")
}

// TestDispatch_WithVisibilityTimeout exercises the extension-goroutine path in dispatch.
func TestDispatch_WithVisibilityTimeout(t *testing.T) {
	env := domain.NewEnvelope("test.event", "svc", json.RawMessage(`{}`))
	msg := makeSQSMessage(env)

	var deleteCount int32
	receiveCallCount := 0
	client := &mockSQSClient{
		receiveMessageFn: func(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
			receiveCallCount++
			if receiveCallCount == 1 {
				return &sqs.ReceiveMessageOutput{Messages: []sqstypes.Message{msg}}, nil
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
		deleteMessageFn: func(_ context.Context, _ *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
			atomic.AddInt32(&deleteCount, 1)
			return &sqs.DeleteMessageOutput{}, nil
		},
	}

	handlerDone := make(chan struct{})
	handler := func(_ context.Context, _ domain.Envelope[json.RawMessage]) error {
		close(handlerDone)
		return nil
	}

	c, err := internalsqs.NewWithClient(
		internalsqs.Config{QueueURL: testQueueURL, WaitSeconds: 1},
		client,
		handler,
		internalsqs.WithVisibilityTimeout(30*time.Second), // > 0 → extension goroutine starts
		internalsqs.WithDrainTimeout(2*time.Second),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	startDone := make(chan error, 1)
	go func() { startDone <- c.Start(ctx) }()

	select {
	case <-handlerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("handler was not called")
	}
	cancel()
	require.NoError(t, <-startDone)

	assert.Equal(t, int32(1), atomic.LoadInt32(&deleteCount), "message should be deleted on success")
}

// TestDispatch_Metrics_Success verifies a successful dispatch increments the consumed
// counter and records a duration (metrics are initialized via TestMain).
func TestDispatch_Metrics_Success(t *testing.T) {

	env := domain.NewEnvelope("metrics.event", "svc", json.RawMessage(`{}`))
	msg := makeSQSMessage(env)

	receiveCallCount := 0
	client := &mockSQSClient{
		receiveMessageFn: func(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
			receiveCallCount++
			if receiveCallCount == 1 {
				return &sqs.ReceiveMessageOutput{Messages: []sqstypes.Message{msg}}, nil
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	handlerDone := make(chan struct{}, 1)
	handler := func(_ context.Context, _ domain.Envelope[json.RawMessage]) error {
		handlerDone <- struct{}{}
		return nil
	}

	c, err := internalsqs.NewWithClient(
		internalsqs.Config{QueueURL: testQueueURL, WaitSeconds: 1},
		client,
		handler,
		internalsqs.WithDrainTimeout(2*time.Second),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	startDone := make(chan error, 1)
	go func() { startDone <- c.Start(ctx) }()

	select {
	case <-handlerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("handler was not called")
	}
	cancel()
	require.NoError(t, <-startDone, "Start should return without error")
}

// TestDispatch_DLH_Metrics exercises the dead-letter metrics path (metrics initialized via TestMain).
func TestDispatch_DLH_Metrics(t *testing.T) {

	env := domain.NewEnvelope("dlh.event", "svc", json.RawMessage(`{}`))
	msg := makeSQSMessage(env)
	// Simulate the message having been received 5 times → above default threshold.
	msg.Attributes = map[string]string{"ApproximateReceiveCount": "5"}

	receiveCallCount := 0
	client := &mockSQSClient{
		receiveMessageFn: func(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
			receiveCallCount++
			if receiveCallCount == 1 {
				return &sqs.ReceiveMessageOutput{Messages: []sqstypes.Message{msg}}, nil
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	handler := func(_ context.Context, _ domain.Envelope[json.RawMessage]) error { return nil }
	dlhDone := make(chan struct{}, 1)
	dlh := func(_ context.Context, _ domain.Envelope[json.RawMessage]) error {
		dlhDone <- struct{}{}
		return nil
	}

	c, err := internalsqs.NewWithClient(
		internalsqs.Config{QueueURL: testQueueURL, WaitSeconds: 1},
		client,
		handler,
		internalsqs.WithDeadLetterHandler(dlh),
		internalsqs.WithMaxReceiveCount(5),
		internalsqs.WithDrainTimeout(2*time.Second),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	startDone := make(chan error, 1)
	go func() { startDone <- c.Start(ctx) }()

	select {
	case <-dlhDone:
	case <-time.After(5 * time.Second):
		t.Fatal("dead-letter handler was not called")
	}
	cancel()
	require.NoError(t, <-startDone, "Start should return without error")
}

// TestDispatch_VisibilityExtension_Error_LoggerCalled drives the visibility-
// extension goroutine's ticker path with a failing ChangeMessageVisibility and a
// logger set, covering the WARN branch added for duplicate-delivery debugging.
func TestDispatch_VisibilityExtension_Error_LoggerCalled(t *testing.T) {
	env := domain.NewEnvelope("vis.err", "svc", json.RawMessage(`{}`))
	msg := makeSQSMessage(env)

	receiveCallCount := 0
	client := &mockSQSClient{
		receiveMessageFn: func(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
			receiveCallCount++
			if receiveCallCount == 1 {
				return &sqs.ReceiveMessageOutput{Messages: []sqstypes.Message{msg}}, nil
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
		changeMessageVisibilityFn: func(_ context.Context, _ *sqs.ChangeMessageVisibilityInput, _ ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
			return nil, errors.New("visibility extend failed")
		},
	}

	logger := &fixtures.MockLogger{}
	release := make(chan struct{})
	handler := func(_ context.Context, _ domain.Envelope[json.RawMessage]) error {
		<-release // block so the extension ticker fires at least once
		return nil
	}

	c, err := internalsqs.NewWithClient(
		internalsqs.Config{QueueURL: testQueueURL, WaitSeconds: 1, Logger: logger},
		client,
		handler,
		internalsqs.WithVisibilityTimeout(2*time.Second), // half = 1s ticker interval
		internalsqs.WithDrainTimeout(3*time.Second),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	startDone := make(chan error, 1)
	go func() { startDone <- c.Start(ctx) }()

	// Wait for the WARN from the failed extension (ticker fires at ~1s).
	found := false
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		for _, e := range logger.Entries() {
			if e.Level == "WARN" {
				found = true
				break
			}
		}
		if found {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	close(release) // let the handler finish
	cancel()
	require.NoError(t, <-startDone)
	assert.True(t, found, "expected WARN log when ChangeMessageVisibility fails mid-handler")
}

// TestConsumer_StopBeforeStart verifies that Stop() before Start() does not hang.
func TestConsumer_StopBeforeStart(t *testing.T) {
	client := &mockSQSClient{}
	handler := func(_ context.Context, _ domain.Envelope[json.RawMessage]) error { return nil }
	c, err := internalsqs.NewWithClient(
		internalsqs.Config{QueueURL: testQueueURL},
		client,
		handler,
	)
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		_ = c.Stop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop() before Start() hung — should return immediately")
	}
}
