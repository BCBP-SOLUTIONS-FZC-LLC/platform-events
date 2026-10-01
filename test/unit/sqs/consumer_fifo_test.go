package sqs_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	internalsqs "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/internal/adapter/outbound/sqs"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/internal/core/domain"
)

const testFIFOQueueURL = "https://sqs.us-east-1.amazonaws.com/123456789/test-queue.fifo"

// fifoMessage builds a FIFO message of group with the subject as its label.
func fifoMessage(group, label string) sqstypes.Message {
	env := domain.NewEnvelope("test.event", "svc", json.RawMessage(`{}`))
	env.Subject = label
	msg := makeSQSMessage(env)
	msg.Attributes = map[string]string{string(sqstypes.MessageSystemAttributeNameMessageGroupId): group}
	return msg
}

// fifoClient delivers batch once, then blocks; it records deletes, releases
// (visibility 0) and every ReceiveMessage input.
type fifoClient struct {
	mockSQSClient
	mu       sync.Mutex
	inputs   []*sqs.ReceiveMessageInput
	deleted  map[string]bool
	released map[string]bool
}

func newFIFOClient(batch []sqstypes.Message) *fifoClient {
	f := &fifoClient{deleted: map[string]bool{}, released: map[string]bool{}}
	calls := 0
	f.receiveMessageFn = func(ctx context.Context, in *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
		f.mu.Lock()
		f.inputs = append(f.inputs, in)
		calls++
		first := calls == 1
		f.mu.Unlock()
		if first {
			return &sqs.ReceiveMessageOutput{Messages: batch}, nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	f.deleteMessageFn = func(_ context.Context, in *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
		f.mu.Lock()
		f.deleted[aws.ToString(in.ReceiptHandle)] = true
		f.mu.Unlock()
		return &sqs.DeleteMessageOutput{}, nil
	}
	f.changeMessageVisibilityFn = func(_ context.Context, in *sqs.ChangeMessageVisibilityInput, _ ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
		if in.VisibilityTimeout == 0 {
			f.mu.Lock()
			f.released[aws.ToString(in.ReceiptHandle)] = true
			f.mu.Unlock()
		}
		return &sqs.ChangeMessageVisibilityOutput{}, nil
	}
	return f
}

func (f *fifoClient) wasDeleted(m sqstypes.Message) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.deleted[aws.ToString(m.ReceiptHandle)]
}

func (f *fifoClient) wasReleased(m sqstypes.Message) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.released[aws.ToString(m.ReceiptHandle)]
}

// A FIFO queue's message groups are each processed in order by one worker,
// even with spare concurrency; different groups still run in parallel.
func TestFIFO_GroupProcessedInOrder(t *testing.T) {
	a1, a2, a3 := fifoMessage("A", "a1"), fifoMessage("A", "a2"), fifoMessage("A", "a3")
	b1, b2 := fifoMessage("B", "b1"), fifoMessage("B", "b2")
	client := newFIFOClient([]sqstypes.Message{a1, b1, a2, b2, a3})

	var mu sync.Mutex
	order := map[string][]string{}
	done := make(chan struct{})
	handler := func(_ context.Context, env domain.Envelope[json.RawMessage]) error {
		if env.Subject == "a1" {
			time.Sleep(100 * time.Millisecond) // a2 and a3 must not overtake a1
		}
		mu.Lock()
		g := env.Subject[:1]
		order[g] = append(order[g], env.Subject)
		if len(order["a"])+len(order["b"]) == 5 {
			close(done)
		}
		mu.Unlock()
		return nil
	}
	c, err := internalsqs.NewWithClient(internalsqs.Config{QueueURL: testFIFOQueueURL, WaitSeconds: 1}, client, handler,
		internalsqs.WithConcurrency(4), internalsqs.WithVisibilityTimeout(30*time.Second))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = c.Start(ctx) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("not all messages were handled")
	}
	cancel()
	require.NoError(t, c.Stop())

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []string{"a1", "a2", "a3"}, order["a"])
	assert.Equal(t, []string{"b1", "b2"}, order["b"])
	for _, m := range []sqstypes.Message{a1, a2, a3, b1, b2} {
		assert.True(t, client.wasDeleted(m))
	}
}

// A failed message stops its group: the later messages of that group are
// handed back unprocessed (SQS redelivers them after the failed one), while
// other groups are unaffected.
func TestFIFO_FailureStopsGroup(t *testing.T) {
	a1, a2, a3 := fifoMessage("A", "a1"), fifoMessage("A", "a2"), fifoMessage("A", "a3")
	b1 := fifoMessage("B", "b1")
	client := newFIFOClient([]sqstypes.Message{a1, a2, b1, a3})

	var mu sync.Mutex
	var handled []string
	handler := func(_ context.Context, env domain.Envelope[json.RawMessage]) error {
		mu.Lock()
		handled = append(handled, env.Subject)
		mu.Unlock()
		if env.Subject == "a1" {
			return assert.AnError
		}
		return nil
	}
	c, err := internalsqs.NewWithClient(internalsqs.Config{QueueURL: testFIFOQueueURL, WaitSeconds: 1}, client, handler,
		internalsqs.WithConcurrency(2), internalsqs.WithVisibilityTimeout(30*time.Second))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = c.Start(ctx) }()
	require.Eventually(t, func() bool {
		return client.wasDeleted(b1) && client.wasReleased(a2) && client.wasReleased(a3)
	}, 5*time.Second, 10*time.Millisecond)
	cancel()
	require.NoError(t, c.Stop())

	mu.Lock()
	defer mu.Unlock()
	assert.ElementsMatch(t, []string{"a1", "b1"}, handled, "a2 and a3 must not run after a1 failed")
	assert.False(t, client.wasDeleted(a1))
	assert.False(t, client.wasDeleted(a2))
	assert.False(t, client.wasDeleted(a3))
}

// FIFO receives ask for MessageGroupId and carry a fresh
// ReceiveRequestAttemptId per logical receive; standard queues do neither.
func TestFIFO_ReceiveInput(t *testing.T) {
	for _, tc := range []struct {
		url  string
		fifo bool
	}{{testFIFOQueueURL, true}, {testQueueURL, false}} {
		var mu sync.Mutex
		var inputs []sqs.ReceiveMessageInput
		client := &mockSQSClient{receiveMessageFn: func(ctx context.Context, in *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
			mu.Lock()
			inputs = append(inputs, *in)
			n := len(inputs)
			mu.Unlock()
			if n < 3 {
				return &sqs.ReceiveMessageOutput{}, nil
			}
			<-ctx.Done()
			return nil, ctx.Err()
		}}
		handler := func(context.Context, domain.Envelope[json.RawMessage]) error { return nil }
		c, err := internalsqs.NewWithClient(internalsqs.Config{QueueURL: tc.url, WaitSeconds: 1}, client, handler,
			internalsqs.WithVisibilityTimeout(30*time.Second))
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(context.Background())
		go func() { _ = c.Start(ctx) }()
		require.Eventually(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(inputs) >= 3 }, 5*time.Second, 5*time.Millisecond)
		cancel()
		require.NoError(t, c.Stop())

		mu.Lock()
		if tc.fifo {
			assert.Contains(t, inputs[0].MessageSystemAttributeNames, sqstypes.MessageSystemAttributeNameMessageGroupId)
			require.NotNil(t, inputs[0].ReceiveRequestAttemptId)
			require.NotNil(t, inputs[1].ReceiveRequestAttemptId)
			assert.NotEqual(t, *inputs[0].ReceiveRequestAttemptId, *inputs[1].ReceiveRequestAttemptId)
		} else {
			assert.NotContains(t, inputs[0].MessageSystemAttributeNames, sqstypes.MessageSystemAttributeNameMessageGroupId)
			assert.Nil(t, inputs[0].ReceiveRequestAttemptId)
		}
		mu.Unlock()
	}
}

// On a standard queue a message is stopped extending before it is deleted:
// no ChangeMessageVisibility call is made after DeleteMessage.
func TestSettle_NoExtensionAfterDelete(t *testing.T) {
	msg := fifoMessage("", "s1")
	var mu sync.Mutex
	deletedAt := time.Time{}
	var lateExtension bool
	calls := 0
	client := &mockSQSClient{
		receiveMessageFn: func(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
			mu.Lock()
			calls++
			first := calls == 1
			mu.Unlock()
			if first {
				return &sqs.ReceiveMessageOutput{Messages: []sqstypes.Message{msg}}, nil
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
		deleteMessageFn: func(context.Context, *sqs.DeleteMessageInput, ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
			mu.Lock()
			deletedAt = time.Now()
			mu.Unlock()
			time.Sleep(1200 * time.Millisecond) // a tick (1s) would fire during a slow delete
			return &sqs.DeleteMessageOutput{}, nil
		},
		changeMessageVisibilityFn: func(context.Context, *sqs.ChangeMessageVisibilityInput, ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
			mu.Lock()
			if !deletedAt.IsZero() {
				lateExtension = true
			}
			mu.Unlock()
			return &sqs.ChangeMessageVisibilityOutput{}, nil
		},
	}
	handled := make(chan struct{})
	handler := func(context.Context, domain.Envelope[json.RawMessage]) error { close(handled); return nil }
	c, err := internalsqs.NewWithClient(internalsqs.Config{QueueURL: testQueueURL, WaitSeconds: 1}, client, handler,
		internalsqs.WithVisibilityTimeout(time.Second))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = c.Start(ctx) }()
	<-handled
	require.Eventually(t, func() bool { mu.Lock(); defer mu.Unlock(); return !deletedAt.IsZero() }, 5*time.Second, 5*time.Millisecond)
	time.Sleep(1500 * time.Millisecond)
	cancel()
	require.NoError(t, c.Stop())
	mu.Lock()
	defer mu.Unlock()
	assert.False(t, lateExtension, "visibility must not be extended once the delete started")
}

// A dataschema on a plain JSON payload is informational: the consumer passes
// the payload through undecoded (no codec configured here) instead of failing
// every delivery until the message is dead-lettered.
func TestDataschemaOnPlainJSONPayload_PassedThrough(t *testing.T) {
	env := domain.NewEnvelope("test.event", "svc", json.RawMessage(`{"a":1}`))
	env.SchemaID = "550e8400-e29b-41d4-a716-446655440000"
	msg := makeSQSMessage(env)
	client := newFIFOClient([]sqstypes.Message{msg})

	got := make(chan json.RawMessage, 1)
	handler := func(_ context.Context, e domain.Envelope[json.RawMessage]) error {
		got <- e.Payload
		return nil
	}
	c, err := internalsqs.NewWithClient(internalsqs.Config{QueueURL: testQueueURL, WaitSeconds: 1}, client, handler)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = c.Start(ctx) }()
	select {
	case p := <-got:
		assert.JSONEq(t, `{"a":1}`, string(p))
	case <-time.After(5 * time.Second):
		t.Fatal("handler not called — the payload was treated as codec-encoded")
	}
	require.Eventually(t, func() bool { return client.wasDeleted(msg) }, 5*time.Second, 10*time.Millisecond)
	cancel()
	require.NoError(t, c.Stop())
}

// A busy FIFO group whose total processing time exceeds WithHandlerTimeout is
// not cut short: later messages wait for the earlier ones by design, so only
// a message that hangs releases the rest of its group.
func TestFIFO_HandlerTimeout_BusyGroupNotReleased(t *testing.T) {
	var batch []sqstypes.Message
	for _, l := range []string{"a1", "a2", "a3", "a4", "a5"} {
		batch = append(batch, fifoMessage("A", l))
	}
	client := newFIFOClient(batch)
	var mu sync.Mutex
	var handled []string
	handler := func(_ context.Context, env domain.Envelope[json.RawMessage]) error {
		time.Sleep(300 * time.Millisecond) // 5 × 300ms spans the 1s extension tick, well past the 400ms timeout
		mu.Lock()
		handled = append(handled, env.Subject)
		mu.Unlock()
		return nil
	}
	c, err := internalsqs.NewWithClient(internalsqs.Config{QueueURL: testFIFOQueueURL, WaitSeconds: 1}, client, handler,
		internalsqs.WithVisibilityTimeout(2*time.Second), internalsqs.WithHandlerTimeout(400*time.Millisecond))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = c.Start(ctx) }()
	require.Eventually(t, func() bool { return client.wasDeleted(batch[4]) }, 5*time.Second, 10*time.Millisecond)
	cancel()
	require.NoError(t, c.Stop())

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []string{"a1", "a2", "a3", "a4", "a5"}, handled)
	for _, m := range batch {
		assert.False(t, client.wasReleased(m), "no message of a busy group may be handed back")
	}
}

// A message whose delete fails is not settled: its group stops, so the next
// message cannot run (and be deleted) before it is redelivered.
func TestFIFO_FailedDeleteStopsGroup(t *testing.T) {
	a1, a2 := fifoMessage("A", "a1"), fifoMessage("A", "a2")
	client := newFIFOClient([]sqstypes.Message{a1, a2})
	inner := client.deleteMessageFn
	client.deleteMessageFn = func(ctx context.Context, in *sqs.DeleteMessageInput, o ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
		if aws.ToString(in.ReceiptHandle) == aws.ToString(a1.ReceiptHandle) {
			return nil, assert.AnError
		}
		return inner(ctx, in, o...)
	}
	var mu sync.Mutex
	var handled []string
	handler := func(_ context.Context, env domain.Envelope[json.RawMessage]) error {
		mu.Lock()
		handled = append(handled, env.Subject)
		mu.Unlock()
		return nil
	}
	c, err := internalsqs.NewWithClient(internalsqs.Config{QueueURL: testFIFOQueueURL, WaitSeconds: 1}, client, handler,
		internalsqs.WithVisibilityTimeout(30*time.Second))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = c.Start(ctx) }()
	require.Eventually(t, func() bool { return client.wasReleased(a2) }, 5*time.Second, 10*time.Millisecond)
	cancel()
	require.NoError(t, c.Stop())
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []string{"a1"}, handled)
	assert.False(t, client.wasDeleted(a2))
}

// Without a visibility timeout (no extension) a FIFO receive takes one
// message, so no group message ever waits unextended behind another.
func TestFIFO_NoVisibilityTimeout_ReceivesOne(t *testing.T) {
	var mu sync.Mutex
	var maxMsgs []int32
	client := &mockSQSClient{receiveMessageFn: func(ctx context.Context, in *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
		mu.Lock()
		maxMsgs = append(maxMsgs, in.MaxNumberOfMessages)
		n := len(maxMsgs)
		mu.Unlock()
		if n < 2 {
			return &sqs.ReceiveMessageOutput{}, nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	c, err := internalsqs.NewWithClient(internalsqs.Config{QueueURL: testFIFOQueueURL, WaitSeconds: 1}, client,
		func(context.Context, domain.Envelope[json.RawMessage]) error { return nil }, internalsqs.WithConcurrency(4))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = c.Start(ctx) }()
	require.Eventually(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(maxMsgs) >= 2 }, 5*time.Second, 5*time.Millisecond)
	cancel()
	require.NoError(t, c.Stop())
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, int32(1), maxMsgs[0])
}

// A tenant_id with a NUL can never be used as the handler's RLS GUC
// (pgcommon rejects it), so the message is malformed: deleted, never handled.
func TestNULTenant_TreatedAsMalformed(t *testing.T) {
	env := domain.NewEnvelope("test.event", "svc", json.RawMessage(`{}`))
	env.TenantID = "acme\x00"
	msg := makeSQSMessage(env)
	client := newFIFOClient([]sqstypes.Message{msg})
	called := make(chan struct{}, 1)
	c, err := internalsqs.NewWithClient(internalsqs.Config{QueueURL: testQueueURL, WaitSeconds: 1}, client,
		func(context.Context, domain.Envelope[json.RawMessage]) error { called <- struct{}{}; return nil })
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = c.Start(ctx) }()
	require.Eventually(t, func() bool { return client.wasDeleted(msg) }, 5*time.Second, 10*time.Millisecond)
	cancel()
	require.NoError(t, c.Stop())
	assert.Empty(t, called, "the handler must not run")
}
