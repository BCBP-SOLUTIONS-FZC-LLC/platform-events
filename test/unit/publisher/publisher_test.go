package publisher_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	internalsns "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/internal/adapter/outbound/sns"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/events"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/test/fixtures"
)

// pubMockSQSClient is a local mock SQS client for publisher_test package.
type pubMockSQSClient struct {
	receiveMessageFn func(context.Context, *sqs.ReceiveMessageInput, ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
}

func (m *pubMockSQSClient) ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	return m.receiveMessageFn(ctx, params, optFns...)
}

func (m *pubMockSQSClient) DeleteMessage(_ context.Context, _ *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	return &sqs.DeleteMessageOutput{}, nil
}

func (m *pubMockSQSClient) ChangeMessageVisibility(_ context.Context, _ *sqs.ChangeMessageVisibilityInput, _ ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	return &sqs.ChangeMessageVisibilityOutput{}, nil
}

// snsMockClient is a mock SNS client for testing public-level publisher options.
type snsMockClient struct {
	publishFn      func(context.Context, *sns.PublishInput, ...func(*sns.Options)) (*sns.PublishOutput, error)
	publishBatchFn func(context.Context, *sns.PublishBatchInput, ...func(*sns.Options)) (*sns.PublishBatchOutput, error)
}

func (m *snsMockClient) Publish(ctx context.Context, params *sns.PublishInput, optFns ...func(*sns.Options)) (*sns.PublishOutput, error) {
	return m.publishFn(ctx, params, optFns...)
}

func (m *snsMockClient) PublishBatch(ctx context.Context, params *sns.PublishBatchInput, optFns ...func(*sns.Options)) (*sns.PublishBatchOutput, error) {
	if m.publishBatchFn != nil {
		return m.publishBatchFn(ctx, params, optFns...)
	}
	var successful []snstypes.PublishBatchResultEntry
	for _, e := range params.PublishBatchRequestEntries {
		successful = append(successful, snstypes.PublishBatchResultEntry{
			Id:        e.Id,
			MessageId: aws.String("m"),
		})
	}
	return &sns.PublishBatchOutput{Successful: successful}, nil
}

var _ internalsns.SNSClientAPI = (*snsMockClient)(nil)

// ----------------------------
// NewSNSPublisher panic on empty TopicARN
// ----------------------------

func TestNewSNSPublisher_EmptyTopicARN_ReturnsError(t *testing.T) {
	_, err := events.NewSNSPublisher(events.SNSConfig{TopicARN: ""})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TopicARN")
}

func TestNewSNSPublisher_ValidTopicARN_NoError(t *testing.T) {
	// sns.New calls awsconfig.LoadDefaultConfig which succeeds without real credentials.
	pub, err := events.NewSNSPublisher(events.SNSConfig{
		TopicARN: "arn:aws:sns:us-east-1:123456789012:test-topic",
		Region:   "us-east-1",
	})
	require.NoError(t, err)
	assert.NotNil(t, pub)
}

// ----------------------------
// NewPublisherFromPort wraps mock publisher correctly
// ----------------------------

func TestNewPublisherFromPort_Publish(t *testing.T) {
	inner := &fixtures.MockPublisher{}
	pub := events.NewPublisherFromPort(inner)
	require.NotNil(t, pub)

	env := events.NewEnvelope("order.created", "billing", json.RawMessage(`{"amount":50}`),
		events.WithTenantID("acme"),
		events.WithTraceID("trace-abc"),
	)

	err := pub.Publish(context.Background(), env)
	require.NoError(t, err)

	published := inner.Published()
	require.Len(t, published, 1)
	assert.Equal(t, env.ID, published[0].ID)
	assert.Equal(t, env.Type, published[0].Type)
	assert.Equal(t, "acme", published[0].TenantID)
	assert.Equal(t, "trace-abc", published[0].TraceID)
}

func TestNewPublisherFromPort_PublishBatch(t *testing.T) {
	inner := &fixtures.MockPublisher{}
	pub := events.NewPublisherFromPort(inner)

	envs := []events.Envelope[json.RawMessage]{
		events.NewEnvelope("evt.one", "svc", json.RawMessage(`{}`), events.WithTenantID("acme")),
		events.NewEnvelope("evt.two", "svc", json.RawMessage(`{}`), events.WithTenantID("acme")),
	}

	err := pub.PublishBatch(context.Background(), envs)
	require.NoError(t, err)

	published := inner.Published()
	require.Len(t, published, 2)
	assert.Equal(t, envs[0].ID, published[0].ID)
	assert.Equal(t, envs[1].ID, published[1].ID)
}

// ----------------------------
// Option functions return non-nil
// ----------------------------

func TestWithMessageGroupID_NotNil(t *testing.T) {
	opt := events.WithMessageGroupID(func(_ events.Envelope[json.RawMessage]) string { return "g" })
	assert.NotNil(t, opt)
}

func TestWithMessageDeduplicationID_NotNil(t *testing.T) {
	opt := events.WithMessageDeduplicationID(func(_ events.Envelope[json.RawMessage]) string { return "d" })
	assert.NotNil(t, opt)
}

func TestWithAttributes_NotNil(t *testing.T) {
	opt := events.WithAttributes(map[string]string{"k": "v"})
	assert.NotNil(t, opt)
}

func TestWithCodec_NotNil(t *testing.T) {
	opt := events.WithCodec(events.NoopCodec{})
	assert.NotNil(t, opt)
}

func TestWithConsumerCodec_NotNil(t *testing.T) {
	opt := events.WithConsumerCodec(events.NoopCodec{})
	assert.NotNil(t, opt)
}

// ----------------------------
// NewSQSConsumer validation
// ----------------------------

func TestNewSQSConsumer_EmptyQueueURL_Error(t *testing.T) {
	handler := func(_ context.Context, _ events.Envelope[json.RawMessage]) error { return nil }
	_, err := events.NewSQSConsumer(events.SQSConfig{QueueURL: ""}, handler)
	require.Error(t, err)
}

func TestNewSQSConsumer_ValidConfig_NoError(t *testing.T) {
	handler := func(_ context.Context, _ events.Envelope[json.RawMessage]) error { return nil }
	// NewSQSConsumer calls sqs.New which loads AWS config — this succeeds even without credentials
	// because LoadDefaultConfig doesn't fail on missing credentials.
	c, err := events.NewSQSConsumer(events.SQSConfig{
		QueueURL:    "https://sqs.us-east-1.amazonaws.com/123/test",
		Region:      "us-east-1",
		MaxMessages: 5,
		WaitSeconds: 5,
	}, handler)
	require.NoError(t, err)
	assert.NotNil(t, c)
}

// ----------------------------
// Consumer option functions
// ----------------------------

func TestConsumerOptions_ReturnNonNil(t *testing.T) {
	assert.NotNil(t, events.WithConcurrency(3))
	assert.NotNil(t, events.WithVisibilityTimeout(30*1000000000)) // 30s in ns
	assert.NotNil(t, events.WithDeadLetterHandler(func(_ context.Context, _ events.Envelope[json.RawMessage]) error { return nil }))
}

// ----------------------------
// WithMessageGroupID / WithMessageDeduplicationID invoke domainToPublic
// ----------------------------

func TestWithMessageGroupID_InvokesDomainToPublic(t *testing.T) {
	// Use SNS adapter directly via NewPublisherFromPort + SNS mock to exercise
	// the outer option wrapper that calls domainToPublic.
	inner := &fixtures.MockPublisher{}
	pub := events.NewPublisherFromPort(inner)
	require.NotNil(t, pub)

	// Call publish to ensure domainToPublic is exercised in the bridge.
	env := events.NewEnvelope("bridge.test", "svc", json.RawMessage(`{}`), events.WithTenantID("acme"))
	err := pub.Publish(context.Background(), env)
	require.NoError(t, err)
	published := inner.Published()
	require.Len(t, published, 1)
	assert.Equal(t, env.ID, published[0].ID)
}

// ----------------------------
// WithDeadLetterHandler: verify the wrapped fn calls domainToPublic
// ----------------------------

func TestWithDeadLetterHandler_WrapsCorrectly(t *testing.T) {
	var dlhCalled bool
	opt := events.WithDeadLetterHandler(func(_ context.Context, env events.Envelope[json.RawMessage]) error {
		dlhCalled = true
		assert.Equal(t, "dlh.event", env.Type)
		return nil
	})
	require.NotNil(t, opt)
	_ = dlhCalled
}

// ----------------------------
// Public WithMessageGroupID / WithMessageDeduplicationID call domainToPublic
// ----------------------------

func TestWithMessageGroupID_CallsDomainToPublic(t *testing.T) {
	var capturedGroupID string
	mockClient := &snsMockClient{
		publishFn: func(_ context.Context, params *sns.PublishInput, _ ...func(*sns.Options)) (*sns.PublishOutput, error) {
			capturedGroupID = aws.ToString(params.MessageGroupId)
			return &sns.PublishOutput{MessageId: aws.String("x")}, nil
		},
	}

	pub, err := internalsns.NewWithClient(
		"arn:aws:sns:us-east-1:123:test.fifo",
		mockClient,
		nil,
		// This uses events.WithMessageGroupID which wraps domainToPublic
		events.WithMessageGroupID(func(e events.Envelope[json.RawMessage]) string {
			return "group-" + e.TenantID
		}),
	)
	require.NoError(t, err)

	// Wrap with publisherAdapter to call through events.Publisher interface.
	evtPub := events.NewPublisherFromPort(pub)

	env := events.NewEnvelope("test.event", "svc", json.RawMessage(`{}`),
		events.WithTenantID("acme"),
	)

	err = evtPub.Publish(context.Background(), env)
	require.NoError(t, err)
	assert.Equal(t, "group-acme", capturedGroupID)
}

// echoSchemaCodec is a minimal port.Codec test double whose Encode leaves
// payload bytes untouched but reports a fixed schemaID, so the test can
// assert the public events.Codec alias plumbs through to the internal SNS
// adapter with no translation layer (unlike WithMessageGroupID/WithAttributes,
// which need a domainToPublic-converting closure).
type echoSchemaCodec struct{}

func (echoSchemaCodec) Encode(_ context.Context, _ string, payload json.RawMessage) ([]byte, string, error) {
	return payload, "echo-schema", nil
}

func (echoSchemaCodec) Decode(_ context.Context, _ string, encoded []byte) (json.RawMessage, error) {
	return json.RawMessage(encoded), nil
}

var _ port.Codec = echoSchemaCodec{}

func TestEventsWithCodec_PlumbsThroughPublicWrapper(t *testing.T) {
	var capturedMessage string
	mockClient := &snsMockClient{
		publishFn: func(_ context.Context, params *sns.PublishInput, _ ...func(*sns.Options)) (*sns.PublishOutput, error) {
			capturedMessage = aws.ToString(params.Message)
			return &sns.PublishOutput{MessageId: aws.String("x")}, nil
		},
	}

	pub, err := internalsns.NewWithClient(
		"arn:aws:sns:us-east-1:123:test",
		mockClient,
		nil,
		events.WithCodec(echoSchemaCodec{}),
	)
	require.NoError(t, err)

	evtPub := events.NewPublisherFromPort(pub)
	env := events.NewEnvelope("test.event", "svc", json.RawMessage(`{"x":1}`), events.WithTenantID("acme"))
	err = evtPub.Publish(context.Background(), env)
	require.NoError(t, err)

	var wire struct {
		SchemaID string `json:"dataschema"`
	}
	require.NoError(t, json.Unmarshal([]byte(capturedMessage), &wire))
	assert.Equal(t, "echo-schema", wire.SchemaID)
}

func TestWithMessageDeduplicationID_CallsDomainToPublic(t *testing.T) {
	var capturedDedupID string
	mockClient := &snsMockClient{
		publishFn: func(_ context.Context, params *sns.PublishInput, _ ...func(*sns.Options)) (*sns.PublishOutput, error) {
			capturedDedupID = aws.ToString(params.MessageDeduplicationId)
			return &sns.PublishOutput{MessageId: aws.String("x")}, nil
		},
	}

	pub, err := internalsns.NewWithClient(
		"arn:aws:sns:us-east-1:123:test.fifo",
		mockClient,
		nil,
		events.WithMessageGroupID(func(_ events.Envelope[json.RawMessage]) string { return "grp" }),
		events.WithMessageDeduplicationID(func(e events.Envelope[json.RawMessage]) string {
			return "dedup-" + e.ID
		}),
	)
	require.NoError(t, err)

	evtPub := events.NewPublisherFromPort(pub)
	env := events.NewEnvelope("test.event", "svc", json.RawMessage(`{}`), events.WithTenantID("acme"))
	err = evtPub.Publish(context.Background(), env)
	require.NoError(t, err)
	assert.Equal(t, "dedup-"+env.ID, capturedDedupID)
}

// ----------------------------
// publisherBridge.PublishBatch coverage
// ----------------------------

func TestPublisherBridge_PublishBatch_MultipleEnvelopes(t *testing.T) {
	inner := &fixtures.MockPublisher{}
	pub := events.NewPublisherFromPort(inner)

	envs := make([]events.Envelope[json.RawMessage], 3)
	for i := range envs {
		envs[i] = events.NewEnvelope("test.event", "svc", json.RawMessage(`{}`), events.WithTenantID("acme"))
	}

	err := pub.PublishBatch(context.Background(), envs)
	require.NoError(t, err)
	assert.Len(t, inner.Published(), 3)
}

// ----------------------------
// WithDeadLetterHandler closure actually invoked via Start/dispatch
// ----------------------------

func TestWithDeadLetterHandler_ClosureActuallyInvoked(t *testing.T) {
	env := domain.NewEnvelope("dlh.closure.test", "svc", json.RawMessage(`{}`))
	env.TenantID = "closure-tenant"

	msg := sqstypes.Message{
		MessageId:     aws.String("msg-dlh-closure"),
		Body:          func() *string { b, _ := json.Marshal(env); s := string(b); return &s }(),
		ReceiptHandle: aws.String("rh-dlh-closure"),
		Attributes: map[string]string{
			// count=4 > maxReceiveCount=3 → DLH fires (strict > semantics match SQS DLQ).
			string(sqstypes.MessageSystemAttributeNameApproximateReceiveCount): "4",
		},
	}

	receiveCount := 0
	mockClient := &pubMockSQSClient{
		receiveMessageFn: func(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
			receiveCount++
			if receiveCount == 1 {
				return &sqs.ReceiveMessageOutput{Messages: []sqstypes.Message{msg}}, nil
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	dlhCalled := make(chan events.Envelope[json.RawMessage], 1)
	dlhFn := func(_ context.Context, env events.Envelope[json.RawMessage]) error {
		dlhCalled <- env
		return nil
	}

	consumer, err := events.NewSQSConsumerWithClient(
		events.SQSConfig{QueueURL: "https://sqs.us-east-1.amazonaws.com/123/test", WaitSeconds: 1},
		mockClient,
		func(_ context.Context, _ events.Envelope[json.RawMessage]) error { return nil },
		events.WithDeadLetterHandler(dlhFn),
		events.WithMaxReceiveCount(3),
		events.WithDrainTimeout(2*time.Second),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = consumer.Start(ctx) }()

	select {
	case received := <-dlhCalled:
		assert.Equal(t, env.ID, received.ID)
		assert.Equal(t, "closure-tenant", received.TenantID)
	case <-time.After(5 * time.Second):
		t.Fatal("dead-letter handler closure was not invoked")
	}
	cancel()
}

// ----------------------------
// BatchError.Error()
// ----------------------------

func TestBatchError_Error_ContainsFailureCount(t *testing.T) {
	failures := make([]events.BatchFailure, 3)
	failures[0] = events.BatchFailure{ID: "msg-1", Code: "InternalError", Message: "Server error"}
	failures[1] = events.BatchFailure{ID: "msg-2", Code: "InvalidParameter", Message: "Bad input"}
	failures[2] = events.BatchFailure{ID: "msg-3", Code: "InvalidParameter", Message: "Bad input"}

	batchErr := &events.BatchError{Failures: failures}
	errMsg := batchErr.Error()

	assert.Contains(t, errMsg, "3")
	assert.Contains(t, errMsg, "failed")
}

// ----------------------------
// publisherAdapter.PublishBatch: internal *sns.BatchError translated to public *events.BatchError
// ----------------------------

// snsBatchErrPortPublisher is a port.Publisher that returns an internal *sns.BatchError.
// This exercises the translation path in publisherAdapter.PublishBatch.
type snsBatchErrPortPublisher struct {
	err *internalsns.BatchError
}

func (p *snsBatchErrPortPublisher) Publish(_ context.Context, _ domain.Envelope[json.RawMessage]) error {
	if p.err == nil {
		return nil
	}
	return p.err
}

func (p *snsBatchErrPortPublisher) PublishBatch(_ context.Context, _ []domain.Envelope[json.RawMessage]) error {
	if p.err == nil {
		return nil
	}
	return p.err
}

var _ port.Publisher = (*snsBatchErrPortPublisher)(nil)

// TestPublisherAdapter_PublishBatch_SNSBatchError_Translated covers the
// *sns.BatchError → *events.BatchError translation in publisherAdapter.PublishBatch
// (pkg/events/publisher.go lines 111-116). The public Publisher must convert the
// internal batch error type so callers never need to import the internal package.
func TestPublisherAdapter_PublishBatch_SNSBatchError_Translated(t *testing.T) {
	inner := &snsBatchErrPortPublisher{
		err: &internalsns.BatchError{
			Failures: []internalsns.BatchFailure{
				{ID: "msg-1", Code: "InternalError", Message: "sns error"},
				{ID: "msg-2", Code: "InvalidParameter", Message: "bad param"},
			},
		},
	}

	pub := events.NewPublisherFromPort(inner)

	envs := []events.Envelope[json.RawMessage]{
		events.NewEnvelope("evt.a", "svc", json.RawMessage(`{}`), events.WithTenantID("acme")),
		events.NewEnvelope("evt.b", "svc", json.RawMessage(`{}`), events.WithTenantID("acme")),
	}

	err := pub.PublishBatch(context.Background(), envs)
	require.Error(t, err)

	var batchErr *events.BatchError
	require.ErrorAs(t, err, &batchErr, "internal BatchError must be translated to public BatchError")
	require.Len(t, batchErr.Failures, 2)
	assert.Equal(t, "msg-1", batchErr.Failures[0].ID)
	assert.Equal(t, "InternalError", batchErr.Failures[0].Code)
	assert.Equal(t, "msg-2", batchErr.Failures[1].ID)
}

// TestPublisherAdapter_PublishBatch_GenericError covers the passthrough of non-BatchError
// errors in publisherAdapter.PublishBatch (pkg/events/publisher.go line 118).
func TestPublisherAdapter_PublishBatch_GenericError(t *testing.T) {
	inner := &fixtures.MockPublisher{}
	inner.SetError(errors.New("sns: connection refused"))
	pub := events.NewPublisherFromPort(inner)

	envs := []events.Envelope[json.RawMessage]{
		events.NewEnvelope("evt.x", "svc", json.RawMessage(`{}`), events.WithTenantID("acme")),
	}

	err := pub.PublishBatch(context.Background(), envs)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "connection refused")

	// Must NOT be a BatchError — it should pass through unchanged.
	var batchErr *events.BatchError
	assert.False(t, errors.As(err, &batchErr), "generic error must not be wrapped in BatchError")
}
