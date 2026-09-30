package sqs_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/adapter/outbound/metrics"
	internalsqs "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/adapter/outbound/sqs"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/events"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/test/fixtures"
)

const (
	dlqSourceURL = "https://sqs.us-east-1.amazonaws.com/123456789012/orders"
	dlqARN       = "arn:aws:sqs:us-east-1:123456789012:orders-dlq"
	dlqURL       = "https://sqs.us-east-1.amazonaws.com/123456789012/orders-dlq"
)

// mockDLQClient is a test double for DLQClientAPI. Unset funcs return a
// source queue with a valid RedrivePolicy pointing at dlqARN.
type mockDLQClient struct {
	getQueueAttributesFn func(ctx context.Context, params *sqs.GetQueueAttributesInput) (*sqs.GetQueueAttributesOutput, error)
	getQueueURLFn        func(ctx context.Context, params *sqs.GetQueueUrlInput) (*sqs.GetQueueUrlOutput, error)
	sendMessageFn        func(ctx context.Context, params *sqs.SendMessageInput) (*sqs.SendMessageOutput, error)

	getAttrCalls atomic.Int32
	getURLCalls  atomic.Int32
	mu           sync.Mutex
	sent         []*sqs.SendMessageInput
}

func (m *mockDLQClient) GetQueueAttributes(ctx context.Context, params *sqs.GetQueueAttributesInput, _ ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error) {
	m.getAttrCalls.Add(1)
	if m.getQueueAttributesFn != nil {
		return m.getQueueAttributesFn(ctx, params)
	}
	return redrivePolicyOutput(`{"deadLetterTargetArn":"` + dlqARN + `","maxReceiveCount":"5"}`), nil
}

func (m *mockDLQClient) GetQueueUrl(ctx context.Context, params *sqs.GetQueueUrlInput, _ ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
	m.getURLCalls.Add(1)
	if m.getQueueURLFn != nil {
		return m.getQueueURLFn(ctx, params)
	}
	return &sqs.GetQueueUrlOutput{QueueUrl: aws.String("https://sqs.us-east-1.amazonaws.com/" + aws.ToString(params.QueueOwnerAWSAccountId) + "/" + aws.ToString(params.QueueName))}, nil
}

func (m *mockDLQClient) SendMessage(ctx context.Context, params *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	if m.sendMessageFn != nil {
		return m.sendMessageFn(ctx, params)
	}
	m.mu.Lock()
	m.sent = append(m.sent, params)
	m.mu.Unlock()
	return &sqs.SendMessageOutput{MessageId: aws.String("dlq-msg-1")}, nil
}

func (m *mockDLQClient) lastSent(t *testing.T) *sqs.SendMessageInput {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	require.NotEmpty(t, m.sent, "expected a SendMessage call")
	return m.sent[len(m.sent)-1]
}

var _ internalsqs.DLQClientAPI = (*mockDLQClient)(nil)

func redrivePolicyOutput(policy string) *sqs.GetQueueAttributesOutput {
	return &sqs.GetQueueAttributesOutput{Attributes: map[string]string{"RedrivePolicy": policy}}
}

func newTestDLQPublisher(t *testing.T, client internalsqs.DLQClientAPI, consumerName string) *internalsqs.DLQPublisher {
	t.Helper()
	p, err := internalsqs.NewDLQPublisherWithClient(internalsqs.DLQConfig{
		ConsumerName: consumerName,
		Clock:        fixtures.NewFakeClock(time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)),
		Logger:       &fixtures.MockLogger{},
	}, client)
	require.NoError(t, err)
	return p
}

func envelopeBody(t *testing.T) []byte {
	t.Helper()
	env := events.NewEnvelope("order.created", "orders-svc", json.RawMessage(`{"order_id":"o-1"}`), events.WithTenantID("t-1"))
	b, err := json.Marshal(env)
	require.NoError(t, err)
	return b
}

func attr(in *sqs.SendMessageInput, k string) string {
	v, ok := in.MessageAttributes[k]
	if !ok {
		return ""
	}
	return aws.ToString(v.StringValue)
}

// ----------------------------
// RedrivePolicy / ARN parsing
// ----------------------------

func TestParseRedrivePolicy_Valid(t *testing.T) {
	for name, policy := range map[string]string{
		"string maxReceiveCount": `{"deadLetterTargetArn":"` + dlqARN + `","maxReceiveCount":"5"}`,
		"number maxReceiveCount": `{"deadLetterTargetArn":"` + dlqARN + `","maxReceiveCount":5}`,
		"arn only":               `{"deadLetterTargetArn":"` + dlqARN + `"}`,
	} {
		t.Run(name, func(t *testing.T) {
			got, err := internalsqs.ParseRedrivePolicy(policy)
			require.NoError(t, err)
			assert.Equal(t, dlqARN, got)
		})
	}
}

func TestParseRedrivePolicy_Malformed(t *testing.T) {
	for name, policy := range map[string]string{
		"not json":       `deadLetterTargetArn=foo`,
		"missing arn":    `{"maxReceiveCount":"5"}`,
		"empty arn":      `{"deadLetterTargetArn":""}`,
		"wrong arn type": `{"deadLetterTargetArn":42}`,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := internalsqs.ParseRedrivePolicy(policy)
			require.Error(t, err)
		})
	}
}

func TestParseSQSQueueARN(t *testing.T) {
	name, account, region, err := internalsqs.ParseSQSQueueARN(dlqARN)
	require.NoError(t, err)
	assert.Equal(t, "orders-dlq", name)
	assert.Equal(t, "123456789012", account)
	assert.Equal(t, "us-east-1", region)

	name, _, _, err = internalsqs.ParseSQSQueueARN("arn:aws-us-gov:sqs:us-gov-west-1:123456789012:q.fifo")
	require.NoError(t, err)
	assert.Equal(t, "q.fifo", name)
}

func TestParseSQSQueueARN_Invalid(t *testing.T) {
	for _, arn := range []string{
		"",
		"orders-dlq",
		"arn:aws:sns:us-east-1:123456789012:topic",
		"arn:aws:sqs:us-east-1:123456789012",
		"arn:aws:sqs:us-east-1::orders-dlq",
		"arn:aws:sqs::123456789012:orders-dlq",
		"arn:aws:sqs:us-east-1:123456789012:",
		"urn:aws:sqs:us-east-1:123456789012:orders-dlq",
	} {
		t.Run(arn, func(t *testing.T) {
			_, _, _, err := internalsqs.ParseSQSQueueARN(arn)
			require.Error(t, err)
		})
	}
}

// ----------------------------
// Construction
// ----------------------------

func TestNewDLQPublisherWithClient_NilClient(t *testing.T) {
	_, err := internalsqs.NewDLQPublisherWithClient(internalsqs.DLQConfig{}, nil)
	require.ErrorIs(t, err, internalsqs.ErrNilDLQClient)

	pub, err := events.NewSQSDLQPublisherWithClient(events.DLQConfig{}, nil)
	require.ErrorIs(t, err, internalsqs.ErrNilDLQClient)
	assert.Nil(t, pub, "must be an untyped nil interface")
}

// ----------------------------
// Queue resolution
// ----------------------------

func TestResolveDLQ_ResolvesAndCaches(t *testing.T) {
	client := &mockDLQClient{}
	var gotURLInput *sqs.GetQueueUrlInput
	client.getQueueURLFn = func(_ context.Context, in *sqs.GetQueueUrlInput) (*sqs.GetQueueUrlOutput, error) {
		gotURLInput = in
		return &sqs.GetQueueUrlOutput{QueueUrl: aws.String(dlqURL)}, nil
	}
	var gotAttrInput *sqs.GetQueueAttributesInput
	client.getQueueAttributesFn = func(_ context.Context, in *sqs.GetQueueAttributesInput) (*sqs.GetQueueAttributesOutput, error) {
		gotAttrInput = in
		return redrivePolicyOutput(`{"deadLetterTargetArn":"` + dlqARN + `","maxReceiveCount":"5"}`), nil
	}
	p := newTestDLQPublisher(t, client, "")

	for range 3 {
		got, err := p.ResolveDLQ(context.Background(), dlqSourceURL)
		require.NoError(t, err)
		assert.Equal(t, dlqURL, got)
	}
	assert.Equal(t, int32(1), client.getAttrCalls.Load(), "GetQueueAttributes must be cached")
	assert.Equal(t, int32(1), client.getURLCalls.Load(), "GetQueueUrl must be cached")

	assert.Equal(t, dlqSourceURL, aws.ToString(gotAttrInput.QueueUrl))
	assert.Equal(t, []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameRedrivePolicy}, gotAttrInput.AttributeNames)
	assert.Equal(t, "orders-dlq", aws.ToString(gotURLInput.QueueName))
	assert.Equal(t, "123456789012", aws.ToString(gotURLInput.QueueOwnerAWSAccountId))
}

func TestResolveDLQ_CachePerSourceQueue(t *testing.T) {
	client := &mockDLQClient{}
	p := newTestDLQPublisher(t, client, "")
	_, err := p.ResolveDLQ(context.Background(), dlqSourceURL)
	require.NoError(t, err)
	_, err = p.ResolveDLQ(context.Background(), dlqSourceURL+"-other")
	require.NoError(t, err)
	assert.Equal(t, int32(2), client.getAttrCalls.Load())
}

func TestResolveDLQ_FailuresAreNotCached(t *testing.T) {
	client := &mockDLQClient{}
	var configured atomic.Bool
	client.getQueueAttributesFn = func(context.Context, *sqs.GetQueueAttributesInput) (*sqs.GetQueueAttributesOutput, error) {
		if !configured.Load() {
			return &sqs.GetQueueAttributesOutput{Attributes: map[string]string{}}, nil
		}
		return redrivePolicyOutput(`{"deadLetterTargetArn":"` + dlqARN + `"}`), nil
	}
	p := newTestDLQPublisher(t, client, "")

	_, err := p.ResolveDLQ(context.Background(), dlqSourceURL)
	require.ErrorIs(t, err, events.ErrDLQNotConfigured)

	configured.Store(true)
	got, err := p.ResolveDLQ(context.Background(), dlqSourceURL)
	require.NoError(t, err)
	assert.Equal(t, dlqURL, got)
}

func TestResolveDLQ_MissingRedrivePolicy(t *testing.T) {
	for name, out := range map[string]*sqs.GetQueueAttributesOutput{
		"nil attributes":   {},
		"empty attributes": {Attributes: map[string]string{}},
		"blank policy":     redrivePolicyOutput("  "),
	} {
		t.Run(name, func(t *testing.T) {
			client := &mockDLQClient{getQueueAttributesFn: func(context.Context, *sqs.GetQueueAttributesInput) (*sqs.GetQueueAttributesOutput, error) {
				return out, nil
			}}
			_, err := newTestDLQPublisher(t, client, "").ResolveDLQ(context.Background(), dlqSourceURL)
			require.ErrorIs(t, err, events.ErrDLQNotConfigured)
			assert.NotErrorIs(t, err, events.ErrRetryable)
			assert.Contains(t, err.Error(), dlqSourceURL)
			assert.Equal(t, int32(0), client.getURLCalls.Load())
		})
	}
}

func TestResolveDLQ_MalformedRedrivePolicy(t *testing.T) {
	for name, policy := range map[string]string{
		"not json":      `{not json`,
		"no target arn": `{"maxReceiveCount":"5"}`,
		"non-sqs arn":   `{"deadLetterTargetArn":"arn:aws:sns:us-east-1:123456789012:topic"}`,
	} {
		t.Run(name, func(t *testing.T) {
			client := &mockDLQClient{getQueueAttributesFn: func(context.Context, *sqs.GetQueueAttributesInput) (*sqs.GetQueueAttributesOutput, error) {
				return redrivePolicyOutput(policy), nil
			}}
			_, err := newTestDLQPublisher(t, client, "").ResolveDLQ(context.Background(), dlqSourceURL)
			require.ErrorIs(t, err, events.ErrDLQInvalidRedrivePolicy)
			var dlqErr *events.DLQError
			require.ErrorAs(t, err, &dlqErr)
			assert.Equal(t, dlqSourceURL, dlqErr.SourceQueue)
			assert.Error(t, dlqErr.Cause)
		})
	}
}

func TestResolveDLQ_GetQueueAttributesError(t *testing.T) {
	cases := map[string]struct {
		err       error
		retryable bool
	}{
		"throttled":     {&smithy.GenericAPIError{Code: "RequestThrottled"}, true},
		"unavailable":   {&smithy.GenericAPIError{Code: "ServiceUnavailable"}, true},
		"access denied": {&smithy.GenericAPIError{Code: "AccessDenied"}, false},
		"no such queue": {&smithy.GenericAPIError{Code: "AWS.SimpleQueueService.NonExistentQueue"}, false},
		"plain error":   {errors.New("boom"), false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			client := &mockDLQClient{getQueueAttributesFn: func(context.Context, *sqs.GetQueueAttributesInput) (*sqs.GetQueueAttributesOutput, error) {
				return nil, tc.err
			}}
			_, err := newTestDLQPublisher(t, client, "").ResolveDLQ(context.Background(), dlqSourceURL)
			require.ErrorIs(t, err, events.ErrDLQUnresolved)
			require.ErrorIs(t, err, tc.err, "original cause must stay reachable")
			assert.Equal(t, tc.retryable, errors.Is(err, events.ErrRetryable))
		})
	}
}

func TestResolveDLQ_GetQueueUrlError(t *testing.T) {
	client := &mockDLQClient{getQueueURLFn: func(context.Context, *sqs.GetQueueUrlInput) (*sqs.GetQueueUrlOutput, error) {
		return nil, &smithy.GenericAPIError{Code: "AWS.SimpleQueueService.NonExistentQueue"}
	}}
	_, err := newTestDLQPublisher(t, client, "").ResolveDLQ(context.Background(), dlqSourceURL)
	require.ErrorIs(t, err, events.ErrDLQUnresolved)
	assert.NotErrorIs(t, err, events.ErrRetryable)
	assert.Contains(t, err.Error(), dlqARN)
}

func TestResolveDLQ_EmptyURLReturned(t *testing.T) {
	client := &mockDLQClient{getQueueURLFn: func(context.Context, *sqs.GetQueueUrlInput) (*sqs.GetQueueUrlOutput, error) {
		return &sqs.GetQueueUrlOutput{}, nil
	}}
	_, err := newTestDLQPublisher(t, client, "").ResolveDLQ(context.Background(), dlqSourceURL)
	require.ErrorIs(t, err, events.ErrDLQUnresolved)
}

func TestResolveDLQ_EmptySourceURL(t *testing.T) {
	_, err := newTestDLQPublisher(t, &mockDLQClient{}, "").ResolveDLQ(context.Background(), "")
	require.ErrorIs(t, err, events.ErrDLQInvalidMessage)
}

// ----------------------------
// SendToDLQ
// ----------------------------

func TestSendToDLQ_Success_PopulatesAttributes(t *testing.T) {
	client := &mockDLQClient{}
	p := newTestDLQPublisher(t, client, "billing-consumer")
	body := envelopeBody(t)

	err := p.SendToDLQ(context.Background(), dlqSourceURL, body, map[string]string{
		"TenantID":    "t-1",
		"traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
		"Empty":       "",
	}, "handler: invalid order state")
	require.NoError(t, err)

	in := client.lastSent(t)
	assert.Equal(t, dlqURL, aws.ToString(in.QueueUrl))
	assert.Equal(t, string(body), aws.ToString(in.MessageBody), "body must be forwarded verbatim")
	assert.Nil(t, in.MessageGroupId, "standard DLQ must not get a MessageGroupId")
	assert.Nil(t, in.MessageDeduplicationId)

	assert.Equal(t, "order.created", attr(in, events.DLQAttrEventType))
	assert.Equal(t, "handler: invalid order state", attr(in, events.DLQAttrReason))
	assert.Equal(t, dlqSourceURL, attr(in, events.DLQAttrOriginalQueue))
	assert.Equal(t, "billing-consumer", attr(in, events.DLQAttrConsumerName))
	assert.Equal(t, "2026-09-30T12:00:00Z", attr(in, events.DLQAttrFailedAt))
	assert.Equal(t, "t-1", attr(in, "TenantID"), "caller attributes must be preserved")
	assert.NotEmpty(t, attr(in, "traceparent"))
	assert.NotContains(t, in.MessageAttributes, "Empty", "empty-valued attributes are dropped")
	assert.Len(t, in.MessageAttributes, 7)
	for k, v := range in.MessageAttributes {
		assert.Equal(t, "String", aws.ToString(v.DataType), k)
	}
}

func TestSendToDLQ_StandardAttributesOverrideCaller(t *testing.T) {
	client := &mockDLQClient{}
	p := newTestDLQPublisher(t, client, "real-consumer")
	err := p.SendToDLQ(context.Background(), dlqSourceURL, envelopeBody(t), map[string]string{
		events.DLQAttrEventType:     "spoofed.type",
		events.DLQAttrReason:        "spoofed",
		events.DLQAttrOriginalQueue: "spoofed",
		events.DLQAttrConsumerName:  "spoofed",
		events.DLQAttrFailedAt:      "spoofed",
	}, "real reason")
	require.NoError(t, err)

	in := client.lastSent(t)
	assert.Equal(t, "order.created", attr(in, events.DLQAttrEventType), "envelope type wins over caller EventType")
	assert.Equal(t, "real reason", attr(in, events.DLQAttrReason))
	assert.Equal(t, dlqSourceURL, attr(in, events.DLQAttrOriginalQueue))
	assert.Equal(t, "real-consumer", attr(in, events.DLQAttrConsumerName))
	assert.Equal(t, "2026-09-30T12:00:00Z", attr(in, events.DLQAttrFailedAt))
}

func TestSendToDLQ_EventTypeFallbacks(t *testing.T) {
	client := &mockDLQClient{}
	p := newTestDLQPublisher(t, client, "")

	require.NoError(t, p.SendToDLQ(context.Background(), dlqSourceURL, []byte("not an envelope"), map[string]string{"EventType": "from.attr"}, "r"))
	assert.Equal(t, "from.attr", attr(client.lastSent(t), events.DLQAttrEventType))

	require.NoError(t, p.SendToDLQ(context.Background(), dlqSourceURL, []byte(`{"foo":1}`), nil, "r"))
	in := client.lastSent(t)
	assert.Equal(t, "unknown", attr(in, events.DLQAttrEventType))
	assert.NotContains(t, in.MessageAttributes, events.DLQAttrConsumerName, "ConsumerName omitted when not configured")
	assert.Len(t, in.MessageAttributes, 4)
}

func TestSendToDLQ_TruncatesLongReason(t *testing.T) {
	client := &mockDLQClient{}
	p := newTestDLQPublisher(t, client, "")
	reason := strings.Repeat("é", 1000) // 2000 bytes

	require.NoError(t, p.SendToDLQ(context.Background(), dlqSourceURL, envelopeBody(t), nil, reason))
	got := attr(client.lastSent(t), events.DLQAttrReason)
	assert.LessOrEqual(t, len(got), 1024)
	assert.True(t, strings.HasPrefix(reason, got))
	assert.Equal(t, strings.Repeat("é", 512), got, "must cut on a rune boundary")
}

func TestSendToDLQ_FIFODLQ(t *testing.T) {
	client := &mockDLQClient{getQueueAttributesFn: func(context.Context, *sqs.GetQueueAttributesInput) (*sqs.GetQueueAttributesOutput, error) {
		return redrivePolicyOutput(`{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:123456789012:orders-dlq.fifo"}`), nil
	}}
	p := newTestDLQPublisher(t, client, "")

	body := envelopeBody(t)
	var env events.Envelope[json.RawMessage]
	require.NoError(t, json.Unmarshal(body, &env))
	require.NoError(t, p.SendToDLQ(context.Background(), dlqSourceURL+".fifo", body, nil, "r"))
	in := client.lastSent(t)
	assert.Equal(t, env.ID, aws.ToString(in.MessageGroupId))
	assert.Equal(t, env.ID, aws.ToString(in.MessageDeduplicationId))

	// Non-envelope body: identity falls back to a stable content hash.
	require.NoError(t, p.SendToDLQ(context.Background(), dlqSourceURL+".fifo", []byte("raw"), nil, "r"))
	first := aws.ToString(client.lastSent(t).MessageDeduplicationId)
	require.NoError(t, p.SendToDLQ(context.Background(), dlqSourceURL+".fifo", []byte("raw"), nil, "r"))
	assert.Len(t, first, 64)
	assert.Equal(t, first, aws.ToString(client.lastSent(t).MessageDeduplicationId))
}

func TestSendToDLQ_SendMessageFailure(t *testing.T) {
	cases := map[string]struct {
		err       error
		retryable bool
	}{
		"throttled":       {&smithy.GenericAPIError{Code: "ThrottlingException"}, true},
		"internal":        {&smithy.GenericAPIError{Code: "InternalError"}, true},
		"kms throttled":   {&smithy.GenericAPIError{Code: "KmsThrottled"}, true},
		"invalid message": {&smithy.GenericAPIError{Code: "InvalidMessageContents"}, false},
		"kms disabled":    {&smithy.GenericAPIError{Code: "KmsDisabled"}, false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			client := &mockDLQClient{sendMessageFn: func(context.Context, *sqs.SendMessageInput) (*sqs.SendMessageOutput, error) {
				return nil, tc.err
			}}
			logger := &fixtures.MockLogger{}
			p, err := internalsqs.NewDLQPublisherWithClient(internalsqs.DLQConfig{Logger: logger}, client)
			require.NoError(t, err)

			err = p.SendToDLQ(context.Background(), dlqSourceURL, envelopeBody(t), nil, "r")
			require.ErrorIs(t, err, events.ErrDLQSendFailed)
			require.ErrorIs(t, err, tc.err)
			assert.Equal(t, tc.retryable, errors.Is(err, events.ErrRetryable))
			assert.NotErrorIs(t, err, events.ErrDLQNotConfigured)

			entries := logger.Entries()
			require.NotEmpty(t, entries)
			last := entries[len(entries)-1]
			assert.Equal(t, "ERROR", last.Level)
			assert.Equal(t, dlqURL, last.Fields["dlq_url"])
			assert.Equal(t, dlqARN, last.Fields["dlq_arn"])
			assert.Equal(t, dlqSourceURL, last.Fields["source_queue"])
		})
	}
}

func TestSendToDLQ_ResolutionErrorPropagates(t *testing.T) {
	client := &mockDLQClient{getQueueAttributesFn: func(context.Context, *sqs.GetQueueAttributesInput) (*sqs.GetQueueAttributesOutput, error) {
		return &sqs.GetQueueAttributesOutput{}, nil
	}}
	err := newTestDLQPublisher(t, client, "").SendToDLQ(context.Background(), dlqSourceURL, envelopeBody(t), nil, "r")
	require.ErrorIs(t, err, events.ErrDLQNotConfigured)
	assert.Empty(t, client.sent, "must not send when the DLQ is unresolved")
}

func TestSendToDLQ_InvalidInput(t *testing.T) {
	tooMany := map[string]string{}
	for i := range 7 {
		tooMany[string(rune('a'+i))] = "v"
	}
	cases := map[string]struct {
		source string
		body   []byte
		attrs  map[string]string
		reason string
	}{
		"empty source":        {"", []byte("x"), nil, "r"},
		"empty body":          {dlqSourceURL, nil, nil, "r"},
		"invalid utf8":        {dlqSourceURL, []byte{0xff, 0xfe}, nil, "r"},
		"empty reason":        {dlqSourceURL, []byte("x"), nil, "  "},
		"too many attrs (11)": {dlqSourceURL, []byte("x"), tooMany, "r"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			client := &mockDLQClient{}
			err := newTestDLQPublisher(t, client, "").SendToDLQ(context.Background(), tc.source, tc.body, tc.attrs, tc.reason)
			require.ErrorIs(t, err, events.ErrDLQInvalidMessage)
			assert.NotErrorIs(t, err, events.ErrRetryable)
			assert.Equal(t, int32(0), client.getAttrCalls.Load(), "invalid input must be rejected before any API call")
			assert.Empty(t, client.sent)
		})
	}
}

func TestSendToDLQ_AttributeLimitBoundary(t *testing.T) {
	// 4 reserved (no ConsumerName) + 6 caller = 10: exactly at the SQS limit.
	attrs := map[string]string{}
	for i := range 6 {
		attrs[string(rune('a'+i))] = "v"
	}
	client := &mockDLQClient{}
	require.NoError(t, newTestDLQPublisher(t, client, "").SendToDLQ(context.Background(), dlqSourceURL, []byte("x"), attrs, "r"))
	assert.Len(t, client.lastSent(t).MessageAttributes, 10)

	// With ConsumerName configured the same set is one over.
	err := newTestDLQPublisher(t, &mockDLQClient{}, "c").SendToDLQ(context.Background(), dlqSourceURL, []byte("x"), attrs, "r")
	require.ErrorIs(t, err, events.ErrDLQInvalidMessage)
}

func TestSendToDLQ_PublicConstructor(t *testing.T) {
	client := &mockDLQClient{}
	pub, err := events.NewSQSDLQPublisherWithClient(events.DLQConfig{ConsumerName: "c"}, client)
	require.NoError(t, err)
	require.NoError(t, pub.SendToDLQ(context.Background(), dlqSourceURL, envelopeBody(t), nil, "r"))
	in := client.lastSent(t)
	assert.Equal(t, dlqURL, aws.ToString(in.QueueUrl))
	_, err = time.Parse(time.RFC3339Nano, attr(in, events.DLQAttrFailedAt))
	require.NoError(t, err, "FailedAt must be RFC 3339")
}

func TestSendToDLQ_ConcurrentUse(t *testing.T) {
	client := &mockDLQClient{}
	p := newTestDLQPublisher(t, client, "")
	body := envelopeBody(t)
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			assert.NoError(t, p.SendToDLQ(context.Background(), dlqSourceURL, body, nil, "r"))
		})
	}
	wg.Wait()
	client.mu.Lock()
	defer client.mu.Unlock()
	assert.Len(t, client.sent, 20)
}

func TestDLQError_Message(t *testing.T) {
	err := &events.DLQError{Kind: events.ErrDLQSendFailed, SourceQueue: "q", Cause: errors.New("boom")}
	assert.Equal(t, "events: send to dead-letter queue failed (source queue q): boom", err.Error())
	assert.Equal(t, "events: source queue has no RedrivePolicy", (&events.DLQError{Kind: events.ErrDLQNotConfigured}).Error())
}

// ----------------------------
// Additional edge cases
// ----------------------------

// timeoutErr is a net.Error reporting a timeout, as returned by the HTTP
// transport when a request exceeds its deadline.
type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

var _ net.Error = timeoutErr{}

func TestSendToDLQ_NetworkTimeoutIsRetryable(t *testing.T) {
	cause := &url.Error{Op: "Post", URL: dlqURL, Err: timeoutErr{}}
	client := &mockDLQClient{sendMessageFn: func(context.Context, *sqs.SendMessageInput) (*sqs.SendMessageOutput, error) {
		return nil, cause
	}}
	err := newTestDLQPublisher(t, client, "").SendToDLQ(context.Background(), dlqSourceURL, envelopeBody(t), nil, "r")
	require.ErrorIs(t, err, events.ErrDLQSendFailed)
	require.ErrorIs(t, err, events.ErrRetryable)
	var netErr net.Error
	require.ErrorAs(t, err, &netErr, "net.Error must stay reachable")
}

func TestSendToDLQ_NonTimeoutNetworkErrorIsNotRetryable(t *testing.T) {
	client := &mockDLQClient{sendMessageFn: func(context.Context, *sqs.SendMessageInput) (*sqs.SendMessageOutput, error) {
		return nil, &url.Error{Op: "Post", URL: dlqURL, Err: errors.New("connection refused")}
	}}
	err := newTestDLQPublisher(t, client, "").SendToDLQ(context.Background(), dlqSourceURL, envelopeBody(t), nil, "r")
	require.ErrorIs(t, err, events.ErrDLQSendFailed)
	assert.NotErrorIs(t, err, events.ErrRetryable)
}

func TestSendToDLQ_TruncatesReasonMidRune(t *testing.T) {
	client := &mockDLQClient{}
	// 1 ASCII byte shifts every 2-byte rune so the 1024-byte cut lands mid-rune.
	reason := "a" + strings.Repeat("é", 1000)

	require.NoError(t, newTestDLQPublisher(t, client, "").SendToDLQ(context.Background(), dlqSourceURL, envelopeBody(t), nil, reason))
	got := attr(client.lastSent(t), events.DLQAttrReason)
	assert.Equal(t, "a"+strings.Repeat("é", 511), got)
	assert.Len(t, got, 1023)
	assert.True(t, utf8.ValidString(got))
}

func TestSendToDLQ_ShortReasonUnchanged(t *testing.T) {
	client := &mockDLQClient{}
	reason := strings.Repeat("x", 1024) // exactly at the cap
	require.NoError(t, newTestDLQPublisher(t, client, "").SendToDLQ(context.Background(), dlqSourceURL, envelopeBody(t), nil, reason))
	assert.Equal(t, reason, attr(client.lastSent(t), events.DLQAttrReason))
}

func TestSendToDLQ_FIFO_OversizedEnvelopeIDFallsBackToHash(t *testing.T) {
	client := &mockDLQClient{getQueueAttributesFn: func(context.Context, *sqs.GetQueueAttributesInput) (*sqs.GetQueueAttributesOutput, error) {
		return redrivePolicyOutput(`{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:123456789012:orders-dlq.fifo"}`), nil
	}}
	body := []byte(`{"id":"` + strings.Repeat("x", 129) + `","type":"t"}`)
	require.NoError(t, newTestDLQPublisher(t, client, "").SendToDLQ(context.Background(), dlqSourceURL, body, nil, "r"))
	in := client.lastSent(t)
	sum := sha256.Sum256(body)
	assert.Equal(t, hex.EncodeToString(sum[:]), aws.ToString(in.MessageGroupId), "SQS caps group/dedup IDs at 128 chars")
	assert.Equal(t, aws.ToString(in.MessageGroupId), aws.ToString(in.MessageDeduplicationId))
}

func TestSendToDLQ_ReusesCachedResolution(t *testing.T) {
	client := &mockDLQClient{}
	p := newTestDLQPublisher(t, client, "")
	for range 5 {
		require.NoError(t, p.SendToDLQ(context.Background(), dlqSourceURL, envelopeBody(t), nil, "r"))
	}
	assert.Equal(t, int32(1), client.getAttrCalls.Load())
	assert.Equal(t, int32(1), client.getURLCalls.Load())
	client.mu.Lock()
	defer client.mu.Unlock()
	assert.Len(t, client.sent, 5)
}

type ctxKey struct{}

func TestSendToDLQ_PropagatesContext(t *testing.T) {
	ctx := context.WithValue(context.Background(), ctxKey{}, "v")
	var seen []any
	var mu sync.Mutex
	record := func(c context.Context) {
		mu.Lock()
		seen = append(seen, c.Value(ctxKey{}))
		mu.Unlock()
	}
	client := &mockDLQClient{
		getQueueAttributesFn: func(c context.Context, _ *sqs.GetQueueAttributesInput) (*sqs.GetQueueAttributesOutput, error) {
			record(c)
			return redrivePolicyOutput(`{"deadLetterTargetArn":"` + dlqARN + `"}`), nil
		},
		getQueueURLFn: func(c context.Context, _ *sqs.GetQueueUrlInput) (*sqs.GetQueueUrlOutput, error) {
			record(c)
			return &sqs.GetQueueUrlOutput{QueueUrl: aws.String(dlqURL)}, nil
		},
		sendMessageFn: func(c context.Context, _ *sqs.SendMessageInput) (*sqs.SendMessageOutput, error) {
			record(c)
			return &sqs.SendMessageOutput{}, nil
		},
	}
	require.NoError(t, newTestDLQPublisher(t, client, "").SendToDLQ(ctx, dlqSourceURL, envelopeBody(t), nil, "r"))
	assert.Equal(t, []any{"v", "v", "v"}, seen)
}

func TestSendToDLQ_CancelledContextNotCached(t *testing.T) {
	client := &mockDLQClient{}
	client.getQueueAttributesFn = func(c context.Context, _ *sqs.GetQueueAttributesInput) (*sqs.GetQueueAttributesOutput, error) {
		if err := c.Err(); err != nil {
			return nil, err
		}
		return redrivePolicyOutput(`{"deadLetterTargetArn":"` + dlqARN + `"}`), nil
	}
	p := newTestDLQPublisher(t, client, "")
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	err := p.SendToDLQ(cancelled, dlqSourceURL, envelopeBody(t), nil, "r")
	require.ErrorIs(t, err, events.ErrDLQUnresolved)
	require.ErrorIs(t, err, context.Canceled)

	require.NoError(t, p.SendToDLQ(context.Background(), dlqSourceURL, envelopeBody(t), nil, "r"))
}

func TestSendToDLQ_LogsSuccess(t *testing.T) {
	logger := &fixtures.MockLogger{}
	p, err := internalsqs.NewDLQPublisherWithClient(internalsqs.DLQConfig{Logger: logger}, &mockDLQClient{})
	require.NoError(t, err)
	require.NoError(t, p.SendToDLQ(context.Background(), dlqSourceURL, envelopeBody(t), nil, "bad state"))

	entries := logger.Entries()
	require.Len(t, entries, 1)
	assert.Equal(t, "WARN", entries[0].Level)
	assert.Equal(t, dlqSourceURL, entries[0].Fields["source_queue"])
	assert.Equal(t, dlqURL, entries[0].Fields["dlq_url"])
	assert.Equal(t, dlqARN, entries[0].Fields["dlq_arn"])
	assert.Equal(t, "order.created", entries[0].Fields["event_type"])
	assert.Equal(t, "bad state", entries[0].Fields["reason"])
}

func TestSendToDLQ_NilLoggerDoesNotPanic(t *testing.T) {
	failing := &mockDLQClient{sendMessageFn: func(context.Context, *sqs.SendMessageInput) (*sqs.SendMessageOutput, error) {
		return nil, errors.New("boom")
	}}
	for _, client := range []*mockDLQClient{{}, failing} {
		p, err := internalsqs.NewDLQPublisherWithClient(internalsqs.DLQConfig{}, client)
		require.NoError(t, err)
		assert.NotPanics(t, func() {
			_ = p.SendToDLQ(context.Background(), dlqSourceURL, envelopeBody(t), nil, "r")
		})
	}
}

func TestSendToDLQ_RecordsMetric(t *testing.T) {
	// Metrics are process-global (initialised in TestMain), so assert deltas
	// to stay correct under -count=N.
	okQueue := dlqSourceURL + "-metric-ok"
	errQueue := dlqSourceURL + "-metric-err"
	okCounter := metrics.DLQForwardedTotal.WithLabelValues(okQueue, "order.created", "success")
	errCounter := metrics.DLQForwardedTotal.WithLabelValues(errQueue, "order.created", "error")
	okBefore, errBefore := testutil.ToFloat64(okCounter), testutil.ToFloat64(errCounter)

	p := newTestDLQPublisher(t, &mockDLQClient{}, "")
	require.NoError(t, p.SendToDLQ(context.Background(), okQueue, envelopeBody(t), nil, "r"))
	require.NoError(t, p.SendToDLQ(context.Background(), okQueue, envelopeBody(t), nil, "r"))
	require.Error(t, p.SendToDLQ(context.Background(), errQueue, envelopeBody(t), nil, ""))

	assert.InDelta(t, 2, testutil.ToFloat64(okCounter)-okBefore, 0)
	assert.InDelta(t, 1, testutil.ToFloat64(errCounter)-errBefore, 0)
}

func TestSendToDLQ_NilAttrsAndPlainTextBody(t *testing.T) {
	client := &mockDLQClient{}
	require.NoError(t, newTestDLQPublisher(t, client, "").SendToDLQ(context.Background(), dlqSourceURL, []byte("plain text, not JSON"), nil, "r"))
	in := client.lastSent(t)
	assert.Equal(t, "plain text, not JSON", aws.ToString(in.MessageBody))
	assert.Equal(t, "unknown", attr(in, events.DLQAttrEventType))
}

func TestDLQPublicErrors_AliasDomainSentinels(t *testing.T) {
	assert.Same(t, domain.ErrDLQNotConfigured, events.ErrDLQNotConfigured)
	assert.Same(t, domain.ErrDLQInvalidRedrivePolicy, events.ErrDLQInvalidRedrivePolicy)
	assert.Same(t, domain.ErrDLQUnresolved, events.ErrDLQUnresolved)
	assert.Same(t, domain.ErrDLQSendFailed, events.ErrDLQSendFailed)
	assert.Same(t, domain.ErrDLQInvalidMessage, events.ErrDLQInvalidMessage)
	assert.Same(t, domain.ErrRetryable, events.ErrRetryable)
}

func TestDLQError_KindsAreDistinct(t *testing.T) {
	kinds := []error{events.ErrDLQNotConfigured, events.ErrDLQInvalidRedrivePolicy, events.ErrDLQUnresolved, events.ErrDLQSendFailed, events.ErrDLQInvalidMessage}
	for i, k := range kinds {
		err := error(&events.DLQError{Kind: k})
		for j, other := range kinds {
			assert.Equal(t, i == j, errors.Is(err, other), "%v vs %v", k, other)
		}
		assert.NotErrorIs(t, err, events.ErrRetryable, "no cause → never retryable")
	}
}
