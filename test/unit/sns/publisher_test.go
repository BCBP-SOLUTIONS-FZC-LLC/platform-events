package sns_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	smithy "github.com/aws/smithy-go"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/trace"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/internal/adapter/outbound/metrics"
	internalsns "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/internal/adapter/outbound/sns"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/test/fixtures"
)

// mockAPIError is a minimal smithy.APIError implementation for testing wrapIfRetryable.
type mockAPIError struct {
	code    string
	message string
}

func (e *mockAPIError) Error() string                 { return e.message }
func (e *mockAPIError) ErrorCode() string             { return e.code }
func (e *mockAPIError) ErrorMessage() string          { return e.message }
func (e *mockAPIError) ErrorFault() smithy.ErrorFault { return smithy.FaultClient }

var _ smithy.APIError = (*mockAPIError)(nil)

// mockSNSClient is a test double for SNSClientAPI.
type mockSNSClient struct {
	publishFn      func(ctx context.Context, params *sns.PublishInput, optFns ...func(*sns.Options)) (*sns.PublishOutput, error)
	publishBatchFn func(ctx context.Context, params *sns.PublishBatchInput, optFns ...func(*sns.Options)) (*sns.PublishBatchOutput, error)
}

func (m *mockSNSClient) Publish(ctx context.Context, params *sns.PublishInput, optFns ...func(*sns.Options)) (*sns.PublishOutput, error) {
	return m.publishFn(ctx, params, optFns...)
}

func (m *mockSNSClient) PublishBatch(ctx context.Context, params *sns.PublishBatchInput, optFns ...func(*sns.Options)) (*sns.PublishBatchOutput, error) {
	return m.publishBatchFn(ctx, params, optFns...)
}

var _ internalsns.SNSClientAPI = (*mockSNSClient)(nil)

func makeEnv(eventType string) domain.Envelope[json.RawMessage] {
	env := domain.NewEnvelope(eventType, "test-svc", json.RawMessage(`{"x":1}`))
	env.TenantID = "acme"
	return env
}

// successClient returns a mock that always succeeds Publish with a fixed message ID.
func successClient() *mockSNSClient {
	return &mockSNSClient{
		publishFn: func(_ context.Context, _ *sns.PublishInput, _ ...func(*sns.Options)) (*sns.PublishOutput, error) {
			return &sns.PublishOutput{MessageId: aws.String("msg-001")}, nil
		},
		publishBatchFn: func(_ context.Context, params *sns.PublishBatchInput, _ ...func(*sns.Options)) (*sns.PublishBatchOutput, error) {
			var successful []snstypes.PublishBatchResultEntry
			for _, e := range params.PublishBatchRequestEntries {
				successful = append(successful, snstypes.PublishBatchResultEntry{
					Id:        e.Id,
					MessageId: aws.String("msg-" + aws.ToString(e.Id)),
				})
			}
			return &sns.PublishBatchOutput{Successful: successful}, nil
		},
	}
}

// ----------------------------
// New() panic on empty TopicARN
// ----------------------------

func TestNew_EmptyTopicARN_ReturnsError(t *testing.T) {
	_, err := internalsns.New(internalsns.Config{TopicARN: ""})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TopicARN")
}

func TestNew_InvalidARNFormat_ReturnsError(t *testing.T) {
	cases := []string{
		"not-an-arn",
		"arn:azure:sns:us-east-1:123:topic",
		"arn:aws:sqs:us-east-1:123:topic", // wrong service
		"http://localhost:4566/topic",
	}
	for _, arn := range cases {
		t.Run(arn, func(t *testing.T) {
			_, err := internalsns.New(internalsns.Config{TopicARN: arn})
			require.Error(t, err, "expected error for invalid ARN: %s", arn)
			assert.Contains(t, err.Error(), "TopicARN")
		})
	}
}

func TestNew_ValidARNPrefixes_Accepted(t *testing.T) {
	cases := []string{
		"arn:aws:sns:us-east-1:123456789012:topic",
		"arn:aws-cn:sns:cn-north-1:123456789012:topic",
		"arn:aws-us-gov:sns:us-gov-west-1:123456789012:topic",
	}
	for _, arn := range cases {
		t.Run(arn, func(t *testing.T) {
			_, err := internalsns.New(internalsns.Config{TopicARN: arn})
			require.NoError(t, err, "expected no error for valid ARN: %s", arn)
		})
	}
}

// ----------------------------
// NewWithClient construction
// ----------------------------

func TestNewWithClient_ReturnsPublisher(t *testing.T) {
	client := successClient()
	pub, err := internalsns.NewWithClient("arn:aws:sns:us-east-1:123:test", client, nil)
	require.NoError(t, err)
	assert.NotNil(t, pub)
}

func TestNewWithClient_EmptyTopicARN_ReturnsError(t *testing.T) {
	client := successClient()
	_, err := internalsns.NewWithClient("", client, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TopicARN")
}

func TestNewWithClient_InvalidARNFormat_ReturnsError(t *testing.T) {
	// NewWithClient has its own ARN prefix check; calling New() short-circuits before
	// reaching it. This test calls NewWithClient directly to cover that branch.
	client := successClient()
	_, err := internalsns.NewWithClient("not-an-arn", client, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TopicARN")
	assert.Contains(t, err.Error(), "not-an-arn")
}

// ----------------------------
// Publish success
// ----------------------------

func TestPublish_Success(t *testing.T) {
	client := successClient()
	pub, err := internalsns.NewWithClient("arn:aws:sns:us-east-1:123:test", client, nil)
	require.NoError(t, err)

	env := makeEnv("order.placed")
	err = pub.Publish(context.Background(), env)
	require.NoError(t, err)
}

// ----------------------------
// Publish error + logger
// ----------------------------

func TestPublish_Error_LoggerCalled(t *testing.T) {
	logger := &fixtures.MockLogger{}
	client := &mockSNSClient{
		publishFn: func(_ context.Context, _ *sns.PublishInput, _ ...func(*sns.Options)) (*sns.PublishOutput, error) {
			return nil, errors.New("sns failure")
		},
	}
	pub, err := internalsns.NewWithClient("arn:aws:sns:us-east-1:123:test", client, logger)
	require.NoError(t, err)

	env := makeEnv("order.placed")
	err = pub.Publish(context.Background(), env)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "sns failure")

	entries := logger.Entries()
	require.NotEmpty(t, entries)
	found := false
	for _, e := range entries {
		if e.Level == "ERROR" {
			found = true
			break
		}
	}
	assert.True(t, found, "expected ERROR log entry")
}

// ----------------------------
// Publish with nil logger (no panic)
// ----------------------------

func TestPublish_NilLogger_NoError(t *testing.T) {
	client := successClient()
	pub, err := internalsns.NewWithClient("arn:aws:sns:us-east-1:123:test", client, nil)
	require.NoError(t, err)

	env := makeEnv("test.event")
	err = pub.Publish(context.Background(), env)
	require.NoError(t, err)
}

func TestPublish_Error_NilLogger_NoPanic(t *testing.T) {
	client := &mockSNSClient{
		publishFn: func(_ context.Context, _ *sns.PublishInput, _ ...func(*sns.Options)) (*sns.PublishOutput, error) {
			return nil, errors.New("boom")
		},
	}
	pub, err := internalsns.NewWithClient("arn:aws:sns:us-east-1:123:test", client, nil)
	require.NoError(t, err)

	assert.NotPanics(t, func() {
		_ = pub.Publish(context.Background(), makeEnv("x.y"))
	})
}

// ----------------------------
// BatchError.Error()
// ----------------------------

func TestBatchError_Error(t *testing.T) {
	be := &internalsns.BatchError{
		Failures: []internalsns.BatchFailure{
			{ID: "a", Code: "Throttled", Message: "too many"},
			{ID: "b", Code: "Invalid", Message: "bad"},
		},
	}
	assert.Contains(t, be.Error(), "2")
}

// ----------------------------
// WithMessageGroupID option
// ----------------------------

func TestWithMessageGroupID_Applied(t *testing.T) {
	var capturedInput *sns.PublishInput
	client := &mockSNSClient{
		publishFn: func(_ context.Context, params *sns.PublishInput, _ ...func(*sns.Options)) (*sns.PublishOutput, error) {
			capturedInput = params
			return &sns.PublishOutput{MessageId: aws.String("x")}, nil
		},
	}
	pub, err := internalsns.NewWithClient(
		"arn:aws:sns:us-east-1:123:test.fifo",
		client,
		nil,
		internalsns.WithMessageGroupID(func(env domain.Envelope[json.RawMessage]) string {
			return "grp-" + env.TenantID
		}),
	)
	require.NoError(t, err)

	env := makeEnv("test.event")
	env.TenantID = "acme"
	_ = pub.Publish(context.Background(), env)

	require.NotNil(t, capturedInput)
	assert.Equal(t, "grp-acme", aws.ToString(capturedInput.MessageGroupId))
}

// ----------------------------
// WithMessageDeduplicationID option
// ----------------------------

func TestWithMessageDeduplicationID_Applied(t *testing.T) {
	var capturedInput *sns.PublishInput
	client := &mockSNSClient{
		publishFn: func(_ context.Context, params *sns.PublishInput, _ ...func(*sns.Options)) (*sns.PublishOutput, error) {
			capturedInput = params
			return &sns.PublishOutput{MessageId: aws.String("x")}, nil
		},
	}
	pub, err := internalsns.NewWithClient(
		"arn:aws:sns:us-east-1:123:test.fifo",
		client,
		nil,
		internalsns.WithMessageGroupID(func(_ domain.Envelope[json.RawMessage]) string { return "grp" }),
		internalsns.WithMessageDeduplicationID(func(env domain.Envelope[json.RawMessage]) string {
			return "dedup-" + env.ID
		}),
	)
	require.NoError(t, err)

	env := makeEnv("test.event")
	_ = pub.Publish(context.Background(), env)

	require.NotNil(t, capturedInput)
	assert.Equal(t, "dedup-"+env.ID, aws.ToString(capturedInput.MessageDeduplicationId))
}

// ----------------------------
// WithAttributes option
// ----------------------------

func TestWithAttributes_Applied(t *testing.T) {
	var capturedInput *sns.PublishInput
	client := &mockSNSClient{
		publishFn: func(_ context.Context, params *sns.PublishInput, _ ...func(*sns.Options)) (*sns.PublishOutput, error) {
			capturedInput = params
			return &sns.PublishOutput{MessageId: aws.String("x")}, nil
		},
	}
	pub, err := internalsns.NewWithClient(
		"arn:aws:sns:us-east-1:123:test",
		client,
		nil,
		internalsns.WithAttributes(map[string]string{"Env": "staging"}),
	)
	require.NoError(t, err)

	_ = pub.Publish(context.Background(), makeEnv("test.event"))

	require.NotNil(t, capturedInput)
	attr, ok := capturedInput.MessageAttributes["Env"]
	require.True(t, ok, "Env attribute should be set")
	assert.Equal(t, "staging", aws.ToString(attr.StringValue))
}

// ----------------------------
// FIFO topic: default deduplication ID = env.ID
// ----------------------------

func TestPublish_FIFO_DefaultDeduplicationID(t *testing.T) {
	var capturedInput *sns.PublishInput
	client := &mockSNSClient{
		publishFn: func(_ context.Context, params *sns.PublishInput, _ ...func(*sns.Options)) (*sns.PublishOutput, error) {
			capturedInput = params
			return &sns.PublishOutput{MessageId: aws.String("x")}, nil
		},
	}
	pub, err := internalsns.NewWithClient(
		"arn:aws:sns:us-east-1:123:my-topic.fifo",
		client,
		nil,
		internalsns.WithMessageGroupID(func(_ domain.Envelope[json.RawMessage]) string { return "grp" }),
	)
	require.NoError(t, err)

	env := makeEnv("test.fifo.event")
	_ = pub.Publish(context.Background(), env)

	require.NotNil(t, capturedInput)
	assert.Equal(t, env.ID, aws.ToString(capturedInput.MessageDeduplicationId))
}

// ----------------------------
// FIFO topic: custom group ID fn
// ----------------------------

func TestPublish_FIFO_CustomGroupIDFn(t *testing.T) {
	var capturedInput *sns.PublishInput
	client := &mockSNSClient{
		publishFn: func(_ context.Context, params *sns.PublishInput, _ ...func(*sns.Options)) (*sns.PublishOutput, error) {
			capturedInput = params
			return &sns.PublishOutput{MessageId: aws.String("x")}, nil
		},
	}
	pub, err := internalsns.NewWithClient(
		"arn:aws:sns:us-east-1:123:my-topic.fifo",
		client,
		nil,
		internalsns.WithMessageGroupID(func(_ domain.Envelope[json.RawMessage]) string {
			return "fixed-group"
		}),
	)
	require.NoError(t, err)

	_ = pub.Publish(context.Background(), makeEnv("test.event"))

	require.NotNil(t, capturedInput)
	assert.Equal(t, "fixed-group", aws.ToString(capturedInput.MessageGroupId))
}

// ----------------------------
// PublishBatch splits at 10
// ----------------------------

func TestPublishBatch_SplitsAt10(t *testing.T) {
	callCount := 0
	client := &mockSNSClient{
		publishBatchFn: func(_ context.Context, params *sns.PublishBatchInput, _ ...func(*sns.Options)) (*sns.PublishBatchOutput, error) {
			callCount++
			var successful []snstypes.PublishBatchResultEntry
			for _, e := range params.PublishBatchRequestEntries {
				successful = append(successful, snstypes.PublishBatchResultEntry{
					Id:        e.Id,
					MessageId: aws.String("m"),
				})
			}
			return &sns.PublishBatchOutput{Successful: successful}, nil
		},
	}
	pub, err := internalsns.NewWithClient("arn:aws:sns:us-east-1:123:test", client, nil)
	require.NoError(t, err)

	envs := make([]domain.Envelope[json.RawMessage], 12)
	for i := range envs {
		envs[i] = makeEnv("test.event")
	}

	err = pub.PublishBatch(context.Background(), envs)
	require.NoError(t, err)
	assert.Equal(t, 2, callCount, "expected 2 batch calls for 12 messages")
}

// ----------------------------
// PublishBatch: failed entries return BatchError
// ----------------------------

func TestPublishBatch_FailedEntries_ReturnsBatchError(t *testing.T) {
	client := &mockSNSClient{
		publishBatchFn: func(_ context.Context, params *sns.PublishBatchInput, _ ...func(*sns.Options)) (*sns.PublishBatchOutput, error) {
			// Fail the first entry.
			return &sns.PublishBatchOutput{
				Failed: []snstypes.BatchResultErrorEntry{
					{
						Id:      params.PublishBatchRequestEntries[0].Id,
						Code:    aws.String("Throttled"),
						Message: aws.String("rate exceeded"),
					},
				},
			}, nil
		},
	}
	pub, err := internalsns.NewWithClient("arn:aws:sns:us-east-1:123:test", client, nil)
	require.NoError(t, err)

	envs := []domain.Envelope[json.RawMessage]{makeEnv("test.event")}
	err = pub.PublishBatch(context.Background(), envs)
	require.Error(t, err)

	var be *internalsns.BatchError
	require.ErrorAs(t, err, &be)
	assert.Len(t, be.Failures, 1)
	assert.Equal(t, "Throttled", be.Failures[0].Code)
}

// ----------------------------
// PublishBatch: transport error returns raw error
// ----------------------------

func TestPublishBatch_TransportError(t *testing.T) {
	client := &mockSNSClient{
		publishBatchFn: func(_ context.Context, _ *sns.PublishBatchInput, _ ...func(*sns.Options)) (*sns.PublishBatchOutput, error) {
			return nil, errors.New("network error")
		},
	}
	pub, err := internalsns.NewWithClient("arn:aws:sns:us-east-1:123:test", client, nil)
	require.NoError(t, err)

	envs := []domain.Envelope[json.RawMessage]{makeEnv("test.event")}
	err = pub.PublishBatch(context.Background(), envs)
	require.Error(t, err)
	// Transport errors are now wrapped as BatchError failures so remaining chunks
	// are still attempted. The failure message carries the original error text.
	var be *internalsns.BatchError
	require.True(t, errors.As(err, &be), "expected *BatchError wrapping transport error")
	require.Len(t, be.Failures, 1)
	assert.Contains(t, be.Failures[0].Message, "network error")
	assert.Equal(t, "TransportError", be.Failures[0].Code)
}

// ----------------------------
// publishChunk FIFO with batch entries
// ----------------------------

func TestPublishBatch_FIFO_WithGroupAndDeduplicationIDs(t *testing.T) {
	var capturedInput *sns.PublishBatchInput
	client := &mockSNSClient{
		publishBatchFn: func(_ context.Context, params *sns.PublishBatchInput, _ ...func(*sns.Options)) (*sns.PublishBatchOutput, error) {
			capturedInput = params
			var successful []snstypes.PublishBatchResultEntry
			for _, e := range params.PublishBatchRequestEntries {
				successful = append(successful, snstypes.PublishBatchResultEntry{
					Id:        e.Id,
					MessageId: aws.String("msg-" + aws.ToString(e.Id)),
				})
			}
			return &sns.PublishBatchOutput{Successful: successful}, nil
		},
	}
	pub, err := internalsns.NewWithClient(
		"arn:aws:sns:us-east-1:123:topic.fifo",
		client,
		nil,
		internalsns.WithMessageGroupID(func(e domain.Envelope[json.RawMessage]) string {
			return "grp-" + e.TenantID
		}),
		internalsns.WithMessageDeduplicationID(func(e domain.Envelope[json.RawMessage]) string {
			return "dd-" + e.ID
		}),
	)
	require.NoError(t, err)

	envs := []domain.Envelope[json.RawMessage]{
		makeEnv("test.one"),
		makeEnv("test.two"),
	}
	envs[0].TenantID = "acme"
	envs[1].TenantID = "bcorp"

	err = pub.PublishBatch(context.Background(), envs)
	require.NoError(t, err)

	require.NotNil(t, capturedInput)
	require.Len(t, capturedInput.PublishBatchRequestEntries, 2)
	// First entry should have group ID and dedup ID set.
	first := capturedInput.PublishBatchRequestEntries[0]
	assert.Equal(t, "grp-acme", aws.ToString(first.MessageGroupId))
	assert.Equal(t, "dd-"+envs[0].ID, aws.ToString(first.MessageDeduplicationId))
}

func TestPublishBatch_FIFO_DefaultDeduplicationID(t *testing.T) {
	var capturedInput *sns.PublishBatchInput
	client := &mockSNSClient{
		publishBatchFn: func(_ context.Context, params *sns.PublishBatchInput, _ ...func(*sns.Options)) (*sns.PublishBatchOutput, error) {
			capturedInput = params
			var successful []snstypes.PublishBatchResultEntry
			for _, e := range params.PublishBatchRequestEntries {
				successful = append(successful, snstypes.PublishBatchResultEntry{
					Id:        e.Id,
					MessageId: aws.String("m"),
				})
			}
			return &sns.PublishBatchOutput{Successful: successful}, nil
		},
	}
	pub, err := internalsns.NewWithClient(
		"arn:aws:sns:us-east-1:123:topic.fifo",
		client,
		nil,
		internalsns.WithMessageGroupID(func(_ domain.Envelope[json.RawMessage]) string { return "grp" }),
		// No deduplication ID fn — should default to env.ID
	)
	require.NoError(t, err)

	env := makeEnv("test.event")
	err = pub.PublishBatch(context.Background(), []domain.Envelope[json.RawMessage]{env})
	require.NoError(t, err)

	require.NotNil(t, capturedInput)
	entry := capturedInput.PublishBatchRequestEntries[0]
	assert.Equal(t, env.ID, aws.ToString(entry.MessageDeduplicationId))
}

// ----------------------------
// SNS message attributes always set
// ----------------------------

func TestPublish_MessageAttributesSet(t *testing.T) {
	var capturedInput *sns.PublishInput
	client := &mockSNSClient{
		publishFn: func(_ context.Context, params *sns.PublishInput, _ ...func(*sns.Options)) (*sns.PublishOutput, error) {
			capturedInput = params
			return &sns.PublishOutput{MessageId: aws.String("x")}, nil
		},
	}
	pub, err := internalsns.NewWithClient("arn:aws:sns:us-east-1:123:test", client, nil)
	require.NoError(t, err)

	env := makeEnv("user.created")
	env.TenantID = "tenant1"
	env.Source = "iam-svc"
	env.Subject = "users/u1"
	_ = pub.Publish(context.Background(), env)

	require.NotNil(t, capturedInput)
	attrs := capturedInput.MessageAttributes
	assert.Equal(t, "user.created", aws.ToString(attrs["EventType"].StringValue))
	assert.Equal(t, "tenant1", aws.ToString(attrs["TenantID"].StringValue))
	assert.Equal(t, "iam-svc", aws.ToString(attrs["Source"].StringValue))
	assert.Equal(t, env.ID, aws.ToString(attrs["EventID"].StringValue))
	assert.Equal(t, "users/u1", aws.ToString(attrs["Subject"].StringValue))
}

func TestPublish_SubjectAttribute_OmittedWhenEmpty(t *testing.T) {
	var capturedInput *sns.PublishInput
	client := &mockSNSClient{
		publishFn: func(_ context.Context, params *sns.PublishInput, _ ...func(*sns.Options)) (*sns.PublishOutput, error) {
			capturedInput = params
			return &sns.PublishOutput{MessageId: aws.String("x")}, nil
		},
	}
	pub, err := internalsns.NewWithClient("arn:aws:sns:us-east-1:123:test", client, nil)
	require.NoError(t, err)

	env := makeEnv("user.created")
	_ = pub.Publish(context.Background(), env)

	require.NotNil(t, capturedInput)
	_, hasSubject := capturedInput.MessageAttributes["Subject"]
	assert.False(t, hasSubject, "Subject attribute must be absent when envelope.Subject is empty")
}

// ----------------------------
// sns.New with valid config (covers LoadDefaultConfig + EndpointURL branches)
// ----------------------------

func TestSNSNew_ValidConfig_Success(t *testing.T) {
	pub, err := internalsns.New(internalsns.Config{
		TopicARN:    "arn:aws:sns:us-east-1:123456789012:test-topic",
		Region:      "us-east-1",
		EndpointURL: "http://localhost:4566",
	})
	require.NoError(t, err)
	assert.NotNil(t, pub)
}

func TestSNSNew_ValidConfig_NoEndpoint(t *testing.T) {
	pub, err := internalsns.New(internalsns.Config{
		TopicARN: "arn:aws:sns:us-east-1:123456789012:test-topic",
		Region:   "us-east-1",
	})
	require.NoError(t, err)
	assert.NotNil(t, pub)
}

// ----------------------------
// NewWithClient: FIFO topic without WithMessageGroupID returns a construction error
// ----------------------------

func TestNewWithClient_FIFO_NoGroupIDFn_ReturnsError(t *testing.T) {
	client := &mockSNSClient{}

	// FIFO topic without WithMessageGroupID must be rejected at construction time
	// because SNS requires MessageGroupId for every FIFO publish call.
	_, err := internalsns.NewWithClient(
		"arn:aws:sns:us-east-1:123456789012:test-topic.fifo",
		client,
		nil,
		// intentionally no WithMessageGroupID
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "WithMessageGroupID")
}

// ----------------------------
// Publish: WithAttributes reserved key warning via logger
// ----------------------------

func TestPublish_WithAttributes_ReservedKeyWarning(t *testing.T) {
	var capturedInput *sns.PublishInput
	client := &mockSNSClient{
		publishFn: func(_ context.Context, params *sns.PublishInput, _ ...func(*sns.Options)) (*sns.PublishOutput, error) {
			capturedInput = params
			return &sns.PublishOutput{MessageId: aws.String("msg-001")}, nil
		},
	}
	logger := &fixtures.MockLogger{}

	pub, err := internalsns.NewWithClient(
		"arn:aws:sns:us-east-1:123:test",
		client,
		logger,
		internalsns.WithAttributes(map[string]string{
			"EventType":   "override-attempt", // reserved — should be ignored with a warn log
			"CustomField": "allowed",
		}),
	)
	require.NoError(t, err)

	env := makeEnv("user.created")
	err = pub.Publish(context.Background(), env)
	require.NoError(t, err)

	require.NotNil(t, capturedInput)
	// Reserved key must NOT have been overwritten.
	assert.Equal(t, env.Type, aws.ToString(capturedInput.MessageAttributes["EventType"].StringValue))
	// Non-reserved key must be present.
	assert.Equal(t, "allowed", aws.ToString(capturedInput.MessageAttributes["CustomField"].StringValue))
	// Logger should have been warned about the reserved key.
	found := false
	for _, e := range logger.Entries() {
		if e.Level == "WARN" {
			found = true
			break
		}
	}
	assert.True(t, found, "expected WARN log for reserved key conflict")
}

// ----------------------------
// events.NewSNSPublisher: error path (empty TopicARN)
// ----------------------------

func TestNewSNSPublisher_EmptyTopicARN_Error(t *testing.T) {
	// events.NewSNSPublisher wraps sns.New which requires TopicARN.
	// Covered via the internal path: empty ARN → error before AWS SDK call.
	_, err := internalsns.New(internalsns.Config{TopicARN: ""})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TopicARN")
}

// ----------------------------
// Publish: metrics counters and duration histogram updated on success
// ----------------------------

func TestPublish_WithMetrics(t *testing.T) {
	fixtures.InitPlatformMetrics(t)

	env := makeEnv("metrics.event")
	client := successClient()
	p, err := internalsns.NewWithClient("arn:aws:sns:us-east-1:123456789:test", client, nil)
	require.NoError(t, err)

	err = p.Publish(context.Background(), env)
	require.NoError(t, err)
	assert.InDelta(t, 1, testutil.ToFloat64(metrics.CurrentPlatform().MessagesPublished.WithLabelValues("test", "metrics.event", "success")), 0)
}

// ----------------------------
// PublishBatch: metrics updated on success
// ----------------------------

func TestPublishBatch_WithMetrics_Success(t *testing.T) {
	fixtures.InitPlatformMetrics(t)

	envs := []domain.Envelope[json.RawMessage]{
		makeEnv("batch.one"),
		makeEnv("batch.two"),
	}
	client := successClient()
	p, err := internalsns.NewWithClient("arn:aws:sns:us-east-1:123456789:test", client, nil)
	require.NoError(t, err)

	err = p.PublishBatch(context.Background(), envs)
	require.NoError(t, err)
	published := metrics.CurrentPlatform().MessagesPublished
	assert.InDelta(t, 1, testutil.ToFloat64(published.WithLabelValues("test", "batch.one", "success")), 0)
	assert.InDelta(t, 1, testutil.ToFloat64(published.WithLabelValues("test", "batch.two", "success")), 0)
}

// ----------------------------
// PublishBatch: transport error with logger and metrics
// ----------------------------

func TestPublishBatch_TransportError_WithLoggerAndMetrics(t *testing.T) {
	fixtures.InitPlatformMetrics(t)

	logger := &fixtures.MockLogger{}
	env := makeEnv("fail.event")
	client := &mockSNSClient{
		publishBatchFn: func(_ context.Context, _ *sns.PublishBatchInput, _ ...func(*sns.Options)) (*sns.PublishBatchOutput, error) {
			return nil, errors.New("transport failure")
		},
	}
	p, err := internalsns.NewWithClient("arn:aws:sns:us-east-1:123456789:test", client, logger)
	require.NoError(t, err)

	err = p.PublishBatch(context.Background(), []domain.Envelope[json.RawMessage]{env})
	require.Error(t, err)
	// Transport errors are wrapped as BatchError failures; the original message is in Failures[0].
	var be *internalsns.BatchError
	require.True(t, errors.As(err, &be), "expected *BatchError wrapping transport error")
	require.Len(t, be.Failures, 1)
	assert.Contains(t, be.Failures[0].Message, "transport failure")
	assert.InDelta(t, 1, testutil.ToFloat64(metrics.CurrentPlatform().MessagesPublished.WithLabelValues("test", "fail.event", "error")), 0)

	entries := logger.Entries()
	found := false
	for _, e := range entries {
		if e.Level == "ERROR" {
			found = true
			break
		}
	}
	assert.True(t, found, "expected ERROR log for transport failure")
}

// ----------------------------
// wrapIfRetryable: retryable AWS error codes
// ----------------------------

func TestPublish_ThrottlingException_WrappedAsRetryable(t *testing.T) {
	client := &mockSNSClient{
		publishFn: func(_ context.Context, _ *sns.PublishInput, _ ...func(*sns.Options)) (*sns.PublishOutput, error) {
			return nil, &mockAPIError{code: "ThrottlingException", message: "rate exceeded"}
		},
	}
	pub, err := internalsns.NewWithClient("arn:aws:sns:us-east-1:123:test", client, nil)
	require.NoError(t, err)

	pubErr := pub.Publish(context.Background(), makeEnv("order.placed"))
	require.Error(t, pubErr)
	assert.True(t, errors.Is(pubErr, domain.ErrRetryable), "ThrottlingException should be wrapped as ErrRetryable")
}

func TestPublish_ServiceUnavailable_WrappedAsRetryable(t *testing.T) {
	client := &mockSNSClient{
		publishFn: func(_ context.Context, _ *sns.PublishInput, _ ...func(*sns.Options)) (*sns.PublishOutput, error) {
			return nil, &mockAPIError{code: "ServiceUnavailable", message: "service down"}
		},
	}
	pub, err := internalsns.NewWithClient("arn:aws:sns:us-east-1:123:test", client, nil)
	require.NoError(t, err)

	pubErr := pub.Publish(context.Background(), makeEnv("order.placed"))
	require.Error(t, pubErr)
	assert.True(t, errors.Is(pubErr, domain.ErrRetryable))
}

func TestPublish_NonRetryableError_NotWrapped(t *testing.T) {
	client := &mockSNSClient{
		publishFn: func(_ context.Context, _ *sns.PublishInput, _ ...func(*sns.Options)) (*sns.PublishOutput, error) {
			return nil, &mockAPIError{code: "InvalidParameter", message: "bad input"}
		},
	}
	pub, err := internalsns.NewWithClient("arn:aws:sns:us-east-1:123:test", client, nil)
	require.NoError(t, err)

	pubErr := pub.Publish(context.Background(), makeEnv("order.placed"))
	require.Error(t, pubErr)
	assert.False(t, errors.Is(pubErr, domain.ErrRetryable), "InvalidParameter should not be ErrRetryable")
}

func TestPublish_InternalFailure_WrappedAsRetryable(t *testing.T) {
	client := &mockSNSClient{
		publishFn: func(_ context.Context, _ *sns.PublishInput, _ ...func(*sns.Options)) (*sns.PublishOutput, error) {
			return nil, &mockAPIError{code: "InternalFailure", message: "internal server error"}
		},
	}
	pub, err := internalsns.NewWithClient("arn:aws:sns:us-east-1:123:test", client, nil)
	require.NoError(t, err)

	pubErr := pub.Publish(context.Background(), makeEnv("order.placed"))
	require.Error(t, pubErr)
	assert.True(t, errors.Is(pubErr, domain.ErrRetryable))
}

// ----------------------------
// Publish: required field validation (single-message path)
// ----------------------------

func TestPublish_EmptyType_ReturnsError(t *testing.T) {
	// Publish must reject an envelope with empty Type before calling SNS.
	// Empty Type means no EventType message attribute, causing SQS filter
	// policies to silently drop the message.
	var apiCalled bool
	client := &mockSNSClient{
		publishFn: func(_ context.Context, _ *sns.PublishInput, _ ...func(*sns.Options)) (*sns.PublishOutput, error) {
			apiCalled = true
			return &sns.PublishOutput{MessageId: aws.String("x")}, nil
		},
	}
	pub, err := internalsns.NewWithClient("arn:aws:sns:us-east-1:123:test", client, nil)
	require.NoError(t, err)

	env := domain.Envelope[json.RawMessage]{
		ID:      "01926e4f-dead-7000-beef-000000000001",
		Type:    "", // empty
		Source:  "svc",
		Payload: json.RawMessage(`{}`),
	}
	pubErr := pub.Publish(context.Background(), env)
	require.Error(t, pubErr)
	assert.Contains(t, pubErr.Error(), "type=")
	assert.False(t, apiCalled, "SNS API must not be called for invalid envelope")
}

func TestPublish_EmptyID_ReturnsError(t *testing.T) {
	var apiCalled bool
	client := &mockSNSClient{
		publishFn: func(_ context.Context, _ *sns.PublishInput, _ ...func(*sns.Options)) (*sns.PublishOutput, error) {
			apiCalled = true
			return &sns.PublishOutput{MessageId: aws.String("x")}, nil
		},
	}
	pub, err := internalsns.NewWithClient("arn:aws:sns:us-east-1:123:test", client, nil)
	require.NoError(t, err)

	env := domain.Envelope[json.RawMessage]{
		ID:      "", // empty
		Type:    "order.placed",
		Source:  "svc",
		Payload: json.RawMessage(`{}`),
	}
	pubErr := pub.Publish(context.Background(), env)
	require.Error(t, pubErr)
	assert.Contains(t, pubErr.Error(), "id=")
	assert.False(t, apiCalled)
}

func TestPublish_EmptySource_ReturnsError(t *testing.T) {
	var apiCalled bool
	client := &mockSNSClient{
		publishFn: func(_ context.Context, _ *sns.PublishInput, _ ...func(*sns.Options)) (*sns.PublishOutput, error) {
			apiCalled = true
			return &sns.PublishOutput{MessageId: aws.String("x")}, nil
		},
	}
	pub, err := internalsns.NewWithClient("arn:aws:sns:us-east-1:123:test", client, nil)
	require.NoError(t, err)

	env := domain.Envelope[json.RawMessage]{
		ID:      "01926e4f-dead-7000-beef-000000000001",
		Type:    "order.placed",
		Source:  "", // empty
		Payload: json.RawMessage(`{}`),
	}
	pubErr := pub.Publish(context.Background(), env)
	require.Error(t, pubErr)
	assert.Contains(t, pubErr.Error(), "source=")
	assert.False(t, apiCalled)
}

func TestPublish_TooManyAttributes_ReturnsError(t *testing.T) {
	// Single Publish must reject envelopes where combined attributes (fixed + extra)
	// exceed the SNS limit of 10.
	var apiCalled bool
	client := &mockSNSClient{
		publishFn: func(_ context.Context, _ *sns.PublishInput, _ ...func(*sns.Options)) (*sns.PublishOutput, error) {
			apiCalled = true
			return &sns.PublishOutput{MessageId: aws.String("x")}, nil
		},
	}
	// 8 extra + 4 reserved = 12 total → exceeds limit.
	extraAttrs := map[string]string{
		"a1": "v1", "a2": "v2", "a3": "v3", "a4": "v4",
		"a5": "v5", "a6": "v6", "a7": "v7", "a8": "v8",
	}
	pub, err := internalsns.NewWithClient(
		"arn:aws:sns:us-east-1:123:test", client, nil,
		internalsns.WithAttributes(extraAttrs),
	)
	require.NoError(t, err)

	env := domain.NewEnvelope("order.placed", "billing", json.RawMessage(`{}`))
	env.TenantID = "acme" // ensure all 4 reserved attrs are present
	pubErr := pub.Publish(context.Background(), env)
	require.Error(t, pubErr)
	assert.Contains(t, pubErr.Error(), "10")
	assert.False(t, apiCalled, "SNS API must not be called when attribute count exceeds limit")
}

// ----------------------------
// PublishBatch: envelope field validation (batch path must mirror single Publish)
// ----------------------------

func TestPublishBatch_InvalidEnvelope_EmptyType_ReturnsBatchError(t *testing.T) {
	// PublishBatch should reject envelopes with empty Type without calling SNS.
	// An envelope with empty Type has no EventType message attribute, causing SQS
	// filter policies to silently drop the message even though SNS accepts it.
	var apiCalled bool
	client := &mockSNSClient{
		publishBatchFn: func(_ context.Context, _ *sns.PublishBatchInput, _ ...func(*sns.Options)) (*sns.PublishBatchOutput, error) {
			apiCalled = true
			return &sns.PublishBatchOutput{}, nil
		},
	}
	pub, err := internalsns.NewWithClient("arn:aws:sns:us-east-1:123:test", client, nil)
	require.NoError(t, err)

	badEnv := domain.Envelope[json.RawMessage]{
		ID:      "01926e4f-dead-7000-beef-000000000001",
		Type:    "", // empty — must be rejected
		Source:  "svc",
		Payload: json.RawMessage(`{}`),
	}
	batchErr := pub.PublishBatch(context.Background(), []domain.Envelope[json.RawMessage]{badEnv})
	require.Error(t, batchErr)
	var be *internalsns.BatchError
	require.ErrorAs(t, batchErr, &be)
	require.Len(t, be.Failures, 1)
	assert.Equal(t, "InvalidEnvelope", be.Failures[0].Code)
	assert.Contains(t, be.Failures[0].Message, "type=")
	assert.False(t, apiCalled, "SNS API must not be called when all envelopes are invalid")
}

func TestPublishBatch_MixedInvalidAndValid_ValidDelivered(t *testing.T) {
	// Valid envelopes in the same batch as an invalid one must still be delivered.
	var deliveredIDs []string
	client := &mockSNSClient{
		publishBatchFn: func(_ context.Context, params *sns.PublishBatchInput, _ ...func(*sns.Options)) (*sns.PublishBatchOutput, error) {
			var successful []snstypes.PublishBatchResultEntry
			for _, e := range params.PublishBatchRequestEntries {
				deliveredIDs = append(deliveredIDs, aws.ToString(e.Id))
				successful = append(successful, snstypes.PublishBatchResultEntry{
					Id:        e.Id,
					MessageId: aws.String("msg-" + aws.ToString(e.Id)),
				})
			}
			return &sns.PublishBatchOutput{Successful: successful}, nil
		},
	}
	pub, err := internalsns.NewWithClient("arn:aws:sns:us-east-1:123:test", client, nil)
	require.NoError(t, err)

	validEnv := makeEnv("order.placed")
	badEnv := domain.Envelope[json.RawMessage]{
		ID:      "01926e4f-dead-7000-beef-000000000099",
		Type:    "", // empty — invalid
		Source:  "svc",
		Payload: json.RawMessage(`{}`),
	}
	batchErr := pub.PublishBatch(context.Background(), []domain.Envelope[json.RawMessage]{badEnv, validEnv})
	require.Error(t, batchErr)
	var be *internalsns.BatchError
	require.ErrorAs(t, batchErr, &be)
	// One failure for the invalid envelope.
	require.Len(t, be.Failures, 1)
	assert.Equal(t, "InvalidEnvelope", be.Failures[0].Code)
	// The valid envelope must have been delivered.
	assert.Contains(t, deliveredIDs, validEnv.ID)
}

// ----------------------------
// PublishBatch: TooManyAttributes limit per entry
// ----------------------------

func TestPublishBatch_TooManyAttributes_ReturnsBatchError(t *testing.T) {
	// An entry with > 10 message attributes must be rejected pre-flight with a clear
	// TooManyAttributes code rather than hitting the SNS API and getting an opaque
	// InvalidParameter error.
	var apiCalled bool
	client := &mockSNSClient{
		publishBatchFn: func(_ context.Context, _ *sns.PublishBatchInput, _ ...func(*sns.Options)) (*sns.PublishBatchOutput, error) {
			apiCalled = true
			return &sns.PublishBatchOutput{}, nil
		},
	}
	// Build a publisher with 8 extra attributes (+ 4 reserved = 12 total → exceeds 10).
	extraAttrs := map[string]string{
		"a1": "v1", "a2": "v2", "a3": "v3", "a4": "v4",
		"a5": "v5", "a6": "v6", "a7": "v7", "a8": "v8",
	}
	pub, err := internalsns.NewWithClient(
		"arn:aws:sns:us-east-1:123:test", client, nil,
		internalsns.WithAttributes(extraAttrs),
	)
	require.NoError(t, err)

	env := makeEnv("billing.invoice.settled")
	batchErr := pub.PublishBatch(context.Background(), []domain.Envelope[json.RawMessage]{env})
	require.Error(t, batchErr)
	var be *internalsns.BatchError
	require.ErrorAs(t, batchErr, &be)
	require.Len(t, be.Failures, 1)
	assert.Equal(t, "TooManyAttributes", be.Failures[0].Code)
	assert.Contains(t, be.Failures[0].Message, "exceed SNS limit of 10")
	assert.False(t, apiCalled, "SNS API must not be called when entry has too many attributes")
}

func TestPublishBatch_TooManyAttributes_MixedWithValid(t *testing.T) {
	// Valid envelopes in the same batch as a TooManyAttributes-rejected entry
	// must still reach the SNS API. Use 8 publisher extras (4+8=12) on one entry
	// and a separate publisher with 6 extras (4+6=10) is not possible in one call —
	// instead, pair a TooManyAttributes envelope (8 extras) with a valid envelope
	// on a 6-extra publisher where the over-limit entry is rejected pre-flight.
	var deliveredIDs []string
	client := &mockSNSClient{
		publishBatchFn: func(_ context.Context, params *sns.PublishBatchInput, _ ...func(*sns.Options)) (*sns.PublishBatchOutput, error) {
			var successful []snstypes.PublishBatchResultEntry
			for _, e := range params.PublishBatchRequestEntries {
				deliveredIDs = append(deliveredIDs, aws.ToString(e.Id))
				successful = append(successful, snstypes.PublishBatchResultEntry{
					Id:        e.Id,
					MessageId: aws.String("msg-" + aws.ToString(e.Id)),
				})
			}
			return &sns.PublishBatchOutput{Successful: successful}, nil
		},
	}
	// 6 extras + 4 reserved = 10 → at SNS limit.
	extraAttrs := map[string]string{
		"a1": "v1", "a2": "v2", "a3": "v3", "a4": "v4", "a5": "v5", "a6": "v6",
	}
	pub, err := internalsns.NewWithClient("arn:aws:sns:us-east-1:123:test", client, nil,
		internalsns.WithAttributes(extraAttrs))
	require.NoError(t, err)

	envValid := makeEnv("order.placed")
	// 7th extra attribute on this envelope via a reserved-key collision is not
	// possible; use a publisher with 8 extras for the over-limit case only.
	pubOver, err := internalsns.NewWithClient("arn:aws:sns:us-east-1:123:test", client, nil,
		internalsns.WithAttributes(map[string]string{
			"a1": "v1", "a2": "v2", "a3": "v3", "a4": "v4",
			"a5": "v5", "a6": "v6", "a7": "v7", "a8": "v8",
		}))
	require.NoError(t, err)
	envOver := makeEnv("billing.invoice.settled")

	// Over-limit publisher: single envelope rejected, none delivered.
	batchErr := pubOver.PublishBatch(context.Background(), []domain.Envelope[json.RawMessage]{envOver, envValid})
	require.Error(t, batchErr)
	var be *internalsns.BatchError
	require.ErrorAs(t, batchErr, &be)
	assert.Equal(t, "TooManyAttributes", be.Failures[0].Code)

	// At-limit publisher: both envelopes fit (4 reserved + 6 extra = 10).
	err = pub.PublishBatch(context.Background(), []domain.Envelope[json.RawMessage]{envValid, makeEnv("user.created")})
	require.NoError(t, err)
	assert.Len(t, deliveredIDs, 2)
}

// ----------------------------
// publishChunk: combined marshal-error + transport error
// ----------------------------

// TestPublishBatch_MarshalErrAndTransportError exercises the combined-error path in
// publishChunk (sns/publisher.go ~line 456-466). When some entries fail pre-flight
// (InvalidEnvelope/TooManyAttributes) AND the SNS API call also fails with a transport
// error, the returned BatchError must contain failures from BOTH sources with the
// correct Code labels ("InvalidEnvelope" + "TransportError").
func TestPublishBatch_MarshalErrAndTransportError(t *testing.T) {
	client := &mockSNSClient{
		publishBatchFn: func(_ context.Context, _ *sns.PublishBatchInput, _ ...func(*sns.Options)) (*sns.PublishBatchOutput, error) {
			return nil, errors.New("sns: connection reset")
		},
	}
	pub, err := internalsns.NewWithClient("arn:aws:sns:us-east-1:123:test", client, nil)
	require.NoError(t, err)

	// badEnv fails pre-flight (empty Type) → marshalErr set before API call.
	badEnv := domain.Envelope[json.RawMessage]{
		ID:      "01926e4f-dead-7000-beef-000000000001",
		Type:    "",
		Source:  "svc",
		Payload: json.RawMessage(`{}`),
	}
	// validEnv passes pre-flight validation and enters the API call which fails.
	validEnv := makeEnv("order.placed")

	batchErr := pub.PublishBatch(context.Background(), []domain.Envelope[json.RawMessage]{badEnv, validEnv})
	require.Error(t, batchErr)

	var be *internalsns.BatchError
	require.ErrorAs(t, batchErr, &be, "combined failures must be returned as *BatchError")

	codes := make(map[string]bool)
	for _, f := range be.Failures {
		codes[f.Code] = true
	}
	assert.True(t, codes["InvalidEnvelope"], "expected InvalidEnvelope failure from pre-flight rejection")
	assert.True(t, codes["TransportError"], "expected TransportError failure from API call failure")
}

// ----------------------------
// NewWithClient: high WithAttributes count warning
// ----------------------------

func TestNewWithClient_HighAttributesCount_Warns(t *testing.T) {
	logger := &fixtures.MockLogger{}
	extra := make(map[string]string, 5)
	for i := range 5 {
		extra[fmt.Sprintf("attr_%d", i)] = "v"
	}

	_, err := internalsns.NewWithClient(
		"arn:aws:sns:us-east-1:123456789012:test-topic",
		&mockSNSClient{},
		logger,
		internalsns.WithAttributes(extra),
	)
	require.NoError(t, err)

	found := false
	for _, e := range logger.Entries() {
		if e.Level == "WARN" {
			found = true
			break
		}
	}
	assert.True(t, found, "expected WARN when WithAttributes count is high")
}

// ----------------------------
// wrapIfRetryable: network timeout errors
// ----------------------------

type timeoutNetError struct{}

func (timeoutNetError) Error() string   { return "i/o timeout" }
func (timeoutNetError) Timeout() bool   { return true }
func (timeoutNetError) Temporary() bool { return true }

func TestPublish_NetTimeout_WrappedAsRetryable(t *testing.T) {
	client := &mockSNSClient{
		publishFn: func(_ context.Context, _ *sns.PublishInput, _ ...func(*sns.Options)) (*sns.PublishOutput, error) {
			return nil, timeoutNetError{}
		},
	}
	pub, err := internalsns.NewWithClient("arn:aws:sns:us-east-1:123:test", client, nil)
	require.NoError(t, err)

	pubErr := pub.Publish(context.Background(), makeEnv("order.placed"))
	require.Error(t, pubErr)
	assert.True(t, errors.Is(pubErr, domain.ErrRetryable))
}

// ----------------------------
// Publish: OTel trace context injected into message attributes
// ----------------------------

func TestPublish_WithActiveSpan_InjectsTraceCarrier(t *testing.T) {
	var captured *sns.PublishInput
	client := &mockSNSClient{
		publishFn: func(_ context.Context, params *sns.PublishInput, _ ...func(*sns.Options)) (*sns.PublishOutput, error) {
			captured = params
			return &sns.PublishOutput{MessageId: aws.String("mid")}, nil
		},
	}
	pub, err := internalsns.NewWithClient("arn:aws:sns:us-east-1:123:test", client, nil)
	require.NoError(t, err)

	tp := trace.NewTracerProvider()
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	tracer := tp.Tracer("test")
	ctx, span := tracer.Start(context.Background(), "publish-test")
	defer span.End()

	require.NoError(t, pub.Publish(ctx, makeEnv("trace.event")))
	require.NotNil(t, captured)
	assert.Contains(t, captured.MessageAttributes, "traceparent")
}

func TestPublishBatch_WithActiveSpan_InjectsTraceCarrier(t *testing.T) {
	var captured *sns.PublishBatchInput
	client := &mockSNSClient{
		publishBatchFn: func(_ context.Context, params *sns.PublishBatchInput, _ ...func(*sns.Options)) (*sns.PublishBatchOutput, error) {
			captured = params
			var successful []snstypes.PublishBatchResultEntry
			for _, e := range params.PublishBatchRequestEntries {
				successful = append(successful, snstypes.PublishBatchResultEntry{
					Id: e.Id, MessageId: aws.String("mid-" + aws.ToString(e.Id)),
				})
			}
			return &sns.PublishBatchOutput{Successful: successful}, nil
		},
	}
	pub, err := internalsns.NewWithClient("arn:aws:sns:us-east-1:123:test", client, nil)
	require.NoError(t, err)

	tp := trace.NewTracerProvider()
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	tracer := tp.Tracer("test")
	ctx, span := tracer.Start(context.Background(), "publish-batch-test")
	defer span.End()

	env := makeEnv("trace.batch")
	require.NoError(t, pub.PublishBatch(ctx, []domain.Envelope[json.RawMessage]{env}))
	require.NotNil(t, captured)
	require.NotEmpty(t, captured.PublishBatchRequestEntries)
	assert.Contains(t, captured.PublishBatchRequestEntries[0].MessageAttributes, "traceparent")
}

func TestPublish_TooManyAttributes_WithLogger_LogsError(t *testing.T) {
	logger := &fixtures.MockLogger{}
	extraAttrs := map[string]string{
		"a1": "v1", "a2": "v2", "a3": "v3", "a4": "v4",
		"a5": "v5", "a6": "v6", "a7": "v7", "a8": "v8",
	}
	pub, err := internalsns.NewWithClient(
		"arn:aws:sns:us-east-1:123:test", &mockSNSClient{}, logger,
		internalsns.WithAttributes(extraAttrs),
	)
	require.NoError(t, err)

	env := domain.NewEnvelope("order.placed", "billing", json.RawMessage(`{}`))
	env.TenantID = "acme"
	pubErr := pub.Publish(context.Background(), env)
	require.Error(t, pubErr)

	found := false
	for _, e := range logger.Entries() {
		if e.Level == "ERROR" {
			found = true
			break
		}
	}
	assert.True(t, found, "expected ERROR log when attribute count exceeds SNS limit")
}

// ----------------------------
// WithCodec
// ----------------------------

// fakeCodec is a minimal port.Codec test double. reverseCodec below builds one
// that reverses the payload bytes — a stand-in for a real schema-registry codec.
type fakeCodec struct {
	encodeFn func(ctx context.Context, eventType string, payload json.RawMessage) ([]byte, string, error)
	decodeFn func(ctx context.Context, schemaID string, encoded []byte) (json.RawMessage, error)
}

func (f *fakeCodec) Encode(ctx context.Context, eventType string, payload json.RawMessage) ([]byte, string, error) {
	return f.encodeFn(ctx, eventType, payload)
}

func (f *fakeCodec) Decode(ctx context.Context, schemaID string, encoded []byte) (json.RawMessage, error) {
	return f.decodeFn(ctx, schemaID, encoded)
}

var _ port.Codec = (*fakeCodec)(nil)

func reverseBytes(b []byte) []byte {
	out := make([]byte, len(b))
	for i, c := range b {
		out[len(b)-1-i] = c
	}
	return out
}

// reversingCodec reverses payload bytes on Encode and un-reverses on Decode,
// returning the fixed schemaID "fake-schema-v1".
func reversingCodec() *fakeCodec {
	return &fakeCodec{
		encodeFn: func(_ context.Context, _ string, payload json.RawMessage) ([]byte, string, error) {
			return reverseBytes(payload), "fake-schema-v1", nil
		},
		decodeFn: func(_ context.Context, _ string, encoded []byte) (json.RawMessage, error) {
			return reverseBytes(encoded), nil
		},
	}
}

// wireEnvelope mirrors the subset of the envelope's JSON shape needed to
// assert on "data"/"dataschema" without depending on domain.Envelope's
// custom UnmarshalJSON (which decodes Data eagerly into a generic type).
type wireEnvelope struct {
	Data     json.RawMessage `json:"data"`
	SchemaID string          `json:"dataschema"`
}

func TestPublish_NoCodec_WireFormatUnchanged(t *testing.T) {
	var capturedInput *sns.PublishInput
	client := &mockSNSClient{
		publishFn: func(_ context.Context, params *sns.PublishInput, _ ...func(*sns.Options)) (*sns.PublishOutput, error) {
			capturedInput = params
			return &sns.PublishOutput{MessageId: aws.String("x")}, nil
		},
	}
	pub, err := internalsns.NewWithClient("arn:aws:sns:us-east-1:123:test", client, nil)
	require.NoError(t, err)

	err = pub.Publish(context.Background(), makeEnv("test.event"))
	require.NoError(t, err)

	require.NotNil(t, capturedInput)
	var wire wireEnvelope
	require.NoError(t, json.Unmarshal([]byte(aws.ToString(capturedInput.Message)), &wire))
	assert.Empty(t, wire.SchemaID)
	assert.True(t, bytesLooksLikeJSONObject(wire.Data), "data should remain a JSON object without a codec")
}

func bytesLooksLikeJSONObject(b json.RawMessage) bool {
	for _, c := range b {
		switch c {
		case ' ', '\t', '\n', '\r':
			continue
		case '{':
			return true
		default:
			return false
		}
	}
	return false
}

func TestWithCodec_WrapsPayloadAsBase64String(t *testing.T) {
	var capturedInput *sns.PublishInput
	client := &mockSNSClient{
		publishFn: func(_ context.Context, params *sns.PublishInput, _ ...func(*sns.Options)) (*sns.PublishOutput, error) {
			capturedInput = params
			return &sns.PublishOutput{MessageId: aws.String("x")}, nil
		},
	}
	pub, err := internalsns.NewWithClient(
		"arn:aws:sns:us-east-1:123:test", client, nil,
		internalsns.WithCodec(reversingCodec()),
	)
	require.NoError(t, err)

	env := makeEnv("test.event")
	err = pub.Publish(context.Background(), env)
	require.NoError(t, err)

	require.NotNil(t, capturedInput)
	var wire wireEnvelope
	require.NoError(t, json.Unmarshal([]byte(aws.ToString(capturedInput.Message)), &wire))
	assert.Equal(t, "fake-schema-v1", wire.SchemaID)

	var b64 string
	require.NoError(t, json.Unmarshal(wire.Data, &b64), "data should be a base64 JSON string when a codec is configured")
	raw, err := base64.StdEncoding.DecodeString(b64)
	require.NoError(t, err)
	assert.Equal(t, string(env.Payload), string(reverseBytes(raw)))
}

func TestWithCodec_NoopSchemaID_NoWireChange(t *testing.T) {
	var capturedInput *sns.PublishInput
	client := &mockSNSClient{
		publishFn: func(_ context.Context, params *sns.PublishInput, _ ...func(*sns.Options)) (*sns.PublishOutput, error) {
			capturedInput = params
			return &sns.PublishOutput{MessageId: aws.String("x")}, nil
		},
	}
	noop := &fakeCodec{
		encodeFn: func(_ context.Context, _ string, payload json.RawMessage) ([]byte, string, error) {
			return payload, "", nil
		},
	}
	pub, err := internalsns.NewWithClient(
		"arn:aws:sns:us-east-1:123:test", client, nil,
		internalsns.WithCodec(noop),
	)
	require.NoError(t, err)

	err = pub.Publish(context.Background(), makeEnv("test.event"))
	require.NoError(t, err)

	require.NotNil(t, capturedInput)
	var wire wireEnvelope
	require.NoError(t, json.Unmarshal([]byte(aws.ToString(capturedInput.Message)), &wire))
	assert.Empty(t, wire.SchemaID)
	assert.True(t, bytesLooksLikeJSONObject(wire.Data))
}

func TestWithCodec_EncodeError_ReturnsErrorBeforeAPICall(t *testing.T) {
	publishCalled := false
	client := &mockSNSClient{
		publishFn: func(_ context.Context, _ *sns.PublishInput, _ ...func(*sns.Options)) (*sns.PublishOutput, error) {
			publishCalled = true
			return &sns.PublishOutput{MessageId: aws.String("x")}, nil
		},
	}
	failing := &fakeCodec{
		encodeFn: func(_ context.Context, _ string, _ json.RawMessage) ([]byte, string, error) {
			return nil, "", fmt.Errorf("registry unavailable")
		},
	}
	pub, err := internalsns.NewWithClient(
		"arn:aws:sns:us-east-1:123:test", client, nil,
		internalsns.WithCodec(failing),
	)
	require.NoError(t, err)

	err = pub.Publish(context.Background(), makeEnv("test.event"))
	require.Error(t, err)
	assert.False(t, publishCalled, "SNS Publish must not be called when codec Encode fails")
}

func TestWithCodec_PublishBatch_PerEntryCodecFailureIsolated(t *testing.T) {
	client := successClient()
	callCount := 0
	flaky := &fakeCodec{
		encodeFn: func(_ context.Context, eventType string, payload json.RawMessage) ([]byte, string, error) {
			callCount++
			if eventType == "batch.bad" {
				return nil, "", fmt.Errorf("encode failed for %s", eventType)
			}
			return reverseBytes(payload), "fake-schema-v1", nil
		},
	}
	pub, err := internalsns.NewWithClient(
		"arn:aws:sns:us-east-1:123:test", client, nil,
		internalsns.WithCodec(flaky),
	)
	require.NoError(t, err)

	envs := []domain.Envelope[json.RawMessage]{
		makeEnv("batch.good"),
		makeEnv("batch.bad"),
	}
	err = pub.PublishBatch(context.Background(), envs)
	require.Error(t, err)

	var batchErr *internalsns.BatchError
	require.ErrorAs(t, err, &batchErr)
	require.Len(t, batchErr.Failures, 1)
	assert.Equal(t, "CodecEncodeError", batchErr.Failures[0].Code)
	assert.Equal(t, 2, callCount)
}

// Optional trace context (baggage, then tracestate) is dropped to fit the
// SNS limit of 10 attributes, so a publish never fails because of the baggage
// the caller's request carried; traceparent and routing attributes are kept.
func TestPublish_OverLimit_DropsOptionalTraceAttributes(t *testing.T) {
	var captured *sns.PublishInput
	client := &mockSNSClient{
		publishFn: func(_ context.Context, params *sns.PublishInput, _ ...func(*sns.Options)) (*sns.PublishOutput, error) {
			captured = params
			return &sns.PublishOutput{MessageId: aws.String("x")}, nil
		},
	}
	logger := &fixtures.MockLogger{}
	// 4 reserved + Subject + 4 extra + traceparent = 10; tracestate and
	// baggage would make 12.
	pub, err := internalsns.NewWithClient("arn:aws:sns:us-east-1:123:test", client, logger,
		internalsns.WithAttributes(map[string]string{"a1": "1", "a2": "2", "a3": "3", "a4": "4"}))
	require.NoError(t, err)

	tp := trace.NewTracerProvider()
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	t.Cleanup(func() { otel.SetTextMapPropagator(propagation.TraceContext{}) })

	ctx := propagation.TraceContext{}.Extract(context.Background(), propagation.MapCarrier{
		"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		"tracestate":  "vendor=value",
	})
	ctx = propagation.Baggage{}.Extract(ctx, propagation.MapCarrier{"baggage": "user=alice"})
	ctx, span := tp.Tracer("test").Start(ctx, "publish")
	defer span.End()

	env := makeEnv("trace.limit")
	env.TenantID, env.Subject = "acme", "users/1"
	require.NoError(t, pub.Publish(ctx, env))
	require.NotNil(t, captured)
	assert.Len(t, captured.MessageAttributes, 10)
	assert.Contains(t, captured.MessageAttributes, "traceparent")
	assert.Contains(t, captured.MessageAttributes, "Subject")
	assert.NotContains(t, captured.MessageAttributes, "baggage")
	assert.NotContains(t, captured.MessageAttributes, "tracestate")
	warned := 0
	for _, e := range logger.Entries() {
		if strings.Contains(e.Message, "dropped optional trace attributes") {
			warned++
		}
	}
	require.NoError(t, pub.Publish(ctx, env))
	assert.Equal(t, 1, warned, "logged once per publisher")
}
