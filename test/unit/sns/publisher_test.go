package sns_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	internalsns "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/adapter/outbound/sns"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/test/fixtures"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
	return domain.NewEnvelope(eventType, "test-svc", json.RawMessage(`{"x":1}`))
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
	assert.Contains(t, err.Error(), "network error")
	// Should NOT be a BatchError.
	var be *internalsns.BatchError
	assert.False(t, errors.As(err, &be))
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
	_ = pub.Publish(context.Background(), env)

	require.NotNil(t, capturedInput)
	attrs := capturedInput.MessageAttributes
	assert.Equal(t, "user.created", aws.ToString(attrs["EventType"].StringValue))
	assert.Equal(t, "tenant1", aws.ToString(attrs["TenantID"].StringValue))
	assert.Equal(t, "iam-svc", aws.ToString(attrs["Source"].StringValue))
	assert.Equal(t, env.ID, aws.ToString(attrs["EventID"].StringValue))
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
			"EventType":    "override-attempt", // reserved — should be ignored with a warn log
			"CustomField":  "allowed",
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
