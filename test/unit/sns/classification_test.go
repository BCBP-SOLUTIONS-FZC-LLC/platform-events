package sns_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	smithy "github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	internalsns "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/adapter/outbound/sns"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/domain"
)

const classifyTopic = "arn:aws:sns:us-east-1:123:test"

// httpErr builds the error chain the SDK returns for an HTTP error response.
func httpErr(status int, code string) error {
	return &smithy.OperationError{ServiceID: "SNS", OperationName: "PublishBatch", Err: &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{Response: &http.Response{StatusCode: status}},
		Err:      &smithy.GenericAPIError{Code: code, Message: "status " + http.StatusText(status)},
	}}
}

func batchFailures(t *testing.T, err error) []internalsns.BatchFailure {
	t.Helper()
	var be *internalsns.BatchError
	require.True(t, errors.As(err, &be), "expected *BatchError, got %v", err)
	return be.Failures
}

// A request-level failure is "TransportError"/retryable only when transient;
// a permanent AWS error keeps its code and is not retryable, so the outbox
// counts attempts and eventually dead-letters instead of retrying forever.
func TestPublishBatch_RequestErrorClassification(t *testing.T) {
	for _, tc := range []struct {
		name      string
		err       error
		code      string
		retryable bool
	}{
		{"network error", errors.New("dial tcp: connection refused"), "TransportError", true},
		{"deadline", context.DeadlineExceeded, "TransportError", true},
		{"SNS throttled", &mockAPIError{code: "Throttled"}, "TransportError", true},
		{"SNS internal error", &mockAPIError{code: "InternalError"}, "TransportError", true},
		{"KMS throttling", &mockAPIError{code: "KMSThrottling"}, "TransportError", true},
		{"authorization", &mockAPIError{code: "AuthorizationError"}, "AuthorizationError", false},
		{"request too long", &mockAPIError{code: "BatchRequestTooLong"}, "BatchRequestTooLong", false},
		{"topic missing", &mockAPIError{code: "NotFound"}, "NotFound", false},
		{"client validation", smithy.InvalidParamsError{Context: "PublishBatchInput"}, "PublishError", false},
		{"client validation (pointer)", &smithy.InvalidParamsError{Context: "PublishBatchInput"}, "PublishError", false},
		{"serialization", &smithy.SerializationError{Err: errors.New("bad")}, "PublishError", false},
		{"already retryable", &domain.RetryableError{Cause: errors.New("upstream")}, "TransportError", true},
		{"empty-body 502", httpErr(502, "UnknownError"), "TransportError", true},
		{"429 with a permanent-looking code", httpErr(429, "SomethingNew"), "TransportError", true},
		{"empty-body 403 from a proxy", httpErr(403, "UnknownError"), "UnknownError", false},
		{"empty-body 413", httpErr(413, "UnknownError"), "UnknownError", false},
		{"400 InvalidParameter", httpErr(400, "InvalidParameter"), "InvalidParameter", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &mockSNSClient{publishBatchFn: func(context.Context, *sns.PublishBatchInput, ...func(*sns.Options)) (*sns.PublishBatchOutput, error) {
				return nil, tc.err
			}}
			pub, err := internalsns.NewWithClient(classifyTopic, client, nil)
			require.NoError(t, err)
			fs := batchFailures(t, pub.PublishBatch(context.Background(), []domain.Envelope[json.RawMessage]{makeEnv("a.b.c"), makeEnv("a.b.c")}))
			require.Len(t, fs, 2)
			for _, f := range fs {
				assert.Equal(t, tc.code, f.Code)
				assert.Equal(t, tc.retryable, f.Retryable)
			}
		})
	}
}

// Per-entry failures: SNS-side faults and throttle codes are retryable,
// sender faults are not.
func TestPublishBatch_EntryFailureClassification(t *testing.T) {
	envs := []domain.Envelope[json.RawMessage]{makeEnv("a.b.c"), makeEnv("a.b.c"), makeEnv("a.b.c"), makeEnv("a.b.c")}
	client := &mockSNSClient{publishBatchFn: func(_ context.Context, in *sns.PublishBatchInput, _ ...func(*sns.Options)) (*sns.PublishBatchOutput, error) {
		e := in.PublishBatchRequestEntries
		return &sns.PublishBatchOutput{
			Successful: []snstypes.PublishBatchResultEntry{{Id: e[3].Id, MessageId: aws.String("m")}},
			Failed: []snstypes.BatchResultErrorEntry{
				{Id: e[0].Id, Code: aws.String("InvalidParameter"), SenderFault: true},
				{Id: e[1].Id, Code: aws.String("Throttled"), SenderFault: true},
				{Id: e[2].Id, Code: aws.String("InternalError"), SenderFault: false},
			},
		}, nil
	}}
	pub, err := internalsns.NewWithClient(classifyTopic, client, nil)
	require.NoError(t, err)
	byID := map[string]internalsns.BatchFailure{}
	for _, f := range batchFailures(t, pub.PublishBatch(context.Background(), envs)) {
		byID[f.ID] = f
	}
	require.Len(t, byID, 3)
	assert.False(t, byID[envs[0].ID].Retryable, "sender fault")
	assert.True(t, byID[envs[1].ID].Retryable, "throttle code")
	assert.True(t, byID[envs[2].ID].Retryable, "SNS-side fault")
	assert.Equal(t, "InvalidParameter", byID[envs[0].ID].Code)
}

// A chunk over SNS's 256 KiB request limit is sent in several requests so a
// large event does not fail its neighbours with BatchRequestTooLong.
func TestPublishBatch_SplitsChunkByRequestSize(t *testing.T) {
	var mu sync.Mutex
	var sizes []int
	client := &mockSNSClient{publishBatchFn: func(_ context.Context, in *sns.PublishBatchInput, _ ...func(*sns.Options)) (*sns.PublishBatchOutput, error) {
		total := 0
		var ok []snstypes.PublishBatchResultEntry
		for _, e := range in.PublishBatchRequestEntries {
			total += len(aws.ToString(e.Message))
			ok = append(ok, snstypes.PublishBatchResultEntry{Id: e.Id, MessageId: aws.String("m")})
		}
		mu.Lock()
		sizes = append(sizes, len(in.PublishBatchRequestEntries))
		mu.Unlock()
		if total > 256*1024 {
			return nil, &mockAPIError{code: "BatchRequestTooLong"}
		}
		return &sns.PublishBatchOutput{Successful: ok}, nil
	}}
	pub, err := internalsns.NewWithClient(classifyTopic, client, nil)
	require.NoError(t, err)

	big := func() domain.Envelope[json.RawMessage] {
		env := makeEnv("a.b.big")
		env.Payload = json.RawMessage(`{"blob":"` + strings.Repeat("x", 120*1024) + `"}`)
		return env
	}
	envs := []domain.Envelope[json.RawMessage]{big(), big(), big(), makeEnv("a.b.small")}
	require.NoError(t, pub.PublishBatch(context.Background(), envs))
	assert.Equal(t, []int{2, 2}, sizes, "120 KiB ×2 fits; the third starts a new request")
}

// A codec that wraps ErrRetryable (registry outage / throttling) yields a
// retryable batch failure, like the single-Publish path; any other encode
// error stays permanent.
func TestPublishBatch_CodecErrorClassification(t *testing.T) {
	for _, tc := range []struct {
		name      string
		err       error
		retryable bool
	}{
		{"registry throttled", &domain.RetryableError{Cause: errors.New("glue: ThrottlingException")}, true},
		{"schema mismatch", errors.New("glue: schema incompatible"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &mockSNSClient{publishBatchFn: func(context.Context, *sns.PublishBatchInput, ...func(*sns.Options)) (*sns.PublishBatchOutput, error) {
				t.Fatal("no SNS call when every entry failed to encode")
				return nil, nil
			}}
			codec := &fakeCodec{encodeFn: func(context.Context, string, json.RawMessage) ([]byte, string, error) {
				return nil, "", tc.err
			}}
			pub, err := internalsns.NewWithClient(classifyTopic, client, nil, internalsns.WithCodec(codec))
			require.NoError(t, err)
			fs := batchFailures(t, pub.PublishBatch(context.Background(), []domain.Envelope[json.RawMessage]{makeEnv("a.b.c")}))
			require.Len(t, fs, 1)
			assert.Equal(t, "CodecEncodeError", fs[0].Code)
			assert.Equal(t, tc.retryable, fs[0].Retryable)
		})
	}
}
