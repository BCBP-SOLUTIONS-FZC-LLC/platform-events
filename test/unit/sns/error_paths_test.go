package sns_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	internalsns "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/adapter/outbound/sns"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/test/fixtures"
)

const errorPathTopic = "arn:aws:sns:us-east-1:123:errors"

func badPayloadEnv() domain.Envelope[json.RawMessage] {
	env := makeEnv("bad.payload")
	env.Payload = json.RawMessage(`{not json`) // json.Marshal of the envelope fails
	return env
}

// A payload that is not valid JSON fails before any SNS call.
func TestPublish_InvalidPayload_FailsBeforeSNS(t *testing.T) {
	calls := 0
	client := &mockSNSClient{publishFn: func(context.Context, *sns.PublishInput, ...func(*sns.Options)) (*sns.PublishOutput, error) {
		calls++
		return &sns.PublishOutput{}, nil
	}}
	pub, err := internalsns.NewWithClient(errorPathTopic, client, nil)
	require.NoError(t, err)
	require.Error(t, pub.Publish(context.Background(), badPayloadEnv()))
	assert.Zero(t, calls)
}

// In a batch, an envelope that cannot be marshalled is reported as a
// MarshalError failure while the rest of the batch is still published.
func TestPublishBatch_InvalidPayload_ReportedWhileOthersPublish(t *testing.T) {
	var sent []string
	client := &mockSNSClient{publishBatchFn: func(_ context.Context, in *sns.PublishBatchInput, _ ...func(*sns.Options)) (*sns.PublishBatchOutput, error) {
		out := &sns.PublishBatchOutput{}
		for _, e := range in.PublishBatchRequestEntries {
			sent = append(sent, aws.ToString(e.Id))
			out.Successful = append(out.Successful, snstypes.PublishBatchResultEntry{Id: e.Id, MessageId: aws.String("m")})
		}
		return out, nil
	}}
	pub, err := internalsns.NewWithClient(errorPathTopic, client, nil)
	require.NoError(t, err)

	good, bad := makeEnv("good.event"), badPayloadEnv()
	err = pub.PublishBatch(context.Background(), []domain.Envelope[json.RawMessage]{good, bad})
	var batchErr *internalsns.BatchError
	require.ErrorAs(t, err, &batchErr)
	require.Len(t, batchErr.Failures, 1)
	assert.Equal(t, bad.ID, batchErr.Failures[0].ID)
	assert.Equal(t, "MarshalError", batchErr.Failures[0].Code)
	assert.Len(t, sent, 1, "the valid envelope is still published")
}

// A codec failure is logged with the topic and event type, and returned.
func TestPublish_CodecFailure_Logged(t *testing.T) {
	codec := &fakeCodec{encodeFn: func(context.Context, string, json.RawMessage) ([]byte, string, error) {
		return nil, "", errors.New("registry down")
	}}
	logger := &fixtures.MockLogger{}
	pub, err := internalsns.NewWithClient(errorPathTopic, &mockSNSClient{}, logger, internalsns.WithCodec(codec))
	require.NoError(t, err)

	err = pub.Publish(context.Background(), makeEnv("codec.fails"))
	require.ErrorContains(t, err, "registry down")
	var logged bool
	for _, e := range logger.Entries() {
		if e.Level == "ERROR" && e.Message == "sns: codec encode failed" {
			logged = e.Fields["topic"] == errorPathTopic && e.Fields["event_type"] == "codec.fails"
		}
	}
	assert.True(t, logged)
}
