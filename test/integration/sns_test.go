//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/events"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/test/fixtures"
)

func TestSNSPublishRoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	// Set dummy credentials so AWS SDK doesn't try to fetch from IMDS.
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")

	ctx := context.Background()
	ls := fixtures.StartLocalStack(ctx, t)

	topicARN := ls.CreateTopic(ctx, t, "test-topic")
	queueURL := ls.CreateQueue(ctx, t, "test-queue")
	ls.SubscribeQueueToTopic(ctx, t, topicARN, queueURL)

	pub, err := events.NewSNSPublisher(events.SNSConfig{
		TopicARN:    topicARN,
		Region:      "us-east-1",
		EndpointURL: ls.EndpointURL,
	})
	require.NoError(t, err)

	env := events.NewEnvelope("user.created", "platform-iam", json.RawMessage(`{"name":"Alice"}`),
		events.WithTenantID("acme"),
	)

	err = pub.Publish(ctx, env)
	require.NoError(t, err)

	// Receive from the subscribed SQS queue.
	out, err := ls.SQSClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(queueURL),
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     5,
	})
	require.NoError(t, err)
	require.Len(t, out.Messages, 1)

	// RawMessageDelivery is enabled on the subscription, so the SQS body is
	// the envelope JSON directly (not wrapped in an SNS notification envelope).
	received, err := events.ParseEnvelope[json.RawMessage]([]byte(aws.ToString(out.Messages[0].Body)))
	require.NoError(t, err)

	assert.Equal(t, env.ID, received.ID)
	assert.Equal(t, env.Type, received.Type)
	assert.Equal(t, "acme", received.TenantID)
}
