//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/events"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/test/fixtures"
)

func TestDLQPublisher_ForwardsToRedriveTarget(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	ls := fixtures.StartLocalStack(ctx, t)
	dlqURL := ls.CreateQueue(ctx, t, "dlq-it-orders-dlq")
	sourceURL := ls.CreateQueue(ctx, t, "dlq-it-orders")
	bareURL := ls.CreateQueue(ctx, t, "dlq-it-no-redrive")

	dlqAttrs, err := ls.SQSClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(dlqURL),
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	require.NoError(t, err)
	_, err = ls.SQSClient.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{
		QueueUrl: aws.String(sourceURL),
		Attributes: map[string]string{
			"RedrivePolicy": `{"deadLetterTargetArn":"` + dlqAttrs.Attributes["QueueArn"] + `","maxReceiveCount":"3"}`,
		},
	})
	require.NoError(t, err)

	pub, err := events.NewSQSDLQPublisher(events.DLQConfig{
		Region:       "us-east-1",
		EndpointURL:  ls.EndpointURL,
		ConsumerName: "it-consumer",
	})
	require.NoError(t, err)

	resolved, err := pub.ResolveDLQ(ctx, sourceURL)
	require.NoError(t, err)
	assert.Equal(t, dlqURL, resolved)

	_, err = pub.ResolveDLQ(ctx, bareURL)
	require.ErrorIs(t, err, events.ErrDLQNotConfigured)

	env := events.NewEnvelope("order.created", "orders-svc", json.RawMessage(`{"order_id":"o-1"}`))
	body, err := json.Marshal(env)
	require.NoError(t, err)
	require.NoError(t, pub.SendToDLQ(ctx, sourceURL, body, map[string]string{"TenantID": "acme"}, "unprocessable"))

	out, err := ls.SQSClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(dlqURL),
		MessageAttributeNames: []string{"All"},
		WaitTimeSeconds:       5,
	})
	require.NoError(t, err)
	require.Len(t, out.Messages, 1)
	msg := out.Messages[0]
	assert.Equal(t, string(body), aws.ToString(msg.Body))
	get := func(k string) string { return aws.ToString(msg.MessageAttributes[k].StringValue) }
	assert.Equal(t, "order.created", get(events.DLQAttrEventType))
	assert.Equal(t, "unprocessable", get(events.DLQAttrReason))
	assert.Equal(t, sourceURL, get(events.DLQAttrOriginalQueue))
	assert.Equal(t, "it-consumer", get(events.DLQAttrConsumerName))
	assert.NotEmpty(t, get(events.DLQAttrFailedAt))
	assert.Equal(t, "acme", get("TenantID"))
}
