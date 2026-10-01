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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/adapter/outbound/metrics"
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

	emu := fixtures.StartFloci(ctx, t)
	dlqURL := emu.CreateQueue(ctx, t, "dlq-it-orders-dlq")
	sourceURL := emu.CreateQueue(ctx, t, "dlq-it-orders")
	bareURL := emu.CreateQueue(ctx, t, "dlq-it-no-redrive")

	dlqAttrs, err := emu.SQSClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(dlqURL),
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	require.NoError(t, err)
	_, err = emu.SQSClient.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{
		QueueUrl: aws.String(sourceURL),
		Attributes: map[string]string{
			"RedrivePolicy": `{"deadLetterTargetArn":"` + dlqAttrs.Attributes["QueueArn"] + `","maxReceiveCount":"3"}`,
		},
	})
	require.NoError(t, err)

	pub, err := events.NewSQSDLQPublisher(events.DLQConfig{
		Region:       "us-east-1",
		EndpointURL:  emu.EndpointURL,
		ConsumerName: "it-consumer",
	})
	require.NoError(t, err)

	resolved, err := pub.ResolveDLQ(ctx, sourceURL)
	require.NoError(t, err)
	assert.Equal(t, dlqURL, resolved)

	_, err = pub.ResolveDLQ(ctx, bareURL)
	require.ErrorIs(t, err, events.ErrDLQNotConfigured)

	// DLQ forwarding on a queue without a RedrivePolicy fails at Start rather
	// than leaving poison messages redelivered until retention expires.
	bareConsumer, err := events.NewSQSConsumer(events.SQSConfig{QueueURL: bareURL, Region: "us-east-1", EndpointURL: emu.EndpointURL, WaitSeconds: 1},
		func(context.Context, events.Envelope[json.RawMessage]) error { return nil }, events.WithDLQForwarding(pub))
	require.NoError(t, err)
	require.ErrorIs(t, bareConsumer.Start(ctx), events.ErrDLQNotConfigured)

	env := events.NewEnvelope("order.created", "orders-svc", json.RawMessage(`{"order_id":"o-1"}`))
	body, err := json.Marshal(env)
	require.NoError(t, err)
	require.NoError(t, pub.SendToDLQ(ctx, sourceURL, body, map[string]string{"TenantID": "acme"}, "unprocessable"))

	out, err := emu.SQSClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
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

// TestConsumer_WithDLQForwarding_MalformedMessage verifies the end-to-end
// consumer path: a body that is not an envelope is forwarded verbatim, with
// its message attributes, to the source queue's RedrivePolicy DLQ and then
// deleted from the source queue.
func TestConsumer_WithDLQForwarding_MalformedMessage(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	emu := fixtures.StartFloci(ctx, t)
	dlqURL := emu.CreateQueue(ctx, t, "dlqfwd-it-dlq")
	sourceURL := emu.CreateQueue(ctx, t, "dlqfwd-it-source")
	dlqAttrs, err := emu.SQSClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(dlqURL),
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	require.NoError(t, err)
	_, err = emu.SQSClient.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{
		QueueUrl: aws.String(sourceURL),
		Attributes: map[string]string{
			"RedrivePolicy": `{"deadLetterTargetArn":"` + dlqAttrs.Attributes["QueueArn"] + `","maxReceiveCount":"10"}`,
		},
	})
	require.NoError(t, err)

	_, err = emu.SQSClient.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(sourceURL),
		MessageBody: aws.String("definitely not an envelope"),
		MessageAttributes: map[string]sqstypes.MessageAttributeValue{
			"TenantID": {DataType: aws.String("String"), StringValue: aws.String("acme")},
		},
	})
	require.NoError(t, err)

	pub, err := events.NewSQSDLQPublisher(events.DLQConfig{Region: "us-east-1", EndpointURL: emu.EndpointURL, ConsumerName: "it-consumer"})
	require.NoError(t, err)
	consumer, err := events.NewSQSConsumer(events.SQSConfig{QueueURL: sourceURL, Region: "us-east-1", EndpointURL: emu.EndpointURL, WaitSeconds: 1},
		func(context.Context, events.Envelope[json.RawMessage]) error { return nil },
		events.WithDLQForwarding(pub), events.WithDrainTimeout(5*time.Second))
	require.NoError(t, err)
	consumerCtx, stopConsumer := context.WithCancel(ctx)
	go func() { _ = consumer.Start(consumerCtx) }()
	defer func() { stopConsumer(); _ = consumer.Stop() }()

	var msg sqstypes.Message
	require.Eventually(t, func() bool {
		out, err := emu.SQSClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:              aws.String(dlqURL),
			MessageAttributeNames: []string{"All"},
			WaitTimeSeconds:       1,
		})
		if err != nil || len(out.Messages) == 0 {
			return false
		}
		msg = out.Messages[0]
		return true
	}, 30*time.Second, 100*time.Millisecond, "malformed message never reached the DLQ")

	get := func(k string) string { return aws.ToString(msg.MessageAttributes[k].StringValue) }
	assert.Equal(t, "definitely not an envelope", aws.ToString(msg.Body))
	assert.Equal(t, "acme", get("TenantID"))
	assert.Equal(t, "unknown", get(events.DLQAttrEventType))
	assert.Contains(t, get(events.DLQAttrReason), "malformed message body")
	assert.Equal(t, sourceURL, get(events.DLQAttrOriginalQueue))

	// The original was deleted after the forward: nothing left in flight or visible.
	require.Eventually(t, func() bool {
		attrs, err := emu.SQSClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
			QueueUrl: aws.String(sourceURL),
			AttributeNames: []sqstypes.QueueAttributeName{
				sqstypes.QueueAttributeNameApproximateNumberOfMessages,
				sqstypes.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
			},
		})
		return err == nil && attrs.Attributes["ApproximateNumberOfMessages"] == "0" && attrs.Attributes["ApproximateNumberOfMessagesNotVisible"] == "0"
	}, 10*time.Second, 200*time.Millisecond)
}

// TestConsumer_QueueDepthMetrics verifies the depth sampler against a real SQS
// API: the source queue's backlog and that of its RedrivePolicy DLQ (whose URL
// is derived from the policy ARN) land in platform_queue_depth /
// platform_dlq_depth.
func TestConsumer_QueueDepthMetrics(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	emu := fixtures.StartFloci(ctx, t)
	dlqURL := emu.CreateQueue(ctx, t, "depth-it-dlq")
	sourceURL := emu.CreateQueue(ctx, t, "depth-it-source")
	dlqAttrs, err := emu.SQSClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(dlqURL),
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	require.NoError(t, err)
	_, err = emu.SQSClient.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{
		QueueUrl:   aws.String(sourceURL),
		Attributes: map[string]string{"RedrivePolicy": `{"deadLetterTargetArn":"` + dlqAttrs.Attributes["QueueArn"] + `","maxReceiveCount":"5"}`},
	})
	require.NoError(t, err)
	for range 2 {
		_, err = emu.SQSClient.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: aws.String(dlqURL), MessageBody: aws.String("dead")})
		require.NoError(t, err)
	}

	prev := metrics.CurrentPlatform()
	t.Cleanup(func() { metrics.ReplacePlatform(prev) })
	reg := prometheus.NewRegistry()
	_, err = events.InitMetrics(events.MetricsIdentity{Domain: "iam", Service: "it", Environment: "test"}, reg)
	require.NoError(t, err)

	consumer, err := events.NewSQSConsumer(events.SQSConfig{QueueURL: sourceURL, Region: "us-east-1", EndpointURL: emu.EndpointURL, WaitSeconds: 1},
		func(context.Context, events.Envelope[json.RawMessage]) error { return nil },
		events.WithQueueDepthMetrics(10*time.Second), events.WithDrainTimeout(2*time.Second))
	require.NoError(t, err)
	consumerCtx, stopConsumer := context.WithCancel(ctx)
	go func() { _ = consumer.Start(consumerCtx) }()
	defer func() { stopConsumer(); _ = consumer.Stop() }()

	p := metrics.CurrentPlatform()
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(p.DLQDepth.WithLabelValues("depth-it-source")) == 2
	}, 30*time.Second, 200*time.Millisecond, "DLQ depth sampled under the source queue's name")
	assert.Zero(t, testutil.ToFloat64(p.QueueDepth.WithLabelValues("depth-it-source")))
}
