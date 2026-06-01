//go:build smoke

// Package smoke contains tests that run against live AWS resources.
// They require SMOKE_SNS_TOPIC_ARN and SMOKE_SQS_QUEUE_URL to be set.
package smoke_test

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/events"
)

func TestSmokePublish(t *testing.T) {
	topicARN := os.Getenv("SMOKE_SNS_TOPIC_ARN")
	if topicARN == "" {
		t.Skip("SMOKE_SNS_TOPIC_ARN not set — skipping smoke test")
	}

	pub, err := events.NewSNSPublisher(events.SNSConfig{
		TopicARN: topicARN,
		Region:   "us-east-1",
	})
	require.NoError(t, err)

	env := events.NewEnvelope("smoke.test", "platform-events", json.RawMessage(`{"smoke":true}`))
	err = pub.Publish(context.Background(), env)
	require.NoError(t, err)
}
