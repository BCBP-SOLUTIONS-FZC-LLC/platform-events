package config_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/config"
)

func TestRunnerConfigFromEnv_MapsFields(t *testing.T) {
	env := config.OutboxConfigEnv{
		PollInterval:       2 * time.Second,
		BatchSize:          25,
		MaxAttempts:        3,
		ClaimLeaseDuration: 5 * time.Minute,
		StartupJitter:      time.Second,
		PublishConcurrency: 1,
		PublishTimeout:     15 * time.Second,
		DrainTimeout:       45 * time.Second,
	}
	cfg := config.RunnerConfigFromEnv(env, nil, nil, nil)
	assert.Equal(t, 2*time.Second, cfg.PollInterval)
	assert.Equal(t, 25, cfg.BatchSize)
	assert.Equal(t, 3, cfg.MaxAttempts)
	assert.Equal(t, 1, cfg.PublishConcurrency)
}

func TestSQSConsumerOptions_MaxReceiveCount(t *testing.T) {
	env := config.SQSConfigEnv{
		Concurrency:       3,
		VisibilityTimeout: 60 * time.Second,
		MaxReceiveCount:   7,
	}
	opts := config.SQSConsumerOptions(env)
	require.Len(t, opts, 3)
}

func TestSQSConsumerOptions_OmitsMaxReceiveWhenZero(t *testing.T) {
	env := config.SQSConfigEnv{Concurrency: 1, VisibilityTimeout: 30 * time.Second}
	opts := config.SQSConsumerOptions(env)
	require.Len(t, opts, 2)
}

func TestSNSConfigFromEnv(t *testing.T) {
	env := config.SNSConfigEnv{TopicARN: "arn:aws:sns:us-east-1:123:topic", Region: "eu-west-1"}
	cfg := config.SNSConfigFromEnv(env, nil)
	assert.Equal(t, env.TopicARN, cfg.TopicARN)
	assert.Equal(t, env.Region, cfg.Region)
}

func TestLogWarnings_NoPanicOnEmpty(t *testing.T) {
	config.LogWarnings(nil)
	config.LogWarnings([]string{})
}

func TestLogWarnings_WithWarnings(t *testing.T) {
	// Writes to stderr — just verify it doesn't panic with a populated slice.
	config.LogWarnings([]string{
		"SQS_MAX_MESSAGES: invalid value 'abc', using default 10",
		"SQS_CONCURRENCY: invalid value 'xyz', using default 1",
	})
}

func TestSQSConfigFromEnv_MapsFields(t *testing.T) {
	env := config.SQSConfigEnv{
		QueueURL:    "https://sqs.us-east-1.amazonaws.com/123/queue",
		Region:      "eu-west-1",
		EndpointURL: "http://localhost:4566",
		MaxMessages: 5,
		WaitSeconds: 10,
	}
	cfg := config.SQSConfigFromEnv(env, nil)
	assert.Equal(t, env.QueueURL, cfg.QueueURL)
	assert.Equal(t, env.Region, cfg.Region)
	assert.Equal(t, env.EndpointURL, cfg.EndpointURL)
	assert.Equal(t, env.MaxMessages, cfg.MaxMessages)
	assert.Equal(t, env.WaitSeconds, cfg.WaitSeconds)
	assert.Nil(t, cfg.Logger)
}
