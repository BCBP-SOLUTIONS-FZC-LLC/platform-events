package config_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/config"
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
		RetryBackoff:       2 * time.Second,
		MaxRetryBackoff:    10 * time.Minute,
	}
	cfg := config.RunnerConfigFromEnv(env, nil, nil, nil)
	assert.Equal(t, 2*time.Second, cfg.RetryBackoff)
	assert.Equal(t, 10*time.Minute, cfg.MaxRetryBackoff)
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

func TestSQSConsumerOptions_QueueDepthInterval(t *testing.T) {
	env := config.SQSConfigEnv{Concurrency: 1, VisibilityTimeout: 30 * time.Second, QueueDepthInterval: time.Minute}
	require.Len(t, config.SQSConsumerOptions(env), 3)
}

func TestLoadSQS_QueueDepthInterval(t *testing.T) {
	t.Setenv("SQS_QUEUE_DEPTH_INTERVAL", "")
	assert.Zero(t, config.LoadSQS().QueueDepthInterval, "off by default")
	t.Setenv("SQS_QUEUE_DEPTH_INTERVAL", "45s")
	assert.Equal(t, 45*time.Second, config.LoadSQS().QueueDepthInterval)
	t.Setenv("SQS_QUEUE_DEPTH_INTERVAL", "often")
	cfg := config.LoadSQS()
	assert.Zero(t, cfg.QueueDepthInterval)
	assert.NotEmpty(t, cfg.Warnings)
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

func TestLogWarningsTo_EmptyWarnings_Noop(t *testing.T) {
	// nil logger + empty warnings → no-op, no panic.
	config.LogWarningsTo(nil, nil)
	config.LogWarningsTo(nil, []string{})
}

func TestLogWarningsTo_NilLogger_FallsBackToStderr(t *testing.T) {
	// nil logger falls back to stderr via LogWarnings — must not panic.
	config.LogWarningsTo(nil, []string{"OUTBOX_POLL_INTERVAL: invalid, using default 5s"})
}

func TestLogWarningsTo_WithLogger_EmitsWarn(t *testing.T) {
	// A real logger is used so warnings go through the structured log pipeline.
	// Verify each warning is emitted as a WARN-level log entry.
	logger := &mockWarnLogger{}
	config.LogWarningsTo(logger, []string{"warning one", "warning two"})
	assert.Equal(t, 2, logger.count, "expected two WARN entries, one per warning")
}

// mockWarnLogger captures Warn calls to verify LogWarningsTo routing.
type mockWarnLogger struct {
	count int
}

func (l *mockWarnLogger) Debug(_ string, _ map[string]any) {}
func (l *mockWarnLogger) Info(_ string, _ map[string]any)  {}
func (l *mockWarnLogger) Error(_ string, _ map[string]any) {}
func (l *mockWarnLogger) Warn(_ string, _ map[string]any)  { l.count++ }

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

func TestSQSDrainTimeout_LoadedAndWired(t *testing.T) {
	t.Setenv("SQS_DRAIN_TIMEOUT", "45s")
	env := config.LoadSQS()
	assert.Equal(t, 45*time.Second, env.DrainTimeout)
	base := config.SQSConsumerOptions(config.SQSConfigEnv{Concurrency: 1})
	assert.Len(t, config.SQSConsumerOptions(config.SQSConfigEnv{Concurrency: 1, DrainTimeout: 45 * time.Second}), len(base)+1)

	t.Setenv("SQS_DRAIN_TIMEOUT", "soon")
	env = config.LoadSQS()
	assert.Zero(t, env.DrainTimeout)
	assert.NotEmpty(t, env.Warnings)
}
