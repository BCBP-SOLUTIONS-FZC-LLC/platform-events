package config_test

import (
	"testing"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/config"
	"github.com/stretchr/testify/assert"
)

func TestLoadSQS_Defaults(t *testing.T) {
	t.Setenv("SQS_QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/123/test")
	t.Setenv("SQS_MAX_MESSAGES", "")
	t.Setenv("SQS_WAIT_SECONDS", "")
	t.Setenv("SQS_VISIBILITY_TIMEOUT", "")
	t.Setenv("SQS_CONCURRENCY", "")
	t.Setenv("AWS_REGION", "")

	cfg := config.LoadSQS()

	assert.Equal(t, "https://sqs.us-east-1.amazonaws.com/123/test", cfg.QueueURL)
	assert.Equal(t, "us-east-1", cfg.Region)
	assert.Equal(t, int32(10), cfg.MaxMessages)
	assert.Equal(t, int32(20), cfg.WaitSeconds)
	assert.Equal(t, 30*time.Second, cfg.VisibilityTimeout)
	assert.Equal(t, 1, cfg.Concurrency)
}

func TestLoadSQS_CustomValues(t *testing.T) {
	t.Setenv("SQS_QUEUE_URL", "https://sqs.us-west-2.amazonaws.com/123/queue")
	t.Setenv("SQS_MAX_MESSAGES", "5")
	t.Setenv("SQS_WAIT_SECONDS", "10")
	t.Setenv("SQS_VISIBILITY_TIMEOUT", "60s")
	t.Setenv("SQS_CONCURRENCY", "4")
	t.Setenv("AWS_REGION", "us-west-2")

	cfg := config.LoadSQS()

	assert.Equal(t, "us-west-2", cfg.Region)
	assert.Equal(t, int32(5), cfg.MaxMessages)
	assert.Equal(t, int32(10), cfg.WaitSeconds)
	assert.Equal(t, 60*time.Second, cfg.VisibilityTimeout)
	assert.Equal(t, 4, cfg.Concurrency)
}

func TestLoadOutbox_Defaults(t *testing.T) {
	t.Setenv("OUTBOX_POLL_INTERVAL", "")
	t.Setenv("OUTBOX_BATCH_SIZE", "")
	t.Setenv("OUTBOX_MAX_ATTEMPTS", "")

	cfg := config.LoadOutbox()

	assert.Equal(t, 5*time.Second, cfg.PollInterval)
	assert.Equal(t, 50, cfg.BatchSize)
	assert.Equal(t, 5, cfg.MaxAttempts)
}

func TestLoadOTel_DevMode(t *testing.T) {
	t.Setenv("APP_ENV", "dev")
	t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "")

	cfg := config.LoadOTel()
	assert.True(t, cfg.Insecure)
}

func TestLoadOTel_ProductionMode(t *testing.T) {
	t.Setenv("APP_ENV", "production")
	t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "")

	cfg := config.LoadOTel()
	assert.False(t, cfg.Insecure)
}

func TestLoadOTel_ExplicitInsecure(t *testing.T) {
	t.Setenv("APP_ENV", "production")
	t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "true")

	cfg := config.LoadOTel()
	assert.True(t, cfg.Insecure)
}

func TestLoadOTel_InsecureFalse(t *testing.T) {
	t.Setenv("APP_ENV", "production")
	t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "false")

	cfg := config.LoadOTel()
	assert.False(t, cfg.Insecure)
}

func TestLoadOTel_ExporterEndpointDefault(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")

	cfg := config.LoadOTel()
	assert.Equal(t, "localhost:4317", cfg.ExporterEndpoint)
}

func TestLoadOTel_ExporterEndpointCustom(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "otel-collector:4317")

	cfg := config.LoadOTel()
	assert.Equal(t, "otel-collector:4317", cfg.ExporterEndpoint)
}

func TestLoadOTel_LocalEnv(t *testing.T) {
	t.Setenv("APP_ENV", "local")
	t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "")

	cfg := config.LoadOTel()
	assert.True(t, cfg.Insecure)
}

func TestLoadSNS_Defaults(t *testing.T) {
	t.Setenv("SNS_TOPIC_ARN", "")
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_ENDPOINT_URL", "")

	cfg := config.LoadSNS()

	assert.Equal(t, "", cfg.TopicARN)
	assert.Equal(t, "us-east-1", cfg.Region)
	assert.Equal(t, "", cfg.EndpointURL)
}

func TestLoadSNS_WithTopicARN(t *testing.T) {
	t.Setenv("SNS_TOPIC_ARN", "arn:aws:sns:us-east-1:123456789:my-topic")
	t.Setenv("AWS_REGION", "eu-west-1")
	t.Setenv("AWS_ENDPOINT_URL", "http://localhost:4566")

	cfg := config.LoadSNS()

	assert.Equal(t, "arn:aws:sns:us-east-1:123456789:my-topic", cfg.TopicARN)
	assert.Equal(t, "eu-west-1", cfg.Region)
	assert.Equal(t, "http://localhost:4566", cfg.EndpointURL)
}

// ----------------------------
// Warning paths: invalid env var values fall back to defaults
// ----------------------------

func TestEnvIntOrDefault_InvalidValue_FallsBackToDefault(t *testing.T) {
	t.Setenv("SQS_MAX_MESSAGES", "not-a-number")
	cfg := config.LoadSQS()
	assert.Equal(t, int32(10), cfg.MaxMessages, "invalid int should fall back to default")
}

func TestEnvDurationOrDefault_InvalidValue_FallsBackToDefault(t *testing.T) {
	t.Setenv("OUTBOX_POLL_INTERVAL", "not-a-duration")
	cfg := config.LoadOutbox()
	assert.Equal(t, 5*time.Second, cfg.PollInterval, "invalid duration should fall back to default")
}

func TestLoadOTel_InvalidInsecureValue_FallsBackToEnvDefault(t *testing.T) {
	t.Setenv("APP_ENV", "production")
	t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "yes") // unrecognised — not true/false/1/0
	cfg := config.LoadOTel()
	// Falls through to the default: APP_ENV=production → insecure=false
	assert.False(t, cfg.Insecure)
}

func TestLoadOTel_InsecureOne(t *testing.T) {
	t.Setenv("APP_ENV", "production")
	t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "1")
	cfg := config.LoadOTel()
	assert.True(t, cfg.Insecure)
}

func TestLoadOTel_InsecureFalseZero(t *testing.T) {
	t.Setenv("APP_ENV", "dev")
	t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "0")
	cfg := config.LoadOTel()
	assert.False(t, cfg.Insecure)
}

func TestLoadOutbox_CustomValues(t *testing.T) {
	t.Setenv("OUTBOX_POLL_INTERVAL", "10s")
	t.Setenv("OUTBOX_BATCH_SIZE", "100")
	t.Setenv("OUTBOX_MAX_ATTEMPTS", "10")
	t.Setenv("DATABASE_URL", "postgres://localhost/test")

	cfg := config.LoadOutbox()

	assert.Equal(t, 10*time.Second, cfg.PollInterval)
	assert.Equal(t, 100, cfg.BatchSize)
	assert.Equal(t, 10, cfg.MaxAttempts)
	assert.Equal(t, "postgres://localhost/test", cfg.DatabaseURL)
}
