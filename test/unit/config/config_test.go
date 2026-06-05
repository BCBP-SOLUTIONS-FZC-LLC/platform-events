package config_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/config"
)

// ---------------------------------------------------------------------------
// LoadSNS
// ---------------------------------------------------------------------------

func TestLoadSNS_Defaults(t *testing.T) {
	t.Setenv("SNS_TOPIC_ARN", "arn:aws:sns:us-east-1:123:topic")
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_ENDPOINT_URL", "")

	cfg := config.LoadSNS()
	assert.Equal(t, "arn:aws:sns:us-east-1:123:topic", cfg.TopicARN)
	assert.Equal(t, "us-east-1", cfg.Region)
	assert.Equal(t, "", cfg.EndpointURL)
}

func TestLoadSNS_CustomRegionAndEndpoint(t *testing.T) {
	t.Setenv("SNS_TOPIC_ARN", "arn:aws:sns:eu-west-1:456:topic")
	t.Setenv("AWS_REGION", "eu-west-1")
	t.Setenv("AWS_ENDPOINT_URL", "http://localhost:4566")

	cfg := config.LoadSNS()
	assert.Equal(t, "eu-west-1", cfg.Region)
	assert.Equal(t, "http://localhost:4566", cfg.EndpointURL)
}

// ---------------------------------------------------------------------------
// SNSConfigEnv.Validate
// ---------------------------------------------------------------------------

func TestSNSConfigEnv_Validate_MissingTopicARN(t *testing.T) {
	cfg := config.SNSConfigEnv{}
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "SNS_TOPIC_ARN")
}

func TestSNSConfigEnv_Validate_OK(t *testing.T) {
	cfg := config.SNSConfigEnv{TopicARN: "arn:aws:sns:us-east-1:123:topic"}
	assert.NoError(t, cfg.Validate())
}

// ---------------------------------------------------------------------------
// LoadSQS
// ---------------------------------------------------------------------------

func TestLoadSQS_Defaults(t *testing.T) {
	t.Setenv("SQS_QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/123/q")
	t.Setenv("SQS_MAX_MESSAGES", "")
	t.Setenv("SQS_WAIT_SECONDS", "")
	t.Setenv("SQS_VISIBILITY_TIMEOUT", "")
	t.Setenv("SQS_CONCURRENCY", "")
	t.Setenv("SQS_MAX_RECEIVE_COUNT", "")
	t.Setenv("AWS_REGION", "")

	cfg := config.LoadSQS()
	assert.Equal(t, int32(10), cfg.MaxMessages)
	assert.Equal(t, int32(20), cfg.WaitSeconds)
	assert.Equal(t, 30*time.Second, cfg.VisibilityTimeout)
	assert.Equal(t, 1, cfg.Concurrency)
	assert.Equal(t, 0, cfg.MaxReceiveCount)
	assert.Equal(t, "us-east-1", cfg.Region)
	assert.Empty(t, cfg.Warnings)
}

func TestLoadSQS_InvalidIntProducesWarning(t *testing.T) {
	t.Setenv("SQS_QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/123/q")
	t.Setenv("SQS_MAX_MESSAGES", "notanint")
	t.Setenv("SQS_WAIT_SECONDS", "")
	t.Setenv("SQS_VISIBILITY_TIMEOUT", "")
	t.Setenv("SQS_CONCURRENCY", "")
	t.Setenv("SQS_MAX_RECEIVE_COUNT", "")

	cfg := config.LoadSQS()
	assert.Equal(t, int32(10), cfg.MaxMessages, "should fall back to default")
	require.NotEmpty(t, cfg.Warnings)
	assert.Contains(t, cfg.Warnings[0], "SQS_MAX_MESSAGES")
}

func TestLoadSQS_InvalidDurationProducesWarning(t *testing.T) {
	t.Setenv("SQS_QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/123/q")
	t.Setenv("SQS_MAX_MESSAGES", "")
	t.Setenv("SQS_WAIT_SECONDS", "")
	t.Setenv("SQS_VISIBILITY_TIMEOUT", "notaduration")
	t.Setenv("SQS_CONCURRENCY", "")
	t.Setenv("SQS_MAX_RECEIVE_COUNT", "")

	cfg := config.LoadSQS()
	assert.Equal(t, 30*time.Second, cfg.VisibilityTimeout, "should fall back to default")
	require.NotEmpty(t, cfg.Warnings)
	assert.Contains(t, cfg.Warnings[0], "SQS_VISIBILITY_TIMEOUT")
}

func TestLoadSQS_CustomValues(t *testing.T) {
	t.Setenv("SQS_QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/123/q")
	t.Setenv("SQS_MAX_MESSAGES", "5")
	t.Setenv("SQS_WAIT_SECONDS", "10")
	t.Setenv("SQS_VISIBILITY_TIMEOUT", "60s")
	t.Setenv("SQS_CONCURRENCY", "3")
	t.Setenv("SQS_MAX_RECEIVE_COUNT", "7")

	cfg := config.LoadSQS()
	assert.Equal(t, int32(5), cfg.MaxMessages)
	assert.Equal(t, int32(10), cfg.WaitSeconds)
	assert.Equal(t, 60*time.Second, cfg.VisibilityTimeout)
	assert.Equal(t, 3, cfg.Concurrency)
	assert.Equal(t, 7, cfg.MaxReceiveCount)
	assert.Empty(t, cfg.Warnings)
}

// ---------------------------------------------------------------------------
// SQSConfigEnv.Validate
// ---------------------------------------------------------------------------

func TestSQSConfigEnv_Validate_MissingQueueURL(t *testing.T) {
	cfg := config.SQSConfigEnv{}
	require.Error(t, cfg.Validate())
}

func TestSQSConfigEnv_Validate_OK(t *testing.T) {
	cfg := config.SQSConfigEnv{QueueURL: "https://sqs.us-east-1.amazonaws.com/123/q"}
	assert.NoError(t, cfg.Validate())
}

// ---------------------------------------------------------------------------
// LoadOutbox
// ---------------------------------------------------------------------------

func TestLoadOutbox_Defaults(t *testing.T) {
	t.Setenv("OUTBOX_POLL_INTERVAL", "")
	t.Setenv("OUTBOX_BATCH_SIZE", "")
	t.Setenv("OUTBOX_MAX_ATTEMPTS", "")
	t.Setenv("OUTBOX_CLAIM_LEASE_DURATION", "")
	t.Setenv("OUTBOX_STARTUP_JITTER", "")
	t.Setenv("OUTBOX_PUBLISH_CONCURRENCY", "")
	t.Setenv("OUTBOX_PUBLISH_TIMEOUT", "")
	t.Setenv("OUTBOX_DRAIN_TIMEOUT", "")
	t.Setenv("DATABASE_URL", "")

	cfg := config.LoadOutbox()
	assert.Equal(t, 5*time.Second, cfg.PollInterval)
	assert.Equal(t, 50, cfg.BatchSize)
	assert.Equal(t, 5, cfg.MaxAttempts)
	assert.Equal(t, time.Duration(0), cfg.ClaimLeaseDuration)
	assert.Equal(t, time.Duration(0), cfg.StartupJitter)
	assert.Equal(t, 1, cfg.PublishConcurrency)
	assert.Equal(t, 10*time.Second, cfg.PublishTimeout)
	assert.Equal(t, 30*time.Second, cfg.DrainTimeout)
	assert.Empty(t, cfg.Warnings)
}

func TestLoadOutbox_CustomValues(t *testing.T) {
	t.Setenv("OUTBOX_POLL_INTERVAL", "2s")
	t.Setenv("OUTBOX_BATCH_SIZE", "25")
	t.Setenv("OUTBOX_MAX_ATTEMPTS", "3")
	t.Setenv("OUTBOX_CLAIM_LEASE_DURATION", "15m")
	t.Setenv("OUTBOX_STARTUP_JITTER", "500ms")
	t.Setenv("OUTBOX_PUBLISH_CONCURRENCY", "4")
	t.Setenv("OUTBOX_PUBLISH_TIMEOUT", "5s")
	t.Setenv("OUTBOX_DRAIN_TIMEOUT", "45s")
	t.Setenv("DATABASE_URL", "postgres://user:pass@host/db")

	cfg := config.LoadOutbox()
	assert.Equal(t, 2*time.Second, cfg.PollInterval)
	assert.Equal(t, 25, cfg.BatchSize)
	assert.Equal(t, 3, cfg.MaxAttempts)
	assert.Equal(t, 15*time.Minute, cfg.ClaimLeaseDuration)
	assert.Equal(t, 500*time.Millisecond, cfg.StartupJitter)
	assert.Equal(t, 4, cfg.PublishConcurrency)
	assert.Equal(t, 5*time.Second, cfg.PublishTimeout)
	assert.Equal(t, 45*time.Second, cfg.DrainTimeout)
	assert.Equal(t, "postgres://user:pass@host/db", cfg.DatabaseURL)
	assert.Empty(t, cfg.Warnings)
}

func TestLoadOutbox_InvalidValuesProduceWarnings(t *testing.T) {
	t.Setenv("OUTBOX_POLL_INTERVAL", "notaduration")
	t.Setenv("OUTBOX_BATCH_SIZE", "notanint")
	t.Setenv("OUTBOX_MAX_ATTEMPTS", "")
	t.Setenv("OUTBOX_CLAIM_LEASE_DURATION", "")
	t.Setenv("OUTBOX_STARTUP_JITTER", "")
	t.Setenv("OUTBOX_PUBLISH_CONCURRENCY", "")
	t.Setenv("OUTBOX_PUBLISH_TIMEOUT", "")
	t.Setenv("OUTBOX_DRAIN_TIMEOUT", "")

	cfg := config.LoadOutbox()
	assert.Equal(t, 5*time.Second, cfg.PollInterval)
	assert.Equal(t, 50, cfg.BatchSize)
	require.GreaterOrEqual(t, len(cfg.Warnings), 2)
	assert.True(t, strings.Contains(cfg.Warnings[0], "OUTBOX_POLL_INTERVAL") ||
		strings.Contains(cfg.Warnings[1], "OUTBOX_POLL_INTERVAL"))
}

// ---------------------------------------------------------------------------
// OutboxConfigEnv.Validate
// ---------------------------------------------------------------------------

func TestOutboxConfigEnv_Validate_MissingDatabaseURL(t *testing.T) {
	cfg := config.OutboxConfigEnv{}
	require.Error(t, cfg.Validate())
}

func TestOutboxConfigEnv_Validate_OK(t *testing.T) {
	cfg := config.OutboxConfigEnv{DatabaseURL: "postgres://localhost/db"}
	assert.NoError(t, cfg.Validate())
}

// ---------------------------------------------------------------------------
// OutboxConfigEnv.String — DSN masking
// ---------------------------------------------------------------------------

func TestOutboxConfigEnv_String_MasksURLPassword(t *testing.T) {
	cfg := config.OutboxConfigEnv{
		DatabaseURL:  "postgres://user:supersecret@localhost:5432/mydb",
		PollInterval: 5 * time.Second,
		BatchSize:    50,
	}
	s := cfg.String()
	assert.NotContains(t, s, "supersecret")
	assert.Contains(t, s, "***")
}

func TestOutboxConfigEnv_String_MasksKeyValuePassword(t *testing.T) {
	cfg := config.OutboxConfigEnv{
		DatabaseURL: "host=localhost user=app password=topsecret dbname=events",
	}
	s := cfg.String()
	assert.NotContains(t, s, "topsecret")
	assert.Contains(t, s, "password=***")
}

func TestOutboxConfigEnv_String_NoPassword(t *testing.T) {
	cfg := config.OutboxConfigEnv{
		DatabaseURL: "postgres://localhost/mydb",
	}
	s := cfg.String()
	assert.NotEmpty(t, s)
}

func TestOutboxConfigEnv_String_ZeroClaimLease(t *testing.T) {
	cfg := config.OutboxConfigEnv{
		DatabaseURL:        "postgres://localhost/db",
		ClaimLeaseDuration: 0,
	}
	s := cfg.String()
	assert.Contains(t, s, "runner default")
}

func TestOutboxConfigEnv_String_KeyValueNonPassword(t *testing.T) {
	// key-value DSN without password= — maskKeyValueDSN should return unchanged
	cfg := config.OutboxConfigEnv{
		DatabaseURL: "host=localhost user=app dbname=events",
	}
	s := cfg.String()
	assert.Contains(t, s, "host=localhost user=app dbname=events")
}

// ---------------------------------------------------------------------------
// LoadOTel
// ---------------------------------------------------------------------------

func TestLoadOTel_Defaults(t *testing.T) {
	t.Setenv("APP_ENV", "")
	t.Setenv("OTEL_SERVICE_NAME", "")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "")

	cfg := config.LoadOTel()
	assert.Equal(t, "localhost:4317", cfg.ExporterEndpoint)
	assert.False(t, cfg.Insecure)
	assert.Empty(t, cfg.Warnings)
}

func TestLoadOTel_DevEnvSetsInsecure(t *testing.T) {
	for _, env := range []string{"dev", "development", "local"} {
		t.Run(env, func(t *testing.T) {
			t.Setenv("APP_ENV", env)
			t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "")
			cfg := config.LoadOTel()
			assert.True(t, cfg.Insecure)
		})
	}
}

func TestLoadOTel_InsecureEnvTrue(t *testing.T) {
	t.Setenv("APP_ENV", "production")
	t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "true")
	cfg := config.LoadOTel()
	assert.True(t, cfg.Insecure)
	assert.Empty(t, cfg.Warnings)
}

func TestLoadOTel_InsecureEnvFalse(t *testing.T) {
	t.Setenv("APP_ENV", "dev")
	t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "false")
	cfg := config.LoadOTel()
	assert.False(t, cfg.Insecure)
	assert.Empty(t, cfg.Warnings)
}

func TestLoadOTel_InsecureEnvNumeric(t *testing.T) {
	t.Setenv("APP_ENV", "")
	t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "1")
	cfg := config.LoadOTel()
	assert.True(t, cfg.Insecure)

	t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "0")
	cfg = config.LoadOTel()
	assert.False(t, cfg.Insecure)
}

func TestLoadOTel_InvalidInsecureEnvProducesWarning(t *testing.T) {
	t.Setenv("APP_ENV", "")
	t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "maybe")
	cfg := config.LoadOTel()
	require.NotEmpty(t, cfg.Warnings)
	assert.Contains(t, cfg.Warnings[0], "OTEL_EXPORTER_OTLP_INSECURE")
}

func TestLoadOTel_CustomEndpoint(t *testing.T) {
	t.Setenv("APP_ENV", "")
	t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "")
	t.Setenv("OTEL_SERVICE_NAME", "my-service")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "collector:4317")

	cfg := config.LoadOTel()
	assert.Equal(t, "my-service", cfg.ServiceName)
	assert.Equal(t, "collector:4317", cfg.ExporterEndpoint)
}
