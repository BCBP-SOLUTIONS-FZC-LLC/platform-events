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
	t.Setenv("OUTBOX_RETRY_BACKOFF", "")
	t.Setenv("OUTBOX_MAX_RETRY_BACKOFF", "")
	t.Setenv("DATABASE_URL", "")

	cfg := config.LoadOutbox()
	assert.Equal(t, time.Second, cfg.RetryBackoff)
	assert.Equal(t, 5*time.Minute, cfg.MaxRetryBackoff)
	assert.Equal(t, 5*time.Second, cfg.PollInterval)
	assert.Equal(t, 50, cfg.BatchSize)
	assert.Equal(t, 5, cfg.MaxAttempts)
	assert.Equal(t, time.Duration(0), cfg.ClaimLeaseDuration)
	assert.Equal(t, time.Duration(0), cfg.StartupJitter)
	assert.Equal(t, 1, cfg.PublishConcurrency)
	assert.Equal(t, 10*time.Second, cfg.PublishTimeout)
	assert.Equal(t, 30*time.Second, cfg.DrainTimeout)
	// No database env at all: pgcommon's ConfigFromEnv reports it.
	require.Len(t, cfg.Warnings, 1)
	assert.Contains(t, cfg.Warnings[0], "platform-pgcommon: PG_USER/PG_DBNAME")
	assert.Empty(t, cfg.DatabaseURL)
	require.Error(t, cfg.Validate())
}

func TestLoadOutbox_DatabaseConfigFromPgcommon(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("MIGRATION_DATABASE_URL", "")
	t.Setenv("PG_HOST", "db.internal")
	t.Setenv("PG_PORT", "6432")
	t.Setenv("PG_USER", "events")
	t.Setenv("PG_PASSWORD", "s3cret")
	t.Setenv("PG_DBNAME", "platform")
	t.Setenv("PG_SSLMODE", "verify-full")
	t.Setenv("PG_MAX_CONNS", "25")
	t.Setenv("PG_STATEMENT_TIMEOUT", "15s")
	t.Setenv("PG_BOUNCER_MODE", "true")

	cfg := config.LoadOutbox()
	require.NotEmpty(t, cfg.DatabaseURL, "DSN built from PG_* parts by pgcommon")
	assert.Equal(t, cfg.DB.DSN, cfg.DatabaseURL)
	assert.Contains(t, cfg.DatabaseURL, "db.internal")
	assert.Equal(t, int32(25), cfg.DB.MaxConns)
	assert.Equal(t, 15*time.Second, cfg.DB.StatementTimeout)
	assert.True(t, cfg.DB.PGBouncerMode)
	assert.Equal(t, cfg.DatabaseURL, cfg.MigrationDatabaseURL, "falls back to the app DSN")
	assert.NoError(t, cfg.Validate())
	assert.Empty(t, cfg.Warnings)
	assert.NotContains(t, cfg.String(), "s3cret")
}

func TestLoadOutbox_MigrationDatabaseURL(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://app:apppw@pgbouncer:6432/db?sslmode=require")
	t.Setenv("MIGRATION_DATABASE_URL", "postgres://ddl:ddlpw@postgres:5432/db?sslmode=require")

	cfg := config.LoadOutbox()
	assert.Equal(t, "postgres://app:apppw@pgbouncer:6432/db?sslmode=require", cfg.DatabaseURL)
	assert.Equal(t, "postgres://ddl:ddlpw@postgres:5432/db?sslmode=require", cfg.MigrationDatabaseURL)
	s := cfg.String()
	assert.NotContains(t, s, "apppw")
	assert.NotContains(t, s, "ddlpw")
	assert.Contains(t, s, "MigrationDatabaseURL:postgres://***@postgres:5432")
}

func TestLoadOutbox_PgcommonWarningsForwarded(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://u:p@h/db?sslmode=disable")
	t.Setenv("PG_MAX_CONNS", "lots")

	cfg := config.LoadOutbox()
	joined := strings.Join(cfg.Warnings, "\n")
	assert.Contains(t, joined, "platform-pgcommon: DATABASE_URL: sslmode=disable")
	assert.Contains(t, joined, "platform-pgcommon: PG_MAX_CONNS")
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
	t.Setenv("OUTBOX_RETRY_BACKOFF", "2s")
	t.Setenv("OUTBOX_MAX_RETRY_BACKOFF", "10m")
	t.Setenv("DATABASE_URL", "postgres://user:pass@host/db")

	cfg := config.LoadOutbox()
	assert.Equal(t, 2*time.Second, cfg.RetryBackoff)
	assert.Equal(t, 10*time.Minute, cfg.MaxRetryBackoff)
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
	cfg := config.OutboxConfigEnv{ //nolint:gosec
		DatabaseURL:  "postgres://user:hunter2@localhost:5432/mydb",
		PollInterval: 5 * time.Second,
		BatchSize:    50,
	}
	s := cfg.String()
	assert.NotContains(t, s, "supersecret")
	assert.Contains(t, s, "***")
}

func TestOutboxConfigEnv_String_MasksKeyValuePassword(t *testing.T) {
	cfg := config.OutboxConfigEnv{
		DatabaseURL: "host=localhost user=app password=topsecret dbname=events", //nolint:gosec
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
// LoadOTel (deprecated — kept until the next major version)
// ---------------------------------------------------------------------------

func TestLoadOTel_Defaults(t *testing.T) {
	t.Setenv("APP_ENV", "")
	t.Setenv("OTEL_SERVICE_NAME", "")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "")

	cfg := config.LoadOTel() //nolint:staticcheck // exercises the deprecated API
	assert.Equal(t, "localhost:4317", cfg.ExporterEndpoint)
	assert.False(t, cfg.Insecure)
	assert.Empty(t, cfg.Warnings)
}

func TestLoadOTel_DevEnvSetsInsecure(t *testing.T) {
	for _, env := range []string{"dev", "development", "local"} {
		t.Run(env, func(t *testing.T) {
			t.Setenv("APP_ENV", env)
			t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "")
			cfg := config.LoadOTel() //nolint:staticcheck // exercises the deprecated API
			assert.True(t, cfg.Insecure)
		})
	}
}

func TestLoadOTel_InsecureEnvTrue(t *testing.T) {
	t.Setenv("APP_ENV", "production")
	t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "true")
	cfg := config.LoadOTel() //nolint:staticcheck // exercises the deprecated API
	assert.True(t, cfg.Insecure)
	assert.Empty(t, cfg.Warnings)
}

func TestLoadOTel_InsecureEnvFalse(t *testing.T) {
	t.Setenv("APP_ENV", "dev")
	t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "false")
	cfg := config.LoadOTel() //nolint:staticcheck // exercises the deprecated API
	assert.False(t, cfg.Insecure)
	assert.Empty(t, cfg.Warnings)
}

func TestLoadOTel_InsecureEnvNumeric(t *testing.T) {
	t.Setenv("APP_ENV", "")
	t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "1")
	cfg := config.LoadOTel() //nolint:staticcheck // exercises the deprecated API
	assert.True(t, cfg.Insecure)

	t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "0")
	cfg = config.LoadOTel() //nolint:staticcheck // exercises the deprecated API
	assert.False(t, cfg.Insecure)
}

func TestLoadOTel_InvalidInsecureEnvProducesWarning(t *testing.T) {
	t.Setenv("APP_ENV", "")
	t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "maybe")
	cfg := config.LoadOTel() //nolint:staticcheck // exercises the deprecated API
	require.NotEmpty(t, cfg.Warnings)
	assert.Contains(t, cfg.Warnings[0], "OTEL_EXPORTER_OTLP_INSECURE")
}

func TestLoadOTel_CustomEndpoint(t *testing.T) {
	t.Setenv("APP_ENV", "")
	t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "")
	t.Setenv("OTEL_SERVICE_NAME", "my-service")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "collector:4317")

	cfg := config.LoadOTel() //nolint:staticcheck // exercises the deprecated API
	assert.Equal(t, "my-service", cfg.ServiceName)
	assert.Equal(t, "collector:4317", cfg.ExporterEndpoint)
}

// ---------------------------------------------------------------------------
// maskQueryParams (via OutboxConfigEnv.String with URL query-param password)
// ---------------------------------------------------------------------------

func TestOutboxConfigEnv_String_MasksURLQueryParamPassword(t *testing.T) {
	cfg := config.OutboxConfigEnv{ //nolint:gosec
		DatabaseURL: "postgres://user:pass@host/db?password=secret&sslmode=require",
	}
	s := cfg.String()
	assert.NotContains(t, s, "secret", "password query param must be masked")
	assert.Contains(t, s, "***", "masked value must appear in output")
}

func TestOutboxConfigEnv_String_MasksURLQueryParamPasswd(t *testing.T) {
	cfg := config.OutboxConfigEnv{ //nolint:gosec
		DatabaseURL: "postgres://user:pass@host/db?passwd=topsecret",
	}
	s := cfg.String()
	assert.NotContains(t, s, "topsecret", "passwd query param must be masked")
}

// ---------------------------------------------------------------------------
// LoadSQS: additional warning paths (WaitSeconds, Concurrency, MaxReceiveCount)
// ---------------------------------------------------------------------------

func TestLoadSQS_AllRemainingInvalidValuesProduceWarnings(t *testing.T) {
	t.Setenv("SQS_QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/123/q")
	t.Setenv("SQS_MAX_MESSAGES", "")
	t.Setenv("SQS_WAIT_SECONDS", "bad")
	t.Setenv("SQS_VISIBILITY_TIMEOUT", "")
	t.Setenv("SQS_CONCURRENCY", "bad")
	t.Setenv("SQS_MAX_RECEIVE_COUNT", "bad")

	cfg := config.LoadSQS()
	assert.GreaterOrEqual(t, len(cfg.Warnings), 3,
		"invalid WaitSeconds, Concurrency, and MaxReceiveCount should each produce a warning")

	hasWait := false
	hasConc := false
	hasRecv := false
	for _, w := range cfg.Warnings {
		if strings.Contains(w, "SQS_WAIT_SECONDS") {
			hasWait = true
		}
		if strings.Contains(w, "SQS_CONCURRENCY") {
			hasConc = true
		}
		if strings.Contains(w, "SQS_MAX_RECEIVE_COUNT") {
			hasRecv = true
		}
	}
	assert.True(t, hasWait, "expected warning for SQS_WAIT_SECONDS")
	assert.True(t, hasConc, "expected warning for SQS_CONCURRENCY")
	assert.True(t, hasRecv, "expected warning for SQS_MAX_RECEIVE_COUNT")
}

// ---------------------------------------------------------------------------
// LoadOutbox: remaining invalid-value warning paths
// ---------------------------------------------------------------------------

func TestLoadOutbox_AllRemainingInvalidValuesProduceWarnings(t *testing.T) {
	t.Setenv("OUTBOX_POLL_INTERVAL", "")
	t.Setenv("OUTBOX_BATCH_SIZE", "")
	t.Setenv("OUTBOX_MAX_ATTEMPTS", "")
	t.Setenv("OUTBOX_CLAIM_LEASE_DURATION", "bad")
	t.Setenv("OUTBOX_STARTUP_JITTER", "bad")
	t.Setenv("OUTBOX_PUBLISH_CONCURRENCY", "bad")
	t.Setenv("OUTBOX_PUBLISH_TIMEOUT", "bad")
	t.Setenv("OUTBOX_DRAIN_TIMEOUT", "bad")
	t.Setenv("DATABASE_URL", "")

	cfg := config.LoadOutbox()
	assert.GreaterOrEqual(t, len(cfg.Warnings), 5,
		"invalid ClaimLeaseDuration, StartupJitter, PublishConcurrency, PublishTimeout, and DrainTimeout should each produce a warning")

	hasLease := false
	hasJitter := false
	hasConc := false
	hasTimeout := false
	hasDrain := false
	for _, w := range cfg.Warnings {
		if strings.Contains(w, "OUTBOX_CLAIM_LEASE_DURATION") {
			hasLease = true
		}
		if strings.Contains(w, "OUTBOX_STARTUP_JITTER") {
			hasJitter = true
		}
		if strings.Contains(w, "OUTBOX_PUBLISH_CONCURRENCY") {
			hasConc = true
		}
		if strings.Contains(w, "OUTBOX_PUBLISH_TIMEOUT") {
			hasTimeout = true
		}
		if strings.Contains(w, "OUTBOX_DRAIN_TIMEOUT") {
			hasDrain = true
		}
	}
	assert.True(t, hasLease, "expected warning for OUTBOX_CLAIM_LEASE_DURATION")
	assert.True(t, hasJitter, "expected warning for OUTBOX_STARTUP_JITTER")
	assert.True(t, hasConc, "expected warning for OUTBOX_PUBLISH_CONCURRENCY")
	assert.True(t, hasTimeout, "expected warning for OUTBOX_PUBLISH_TIMEOUT")
	assert.True(t, hasDrain, "expected warning for OUTBOX_DRAIN_TIMEOUT")
}

// ---------------------------------------------------------------------------
// maskQueryParams: query part without "=" (covers the continue branch)
// ---------------------------------------------------------------------------

func TestMaskDSN_QueryPartWithoutEquals(t *testing.T) {
	// "sslmode" has no "=" — maskQueryParams must skip it (eqIdx < 0 → continue).
	// "password=secret" should still be masked.
	cfg := config.OutboxConfigEnv{ //nolint:gosec
		DatabaseURL: "postgres://user:pass@host/db?sslmode&password=secret", //nolint:gosec
	}
	s := cfg.String()
	assert.NotContains(t, s, "secret", "password value must be masked")
	assert.Contains(t, s, "***", "masked sentinel must appear")
}

// ---------------------------------------------------------------------------
// maskKeyValueDSN: field without "=" (covers the continue branch)
// ---------------------------------------------------------------------------

func TestMaskDSN_KeyValueFieldWithoutEquals(t *testing.T) {
	// "sslmode" has no "=" — maskKeyValueDSN must skip it (eqIdx < 0 → continue).
	// "password=secret" should still be masked.
	cfg := config.OutboxConfigEnv{ //nolint:gosec
		// No "://" → maskDSN falls through to maskKeyValueDSN
		DatabaseURL: "host=localhost sslmode dbname=test password=secret", //nolint:gosec
	}
	s := cfg.String()
	assert.NotContains(t, s, "secret", "password value must be masked")
	assert.Contains(t, s, "***", "masked sentinel must appear")
}

// ---------------------------------------------------------------------------
// LoadOutbox: OUTBOX_MAX_ATTEMPTS invalid value produces a warning
// ---------------------------------------------------------------------------

func TestLoadOutbox_InvalidMaxAttempts_ProducesWarning(t *testing.T) {
	t.Setenv("OUTBOX_MAX_ATTEMPTS", "not-a-number")

	cfg := config.LoadOutbox()

	found := false
	for _, w := range cfg.Warnings {
		if strings.Contains(w, "OUTBOX_MAX_ATTEMPTS") {
			found = true
			break
		}
	}
	assert.True(t, found, "expected a warning for invalid OUTBOX_MAX_ATTEMPTS")
	assert.Equal(t, 5, cfg.MaxAttempts, "should fall back to default of 5")
}

func TestLoadOutbox_InvalidRetryBackoff_WarnsAndDefaults(t *testing.T) {
	t.Setenv("OUTBOX_RETRY_BACKOFF", "soon")
	t.Setenv("OUTBOX_MAX_RETRY_BACKOFF", "later")
	cfg := config.LoadOutbox()
	assert.Equal(t, time.Second, cfg.RetryBackoff)
	assert.Equal(t, 5*time.Minute, cfg.MaxRetryBackoff)
	joined := strings.Join(cfg.Warnings, "\n")
	assert.Contains(t, joined, "OUTBOX_RETRY_BACKOFF")
	assert.Contains(t, joined, "OUTBOX_MAX_RETRY_BACKOFF")
}

// Quoted libpq values (spaces, escaped quotes) and spaces around "=" are
// masked whole; other keys are kept verbatim.
func TestMaskDSN_KeyValueQuotedPassword(t *testing.T) {
	for _, dsn := range []string{
		`host=db password='s3cr et' dbname=app`,
		`host=db password = 'it\'s secret' dbname=app`,
		`host=db PASSWD='a b c'`,
		`host=db password='unterminated secret`,
	} {
		s := config.OutboxConfigEnv{DatabaseURL: dsn}.String() //nolint:gosec
		assert.NotContains(t, s, "secret", dsn)
		assert.NotContains(t, s, "s3cr", dsn)
		assert.NotContains(t, s, " b c", dsn)
		assert.Contains(t, s, "host=db", dsn)
	}
	s := config.OutboxConfigEnv{DatabaseURL: `host=db password='x y' dbname=app`}.String() //nolint:gosec
	assert.Contains(t, s, "dbname=app", "keys after a quoted value are kept")
}

func TestMaskDSN_KeyValueTrailingSpace(t *testing.T) {
	s := config.OutboxConfigEnv{DatabaseURL: "host=db password=secret   "}.String() //nolint:gosec
	assert.NotContains(t, s, "secret")
}

func TestMaskDSN_KeyValueEscapedSpaceUnquoted(t *testing.T) {
	s := config.OutboxConfigEnv{DatabaseURL: `host=db password=top\ secret dbname=app`}.String() //nolint:gosec
	assert.NotContains(t, s, "secret")
	assert.Contains(t, s, "dbname=app")
}

func TestLoad_NewDurationVars(t *testing.T) {
	t.Setenv("SQS_HANDLER_TIMEOUT", "5m")
	t.Setenv("OUTBOX_GAUGE_INTERVAL", "30s")
	assert.Equal(t, 5*time.Minute, config.LoadSQS().HandlerTimeout)
	assert.Equal(t, 30*time.Second, config.LoadOutbox().GaugeInterval)
	opts := config.SQSConsumerOptions(config.LoadSQS())
	assert.NotEmpty(t, opts)

	t.Setenv("SQS_HANDLER_TIMEOUT", "forever")
	t.Setenv("OUTBOX_GAUGE_INTERVAL", "often")
	sqsCfg, outboxCfg := config.LoadSQS(), config.LoadOutbox()
	assert.Zero(t, sqsCfg.HandlerTimeout)
	assert.Equal(t, 15*time.Second, outboxCfg.GaugeInterval)
	assert.Contains(t, strings.Join(sqsCfg.Warnings, "\n"), "SQS_HANDLER_TIMEOUT")
	assert.Contains(t, strings.Join(outboxCfg.Warnings, "\n"), "OUTBOX_GAUGE_INTERVAL")
}

func TestLoadOutbox_StrictOrdering(t *testing.T) {
	t.Setenv("OUTBOX_STRICT_ORDERING", "true")
	cfg := config.LoadOutbox()
	assert.True(t, cfg.StrictOrdering)
	assert.True(t, config.RunnerConfigFromEnv(cfg, nil, nil, nil).StrictOrdering)
	t.Setenv("OUTBOX_STRICT_ORDERING", "sometimes")
	cfg = config.LoadOutbox()
	assert.False(t, cfg.StrictOrdering)
	assert.Contains(t, strings.Join(cfg.Warnings, "\n"), "OUTBOX_STRICT_ORDERING")
}
