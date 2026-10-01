// Package config loads environment variables and maps them to pkg/events and pkg/outbox constructors.
package config

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"
)

// SNSConfigEnv holds environment-derived SNS publisher configuration.
type SNSConfigEnv struct {
	TopicARN    string
	Region      string
	EndpointURL string // AWS_ENDPOINT_URL (e.g. http://localhost:4574 for the local floci stack)
}

// SQSConfigEnv holds environment-derived SQS consumer configuration.
type SQSConfigEnv struct {
	QueueURL          string
	Region            string
	EndpointURL       string
	MaxMessages       int32
	WaitSeconds       int32
	VisibilityTimeout time.Duration
	Concurrency       int
	MaxReceiveCount   int // 0 = unset; apply WithMaxReceiveCount when wiring the consumer
	// QueueDepthInterval (SQS_QUEUE_DEPTH_INTERVAL, e.g. "60s") enables the
	// platform_queue_depth / platform_dlq_depth sampler; 0 (default) = off.
	QueueDepthInterval time.Duration
	// HandlerTimeout (SQS_HANDLER_TIMEOUT, e.g. "5m") bounds each handler call
	// (events.WithHandlerTimeout); 0 (default) = unbounded.
	HandlerTimeout time.Duration
	// DrainTimeout (SQS_DRAIN_TIMEOUT, e.g. "45s") is how long Stop waits for
	// in-flight handlers (events.WithDrainTimeout); 0 (default) = the
	// consumer's 30s. Keep it below the pod's terminationGracePeriodSeconds.
	DrainTimeout time.Duration
	// Warnings is non-empty when one or more env vars were set to invalid values
	// and defaults were applied. Log these at startup so operators can detect
	// misconfiguration without relying on unstructured stderr output.
	Warnings []string
}

// OutboxConfigEnv holds environment-derived outbox runner configuration.
//
// Log it with %v / %s / %#v (String / GoString mask credentials). DB,
// DatabaseURL and MigrationDatabaseURL carry the raw DSN: never log them, or
// the struct through json.Marshal or a reflection-based logger field.
type OutboxConfigEnv struct {
	PollInterval time.Duration
	BatchSize    int
	MaxAttempts  int
	// DB is the Postgres pool configuration, loaded by platform-pgcommon's
	// ConfigFromEnv (DATABASE_URL, or PG_HOST/PG_PORT/PG_USER/PG_PASSWORD/
	// PG_DBNAME/PG_SSLMODE, plus PG_MAX_CONNS, PG_STATEMENT_TIMEOUT, …). Pass
	// it to pgcommon.NewPool; set DB.Logger / DB.GUCProvider first if needed.
	DB pgcommon.Config
	// DatabaseURL is DB.DSN, kept for callers that only need the DSN.
	DatabaseURL string
	// MigrationDatabaseURL is the DSN for outbox.ApplySchema / inbox.ApplySchema:
	// MIGRATION_DATABASE_URL (a DDL-privileged role connecting directly, not
	// through PgBouncer), else DatabaseURL — per pgcommon.MigrationDSNFromEnv.
	MigrationDatabaseURL string
	ClaimLeaseDuration   time.Duration
	StartupJitter        time.Duration
	PublishConcurrency   int
	PublishTimeout       time.Duration
	DrainTimeout         time.Duration
	// RetryBackoff / MaxRetryBackoff: a record's n-th failed publish delays its
	// next attempt by RetryBackoff·2^(n-1), capped at MaxRetryBackoff
	// (OUTBOX_RETRY_BACKOFF, default 1s; OUTBOX_MAX_RETRY_BACKOFF, default 5m).
	RetryBackoff    time.Duration
	MaxRetryBackoff time.Duration
	// GaugeInterval (OUTBOX_GAUGE_INTERVAL, default 15s) is how often the
	// runner refreshes the outbox backlog gauges.
	GaugeInterval time.Duration
	// Warnings is non-empty when one or more env vars were set to invalid values
	// and defaults were applied. Log these at startup so operators can detect
	// misconfiguration without relying on unstructured stderr output.
	Warnings []string
}

// String returns a safe representation of the config that masks the password
// component of DatabaseURL so the struct can be logged without leaking credentials.
func (c OutboxConfigEnv) String() string {
	masked := maskDSN(c.DatabaseURL)
	maskedMigration := maskDSN(c.MigrationDatabaseURL)
	claimLease := c.ClaimLeaseDuration.String()
	if c.ClaimLeaseDuration == 0 {
		claimLease = "0 (runner default: 10m)"
	}
	return fmt.Sprintf("{PollInterval:%s BatchSize:%d MaxAttempts:%d DatabaseURL:%s MigrationDatabaseURL:%s ClaimLeaseDuration:%s StartupJitter:%s PublishConcurrency:%d PublishTimeout:%s DrainTimeout:%s RetryBackoff:%s MaxRetryBackoff:%s GaugeInterval:%s}",
		c.PollInterval, c.BatchSize, c.MaxAttempts, masked, maskedMigration, claimLease, c.StartupJitter, c.PublishConcurrency, c.PublishTimeout, c.DrainTimeout, c.RetryBackoff, c.MaxRetryBackoff, c.GaugeInterval)
}

// GoString masks credentials for %#v as String does for %v / %s. The DB field
// (pgcommon.Config) is omitted: it carries the raw DSN and password.
func (c OutboxConfigEnv) GoString() string {
	return "config.OutboxConfigEnv" + c.String()
}

func maskDSN(dsn string) string {
	const sep = "://"
	idx := strings.Index(dsn, sep)
	if idx >= 0 {
		rest := dsn[idx+len(sep):]
		atIdx := strings.LastIndex(rest, "@")
		// Mask the userinfo (user:password) section before "@".
		if atIdx >= 0 {
			dsn = dsn[:idx+len(sep)] + "***" + rest[atIdx:]
		}
		// Also mask any password= / passwd= appearing in URL query params,
		// e.g. postgres://user:pass@host/db?password=extra prevents credential leak.
		if qIdx := strings.Index(dsn, "?"); qIdx >= 0 {
			dsn = dsn[:qIdx+1] + maskQueryParams(dsn[qIdx+1:])
		}
		return dsn
	}
	return maskKeyValueDSN(dsn)
}

// maskQueryParams replaces the value of any key named "password", "passwd" or
// "sslpassword" (the client-key passphrase) in
// an ampersand-delimited query string (e.g. "host=h&password=secret&sslmode=require").
// Percent-decoded key names are compared so that %70assword=secret is also masked.
func maskQueryParams(query string) string {
	parts := strings.Split(query, "&")
	for i, part := range parts {
		eqIdx := strings.Index(part, "=")
		if eqIdx < 0 {
			continue
		}
		key := part[:eqIdx]
		if decoded, err := url.QueryUnescape(key); err == nil {
			key = decoded
		}
		switch strings.ToLower(key) {
		case "password", "passwd", "sslpassword":
			parts[i] = part[:eqIdx+1] + "***"
		}
	}
	return strings.Join(parts, "&")
}

// maskKeyValueDSN masks password/passwd/sslpassword in a libpq keyword/value string
// ("host=h password='a b' dbname=d"). Values follow libpq's rules: spaces are
// allowed around "=", and a value may be single-quoted with backslash escapes,
// so a quoted password containing spaces or quotes is masked whole.
func maskKeyValueDSN(dsn string) string {
	var out strings.Builder
	i, n := 0, len(dsn)
	isSpace := func(c byte) bool { return c == ' ' || c == '\t' || c == '\n' || c == '\r' }
	for i < n {
		// Copy whitespace between pairs.
		for i < n && isSpace(dsn[i]) {
			out.WriteByte(dsn[i])
			i++
		}
		if i >= n {
			break
		}
		keyStart := i
		for i < n && dsn[i] != '=' && !isSpace(dsn[i]) {
			i++
		}
		key := dsn[keyStart:i]
		j := i
		for j < n && isSpace(dsn[j]) {
			j++
		}
		if j >= n || dsn[j] != '=' {
			// Not a keyword=value pair: copy the token through.
			out.WriteString(dsn[keyStart:i])
			continue
		}
		j++ // past '='
		for j < n && isSpace(dsn[j]) {
			j++
		}
		valStart := j
		if j < n && dsn[j] == '\'' {
			j++
			for j < n && dsn[j] != '\'' {
				if dsn[j] == '\\' && j+1 < n {
					j++
				}
				j++
			}
			if j < n {
				j++ // closing quote
			}
		} else {
			// Unquoted: ends at whitespace, but libpq honours backslash
			// escapes here too (password=a\ b is "a b").
			for j < n && !isSpace(dsn[j]) {
				if dsn[j] == '\\' && j+1 < n {
					j++
				}
				j++
			}
		}
		switch strings.ToLower(key) {
		case "password", "passwd", "sslpassword":
			out.WriteString(key + "=***")
		default:
			out.WriteString(key + "=" + dsn[valStart:j])
		}
		i = j
	}
	return out.String()
}

// OTelConfigEnv holds environment-derived OpenTelemetry configuration.
//
// Deprecated: platform-events never initialises OpenTelemetry — it uses the
// global tracer provider and propagator the consuming service installs with
// platform-gincommon's InitTracingFromEnv, which owns the OTEL_* variables.
// This struct duplicates (and already diverges from) that parsing; it will be
// removed in the next major version.
type OTelConfigEnv struct {
	ServiceName      string
	ExporterEndpoint string
	Insecure         bool
	// Warnings is non-empty when one or more env vars were set to invalid values
	// and defaults were applied.
	Warnings []string
}

// Validate returns an error if required fields are missing.
func (c SNSConfigEnv) Validate() error {
	if c.TopicARN == "" {
		return fmt.Errorf("platform-events: SNS_TOPIC_ARN is required but not set")
	}
	return nil
}

// Validate returns an error if required fields are missing.
func (c SQSConfigEnv) Validate() error {
	if c.QueueURL == "" {
		return fmt.Errorf("platform-events: SQS_QUEUE_URL is required but not set")
	}
	return nil
}

// Validate returns an error if required fields are missing.
func (c OutboxConfigEnv) Validate() error {
	if c.DatabaseURL == "" {
		return fmt.Errorf("platform-events: DATABASE_URL (or PG_USER and PG_DBNAME) is required but not set")
	}
	return nil
}

// LoadSNS loads SNS configuration from environment variables.
func LoadSNS() SNSConfigEnv {
	return SNSConfigEnv{
		TopicARN:    os.Getenv("SNS_TOPIC_ARN"),
		Region:      awsRegion(),
		EndpointURL: os.Getenv("AWS_ENDPOINT_URL"),
	}
}

// LoadSQS loads SQS configuration from environment variables.
func LoadSQS() SQSConfigEnv {
	var warnings []string
	maxMsg, w := envIntOrDefault("SQS_MAX_MESSAGES", 10)
	if w != "" {
		warnings = append(warnings, w)
	}
	waitSec, w := envIntOrDefault("SQS_WAIT_SECONDS", 20)
	if w != "" {
		warnings = append(warnings, w)
	}
	visTm, w := envDurationOrDefault("SQS_VISIBILITY_TIMEOUT", 30*time.Second)
	if w != "" {
		warnings = append(warnings, w)
	}
	conc, w := envIntOrDefault("SQS_CONCURRENCY", 1)
	if w != "" {
		warnings = append(warnings, w)
	}
	maxRecv, w := envIntOrDefault("SQS_MAX_RECEIVE_COUNT", 0)
	if w != "" {
		warnings = append(warnings, w)
	}
	depthInterval, w := envDurationOrDefault("SQS_QUEUE_DEPTH_INTERVAL", 0)
	if w != "" {
		warnings = append(warnings, w)
	}
	handlerTimeout, w := envDurationOrDefault("SQS_HANDLER_TIMEOUT", 0)
	if w != "" {
		warnings = append(warnings, w)
	}
	drainTimeout, w := envDurationOrDefault("SQS_DRAIN_TIMEOUT", 0)
	if w != "" {
		warnings = append(warnings, w)
	}
	return SQSConfigEnv{
		QueueURL:           os.Getenv("SQS_QUEUE_URL"),
		Region:             awsRegion(),
		EndpointURL:        os.Getenv("AWS_ENDPOINT_URL"),
		MaxMessages:        int32(maxMsg),
		WaitSeconds:        int32(waitSec),
		VisibilityTimeout:  visTm,
		Concurrency:        conc,
		MaxReceiveCount:    maxRecv,
		QueueDepthInterval: depthInterval,
		HandlerTimeout:     handlerTimeout,
		DrainTimeout:       drainTimeout,
		Warnings:           warnings,
	}
}

// LoadOutbox loads outbox runner configuration from environment variables.
func LoadOutbox() OutboxConfigEnv {
	var warnings []string
	pollInterval, w := envDurationOrDefault("OUTBOX_POLL_INTERVAL", 5*time.Second)
	if w != "" {
		warnings = append(warnings, w)
	}
	batchSize, w := envIntOrDefault("OUTBOX_BATCH_SIZE", 50)
	if w != "" {
		warnings = append(warnings, w)
	}
	maxAttempts, w := envIntOrDefault("OUTBOX_MAX_ATTEMPTS", 5)
	if w != "" {
		warnings = append(warnings, w)
	}
	claimLease, w := envDurationOrDefault("OUTBOX_CLAIM_LEASE_DURATION", 0)
	if w != "" {
		warnings = append(warnings, w)
	}
	startupJitter, w := envDurationOrDefault("OUTBOX_STARTUP_JITTER", 0)
	if w != "" {
		warnings = append(warnings, w)
	}
	publishConcurrency, w := envIntOrDefault("OUTBOX_PUBLISH_CONCURRENCY", 1)
	if w != "" {
		warnings = append(warnings, w)
	}
	publishTimeout, w := envDurationOrDefault("OUTBOX_PUBLISH_TIMEOUT", 10*time.Second)
	if w != "" {
		warnings = append(warnings, w)
	}
	drainTimeout, w := envDurationOrDefault("OUTBOX_DRAIN_TIMEOUT", 30*time.Second)
	if w != "" {
		warnings = append(warnings, w)
	}
	retryBackoff, w := envDurationOrDefault("OUTBOX_RETRY_BACKOFF", time.Second)
	if w != "" {
		warnings = append(warnings, w)
	}
	maxRetryBackoff, w := envDurationOrDefault("OUTBOX_MAX_RETRY_BACKOFF", 5*time.Minute)
	if w != "" {
		warnings = append(warnings, w)
	}
	gaugeInterval, w := envDurationOrDefault("OUTBOX_GAUGE_INTERVAL", 15*time.Second)
	if w != "" {
		warnings = append(warnings, w)
	}
	// Database configuration is owned by platform-pgcommon: the same env vars,
	// defaults and validation as every other service using pgcommon.NewPool.
	db, dbWarnings := pgcommon.ConfigFromEnv()
	for _, dw := range dbWarnings {
		warnings = append(warnings, fmt.Sprintf("platform-pgcommon: %s: %s", dw.Key, dw.Reason))
	}
	return OutboxConfigEnv{
		PollInterval:         pollInterval,
		BatchSize:            batchSize,
		MaxAttempts:          maxAttempts,
		DB:                   db,
		DatabaseURL:          db.DSN,
		MigrationDatabaseURL: pgcommon.MigrationDSNFromEnv(),
		ClaimLeaseDuration:   claimLease,
		StartupJitter:        startupJitter,
		PublishConcurrency:   publishConcurrency,
		PublishTimeout:       publishTimeout,
		DrainTimeout:         drainTimeout,
		RetryBackoff:         retryBackoff,
		MaxRetryBackoff:      maxRetryBackoff,
		GaugeInterval:        gaugeInterval,
		Warnings:             warnings,
	}
}

// LoadOTel loads OpenTelemetry configuration from environment variables.
//
// Deprecated: nothing in platform-events uses it, and its parsing differs from
// the tracer the service actually runs (OTEL_SERVICE_NAME has no APP_NAME
// fallback, "yes"/"no" are rejected, APP_ENV is case-folded, sampler and
// baggage variables are ignored). Initialise tracing with platform-gincommon's
// InitTracingFromEnv and read its configuration there.
func LoadOTel() OTelConfigEnv {
	var warnings []string
	appEnv := strings.ToLower(os.Getenv("APP_ENV"))
	insecure := appEnv == "dev" || appEnv == "development" || appEnv == "local"
	if v := os.Getenv("OTEL_EXPORTER_OTLP_INSECURE"); v != "" {
		switch strings.ToLower(v) {
		case "true", "1":
			insecure = true
		case "false", "0":
			insecure = false
		default:
			warnings = append(warnings, fmt.Sprintf("platform-events: OTEL_EXPORTER_OTLP_INSECURE=%q is not a valid boolean; expected true/false/1/0; effective value: %v", v, insecure))
		}
	}
	return OTelConfigEnv{
		ServiceName:      os.Getenv("OTEL_SERVICE_NAME"),
		ExporterEndpoint: envOrDefault("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4317"),
		Insecure:         insecure,
		Warnings:         warnings,
	}
}

// awsRegion resolves the region like the AWS SDK's environment chain:
// AWS_REGION, then AWS_DEFAULT_REGION, then us-east-1. Passing us-east-1 when
// only AWS_DEFAULT_REGION is set would point the client at the wrong region.
func awsRegion() string {
	if r := os.Getenv("AWS_REGION"); r != "" {
		return r
	}
	return envOrDefault("AWS_DEFAULT_REGION", "us-east-1")
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envIntOrDefault(key string, def int) (int, string) {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n, ""
		}
		return def, fmt.Sprintf("platform-events: %s=%q is not a valid integer; using default %d", key, v, def)
	}
	return def, ""
}

func envDurationOrDefault(key string, def time.Duration) (time.Duration, string) {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d, ""
		}
		return def, fmt.Sprintf("platform-events: %s=%q is not a valid duration; using default %s", key, v, def)
	}
	return def, ""
}
