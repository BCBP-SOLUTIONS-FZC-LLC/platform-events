// Package config loads environment variables and maps them to pkg/events and pkg/outbox constructors.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// SNSConfigEnv holds environment-derived SNS publisher configuration.
type SNSConfigEnv struct {
	TopicARN    string
	Region      string
	EndpointURL string // AWS_ENDPOINT_URL (e.g. http://localhost:4566 for LocalStack)
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
	// Warnings is non-empty when one or more env vars were set to invalid values
	// and defaults were applied. Log these at startup so operators can detect
	// misconfiguration without relying on unstructured stderr output.
	Warnings []string
}

// OutboxConfigEnv holds environment-derived outbox runner configuration.
type OutboxConfigEnv struct {
	PollInterval       time.Duration
	BatchSize          int
	MaxAttempts        int
	DatabaseURL        string
	ClaimLeaseDuration time.Duration
	StartupJitter      time.Duration
	PublishConcurrency int
	PublishTimeout     time.Duration
	DrainTimeout       time.Duration
	// Warnings is non-empty when one or more env vars were set to invalid values
	// and defaults were applied. Log these at startup so operators can detect
	// misconfiguration without relying on unstructured stderr output.
	Warnings []string
}

// String returns a safe representation of the config that masks the password
// component of DatabaseURL so the struct can be logged without leaking credentials.
func (c OutboxConfigEnv) String() string {
	masked := maskDSN(c.DatabaseURL)
	claimLease := c.ClaimLeaseDuration.String()
	if c.ClaimLeaseDuration == 0 {
		claimLease = "0 (runner default: 10m)"
	}
	return fmt.Sprintf("{PollInterval:%s BatchSize:%d MaxAttempts:%d DatabaseURL:%s ClaimLeaseDuration:%s StartupJitter:%s PublishConcurrency:%d PublishTimeout:%s DrainTimeout:%s}",
		c.PollInterval, c.BatchSize, c.MaxAttempts, masked, claimLease, c.StartupJitter, c.PublishConcurrency, c.PublishTimeout, c.DrainTimeout)
}

func maskDSN(dsn string) string {
	const sep = "://"
	idx := strings.Index(dsn, sep)
	if idx >= 0 {
		rest := dsn[idx+len(sep):]
		atIdx := strings.LastIndex(rest, "@")
		if atIdx < 0 {
			return dsn
		}
		return dsn[:idx+len(sep)] + "***" + rest[atIdx:]
	}
	return maskKeyValueDSN(dsn)
}

func maskKeyValueDSN(dsn string) string {
	parts := strings.Fields(dsn)
	for i, part := range parts {
		eqIdx := strings.Index(part, "=")
		if eqIdx < 0 {
			continue
		}
		switch strings.ToLower(part[:eqIdx]) {
		case "password", "passwd":
			parts[i] = part[:eqIdx+1] + "***"
		}
	}
	return strings.Join(parts, " ")
}

// OTelConfigEnv holds environment-derived OpenTelemetry configuration.
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
		return fmt.Errorf("platform-events: DATABASE_URL is required but not set")
	}
	return nil
}

// LoadSNS loads SNS configuration from environment variables.
func LoadSNS() SNSConfigEnv {
	return SNSConfigEnv{
		TopicARN:    os.Getenv("SNS_TOPIC_ARN"),
		Region:      envOrDefault("AWS_REGION", "us-east-1"),
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
	return SQSConfigEnv{
		QueueURL:          os.Getenv("SQS_QUEUE_URL"),
		Region:            envOrDefault("AWS_REGION", "us-east-1"),
		EndpointURL:       os.Getenv("AWS_ENDPOINT_URL"),
		MaxMessages:       int32(maxMsg),
		WaitSeconds:       int32(waitSec),
		VisibilityTimeout: visTm,
		Concurrency:       conc,
		MaxReceiveCount:   maxRecv,
		Warnings:          warnings,
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
	return OutboxConfigEnv{
		PollInterval:       pollInterval,
		BatchSize:          batchSize,
		MaxAttempts:        maxAttempts,
		DatabaseURL:        os.Getenv("DATABASE_URL"),
		ClaimLeaseDuration: claimLease,
		StartupJitter:      startupJitter,
		PublishConcurrency: publishConcurrency,
		PublishTimeout:     publishTimeout,
		DrainTimeout:       drainTimeout,
		Warnings:           warnings,
	}
}

// LoadOTel loads OpenTelemetry configuration from environment variables.
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
