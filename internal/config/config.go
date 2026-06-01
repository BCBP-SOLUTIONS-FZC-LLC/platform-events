// Package config loads environment variables for platform-events components.
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
}

// OutboxConfigEnv holds environment-derived outbox runner configuration.
type OutboxConfigEnv struct {
	PollInterval time.Duration
	BatchSize    int
	MaxAttempts  int
	DatabaseURL  string
}

// OTelConfigEnv holds environment-derived OpenTelemetry configuration.
type OTelConfigEnv struct {
	ServiceName      string
	ExporterEndpoint string
	Insecure         bool
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
	return SQSConfigEnv{
		QueueURL:          os.Getenv("SQS_QUEUE_URL"),
		Region:            envOrDefault("AWS_REGION", "us-east-1"),
		EndpointURL:       os.Getenv("AWS_ENDPOINT_URL"),
		MaxMessages:       int32(envIntOrDefault("SQS_MAX_MESSAGES", 10)),
		WaitSeconds:       int32(envIntOrDefault("SQS_WAIT_SECONDS", 20)),
		VisibilityTimeout: envDurationOrDefault("SQS_VISIBILITY_TIMEOUT", 30*time.Second),
		Concurrency:       envIntOrDefault("SQS_CONCURRENCY", 1),
	}
}

// LoadOutbox loads outbox runner configuration from environment variables.
func LoadOutbox() OutboxConfigEnv {
	return OutboxConfigEnv{
		PollInterval: envDurationOrDefault("OUTBOX_POLL_INTERVAL", 5*time.Second),
		BatchSize:    envIntOrDefault("OUTBOX_BATCH_SIZE", 50),
		MaxAttempts:  envIntOrDefault("OUTBOX_MAX_ATTEMPTS", 5),
		DatabaseURL:  os.Getenv("DATABASE_URL"),
	}
}

// LoadOTel loads OpenTelemetry configuration from environment variables.
// APP_ENV=dev/development/local forces insecure mode.
func LoadOTel() OTelConfigEnv {
	appEnv := strings.ToLower(os.Getenv("APP_ENV"))
	insecure := appEnv == "dev" || appEnv == "development" || appEnv == "local"
	if v := os.Getenv("OTEL_EXPORTER_OTLP_INSECURE"); v != "" {
		switch strings.ToLower(v) {
		case "true", "1":
			insecure = true
		case "false", "0":
			insecure = false
		default:
			fmt.Fprintf(os.Stderr, "platform-events: invalid value for OTEL_EXPORTER_OTLP_INSECURE=%q, expected true/false/1/0; effective value: %v\n", v, insecure)
		}
	}
	return OTelConfigEnv{
		ServiceName:      os.Getenv("OTEL_SERVICE_NAME"),
		ExporterEndpoint: envOrDefault("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4317"),
		Insecure:         insecure,
	}
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envIntOrDefault(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
		fmt.Fprintf(os.Stderr, "platform-events: invalid value for %s=%q, using default %d\n", key, v, def)
	}
	return def
}

func envDurationOrDefault(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
		fmt.Fprintf(os.Stderr, "platform-events: invalid value for %s=%q, using default %s\n", key, v, def)
	}
	return def
}
