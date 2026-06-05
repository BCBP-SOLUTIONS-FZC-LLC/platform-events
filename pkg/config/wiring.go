package config

import (
	"fmt"
	"os"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/events"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/outbox"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/pgcommon"
)

// LogWarnings writes configuration warnings to stderr. Call after LoadSQS / LoadOutbox
// when Warnings is non-empty so operators see misconfigured env vars at startup.
func LogWarnings(warnings []string) {
	if len(warnings) == 0 {
		return
	}
	fmt.Fprintln(os.Stderr, "platform-events: configuration warnings (defaults applied):")
	for _, w := range warnings {
		fmt.Fprintf(os.Stderr, "  - %s\n", w)
	}
}

// SNSConfigFromEnv maps SNSConfigEnv to events.SNSConfig.
func SNSConfigFromEnv(env SNSConfigEnv, logger port.Logger) events.SNSConfig {
	return events.SNSConfig{
		TopicARN:    env.TopicARN,
		Region:      env.Region,
		EndpointURL: env.EndpointURL,
		Logger:      logger,
	}
}

// SQSConfigFromEnv maps SQSConfigEnv to events.SQSConfig.
func SQSConfigFromEnv(env SQSConfigEnv, logger port.Logger) events.SQSConfig {
	return events.SQSConfig{
		QueueURL:    env.QueueURL,
		Region:      env.Region,
		EndpointURL: env.EndpointURL,
		MaxMessages: env.MaxMessages,
		WaitSeconds: env.WaitSeconds,
		Logger:      logger,
	}
}

// SQSConsumerOptions returns ConsumerOption values derived from SQSConfigEnv.
// Pass alongside SQSConfigFromEnv when constructing NewSQSConsumer.
func SQSConsumerOptions(env SQSConfigEnv) []events.ConsumerOption {
	opts := []events.ConsumerOption{
		events.WithConcurrency(env.Concurrency),
		events.WithVisibilityTimeout(env.VisibilityTimeout),
	}
	if env.MaxReceiveCount > 0 {
		opts = append(opts, events.WithMaxReceiveCount(env.MaxReceiveCount))
	}
	return opts
}

// RunnerConfigFromEnv maps OutboxConfigEnv onto outbox.Config.
// Caller supplies Pool, Publisher, and Logger — these cannot come from env alone.
func RunnerConfigFromEnv(env OutboxConfigEnv, pool *pgcommon.Pool, publisher events.Publisher, logger port.Logger) outbox.Config {
	return outbox.Config{
		Pool:               pool,
		Publisher:          publisher,
		Logger:             logger,
		PollInterval:       env.PollInterval,
		BatchSize:          env.BatchSize,
		MaxAttempts:        env.MaxAttempts,
		ClaimLeaseDuration: env.ClaimLeaseDuration,
		StartupJitter:      env.StartupJitter,
		PublishConcurrency: env.PublishConcurrency,
		PublishTimeout:     env.PublishTimeout,
		DrainTimeout:       env.DrainTimeout,
	}
}
