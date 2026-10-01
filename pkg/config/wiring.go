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
// Use LogWarningsTo when a structured logger is available so warnings are captured
// by the log pipeline (e.g. Loki, CloudWatch) rather than only on stderr.
func LogWarnings(warnings []string) {
	if len(warnings) == 0 {
		return
	}
	fmt.Fprintln(os.Stderr, "platform-events: configuration warnings (defaults applied):")
	for _, w := range warnings {
		fmt.Fprintf(os.Stderr, "  - %s\n", w)
	}
}

// LogWarningsTo emits configuration warnings via the provided structured logger.
// Each warning is logged at WARN level with a "warning" field so log aggregators
// capture it alongside application logs. Falls back to stderr when logger is nil.
// Call after LoadSQS / LoadOutbox when Warnings is non-empty:
//
//	sqsEnv := config.LoadSQS()
//	config.LogWarningsTo(logger, sqsEnv.Warnings)
func LogWarningsTo(logger port.Logger, warnings []string) {
	if len(warnings) == 0 {
		return
	}
	if logger == nil {
		LogWarnings(warnings)
		return
	}
	for _, w := range warnings {
		logger.Warn("platform-events: configuration warning (default applied)", map[string]interface{}{
			"warning": w,
		})
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
//
// IMPORTANT: this helper does NOT wire WithDeadLetterHandler or
// WithDLQForwarding — neither can be configured from env vars alone. The
// SQS_MAX_RECEIVE_COUNT threshold only takes effect with at least one of them:
//
//	opts := config.SQSConsumerOptions(sqsEnv)
//	opts = append(opts, events.WithDLQForwarding(dlq))           // forward to the queue's DLQ
//	opts = append(opts, events.WithDeadLetterHandler(myDLHFunc)) // and/or run a callback first
//
// With neither, there is no consumer-side threshold: a message that keeps
// failing goes to your normal handler on every delivery until the queue's own
// RedrivePolicy moves it to the DLQ.
func SQSConsumerOptions(env SQSConfigEnv) []events.ConsumerOption {
	opts := []events.ConsumerOption{
		events.WithConcurrency(env.Concurrency),
		events.WithVisibilityTimeout(env.VisibilityTimeout),
	}
	if env.MaxReceiveCount > 0 {
		opts = append(opts, events.WithMaxReceiveCount(env.MaxReceiveCount))
	}
	if env.QueueDepthInterval > 0 {
		opts = append(opts, events.WithQueueDepthMetrics(env.QueueDepthInterval))
	}
	if env.HandlerTimeout > 0 {
		opts = append(opts, events.WithHandlerTimeout(env.HandlerTimeout))
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
		RetryBackoff:       env.RetryBackoff,
		MaxRetryBackoff:    env.MaxRetryBackoff,
		GaugeInterval:      env.GaugeInterval,
	}
}
