package events

import (
	"context"
	"encoding/json"
	"time"

	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"

	internalsqs "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/adapter/outbound/sqs"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/port"
)

// Consumer receives and dispatches event messages from a queue.
type Consumer interface {
	Start(ctx context.Context) error
	Stop() error
}

// Handler processes a single event message.
// Returning non-nil skips message deletion; the message becomes visible after
// the visibility timeout for retry.
//
// Context contract: the ctx passed to each handler has its cancellation signal
// stripped (via context.WithoutCancel) so handlers run to completion during
// graceful shutdown. As a consequence, ctx.Deadline() always returns a zero
// time — handlers must set their own timeouts instead of relying on the
// parent deadline. Cancellation is delivered only when the consumer's drain
// timeout expires, after which the context is cancelled.
type Handler func(ctx context.Context, env Envelope[json.RawMessage]) error

// SQSConfig configures an SQS consumer.
//
// SNS subscription requirement: if this queue receives messages via an SNS
// subscription, the subscription MUST be created with RawMessageDelivery=true.
// Without raw delivery, SNS wraps each message in a notification envelope that
// the consumer cannot parse — messages are permanently deleted as malformed.
type SQSConfig struct {
	// QueueURL is required.
	QueueURL    string
	Region      string
	EndpointURL string // optional — LocalStack endpoint for testing
	MaxMessages int32  // 1-10; defaults to 10
	WaitSeconds int32  // long-poll duration; defaults to 20
	Logger      port.Logger
}

// ConsumerOption is a functional option for the SQS consumer.
type ConsumerOption = internalsqs.ConsumerOption

// WithConcurrency sets the number of concurrent handler goroutines.
func WithConcurrency(n int) ConsumerOption {
	return internalsqs.WithConcurrency(n)
}

// WithVisibilityTimeout sets the SQS visibility timeout for in-flight messages.
func WithVisibilityTimeout(d time.Duration) ConsumerOption {
	return internalsqs.WithVisibilityTimeout(d)
}

// WithDeadLetterHandler sets a handler for messages that have exhausted retries.
func WithDeadLetterHandler(fn Handler) ConsumerOption {
	return internalsqs.WithDeadLetterHandler(func(ctx context.Context, env domain.Envelope[json.RawMessage]) error {
		return fn(ctx, domainToPublic(env))
	})
}

// WithDrainTimeout sets how long Stop() waits for in-flight handlers to finish.
func WithDrainTimeout(d time.Duration) ConsumerOption {
	return internalsqs.WithDrainTimeout(d)
}

// WithMaxReceiveCount sets the ApproximateReceiveCount threshold at which a message
// is routed to the dead-letter handler. Requires WithDeadLetterHandler to be set.
func WithMaxReceiveCount(n int) ConsumerOption {
	return internalsqs.WithMaxReceiveCount(n)
}

// WithConsumerCodec sets the Codec used to decode incoming envelope payloads
// whose SchemaID is non-empty. Unset (nil), all messages are treated as
// plain JSON exactly as before this option existed. Named distinctly from
// the publisher's WithCodec because both option constructors live in this
// package and Go forbids two functions with the same name.
func WithConsumerCodec(codec Codec) ConsumerOption {
	return internalsqs.WithCodec(codec)
}

// SQSClientLike is the subset of the AWS SQS client API used by the consumer.
// *sqs.Client from aws-sdk-go-v2 satisfies this interface. Implement it in
// tests to inject a mock SQS client without real AWS credentials.
type SQSClientLike interface {
	ReceiveMessage(ctx context.Context, params *awssqs.ReceiveMessageInput, optFns ...func(*awssqs.Options)) (*awssqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, params *awssqs.DeleteMessageInput, optFns ...func(*awssqs.Options)) (*awssqs.DeleteMessageOutput, error)
	ChangeMessageVisibility(ctx context.Context, params *awssqs.ChangeMessageVisibilityInput, optFns ...func(*awssqs.Options)) (*awssqs.ChangeMessageVisibilityOutput, error)
}

// NewSQSConsumer constructs an SQS-backed Consumer.
func NewSQSConsumer(cfg SQSConfig, handler Handler, opts ...ConsumerOption) (Consumer, error) {
	wrappedHandler := func(ctx context.Context, env domain.Envelope[json.RawMessage]) error {
		return handler(ctx, domainToPublic(env))
	}
	return internalsqs.New(internalsqs.Config{
		QueueURL:    cfg.QueueURL,
		Region:      cfg.Region,
		EndpointURL: cfg.EndpointURL,
		MaxMessages: cfg.MaxMessages,
		WaitSeconds: cfg.WaitSeconds,
		Logger:      cfg.Logger,
	}, port.Handler(wrappedHandler), opts...)
}

// NewSQSConsumerWithClient constructs an SQS-backed Consumer using an injected
// SQS client. Useful in unit tests to avoid real AWS credentials.
func NewSQSConsumerWithClient(cfg SQSConfig, client SQSClientLike, handler Handler, opts ...ConsumerOption) (Consumer, error) {
	wrappedHandler := func(ctx context.Context, env domain.Envelope[json.RawMessage]) error {
		return handler(ctx, domainToPublic(env))
	}
	return internalsqs.NewWithClient(internalsqs.Config{
		QueueURL:    cfg.QueueURL,
		Region:      cfg.Region,
		EndpointURL: cfg.EndpointURL,
		MaxMessages: cfg.MaxMessages,
		WaitSeconds: cfg.WaitSeconds,
		Logger:      cfg.Logger,
	}, client, port.Handler(wrappedHandler), opts...)
}
