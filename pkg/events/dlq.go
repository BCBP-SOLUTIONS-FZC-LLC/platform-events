package events

import (
	"context"

	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"

	internalsqs "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/adapter/outbound/sqs"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/port"
)

// Standard message attributes set on every message forwarded by a DLQPublisher.
const (
	DLQAttrEventType     = internalsqs.DLQAttrEventType
	DLQAttrReason        = internalsqs.DLQAttrReason
	DLQAttrOriginalQueue = internalsqs.DLQAttrOriginalQueue
	DLQAttrConsumerName  = internalsqs.DLQAttrConsumerName
	DLQAttrFailedAt      = internalsqs.DLQAttrFailedAt
)

// DLQPublisher forwards failed (poison) messages to the dead-letter queue
// already configured on a source queue's RedrivePolicy, so consumers can
// dead-letter a message explicitly without importing the AWS SQS SDK.
//
// Every error is a *DLQError; branch on it with errors.Is:
//
//	switch {
//	case errors.Is(err, events.ErrRetryable):
//	    // transient (throttling, service unavailable, timeout) — retry later
//	case errors.Is(err, events.ErrDLQNotConfigured),
//	    errors.Is(err, events.ErrDLQInvalidRedrivePolicy):
//	    // queue configuration problem — alert, do not retry
//	}
//
// Typical use is from a WithDeadLetterHandler callback, or from a handler that
// detects a message it can never process: forward it, then return nil so the
// consumer deletes the original. Return the error instead when SendToDLQ
// fails, so the original stays on the source queue.
type DLQPublisher interface {
	// SendToDLQ publishes body to sourceQueueURL's DLQ. attrs are forwarded
	// as String message attributes; DLQReason (reason, required), OriginalQueue,
	// FailedAt (RFC 3339, UTC), ConsumerName (when configured) and EventType
	// (from the envelope body, else attrs["EventType"], else "unknown") are
	// added. SQS allows 10 attributes per message, 4–5 of which are reserved.
	SendToDLQ(ctx context.Context, sourceQueueURL string, body []byte, attrs map[string]string, reason string) error

	// ResolveDLQ returns the URL of sourceQueueURL's DLQ. The lookup is cached.
	// Call it at startup to fail fast on a missing or malformed RedrivePolicy.
	ResolveDLQ(ctx context.Context, sourceQueueURL string) (string, error)
}

// DLQConfig configures a DLQPublisher.
//
// IAM: the caller needs sqs:GetQueueAttributes on each source queue and
// sqs:GetQueueUrl + sqs:SendMessage on its DLQ (plus kms:GenerateDataKey and
// kms:Decrypt when the DLQ uses a customer-managed KMS key).
type DLQConfig struct {
	Region      string
	EndpointURL string // optional — LocalStack endpoint for testing
	// ConsumerName is attached to every forwarded message as ConsumerName.
	ConsumerName string
	Logger       port.Logger
}

// DLQClientLike is the subset of the AWS SQS client API used by the DLQ
// publisher. *sqs.Client from aws-sdk-go-v2 satisfies this interface.
// Consuming services should prefer mock.DLQPublisher in their tests.
type DLQClientLike interface {
	GetQueueAttributes(ctx context.Context, params *awssqs.GetQueueAttributesInput, optFns ...func(*awssqs.Options)) (*awssqs.GetQueueAttributesOutput, error)
	GetQueueUrl(ctx context.Context, params *awssqs.GetQueueUrlInput, optFns ...func(*awssqs.Options)) (*awssqs.GetQueueUrlOutput, error)
	SendMessage(ctx context.Context, params *awssqs.SendMessageInput, optFns ...func(*awssqs.Options)) (*awssqs.SendMessageOutput, error)
}

// NewSQSDLQPublisher constructs an SQS-backed DLQPublisher.
func NewSQSDLQPublisher(cfg DLQConfig) (DLQPublisher, error) {
	p, err := internalsqs.NewDLQPublisher(toInternalDLQConfig(cfg))
	if err != nil {
		return nil, err
	}
	return p, nil
}

// NewSQSDLQPublisherWithClient constructs an SQS-backed DLQPublisher using an
// injected SQS client. Useful in unit tests to avoid real AWS credentials.
func NewSQSDLQPublisherWithClient(cfg DLQConfig, client DLQClientLike) (DLQPublisher, error) {
	if client == nil {
		// Checked here too: a nil DLQClientLike would otherwise arrive at the
		// internal constructor as a non-nil interface.
		return nil, internalsqs.ErrNilDLQClient
	}
	p, err := internalsqs.NewDLQPublisherWithClient(toInternalDLQConfig(cfg), client)
	if err != nil {
		return nil, err
	}
	return p, nil
}

func toInternalDLQConfig(cfg DLQConfig) internalsqs.DLQConfig {
	return internalsqs.DLQConfig{
		Region:       cfg.Region,
		EndpointURL:  cfg.EndpointURL,
		ConsumerName: cfg.ConsumerName,
		Logger:       cfg.Logger,
	}
}
