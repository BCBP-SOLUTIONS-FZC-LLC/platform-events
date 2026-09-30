package events

import (
	"context"
	"time"

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
// Most consumers need no direct calls: pass the publisher to the consumer via
// WithDLQForwarding and malformed or over-threshold messages are forwarded
// automatically. To dead-letter explicitly from a handler that detects a
// message it can never process, forward the original transport message from
// SourceMessageFromContext — not a re-serialised Envelope, which has lost the
// message attributes and, for codec-encoded events, holds a decoded payload
// under a still-set SchemaID — then return nil so the consumer deletes the
// original. Return the error instead when SendToDLQ fails, so the original
// stays on the source queue.
//
// SendToDLQ(ctx, sourceQueueURL, body, attrs, reason) publishes body to
// sourceQueueURL's DLQ. attrs are forwarded as String message attributes;
// DLQReason (reason, required), OriginalQueue, FailedAt (RFC 3339, UTC),
// ConsumerName (when configured) and EventType (from the envelope body, else
// attrs["EventType"], else "unknown") are added. SQS allows 10 attributes per
// message, 4–5 of which are reserved; excess caller attributes are dropped
// lowest-priority first unless DLQConfig.StrictAttributes is set.
//
// ResolveDLQ(ctx, sourceQueueURL) returns the URL of sourceQueueURL's DLQ. The
// lookup is cached (DLQConfig.CacheTTL). Call it at startup to fail fast on a
// missing or malformed RedrivePolicy.
type DLQPublisher = port.DLQPublisher

// SourceMessage is the transport message a handler's envelope was parsed
// from, before codec decoding. See SourceMessageFromContext.
type SourceMessage = port.SourceMessage

// SourceMessageFromContext returns the original SQS message (raw body, String
// and Number attributes, queue URL, receive count) for the envelope being
// handled. It is set on the ctx passed to Handler and WithDeadLetterHandler
// callbacks by the SQS consumer; ok is false elsewhere.
//
//	if src, ok := events.SourceMessageFromContext(ctx); ok {
//	    err := dlq.SendToDLQ(ctx, src.QueueURL, src.Body, src.Attributes, "unsupported tenant")
//	}
func SourceMessageFromContext(ctx context.Context) (SourceMessage, bool) {
	return port.SourceMessageFromContext(ctx)
}

// DLQConfig configures a DLQPublisher.
//
// IAM: the caller needs sqs:GetQueueAttributes on each source queue and
// sqs:GetQueueUrl + sqs:SendMessage on its DLQ (plus kms:GenerateDataKey and
// kms:Decrypt when the DLQ uses a customer-managed KMS key).
type DLQConfig struct {
	Region      string
	EndpointURL string // optional — AWS emulator (floci) endpoint for local runs and tests
	// ConsumerName is attached to every forwarded message as ConsumerName.
	ConsumerName string
	Logger       port.Logger
	// CacheTTL bounds how long a resolved DLQ is reused before the source
	// queue's RedrivePolicy is read again. Zero means 15 minutes; negative
	// disables expiry.
	CacheTTL time.Duration
	// StrictAttributes rejects (ErrDLQInvalidMessage) a message whose
	// attributes exceed the SQS limit of 10, instead of dropping the
	// lowest-priority caller attributes.
	StrictAttributes bool
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
		Region:           cfg.Region,
		EndpointURL:      cfg.EndpointURL,
		ConsumerName:     cfg.ConsumerName,
		Logger:           cfg.Logger,
		CacheTTL:         cfg.CacheTTL,
		StrictAttributes: cfg.StrictAttributes,
	}
}
