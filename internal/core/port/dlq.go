package port

import "context"

// DLQPublisher forwards a failed message to the dead-letter queue configured
// on its source queue. Implemented in adapter/outbound/sqs; consumed by the
// SQS consumer's DLQ forwarding and exposed to services as events.DLQPublisher.
type DLQPublisher interface {
	SendToDLQ(ctx context.Context, sourceQueueURL string, body []byte, attrs map[string]string, reason string) error
	ResolveDLQ(ctx context.Context, sourceQueueURL string) (string, error)
}
