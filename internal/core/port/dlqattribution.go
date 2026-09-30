package port

import (
	"context"
	"sync/atomic"
)

// DLQAttribution carries, through a handler's context, why a message would be
// dead-lettered and whether a DLQPublisher has already counted it. The SQS
// consumer attaches one per dispatch; the DLQ publisher reads the reason for
// platform_dlq_messages_total and marks the message counted, so a message is
// counted once whether it was forwarded automatically, by the dead-letter
// handler, or explicitly from a handler.
type DLQAttribution struct {
	reason   string
	recorded atomic.Bool
}

type dlqAttributionKey struct{}

// WithDLQAttribution returns ctx carrying a new attribution with reason.
func WithDLQAttribution(ctx context.Context, reason string) (context.Context, *DLQAttribution) {
	a := &DLQAttribution{reason: reason}
	return context.WithValue(ctx, dlqAttributionKey{}, a), a
}

// DLQAttributionFromContext returns the attribution in ctx, or nil.
func DLQAttributionFromContext(ctx context.Context) *DLQAttribution {
	a, _ := ctx.Value(dlqAttributionKey{}).(*DLQAttribution)
	return a
}

// Reason returns the attributed reason, or "explicit" for a nil attribution
// (a direct SendToDLQ call outside a consumer dispatch).
func (a *DLQAttribution) Reason() string {
	if a == nil || a.reason == "" {
		return "explicit"
	}
	return a.reason
}

// MarkRecorded notes that the message has been counted. Safe on nil.
func (a *DLQAttribution) MarkRecorded() {
	if a != nil {
		a.recorded.Store(true)
	}
}

// Recorded reports whether the message has been counted. False on nil.
func (a *DLQAttribution) Recorded() bool { return a != nil && a.recorded.Load() }
