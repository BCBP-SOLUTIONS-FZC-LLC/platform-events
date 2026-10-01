package port

import "context"

// envelopeTraceIDKey is the unexported context key used to carry an event
// envelope's TraceID through the SQS consumer handler call chain.
type envelopeTraceIDKey struct{}

// WithEnvelopeTraceID stores traceID in ctx so that handler code can retrieve
// it via EnvelopeTraceIDFromContext.
func WithEnvelopeTraceID(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, envelopeTraceIDKey{}, traceID)
}

// EnvelopeTraceIDFromContext returns the envelope TraceID stored by
// WithEnvelopeTraceID, or an empty string if none is present.
func EnvelopeTraceIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(envelopeTraceIDKey{}).(string); ok {
		return v
	}
	return ""
}

// SourceMessage is the transport message a consumer handler's envelope was
// parsed from, exactly as received — before codec decoding. Forward Body and
// Attributes (not a re-serialised Envelope) when dead-lettering, so a codec-
// encoded payload keeps its wire format and the original attributes survive.
type SourceMessage struct {
	// QueueURL is the queue the message was received from.
	QueueURL string
	// MessageID is the transport-assigned message ID.
	MessageID string
	// Body is the raw message body.
	Body []byte
	// Attributes holds the String and Number message attributes by name.
	// Binary attributes are omitted.
	Attributes map[string]string
	// ReceiveCount is the transport's approximate delivery count (0 if unknown).
	ReceiveCount int
}

type sourceMessageKey struct{}

// WithSourceMessage stores load in ctx for SourceMessageFromContext. load is
// called on every retrieval and must return a fresh SourceMessage (its own
// Body and Attributes) each time; deferring the copy keeps it off the hot path
// of handlers that never ask for the source message.
func WithSourceMessage(ctx context.Context, load func() SourceMessage) context.Context {
	return context.WithValue(ctx, sourceMessageKey{}, load)
}

// SourceMessageFromContext returns the SourceMessage stored by
// WithSourceMessage. Body and Attributes are copies the caller may modify.
func SourceMessageFromContext(ctx context.Context) (SourceMessage, bool) {
	load, ok := ctx.Value(sourceMessageKey{}).(func() SourceMessage)
	if !ok || load == nil {
		return SourceMessage{}, false
	}
	return load(), true
}
