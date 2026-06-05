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
