package port_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/internal/core/port"
)

func TestEnvelopeTraceIDFromContext_Present(t *testing.T) {
	ctx := port.WithEnvelopeTraceID(context.Background(), "trace-abc-123")
	got := port.EnvelopeTraceIDFromContext(ctx)
	assert.Equal(t, "trace-abc-123", got)
}

func TestEnvelopeTraceIDFromContext_Missing(t *testing.T) {
	got := port.EnvelopeTraceIDFromContext(context.Background())
	assert.Equal(t, "", got)
}

func TestEnvelopeTraceIDFromContext_Overwrite(t *testing.T) {
	ctx := port.WithEnvelopeTraceID(context.Background(), "first")
	ctx = port.WithEnvelopeTraceID(ctx, "second")
	got := port.EnvelopeTraceIDFromContext(ctx)
	assert.Equal(t, "second", got)
}

func TestEnvelopeTraceIDFromContext_EmptyString(t *testing.T) {
	ctx := port.WithEnvelopeTraceID(context.Background(), "")
	got := port.EnvelopeTraceIDFromContext(ctx)
	assert.Equal(t, "", got)
}
