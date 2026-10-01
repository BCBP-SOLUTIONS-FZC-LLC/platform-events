package port

import (
	"context"
	"encoding/json"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/internal/core/domain"
)

// Handler processes a single event message.
// Returning a non-nil error skips message deletion; the message becomes visible
// again after the visibility timeout for retry.
type Handler func(ctx context.Context, env domain.Envelope[json.RawMessage]) error

// Consumer receives and dispatches event messages from a queue.
type Consumer interface {
	Start(ctx context.Context) error
	Stop() error
}
