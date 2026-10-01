package port

import (
	"context"
	"encoding/json"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/internal/core/domain"
)

// Publisher publishes event envelopes to a message broker.
type Publisher interface {
	Publish(ctx context.Context, env domain.Envelope[json.RawMessage]) error
	PublishBatch(ctx context.Context, envs []domain.Envelope[json.RawMessage]) error
}
