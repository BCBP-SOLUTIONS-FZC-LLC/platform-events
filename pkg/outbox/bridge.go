package outbox

import (
	"context"
	"encoding/json"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/adapter/outbound/metrics"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/events"
)

// publisherBridge adapts events.Publisher (public) to port.Publisher (internal).
type publisherBridge struct {
	pub events.Publisher
}

// NewPublisherBridge wraps an events.Publisher as the internal port.Publisher.
// Exported for testing; callers that need a port.Publisher from an events.Publisher
// can use this instead of constructing a Runner.
func NewPublisherBridge(pub events.Publisher) port.Publisher {
	return &publisherBridge{pub: pub}
}

func (b *publisherBridge) Publish(ctx context.Context, env domain.Envelope[json.RawMessage]) error {
	pub := events.Envelope[json.RawMessage]{
		ID:            env.ID,
		Type:          env.Type,
		Source:        env.Source,
		TenantID:      env.TenantID,
		TraceID:       env.TraceID,
		CorrelationID: env.CorrelationID,
		Timestamp:     env.Timestamp,
		Payload:       env.Payload,
	}
	err := b.pub.Publish(ctx, pub)

	// Track every publish attempt (success or failure) as a per-record metric.
	if metrics.OutboxAttemptsTotal != nil {
		metrics.OutboxAttemptsTotal.WithLabelValues().Inc()
	}
	if metrics.OutboxPublishedTotal != nil {
		status := "success"
		if err != nil {
			status = "error"
		}
		metrics.OutboxPublishedTotal.WithLabelValues(status).Inc()
	}
	return err
}

func (b *publisherBridge) PublishBatch(ctx context.Context, envs []domain.Envelope[json.RawMessage]) error {
	pubEnvs := make([]events.Envelope[json.RawMessage], len(envs))
	for i, e := range envs {
		pubEnvs[i] = events.Envelope[json.RawMessage]{
			ID:            e.ID,
			Type:          e.Type,
			Source:        e.Source,
			TenantID:      e.TenantID,
			TraceID:       e.TraceID,
			CorrelationID: e.CorrelationID,
			Timestamp:     e.Timestamp,
			Payload:       e.Payload,
		}
	}
	return b.pub.PublishBatch(ctx, pubEnvs)
}
