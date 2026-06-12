package outbox

import (
	"context"
	"encoding/json"
	"errors"

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
		SchemaVersion: env.SchemaVersion,
		TenantID:      env.TenantID,
		TraceID:       env.TraceID,
		CorrelationID: env.CorrelationID,
		Timestamp:     env.Timestamp,
		Payload:       env.Payload,
	}
	err := b.pub.Publish(ctx, pub)

	metrics.RecordOutboxAttempt(env.Type)
	status := "success"
	if err != nil {
		status = "error"
	}
	metrics.RecordOutboxPublished(env.Type, status)
	return err
}

func (b *publisherBridge) PublishBatch(ctx context.Context, envs []domain.Envelope[json.RawMessage]) error {
	pubEnvs := make([]events.Envelope[json.RawMessage], len(envs))
	for i, e := range envs {
		pubEnvs[i] = events.Envelope[json.RawMessage]{
			ID:            e.ID,
			Type:          e.Type,
			Source:        e.Source,
			SchemaVersion: e.SchemaVersion,
			TenantID:      e.TenantID,
			TraceID:       e.TraceID,
			CorrelationID: e.CorrelationID,
			Timestamp:     e.Timestamp,
			Payload:       e.Payload,
		}
	}
	err := b.pub.PublishBatch(ctx, pubEnvs)

	// Record metrics post-call so attempt and published counters are always
	// consistent: a panicking publisher leaves both at 0 rather than creating a
	// permanent mismatch (attempts > published{success+error}) on a Grafana dashboard.

	// For partial failures (*events.BatchError), record per-message status so
	// outbox_published_total accurately reflects which messages were delivered.
	var batchErr *events.BatchError
	if errors.As(err, &batchErr) {
		failedIDs := make(map[string]struct{}, len(batchErr.Failures))
		for _, f := range batchErr.Failures {
			failedIDs[f.ID] = struct{}{}
		}
		for _, e := range envs {
			metrics.RecordOutboxAttempt(e.Type)
			if _, failed := failedIDs[e.ID]; failed {
				metrics.RecordOutboxPublished(e.Type, "error")
			} else {
				metrics.RecordOutboxPublished(e.Type, "success")
			}
		}
		return toDomainBatchError(batchErr)
	}
	// Transport-level error or nil: all messages share the same outcome.
	status := "success"
	if err != nil {
		status = "error"
	}
	for _, e := range envs {
		metrics.RecordOutboxAttempt(e.Type)
		metrics.RecordOutboxPublished(e.Type, status)
	}
	return err
}

func toDomainBatchError(batchErr *events.BatchError) *domain.BatchError {
	out := &domain.BatchError{Failures: make([]domain.BatchFailure, len(batchErr.Failures))}
	for i, f := range batchErr.Failures {
		out.Failures[i] = domain.BatchFailure{
			ID:      f.ID,
			Code:    f.Code,
			Message: f.Message,
		}
	}
	return out
}
