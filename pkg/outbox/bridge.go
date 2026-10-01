package outbox

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/internal/adapter/outbound/metrics"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/events"
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
		Subject:       env.Subject,
		Actor:         env.Actor,
		IPAddress:     env.IPAddress,
		UserAgent:     env.UserAgent,
		SchemaID:      env.SchemaID,
		Timestamp:     env.Timestamp,
		Payload:       env.Payload,
	}
	err := b.pub.Publish(ctx, pub)
	status := "success"
	if err != nil {
		status = "error"
	}
	metrics.IncOutboxPublishAttempt(env.Type, status)
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
			Subject:       e.Subject,
			Actor:         e.Actor,
			IPAddress:     e.IPAddress,
			UserAgent:     e.UserAgent,
			SchemaID:      e.SchemaID,
			Timestamp:     e.Timestamp,
			Payload:       e.Payload,
		}
	}
	err := b.pub.PublishBatch(ctx, pubEnvs)

	// Record metrics post-call, so a panicking publisher records nothing. For
	// partial failures (*events.BatchError), record a per-message outcome so
	// platform_outbox_publish_attempts_total reflects which messages were delivered.
	var batchErr *events.BatchError
	if errors.As(err, &batchErr) {
		failedIDs := make(map[string]struct{}, len(batchErr.Failures))
		for _, f := range batchErr.Failures {
			failedIDs[f.ID] = struct{}{}
		}
		for _, e := range envs {
			if _, failed := failedIDs[e.ID]; failed {
				metrics.IncOutboxPublishAttempt(e.Type, "error")
			} else {
				metrics.IncOutboxPublishAttempt(e.Type, "success")
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
		metrics.IncOutboxPublishAttempt(e.Type, status)
	}
	return err
}

func toDomainBatchError(batchErr *events.BatchError) *domain.BatchError {
	out := &domain.BatchError{Failures: make([]domain.BatchFailure, len(batchErr.Failures))}
	for i, f := range batchErr.Failures {
		out.Failures[i] = domain.BatchFailure{
			ID:        f.ID,
			Code:      f.Code,
			Message:   f.Message,
			Retryable: f.Retryable,
		}
	}
	return out
}
