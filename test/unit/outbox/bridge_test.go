package outbox_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/adapter/outbound/metrics"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/events"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/outbox"
)

// batchErrEventPublisher is an events.Publisher that returns a fixed *events.BatchError.
type batchErrEventPublisher struct {
	err *events.BatchError
}

func (p *batchErrEventPublisher) Publish(_ context.Context, _ events.Envelope[json.RawMessage]) error {
	if p.err == nil {
		return nil
	}
	return p.err
}

func (p *batchErrEventPublisher) PublishBatch(_ context.Context, _ []events.Envelope[json.RawMessage]) error {
	if p.err == nil {
		return nil
	}
	return p.err
}

var _ events.Publisher = (*batchErrEventPublisher)(nil)

// transportErrPublisher returns a plain transport error from PublishBatch.
type transportErrPublisher struct {
	err error
}

func (p *transportErrPublisher) Publish(_ context.Context, _ events.Envelope[json.RawMessage]) error {
	return p.err
}

func (p *transportErrPublisher) PublishBatch(_ context.Context, _ []events.Envelope[json.RawMessage]) error {
	return p.err
}

var _ events.Publisher = (*transportErrPublisher)(nil)

// TestPublisherBridge_PublishBatch_BatchError exercises the *events.BatchError path in
// bridge.go PublishBatch (lines 69-82). When the underlying publisher returns a BatchError
// with partial failures the bridge must record per-message status for failed vs succeeded
// IDs and return a domain.BatchError to the caller.
func TestPublisherBridge_PublishBatch_BatchError(t *testing.T) {
	reg := prometheus.NewRegistry()
	metrics.InitWithRegisterer("bridge-batch-err-test", "v0.0.1", reg)

	envA := domain.NewEnvelope("event.a", "svc", json.RawMessage(`{}`))
	envB := domain.NewEnvelope("event.b", "svc", json.RawMessage(`{}`))
	envC := domain.NewEnvelope("event.c", "svc", json.RawMessage(`{}`))

	// envA and envB fail; envC succeeds (not in Failures slice).
	batchErr := &events.BatchError{
		Failures: []events.BatchFailure{
			{ID: envA.ID, Code: "InternalError", Message: "delivery failed"},
			{ID: envB.ID, Code: "InternalError", Message: "delivery failed"},
		},
	}

	pub := &batchErrEventPublisher{err: batchErr}
	bridge := outbox.NewPublisherBridge(pub)

	envs := []domain.Envelope[json.RawMessage]{envA, envB, envC}
	err := bridge.PublishBatch(context.Background(), envs)

	require.Error(t, err)
	var returned *domain.BatchError
	require.ErrorAs(t, err, &returned, "bridge must return a *domain.BatchError")
	assert.Len(t, returned.Failures, 2, "both failures must be present in returned error")
}

// TestPublisherBridge_PublishBatch_TransportError exercises the transport-level error path
// (non-BatchError) where all messages share the same "error" status.
func TestPublisherBridge_PublishBatch_TransportError(t *testing.T) {
	reg := prometheus.NewRegistry()
	metrics.InitWithRegisterer("bridge-batch-transport-err-test", "v0.0.2", reg)

	pub := &transportErrPublisher{err: errors.New("sns unavailable")}
	bridge := outbox.NewPublisherBridge(pub)

	envA := domain.NewEnvelope("event.a", "svc", json.RawMessage(`{}`))
	err := bridge.PublishBatch(context.Background(), []domain.Envelope[json.RawMessage]{envA})
	require.Error(t, err)
}
