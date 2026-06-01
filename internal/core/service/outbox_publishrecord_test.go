package service

// White-box test for publishRecord's ctx.Err() fast-path (lines 154-162).
// The fast-path fires when a goroutine is dispatched from PublishBatch just
// before the context is cancelled — a narrow race window that cannot be
// triggered reliably from black-box tests. Calling publishRecord directly with
// a pre-cancelled context is the only deterministic way to cover these lines.

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/port"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
)

// stubStore is an in-memory port.OutboxStore for the white-box test.
type stubStore struct {
	failed map[string]string
}

func newStubStore() *stubStore { return &stubStore{failed: make(map[string]string)} }

func (s *stubStore) Enqueue(_ context.Context, _ pgx.Tx, _ domain.OutboxRecord) error { return nil }
func (s *stubStore) ClaimBatch(_ context.Context, _ int) ([]domain.OutboxRecord, error) {
	return nil, nil
}
func (s *stubStore) MarkPublished(_ context.Context, _ string) error { return nil }
func (s *stubStore) MarkFailed(_ context.Context, id, reason string, _ int) error {
	s.failed[id] = reason
	return nil
}
func (s *stubStore) PendingCount(_ context.Context) (int64, error) { return 0, nil }

var _ port.OutboxStore = (*stubStore)(nil)

// stubPublisher is a no-op port.Publisher.
type stubPublisher struct{}

func (stubPublisher) Publish(_ context.Context, _ domain.Envelope[json.RawMessage]) error {
	return nil
}
func (stubPublisher) PublishBatch(_ context.Context, _ []domain.Envelope[json.RawMessage]) error {
	return nil
}

var _ port.Publisher = stubPublisher{}

// TestPublishRecord_CtxCancelledBeforeRun covers the ctx.Err() != nil fast-path
// at the start of publishRecord (lines 154-162 in outbox_service.go).
func TestPublishRecord_CtxCancelledBeforeRun(t *testing.T) {
	store := newStubStore()
	svc := NewOutboxService(store, stubPublisher{}, nil, nil, 5, 1, 0)

	env := domain.NewEnvelope("cancel.event", "svc", json.RawMessage(`{}`))
	payload, _ := json.Marshal(env)
	rec := domain.OutboxRecord{ID: env.ID, EventType: env.Type, Payload: payload}

	// Pre-cancel the context so publishRecord immediately takes the fast-path.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	bookkeepCtx := context.Background()
	svc.publishRecord(ctx, bookkeepCtx, rec)

	assert.Contains(t, store.failed, rec.ID,
		"publishRecord should call MarkFailed when ctx is already cancelled")
	assert.Contains(t, store.failed[rec.ID], "context",
		"failure reason should mention context cancellation")
}
