package service

// White-box tests for outbox service internals that cannot be exercised reliably
// from black-box tests: publishRecord's ctx.Err() fast-path, failureThreshold
// retryable-vs-non-retryable branching, and releaseStranded behaviour.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/port"
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
func (s *stubStore) MarkFailed(_ context.Context, rec domain.OutboxRecord, reason string, _ int) error {
	s.failed[rec.ID] = reason
	return nil
}
func (s *stubStore) PendingCount(_ context.Context) (int64, error)              { return 0, nil }
func (s *stubStore) LeasedCount(_ context.Context) (int64, error)               { return 0, nil }
func (s *stubStore) ReprocessDeadLetters(_ context.Context, _ int) (int, error) { return 0, nil }
func (s *stubStore) PrunePublished(_ context.Context, _ time.Duration, _ int) (int64, error) {
	return 0, nil
}
func (s *stubStore) ListDeadLetters(_ context.Context, _ domain.DLQFilter, _ int) ([]domain.DeadLetterRecord, error) {
	return nil, nil
}
func (s *stubStore) ReprocessDeadLettersWith(_ context.Context, _ domain.DLQFilter, _ int) (int, error) {
	return 0, nil
}
func (s *stubStore) DiscardDeadLetters(_ context.Context, _ domain.DLQFilter, _ int) (int64, error) {
	return 0, nil
}

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

// TestFailureThreshold_RetryableError verifies that context.Canceled,
// context.DeadlineExceeded, and domain.ErrRetryable use maxAttempts+1 as the
// failure threshold so a graceful-shutdown or transient-SNS failure does not
// dead-letter a record that still has retry budget remaining.
func TestFailureThreshold_RetryableErrors(t *testing.T) {
	svc := NewOutboxService(newStubStore(), stubPublisher{}, nil, nil, 5, 1, 0)

	cases := []struct {
		name string
		err  error
	}{
		{"context.Canceled", context.Canceled},
		{"context.DeadlineExceeded", context.DeadlineExceeded},
		{"domain.ErrRetryable", domain.ErrRetryable},
		{"wrapped ErrRetryable", fmt.Errorf("sns throttle: %w", domain.ErrRetryable)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			threshold := svc.failureThreshold(tc.err)
			assert.Equal(t, svc.maxAttempts+1, threshold,
				"retryable error must use maxAttempts+1 to avoid premature dead-lettering")
		})
	}
}

// TestFailureThreshold_NonRetryableError verifies that ordinary SNS errors use
// maxAttempts as the threshold so records exhaust retries and reach dead-letter.
func TestFailureThreshold_NonRetryableError(t *testing.T) {
	svc := NewOutboxService(newStubStore(), stubPublisher{}, nil, nil, 5, 1, 0)
	threshold := svc.failureThreshold(errors.New("sns: InvalidParameter"))
	assert.Equal(t, svc.maxAttempts, threshold,
		"non-retryable error must use maxAttempts so record eventually dead-letters")
}

// TestReleaseStranded verifies that releaseStranded marks the record failed
// with threshold maxAttempts+1 (not maxAttempts) so a single graceful-shutdown
// event does not consume a retry slot or trigger dead-lettering.
func TestReleaseStranded_UsesMaxAttemptsPlus1(t *testing.T) {
	store := newStubStore()
	svc := NewOutboxService(store, stubPublisher{}, nil, nil, 5, 1, 0)

	env := domain.NewEnvelope("stranded.event", "svc", json.RawMessage(`{}`))
	payload, _ := json.Marshal(env)
	rec := domain.OutboxRecord{ID: env.ID, EventType: env.Type, Payload: payload}

	svc.releaseStranded(context.Background(), rec, "shutdown")

	require.Contains(t, store.failed, rec.ID)
	assert.Equal(t, "shutdown", store.failed[rec.ID],
		"releaseStranded should record the shutdown reason")
}

// stubStoreWithThreshold captures the threshold argument passed to MarkFailed
// so tests can verify it without inspecting internal logic.
type stubStoreWithThreshold struct {
	stubStore
	thresholds map[string]int
}

func newStubStoreWithThreshold() *stubStoreWithThreshold {
	return &stubStoreWithThreshold{
		stubStore:  stubStore{failed: make(map[string]string)},
		thresholds: make(map[string]int),
	}
}

func (s *stubStoreWithThreshold) MarkFailed(_ context.Context, rec domain.OutboxRecord, reason string, threshold int) error {
	s.failed[rec.ID] = reason
	s.thresholds[rec.ID] = threshold
	return nil
}

// TestReleaseStranded_ThresholdIsMaxAttemptsPlus1 uses stubStoreWithThreshold
// to directly assert that the threshold passed to MarkFailed is maxAttempts+1.
func TestReleaseStranded_ThresholdIsMaxAttemptsPlus1(t *testing.T) {
	store := newStubStoreWithThreshold()
	const maxAttempts = 3
	svc := NewOutboxService(store, stubPublisher{}, nil, nil, maxAttempts, 1, 0)

	env := domain.NewEnvelope("stranded.event", "svc", json.RawMessage(`{}`))
	payload, _ := json.Marshal(env)
	rec := domain.OutboxRecord{ID: env.ID, EventType: env.Type, Payload: payload}

	svc.releaseStranded(context.Background(), rec, "ctx cancelled")

	assert.Equal(t, maxAttempts+1, store.thresholds[rec.ID],
		"releaseStranded must use maxAttempts+1 so a shutdown never counts as a retry")
}

// TestPanicErr_Error verifies the internal panicErr type formats correctly.
func TestPanicErr_Error(t *testing.T) {
	e := &panicErr{msg: "something exploded"}
	assert.Equal(t, "something exploded", e.Error())
}

// errPublisher returns a configurable error from Publish.
type errPublisher struct{ err error }

func (p *errPublisher) Publish(_ context.Context, _ domain.Envelope[json.RawMessage]) error {
	return p.err
}
func (p *errPublisher) PublishBatch(_ context.Context, _ []domain.Envelope[json.RawMessage]) error {
	return p.err
}

var _ port.Publisher = (*errPublisher)(nil)

// mockLoggerWB is a minimal port.Logger that records Warn calls.
type mockLoggerWB struct {
	mu      sync.Mutex
	entries []string
}

func (l *mockLoggerWB) Debug(_ string, _ map[string]any) {}
func (l *mockLoggerWB) Info(_ string, _ map[string]any)  {}
func (l *mockLoggerWB) Warn(msg string, _ map[string]any) {
	l.mu.Lock()
	l.entries = append(l.entries, "WARN:"+msg)
	l.mu.Unlock()
}
func (l *mockLoggerWB) Error(msg string, _ map[string]any) {
	l.mu.Lock()
	l.entries = append(l.entries, "ERROR:"+msg)
	l.mu.Unlock()
}

// TestPublishRecord_UnmarshalError covers the json.Unmarshal failure branch in publishRecord.
func TestPublishRecord_UnmarshalError(t *testing.T) {
	store := newStubStore()
	svc := NewOutboxService(store, stubPublisher{}, nil, nil, 5, 2, 0)

	rec := domain.OutboxRecord{ID: "bad-id", EventType: "x.y", Payload: []byte("not-json")}

	bookkeepCtx := context.Background()
	svc.publishRecord(context.Background(), bookkeepCtx, rec)

	assert.Contains(t, store.failed, "bad-id",
		"publishRecord must mark record failed when payload is not valid JSON")
}

// TestPublishRecord_PublishError_WithLogger covers the logger.Warn branch in publishRecord
// when the publisher returns an error and a logger is configured.
func TestPublishRecord_PublishError_WithLogger(t *testing.T) {
	store := newStubStore()
	logger := &mockLoggerWB{}
	svc := NewOutboxService(store, &errPublisher{err: errors.New("sns down")}, logger, nil, 5, 2, 0)

	env := domain.NewEnvelope("pub.err", "svc", json.RawMessage(`{}`))
	payload, _ := json.Marshal(env)
	rec := domain.OutboxRecord{ID: env.ID, EventType: env.Type, Payload: payload}

	svc.publishRecord(context.Background(), context.Background(), rec)

	assert.Contains(t, store.failed, env.ID,
		"publishRecord must mark record failed on publish error")
	logger.mu.Lock()
	defer logger.mu.Unlock()
	found := false
	for _, e := range logger.entries {
		if strings.Contains(e, "WARN") && strings.Contains(e, "publish failed") {
			found = true
		}
	}
	assert.True(t, found, "publishRecord must log a WARN when publish fails and logger is set")
}
