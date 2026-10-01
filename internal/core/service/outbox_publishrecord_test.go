package service

// White-box tests for outbox service internals that cannot be exercised reliably
// from black-box tests: publishRecord's ctx.Err() fast-path, transient vs
// permanent failure handling, the retry backoff, and releaseStranded.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/port"
)

// stubStore is an in-memory port.OutboxStore for the white-box test. failed
// records MarkFailed calls (attempt counted); released records ReleaseLease
// calls (no attempt) with their retry delays.
type stubStore struct {
	failed     map[string]string
	retryAfter map[string]time.Duration
	released   map[string]string
	releasedIn map[string]time.Duration
}

func newStubStore() *stubStore {
	return &stubStore{
		failed: map[string]string{}, retryAfter: map[string]time.Duration{},
		released: map[string]string{}, releasedIn: map[string]time.Duration{},
	}
}

func (s *stubStore) Enqueue(_ context.Context, _ pgcommon.Tx, _ domain.OutboxRecord) error {
	return nil
}
func (s *stubStore) ClaimBatch(_ context.Context, _ int) ([]domain.OutboxRecord, error) {
	return nil, nil
}
func (s *stubStore) MarkPublished(_ context.Context, _ string) error { return nil }
func (s *stubStore) MarkFailed(_ context.Context, rec domain.OutboxRecord, reason string, _ int, retryAfter time.Duration) error {
	s.failed[rec.ID] = reason
	s.retryAfter[rec.ID] = retryAfter
	return nil
}
func (s *stubStore) ReleaseLease(_ context.Context, id, reason string, retryAfter time.Duration) error {
	s.released[id] = reason
	s.releasedIn[id] = retryAfter
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

func testRecord(t *testing.T, eventType string) domain.OutboxRecord {
	t.Helper()
	env := domain.NewEnvelope(eventType, "svc", json.RawMessage(`{}`))
	payload, err := json.Marshal(env)
	require.NoError(t, err)
	return domain.OutboxRecord{ID: env.ID, EventType: env.Type, Payload: payload}
}

// TestPublishRecord_CtxCancelledBeforeRun: a record claimed just before
// shutdown is released immediately without counting an attempt.
func TestPublishRecord_CtxCancelledBeforeRun(t *testing.T) {
	store := newStubStore()
	svc := NewOutboxService(store, stubPublisher{}, nil, nil, 5, 1, 0)
	rec := testRecord(t, "cancel.event")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	svc.publishRecord(ctx, context.Background(), rec)

	assert.NotContains(t, store.failed, rec.ID, "shutdown must not count an attempt")
	require.Contains(t, store.released, rec.ID)
	assert.Contains(t, store.released[rec.ID], "context")
	assert.Zero(t, store.releasedIn[rec.ID], "a stranded record is claimable immediately")
}

// TestPublishRecord_TransientErrors_ReleaseWithoutAttempt: timeouts and
// ErrRetryable describe the publisher, so they release the lease (no attempt)
// with a growing shared backoff.
func TestPublishRecord_TransientErrors_ReleaseWithoutAttempt(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"context.DeadlineExceeded", context.DeadlineExceeded},
		{"domain.ErrRetryable", domain.ErrRetryable},
		{"wrapped ErrRetryable", fmt.Errorf("sns throttle: %w", domain.ErrRetryable)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newStubStore()
			svc := NewOutboxService(store, &errPublisher{err: tc.err}, nil, nil, 5, 2, 0)
			svc.SetRetryBackoff(time.Second, time.Minute)
			var delays []time.Duration
			for range 4 {
				svc.beginPoll() // one failure per poll cycle
				rec := testRecord(t, "transient.event")
				svc.publishRecord(context.Background(), context.Background(), rec)
				assert.NotContains(t, store.failed, rec.ID)
				require.Contains(t, store.released, rec.ID)
				delays = append(delays, store.releasedIn[rec.ID])
			}
			// Equal jitter: the n-th delay lies in [d/2, d] with d = 1s·2^(n-1).
			for i, d := range delays {
				hi := time.Second << i
				assert.GreaterOrEqual(t, d, hi/2, "delay %d", i)
				assert.LessOrEqual(t, d, hi, "delay %d", i)
			}
		})
	}
}

// TestPublishRecord_SuccessResetsTransientStreak: one successful publish ends
// the outage backoff.
func TestPublishRecord_SuccessResetsTransientStreak(t *testing.T) {
	store := newStubStore()
	pub := &errPublisher{err: domain.ErrRetryable}
	svc := NewOutboxService(store, pub, nil, nil, 5, 2, 0)
	for range 5 {
		svc.beginPoll()
		svc.publishRecord(context.Background(), context.Background(), testRecord(t, "e"))
	}
	assert.Equal(t, 5, svc.transientCount)
	pub.err = nil
	svc.publishRecord(context.Background(), context.Background(), testRecord(t, "e"))
	assert.Zero(t, svc.transientCount)
}

// TestTransientBackoff_OncePerPoll: many records failing in one poll cycle
// (PublishConcurrency > 1) advance the shared backoff once, not once each —
// a one-second SNS blip must not park the batch for minutes.
func TestTransientBackoff_OncePerPoll(t *testing.T) {
	store := newStubStore()
	svc := NewOutboxService(store, &errPublisher{err: domain.ErrRetryable}, nil, nil, 5, 8, 0)
	svc.SetRetryBackoff(time.Second, 5*time.Minute)
	svc.beginPoll()
	for range 50 {
		svc.publishRecord(context.Background(), context.Background(), testRecord(t, "e"))
	}
	assert.Equal(t, 1, svc.transientCount)
	for id, d := range store.releasedIn {
		assert.LessOrEqual(t, d, time.Second, "record %s parked for %s", id, d)
	}
}

// TestPublishRecord_PermanentError_CountsAttemptWithBackoff: an ordinary
// error counts an attempt and delays the retry by the per-record backoff.
func TestPublishRecord_PermanentError_CountsAttemptWithBackoff(t *testing.T) {
	store := newStubStore()
	svc := NewOutboxService(store, &errPublisher{err: errors.New("sns: InvalidParameter")}, nil, nil, 5, 2, 0)
	svc.SetRetryBackoff(2*time.Second, time.Hour)
	rec := testRecord(t, "perm.event")
	rec.Attempts = 3 // this failure is the 4th: 2s·2^3 = 16s
	svc.publishRecord(context.Background(), context.Background(), rec)

	require.Contains(t, store.failed, rec.ID)
	assert.NotContains(t, store.released, rec.ID)
	assert.GreaterOrEqual(t, store.retryAfter[rec.ID], 8*time.Second)
	assert.LessOrEqual(t, store.retryAfter[rec.ID], 16*time.Second)
}

func TestBackoff_CapsAndDefaults(t *testing.T) {
	svc := NewOutboxService(newStubStore(), stubPublisher{}, nil, nil, 5, 1, 0)
	assert.LessOrEqual(t, svc.backoff(1), DefaultRetryBackoff)
	assert.LessOrEqual(t, svc.backoff(1000), DefaultMaxRetryBackoff, "no overflow, capped")
	assert.GreaterOrEqual(t, svc.backoff(1000), DefaultMaxRetryBackoff/2)

	svc.SetRetryBackoff(time.Minute, time.Second) // max < base → max raised to base
	assert.Equal(t, time.Minute, svc.maxRetryBackoff)
	svc.SetRetryBackoff(0, 0) // non-positive keeps current values
	assert.Equal(t, time.Minute, svc.retryBackoff)
}

// TestReleaseStranded_NoAttempt: shutdown never counts toward MaxAttempts.
func TestReleaseStranded_NoAttempt(t *testing.T) {
	store := newStubStore()
	svc := NewOutboxService(store, stubPublisher{}, nil, nil, 3, 1, 0)
	rec := testRecord(t, "stranded.event")

	svc.releaseStranded(context.Background(), rec, "shutdown")

	assert.NotContains(t, store.failed, rec.ID)
	assert.Equal(t, "shutdown", store.released[rec.ID])
	assert.Zero(t, store.releasedIn[rec.ID])
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

// cancelOnPublish cancels the batch context mid-publish (a shutdown) and fails.
type cancelOnPublish struct{ cancel context.CancelFunc }

func (p cancelOnPublish) Publish(context.Context, domain.Envelope[json.RawMessage]) error {
	p.cancel()
	return errors.New("connection reset")
}
func (p cancelOnPublish) PublishBatch(context.Context, []domain.Envelope[json.RawMessage]) error {
	p.cancel()
	return errors.New("connection reset")
}

// TestPublishRecord_FailsDuringShutdown: a publish that fails because the
// runner is stopping releases the lease immediately without counting an
// attempt, whatever the error.
func TestPublishRecord_FailsDuringShutdown(t *testing.T) {
	store := newStubStore()
	ctx, cancel := context.WithCancel(context.Background())
	svc := NewOutboxService(store, cancelOnPublish{cancel}, nil, nil, 5, 2, 0)
	rec := testRecord(t, "shutdown.event")

	svc.publishRecord(ctx, context.Background(), rec)

	assert.NotContains(t, store.failed, rec.ID)
	require.Contains(t, store.released, rec.ID)
	assert.Contains(t, store.released[rec.ID], "publish interrupted by shutdown")
	assert.Zero(t, store.releasedIn[rec.ID])
}

// batchFailPublisher fails every envelope of a batch under a given ID spelling.
type batchFailPublisher struct{ spell func(string) string }

func (batchFailPublisher) Publish(context.Context, domain.Envelope[json.RawMessage]) error {
	return nil
}
func (p batchFailPublisher) PublishBatch(_ context.Context, envs []domain.Envelope[json.RawMessage]) error {
	be := &domain.BatchError{}
	for _, e := range envs {
		be.Failures = append(be.Failures, domain.BatchFailure{ID: p.spell(e.ID), Code: "InvalidParameter", Message: "bad"})
	}
	return be
}

type claimStore struct {
	*stubStore
	recs      []domain.OutboxRecord
	published []string
}

func (s *claimStore) ClaimBatch(context.Context, int) ([]domain.OutboxRecord, error) {
	return s.recs, nil
}
func (s *claimStore) MarkPublished(_ context.Context, id string) error {
	s.published = append(s.published, id)
	return nil
}

// TestPublishBatch_LegacyNonCanonicalID_FailureMatched: a row whose payload ID
// is uppercase (written before Enqueue required canonical IDs) is read back
// canonicalised; its failure must still match, never be marked published.
func TestPublishBatch_LegacyNonCanonicalID_FailureMatched(t *testing.T) {
	env := domain.NewEnvelope("legacy.event", "svc", json.RawMessage(`{}`))
	env.ID = strings.ToUpper(env.ID)
	payload, err := json.Marshal(env)
	require.NoError(t, err)
	store := &claimStore{stubStore: newStubStore(), recs: []domain.OutboxRecord{{ID: strings.ToLower(env.ID), EventType: env.Type, Payload: payload}}}
	svc := NewOutboxService(store, batchFailPublisher{spell: func(id string) string { return id }}, nil, nil, 5, 1, 0)

	require.NoError(t, svc.PublishBatch(context.Background(), 10))
	assert.Empty(t, store.published, "a failed publish must not be marked published")
	assert.Contains(t, store.failed, strings.ToLower(env.ID))
}

func TestCanonicalID(t *testing.T) {
	id := uuid.NewString()
	assert.Equal(t, id, canonicalID(strings.ToUpper(id)))
	assert.Equal(t, id, canonicalID("{"+id+"}"))
	assert.Equal(t, "not-a-uuid", canonicalID("not-a-uuid"), "non-UUIDs are compared as-is")
}
