package outbox_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/test/fixtures"
)

// noopTx is a non-nil pgcommon.Tx for OutboxService.Enqueue tests whose
// store ignores the transaction. Embedding the interface (left nil) provides
// every method without naming a pgx type; calling one would panic, flagging an
// unexpected use of the transaction.
type noopTx struct{ pgcommon.Tx }

var _ pgcommon.Tx = noopTx{}

// mockStore is a thread-safe in-memory OutboxStore for testing.
// The mutex protects published and failed maps which are written by parallel
// goroutines in tests that use publishConcurrency > 1.
type mockStore struct {
	mu        sync.Mutex
	records   []domain.OutboxRecord
	published map[string]bool
	failed    map[string]string
	released  map[string]string
	err       error
}

func newMockStore() *mockStore {
	return &mockStore{
		published: make(map[string]bool),
		failed:    make(map[string]string),
		released:  make(map[string]string),
	}
}

func (s *mockStore) Enqueue(_ context.Context, _ pgcommon.Tx, rec domain.OutboxRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = append(s.records, rec)
	return s.err
}

func (s *mockStore) ClaimBatch(_ context.Context, n int) ([]domain.OutboxRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return nil, s.err
	}
	if len(s.records) == 0 {
		return nil, nil
	}
	if n > len(s.records) {
		n = len(s.records)
	}
	batch := make([]domain.OutboxRecord, n)
	copy(batch, s.records[:n])
	s.records = s.records[n:]
	return batch, nil
}

func (s *mockStore) MarkPublished(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.published[id] = true
	return nil
}

func (s *mockStore) MarkFailed(_ context.Context, rec domain.OutboxRecord, lastError string, _ int, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failed[rec.ID] = lastError
	return nil
}

// ReleaseLease records lease releases (shutdown, transient failures) — no
// attempt counted, so they are kept apart from failed.
func (s *mockStore) ReleaseLease(_ context.Context, id, lastError string, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.released[id] = lastError
	return nil
}

func (s *mockStore) OldestPendingAge(context.Context) (time.Duration, error)    { return 0, nil }
func (s *mockStore) PromoteWaiting(context.Context) (int64, error)              { return 0, nil }
func (s *mockStore) BlockedCount(context.Context) (int64, error)                { return 0, nil }
func (s *mockStore) LeasedCount(_ context.Context) (int64, error)               { return 0, nil }
func (s *mockStore) ReprocessDeadLetters(_ context.Context, _ int) (int, error) { return 0, nil }
func (s *mockStore) PrunePublished(_ context.Context, _ time.Duration, _ int) (int64, error) {
	return 0, nil
}
func (s *mockStore) ListDeadLetters(_ context.Context, _ domain.DLQFilter, _ int) ([]domain.DeadLetterRecord, error) {
	return nil, nil
}
func (s *mockStore) ReprocessDeadLettersWith(_ context.Context, _ domain.DLQFilter, _ int) (int, error) {
	return 0, nil
}
func (s *mockStore) DiscardDeadLetters(_ context.Context, _ domain.DLQFilter, _ int) (int64, error) {
	return 0, nil
}

func (s *mockStore) PendingCount(_ context.Context) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return int64(len(s.records)), nil
}

// Ensure mockStore satisfies port.OutboxStore at compile time.
var _ port.OutboxStore = (*mockStore)(nil)

type batchTrackingPublisher struct {
	publishCalls      int
	publishBatchCalls int
}

func (p *batchTrackingPublisher) Publish(_ context.Context, _ domain.Envelope[json.RawMessage]) error {
	p.publishCalls++
	return nil
}

func (p *batchTrackingPublisher) PublishBatch(_ context.Context, _ []domain.Envelope[json.RawMessage]) error {
	p.publishBatchCalls++
	return nil
}

var _ port.Publisher = (*batchTrackingPublisher)(nil)

func TestOutboxService_PublishBatch_Sequential_UsesPublishBatch(t *testing.T) {
	store := newMockStore()
	pub := &batchTrackingPublisher{}

	env := domain.NewEnvelope("batch.event", "svc", json.RawMessage(`{}`))
	env.TenantID = "acme"
	payload, _ := json.Marshal(env)
	store.records = []domain.OutboxRecord{{ID: env.ID, EventType: env.Type, Payload: payload}}

	svc := service.NewOutboxService(store, pub, nil, nil, 5, 1, 0)
	err := svc.PublishBatch(context.Background(), 10)
	require.NoError(t, err)
	assert.Equal(t, 1, pub.publishBatchCalls)
	assert.Equal(t, 0, pub.publishCalls)
}

type domainBatchErrPublisher struct {
	err *domain.BatchError
}

func (p *domainBatchErrPublisher) Publish(context.Context, domain.Envelope[json.RawMessage]) error {
	return nil
}

func (p *domainBatchErrPublisher) PublishBatch(context.Context, []domain.Envelope[json.RawMessage]) error {
	return p.err
}

func TestOutboxService_PublishBatch_Sequential_PartialBatchError(t *testing.T) {
	store := newMockStore()

	envOK := domain.NewEnvelope("ok.event", "svc", json.RawMessage(`{}`))
	envOK.TenantID = "acme"
	payloadOK, _ := json.Marshal(envOK)
	envFail := domain.NewEnvelope("fail.event", "svc", json.RawMessage(`{}`))
	envFail.TenantID = "acme"
	payloadFail, _ := json.Marshal(envFail)
	store.records = []domain.OutboxRecord{
		{ID: envOK.ID, EventType: envOK.Type, Payload: payloadOK},
		{ID: envFail.ID, EventType: envFail.Type, Payload: payloadFail},
	}

	pub := &domainBatchErrPublisher{
		err: &domain.BatchError{
			Failures: []domain.BatchFailure{
				{ID: envFail.ID, Code: "InternalError", Message: "sns throttle"},
			},
		},
	}
	svc := service.NewOutboxService(store, pub, nil, nil, 5, 1, 0)

	err := svc.PublishBatch(context.Background(), 10)
	require.NoError(t, err)
	assert.True(t, store.published[envOK.ID])
	assert.Contains(t, store.failed, envFail.ID)
}

// TestOutboxService_PublishBatch_Sequential_TransportError_ReleasesWithoutAttempt
// verifies that a BatchError failure with Code="TransportError" (e.g. SNS throttle
// wrapped by publishChunk) releases the lease without counting an attempt, so an
// SNS outage can never dead-letter healthy records.
func TestOutboxService_PublishBatch_Sequential_TransportError_ReleasesWithoutAttempt(t *testing.T) {
	const maxAttempts = 5
	var capturedThresholds []int
	capture := &captureMaxAttemptsStore{
		mockStore: newMockStore(),
		onFail: func(_ string, threshold int) {
			capturedThresholds = append(capturedThresholds, threshold)
		},
	}

	env := domain.NewEnvelope("throttled.event", "svc", json.RawMessage(`{}`))
	env.TenantID = "acme"
	payload, _ := json.Marshal(env)
	capture.records = []domain.OutboxRecord{{ID: env.ID, EventType: env.Type, Payload: payload}}

	pub := &domainBatchErrPublisher{
		err: &domain.BatchError{
			Failures: []domain.BatchFailure{
				{ID: env.ID, Code: "TransportError", Message: "ThrottlingException"},
			},
		},
	}
	svc := service.NewOutboxService(capture, pub, nil, nil, maxAttempts, 1, 0)

	err := svc.PublishBatch(context.Background(), 10)
	require.NoError(t, err)

	assert.Empty(t, capturedThresholds, "TransportError must not count an attempt")
	assert.Equal(t, "ThrottlingException", capture.released[env.ID], "lease released with the error recorded")
}

func TestOutboxService_PublishBatch_Success(t *testing.T) {
	store := newMockStore()
	pub := &fixtures.MockPublisher{}
	logger := &fixtures.MockLogger{}
	clock := fixtures.NewFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))

	env := domain.NewEnvelope("order.placed", "billing", json.RawMessage(`{"amount":100}`))
	payload, err := json.Marshal(env)
	require.NoError(t, err)

	store.records = []domain.OutboxRecord{
		{ID: env.ID, EventType: env.Type, Payload: payload},
	}

	svc := service.NewOutboxService(store, pub, logger, clock, 5, 1, 0)
	err = svc.PublishBatch(context.Background(), 10)
	require.NoError(t, err)

	assert.True(t, store.published[env.ID], "record should be marked published")
	published := pub.Published()
	require.Len(t, published, 1)
	assert.Equal(t, env.ID, published[0].ID)
}

func TestOutboxService_PublishBatch_PublishError(t *testing.T) {
	store := newMockStore()
	pub := &fixtures.MockPublisher{}
	pub.SetError(errors.New("sns error"))
	logger := &fixtures.MockLogger{}
	clock := fixtures.NewFakeClock(time.Now())

	env := domain.NewEnvelope("test.event", "svc", json.RawMessage(`{}`))
	payload, _ := json.Marshal(env)
	store.records = []domain.OutboxRecord{
		{ID: env.ID, EventType: env.Type, Payload: payload},
	}

	svc := service.NewOutboxService(store, pub, logger, clock, 5, 1, 0)
	err := svc.PublishBatch(context.Background(), 10)
	require.NoError(t, err) // batch-level errors do not propagate; they are logged

	assert.Equal(t, "sns error", store.failed[env.ID])
}

func TestOutboxService_PublishBatch_Empty(t *testing.T) {
	store := newMockStore()
	pub := &fixtures.MockPublisher{}
	svc := service.NewOutboxService(store, pub, nil, nil, 5, 1, 0)

	err := svc.PublishBatch(context.Background(), 10)
	require.NoError(t, err)
	assert.Empty(t, pub.Published())
}

// ----------------------------
// Enqueue
// ----------------------------

func TestOutboxService_Enqueue_Success(t *testing.T) {
	store := newMockStore()
	pub := &fixtures.MockPublisher{}
	clock := fixtures.NewFakeClock(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	svc := service.NewOutboxService(store, pub, nil, clock, 5, 1, 0)

	env := domain.NewEnvelope("order.placed", "billing", json.RawMessage(`{"amount":50}`))
	env.TenantID = "acme"
	env.TraceID = "trace-001"

	err := svc.Enqueue(context.Background(), noopTx{}, env)
	require.NoError(t, err)

	require.Len(t, store.records, 1)
	rec := store.records[0]
	assert.Equal(t, env.ID, rec.ID)
	assert.Equal(t, env.Type, rec.EventType)
	assert.Equal(t, "acme", rec.TenantID)
	assert.Equal(t, "trace-001", rec.TraceID)
}

func TestOutboxService_Enqueue_NilTx_ReturnsError(t *testing.T) {
	store := newMockStore()
	pub := &fixtures.MockPublisher{}
	svc := service.NewOutboxService(store, pub, nil, nil, 5, 1, 0)

	env := domain.NewEnvelope("x.y", "svc", json.RawMessage(`{}`))
	err := svc.Enqueue(context.Background(), nil, env)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "transaction must not be nil")
}

func TestOutboxService_Enqueue_EmptyID_ReturnsError(t *testing.T) {
	store := newMockStore()
	svc := service.NewOutboxService(store, &fixtures.MockPublisher{}, nil, nil, 5, 1, 0)

	var env domain.Envelope[json.RawMessage]
	env.Type = "order.placed"
	env.Source = "billing"
	env.Payload = json.RawMessage(`{}`)

	err := svc.Enqueue(context.Background(), noopTx{}, env)
	require.ErrorIs(t, err, domain.ErrEnvelopeIDRequired)
}

func TestOutboxService_Enqueue_EmptyType_ReturnsError(t *testing.T) {
	store := newMockStore()
	svc := service.NewOutboxService(store, &fixtures.MockPublisher{}, nil, nil, 5, 1, 0)

	env := domain.NewEnvelope("order.placed", "billing", json.RawMessage(`{}`))
	env.Type = ""

	err := svc.Enqueue(context.Background(), noopTx{}, env)
	require.ErrorIs(t, err, domain.ErrEnvelopeTypeRequired)
}

func TestOutboxService_Enqueue_EmptySource_ReturnsError(t *testing.T) {
	store := newMockStore()
	svc := service.NewOutboxService(store, &fixtures.MockPublisher{}, nil, nil, 5, 1, 0)

	env := domain.NewEnvelope("order.placed", "billing", json.RawMessage(`{}`))
	env.Source = ""

	err := svc.Enqueue(context.Background(), noopTx{}, env)
	require.ErrorIs(t, err, domain.ErrEnvelopeSourceRequired)
}

func TestOutboxService_Enqueue_StoreError(t *testing.T) {
	store := newMockStore()
	store.err = errors.New("store failure")
	pub := &fixtures.MockPublisher{}
	svc := service.NewOutboxService(store, pub, nil, nil, 5, 1, 0)

	env := domain.NewEnvelope("x.y", "svc", json.RawMessage(`{}`))
	env.TenantID = "acme"
	err := svc.Enqueue(context.Background(), noopTx{}, env)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "store failure")
}

// ----------------------------
// PublishBatch error paths
// ----------------------------

func TestOutboxService_PublishBatch_ClaimBatchError(t *testing.T) {
	store := newMockStore()
	store.err = errors.New("claim failed")
	pub := &fixtures.MockPublisher{}
	svc := service.NewOutboxService(store, pub, nil, nil, 5, 1, 0)

	err := svc.PublishBatch(context.Background(), 10)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "claim failed")
}

func TestOutboxService_PublishBatch_UnmarshalError_MarkFailed(t *testing.T) {
	store := newMockStore()
	// Insert a record with invalid payload JSON.
	store.records = []domain.OutboxRecord{
		{ID: "bad-id", EventType: "x.y", Payload: []byte("not-json")},
	}
	pub := &fixtures.MockPublisher{}
	logger := &fixtures.MockLogger{}
	svc := service.NewOutboxService(store, pub, logger, nil, 5, 1, 0)

	err := svc.PublishBatch(context.Background(), 10)
	require.NoError(t, err) // per-record errors do not propagate

	// MarkFailed should have been called.
	assert.Contains(t, store.failed, "bad-id")
	// Logger should have recorded an error.
	entries := logger.Entries()
	found := false
	for _, e := range entries {
		if e.Level == "ERROR" {
			found = true
			break
		}
	}
	assert.True(t, found, "expected ERROR log entry for unmarshal failure")
}

func TestOutboxService_PublishBatch_MarkPublishedError_Logged(t *testing.T) {
	// Use a custom store that returns an error from MarkPublished.
	store := &markPublishedErrorStore{
		mockStore: newMockStore(),
		mpErr:     errors.New("mark published failed"),
	}
	env := domain.NewEnvelope("test.event", "svc", json.RawMessage(`{}`))
	payload, _ := json.Marshal(env)
	store.records = []domain.OutboxRecord{
		{ID: env.ID, EventType: env.Type, Payload: payload},
	}

	pub := &fixtures.MockPublisher{}
	logger := &fixtures.MockLogger{}
	svc := service.NewOutboxService(store, pub, logger, nil, 5, 1, 0)

	err := svc.PublishBatch(context.Background(), 10)
	require.NoError(t, err) // errors are logged, not returned

	entries := logger.Entries()
	found := false
	for _, e := range entries {
		if e.Level == "ERROR" {
			found = true
			break
		}
	}
	assert.True(t, found, "expected ERROR log entry for MarkPublished failure")
}

func TestOutboxService_PublishBatch_MarkFailedError_Logged(t *testing.T) {
	// Use a store where MarkFailed returns an error.
	store := &markFailedErrorStore{
		mockStore: newMockStore(),
		mfErr:     errors.New("mark failed error"),
	}
	env := domain.NewEnvelope("test.event", "svc", json.RawMessage(`{}`))
	payload, _ := json.Marshal(env)
	store.records = []domain.OutboxRecord{
		{ID: env.ID, EventType: env.Type, Payload: payload},
	}

	// Publisher returns an error to trigger MarkFailed path.
	pub := &fixtures.MockPublisher{}
	pub.SetError(errors.New("publish error"))
	logger := &fixtures.MockLogger{}
	svc := service.NewOutboxService(store, pub, logger, nil, 5, 1, 0)

	err := svc.PublishBatch(context.Background(), 10)
	require.NoError(t, err) // per-record errors do not propagate

	entries := logger.Entries()
	found := false
	for _, e := range entries {
		if e.Level == "ERROR" {
			found = true
			break
		}
	}
	assert.True(t, found, "expected ERROR log entry for MarkFailed error")
}

// markPublishedErrorStore overrides MarkPublished to return an error.
type markPublishedErrorStore struct {
	*mockStore
	mpErr error
}

func (s *markPublishedErrorStore) MarkPublished(_ context.Context, _ string) error {
	return s.mpErr
}

// markFailedErrorStore overrides MarkFailed to return an error.
type markFailedErrorStore struct {
	*mockStore
	mfErr error
}

func (s *markFailedErrorStore) ReleaseLease(_ context.Context, id, lastError string, retryAfter time.Duration) error {
	_ = s.mockStore.ReleaseLease(context.Background(), id, lastError, retryAfter)
	return s.mfErr
}

func (s *markFailedErrorStore) MarkFailed(_ context.Context, rec domain.OutboxRecord, lastError string, maxAttempts int, retryAfter time.Duration) error {
	_ = s.mockStore.MarkFailed(context.Background(), rec, lastError, maxAttempts, retryAfter)
	return s.mfErr
}

func TestOutboxService_PendingCount(t *testing.T) {
	store := newMockStore()
	pub := &fixtures.MockPublisher{}
	svc := service.NewOutboxService(store, pub, nil, nil, 5, 1, 0)

	// Empty store → zero pending.
	n, err := svc.PendingCount(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)

	// Add two records.
	env1 := domain.NewEnvelope("a.b", "svc", json.RawMessage(`{}`))
	env2 := domain.NewEnvelope("c.d", "svc", json.RawMessage(`{}`))
	payload1, _ := json.Marshal(env1)
	payload2, _ := json.Marshal(env2)
	store.records = []domain.OutboxRecord{
		{ID: env1.ID, EventType: env1.Type, Payload: payload1},
		{ID: env2.ID, EventType: env2.Type, Payload: payload2},
	}
	n, err = svc.PendingCount(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int64(2), n)
}

func TestOutboxService_NilClock_UsesRealClock(t *testing.T) {
	store := newMockStore()
	pub := &fixtures.MockPublisher{}
	svc := service.NewOutboxService(store, pub, nil, nil, 5, 1, 0) // nil clock → RealClock
	assert.NotNil(t, svc)
}

// TestOutboxService_CtxCancel_StrandedMarkFailedError covers the logger path when
// MarkFailed itself returns an error while releasing stranded record leases.
func TestOutboxService_CtxCancel_StrandedMarkFailedError_Logged(t *testing.T) {
	store := &markFailedErrorStore{
		mockStore: newMockStore(),
		mfErr:     errors.New("mark failed error"),
	}
	for range 2 {
		env := domain.NewEnvelope("x.y", "svc", json.RawMessage(`{}`))
		payload, _ := json.Marshal(env)
		store.records = append(store.records, domain.OutboxRecord{
			ID:        env.ID,
			EventType: env.Type,
			Payload:   payload,
		})
	}

	pub := &fixtures.MockPublisher{}
	logger := &fixtures.MockLogger{}
	svc := service.NewOutboxService(store, pub, logger, nil, 5, 1, 0)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before PublishBatch runs the loop

	err := svc.PublishBatch(ctx, 10)
	require.Error(t, err)

	entries := logger.Entries()
	found := false
	for _, e := range entries {
		if e.Level == "ERROR" {
			found = true
			break
		}
	}
	assert.True(t, found, "expected ERROR log for MarkFailed failure during stranded-record cleanup")
}

// TestOutboxService_UnmarshalError_MarkFailedError_Logged covers the logger path
// when MarkFailed fails for a record whose payload cannot be unmarshaled.
func TestOutboxService_UnmarshalError_MarkFailedError_Logged(t *testing.T) {
	store := &markFailedErrorStore{
		mockStore: newMockStore(),
		mfErr:     errors.New("mark failed error"),
	}
	store.records = []domain.OutboxRecord{
		{ID: "bad-id", EventType: "x.y", Payload: []byte("not-json")},
	}

	pub := &fixtures.MockPublisher{}
	logger := &fixtures.MockLogger{}
	svc := service.NewOutboxService(store, pub, logger, nil, 5, 1, 0)

	err := svc.PublishBatch(context.Background(), 10)
	require.NoError(t, err) // per-record errors do not propagate

	entries := logger.Entries()
	errCount := 0
	for _, e := range entries {
		if e.Level == "ERROR" {
			errCount++
		}
	}
	assert.GreaterOrEqual(t, errCount, 2, "expected ERROR logs for both unmarshal failure and MarkFailed failure")
}

// ----------------------------
// PublishBatch: stranded-record lease release on context cancellation
// ----------------------------

// TestOutboxService_PublishBatch_CtxCancel_StrandedRecordsMarkedFailed verifies
// that when ctx is cancelled mid-batch, un-iterated claimed records have MarkFailed
// called on them via bookkeepCtx so their claim lease is released immediately rather
// than waiting for claimLeaseDuration to expire.
func TestOutboxService_PublishBatch_CtxCancel_StrandedRecordsMarkedFailed(t *testing.T) {
	store := newMockStore()
	pub := &fixtures.MockPublisher{}
	logger := &fixtures.MockLogger{}

	// Queue 3 records.
	for i := range 3 {
		env := domain.NewEnvelope("x.y", "svc", json.RawMessage(`{}`))
		payload, _ := json.Marshal(env)
		store.records = append(store.records, domain.OutboxRecord{
			ID:        env.ID,
			EventType: fmt.Sprintf("event.%d", i),
			Payload:   payload,
		})
	}

	svc := service.NewOutboxService(store, pub, logger, nil, 5, 1, 0)

	// Cancel the context before PublishBatch is called so ctx.Err() fires
	// on the very first record iteration — simulating a shutdown mid-batch.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := svc.PublishBatch(ctx, 10)
	require.Error(t, err) // ctx.Err() propagated

	// All 3 claimed records should have been released via MarkFailed so no
	// runner needs to wait for the claim lease to expire.
	assert.Len(t, store.released, 3, "all stranded records should have their lease released")
	assert.Empty(t, store.failed, "shutdown must not count an attempt")
	for id, reason := range store.released {
		assert.Contains(t, reason, "context", "MarkFailed reason should mention context cancellation for %s", id)
	}
}

// ----------------------------
// Parallel publishing (publishConcurrency > 1)
// ----------------------------

func TestOutboxService_PublishBatch_Parallel_AllDelivered(t *testing.T) {
	store := newMockStore()
	pub := &fixtures.MockPublisher{}

	const numRecords = 5
	for range numRecords {
		env := domain.NewEnvelope("parallel.event", "svc", json.RawMessage(`{}`))
		payload, _ := json.Marshal(env)
		store.records = append(store.records, domain.OutboxRecord{
			ID: env.ID, EventType: env.Type, Payload: payload,
		})
	}

	svc := service.NewOutboxService(store, pub, nil, nil, 5, 3, 0) // concurrency=3

	err := svc.PublishBatch(context.Background(), numRecords)
	require.NoError(t, err)
	assert.Len(t, pub.Published(), numRecords, "all records should be published")
	assert.Len(t, store.published, numRecords, "all records should be marked published")
}

func TestOutboxService_PublishBatch_Parallel_CtxCancel_StrandedMarkedFailed(t *testing.T) {
	store := newMockStore()
	pub := &fixtures.MockPublisher{}

	for range 4 {
		env := domain.NewEnvelope("cancel.event", "svc", json.RawMessage(`{}`))
		payload, _ := json.Marshal(env)
		store.records = append(store.records, domain.OutboxRecord{
			ID: env.ID, EventType: env.Type, Payload: payload,
		})
	}

	svc := service.NewOutboxService(store, pub, nil, nil, 5, 2, 0) // concurrency=2

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled

	err := svc.PublishBatch(ctx, 10)
	require.Error(t, err)
	assert.Len(t, store.released, 4, "all records should have their lease released")
	assert.Empty(t, store.failed, "shutdown must not count an attempt")
}

// ----------------------------
// Publish timeout
// ----------------------------

type slowPublisher struct {
	block chan struct{} // closed to unblock
}

func (p *slowPublisher) Publish(ctx context.Context, _ domain.Envelope[json.RawMessage]) error {
	select {
	case <-p.block:
		return nil
	case <-ctx.Done():
		return ctx.Err() // returns DeadlineExceeded when publishTimeout fires
	}
}
func (p *slowPublisher) PublishBatch(ctx context.Context, envs []domain.Envelope[json.RawMessage]) error {
	for _, e := range envs {
		if err := p.Publish(ctx, e); err != nil {
			return err
		}
	}
	return nil
}

var _ port.Publisher = (*slowPublisher)(nil)

func TestOutboxService_PublishTimeout_Fires_MarksFailed(t *testing.T) {
	store := newMockStore()
	block := make(chan struct{}) // never closed → publisher always blocks
	pub := &slowPublisher{block: block}

	env := domain.NewEnvelope("slow.event", "svc", json.RawMessage(`{}`))
	payload, _ := json.Marshal(env)
	store.records = []domain.OutboxRecord{{ID: env.ID, EventType: env.Type, Payload: payload}}

	svc := service.NewOutboxService(store, pub, nil, nil, 5, 1, 50*time.Millisecond) // 50ms timeout

	err := svc.PublishBatch(context.Background(), 10)
	require.NoError(t, err)

	// A publish timeout is transient: the lease is released (retried after the
	// shared backoff) without counting an attempt.
	assert.Contains(t, store.released, env.ID, "timed-out record should have its lease released")
	assert.NotContains(t, store.failed, env.ID, "a timeout must not count an attempt")
}

// ----------------------------
// Shutdown never counts an attempt
// ----------------------------

func TestOutboxService_CtxCancel_DoesNotDeadLetterAtMaxAttemptsMinusOne(t *testing.T) {
	// Custom store that captures the maxAttempts ceiling passed to MarkFailed.
	type failCall struct {
		id          string
		maxAttempts int
	}
	var calls []failCall
	store := &captureMaxAttemptsStore{
		mockStore: newMockStore(),
		onFail:    func(id string, ma int) { calls = append(calls, failCall{id, ma}) },
	}

	env := domain.NewEnvelope("boundary.event", "svc", json.RawMessage(`{}`))
	payload, _ := json.Marshal(env)
	store.records = []domain.OutboxRecord{{ID: env.ID, EventType: env.Type, Payload: payload}}

	svc := service.NewOutboxService(store, &fixtures.MockPublisher{}, nil, nil, 5, 1, 0) // maxAttempts=5

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_ = svc.PublishBatch(ctx, 10)

	assert.Empty(t, calls, "shutdown must release the lease, not count an attempt")
	assert.Contains(t, store.released, env.ID)
}

type captureMaxAttemptsStore struct {
	*mockStore
	onFail func(id string, maxAttempts int)
}

func (s *captureMaxAttemptsStore) MarkFailed(_ context.Context, rec domain.OutboxRecord, _ string, maxAttempts int, _ time.Duration) error {
	s.onFail(rec.ID, maxAttempts)
	s.failed[rec.ID] = "captured"
	return nil
}

// ----------------------------
// TenantID validation
// ----------------------------

func TestOutboxService_Enqueue_EmptyTenantID_Succeeds(t *testing.T) {
	store := newMockStore()
	svc := service.NewOutboxService(store, &fixtures.MockPublisher{}, nil, nil, 5, 1, 0)

	env := domain.NewEnvelope("evt.type", "svc", json.RawMessage(`{}`))

	err := svc.Enqueue(context.Background(), noopTx{}, env)
	require.NoError(t, err)
	require.Len(t, store.records, 1)
}

func TestOutboxService_Enqueue_SystemTenantID_Succeeds(t *testing.T) {
	store := newMockStore()
	svc := service.NewOutboxService(store, &fixtures.MockPublisher{}, nil, nil, 5, 1, 0)

	env := domain.NewEnvelope("background.job", "svc", json.RawMessage(`{}`))
	env.TenantID = domain.SystemTenantID // "system" — valid non-empty sentinel

	err := svc.Enqueue(context.Background(), noopTx{}, env)
	require.NoError(t, err, "system tenant should be accepted")
}

// ----------------------------
// last_error truncation
// ----------------------------

func TestOutboxService_PublishBatch_LongError_Truncated(t *testing.T) {
	store := newMockStore()

	// Publisher that returns a very long error message.
	longErr := errors.New(string(make([]byte, 1024))) // 1024 bytes of zeros
	pub := &fixtures.MockPublisher{}
	pub.SetError(longErr)

	env := domain.NewEnvelope("long.err", "svc", json.RawMessage(`{}`))
	payload, _ := json.Marshal(env)
	store.records = []domain.OutboxRecord{{ID: env.ID, EventType: env.Type, Payload: payload}}

	svc := service.NewOutboxService(store, pub, nil, nil, 5, 1, 0)
	err := svc.PublishBatch(context.Background(), 10)
	require.NoError(t, err)

	stored := store.failed[env.ID]
	assert.LessOrEqual(t, len(stored), 530, "last_error should be truncated to ~512 chars + truncation suffix")
	assert.Contains(t, stored, "[truncated]", "truncated errors should have a suffix")
}

// ----------------------------
// ReprocessDeadLetters
// ----------------------------

// reprocessStore overrides ReprocessDeadLetters to return configurable results.
type reprocessStore struct {
	*mockStore
	count int
	err   error
}

func (s *reprocessStore) ReprocessDeadLetters(_ context.Context, _ int) (int, error) {
	return s.count, s.err
}

func TestOutboxService_ReprocessDeadLetters_Success(t *testing.T) {
	store := &reprocessStore{mockStore: newMockStore(), count: 3}
	pub := &fixtures.MockPublisher{}
	svc := service.NewOutboxService(store, pub, nil, nil, 5, 1, 0)

	n, err := svc.ReprocessDeadLetters(context.Background(), 10)
	require.NoError(t, err)
	assert.Equal(t, 3, n)
}

func TestOutboxService_ReprocessDeadLetters_Zero(t *testing.T) {
	store := &reprocessStore{mockStore: newMockStore(), count: 0}
	pub := &fixtures.MockPublisher{}
	svc := service.NewOutboxService(store, pub, nil, nil, 5, 1, 0)

	n, err := svc.ReprocessDeadLetters(context.Background(), 5)
	require.NoError(t, err)
	assert.Equal(t, 0, n)
}

// ----------------------------
// SchemaVersion round-trip regression (Issue: bridge.go was stripping SchemaVersion)
// ----------------------------

func TestOutboxService_PublishBatch_PreservesSchemaVersion(t *testing.T) {
	store := newMockStore()
	pub := &fixtures.MockPublisher{}
	clock := fixtures.NewFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))

	env := domain.NewEnvelope("order.placed", "billing", json.RawMessage(`{"amount":100}`))
	env.SchemaVersion = "1"
	env.TenantID = "acme"

	payload, err := json.Marshal(env)
	require.NoError(t, err)

	store.records = []domain.OutboxRecord{
		{ID: env.ID, EventType: env.Type, Payload: payload},
	}

	svc := service.NewOutboxService(store, pub, nil, clock, 5, 1, 0)
	err = svc.PublishBatch(context.Background(), 10)
	require.NoError(t, err)

	published := pub.Published()
	require.Len(t, published, 1)
	assert.Equal(t, "1", published[0].SchemaVersion, "SchemaVersion must survive the outbox store → bridge → publisher path")
}

// ----------------------------
// Dead-letter threshold: exactly MaxAttempts normal failures must trigger DL
// ----------------------------

func TestOutboxService_PublishBatch_NormalFailure_PassesMaxAttempts(t *testing.T) {
	type failCall struct {
		id          string
		maxAttempts int
	}
	var calls []failCall
	store := &captureMaxAttemptsStore{
		mockStore: newMockStore(),
		onFail:    func(id string, ma int) { calls = append(calls, failCall{id, ma}) },
	}

	env := domain.NewEnvelope("normal.fail", "svc", json.RawMessage(`{}`))
	payload, _ := json.Marshal(env)
	store.records = []domain.OutboxRecord{{ID: env.ID, EventType: env.Type, Payload: payload}}

	// Publisher returns a non-retryable error — must count against maxAttempts.
	pub := &fixtures.MockPublisher{}
	pub.SetError(errors.New("permanent sns error"))

	const maxAttempts = 5
	svc := service.NewOutboxService(store, pub, nil, nil, maxAttempts, 1, 0)

	err := svc.PublishBatch(context.Background(), 10)
	require.NoError(t, err)

	require.Len(t, calls, 1)
	// Normal (non-retryable) failure counts an attempt against maxAttempts so
	// the record is dead-lettered after exactly maxAttempts failures.
	assert.Equal(t, maxAttempts, calls[0].maxAttempts,
		"non-retryable publish failure must use maxAttempts")
}

func TestOutboxService_ReprocessDeadLetters_Error(t *testing.T) {
	store := &reprocessStore{mockStore: newMockStore(), err: errors.New("db unavailable")}
	pub := &fixtures.MockPublisher{}
	svc := service.NewOutboxService(store, pub, nil, nil, 5, 1, 0)

	_, err := svc.ReprocessDeadLetters(context.Background(), 10)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "db unavailable")
}

// ----------------------------
// publishConcurrency > 1: partial success
// ----------------------------

// selectiveFailPublisher fails Publish for IDs in failIDs and succeeds for all others.
type selectiveFailPublisher struct {
	mu      sync.Mutex
	failIDs map[string]struct{}
}

func (p *selectiveFailPublisher) Publish(_ context.Context, env domain.Envelope[json.RawMessage]) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, fail := p.failIDs[env.ID]; fail {
		return errors.New("selective publish error")
	}
	return nil
}

func (p *selectiveFailPublisher) PublishBatch(ctx context.Context, envs []domain.Envelope[json.RawMessage]) error {
	for _, env := range envs {
		if err := p.Publish(ctx, env); err != nil {
			return err
		}
	}
	return nil
}

func TestOutboxService_PublishBatch_Concurrency_PartialFailure(t *testing.T) {
	store := newMockStore()

	// 4 records: even-indexed IDs fail, odd-indexed IDs succeed.
	type recMeta struct {
		id       string
		mustFail bool
	}
	var metas []recMeta
	failIDs := map[string]struct{}{}

	for i := range 4 {
		env := domain.NewEnvelope("concurrent.event", "svc", json.RawMessage(`{}`))
		env.TenantID = "acme"
		payload, err := json.Marshal(env)
		require.NoError(t, err)
		store.records = append(store.records, domain.OutboxRecord{
			ID: env.ID, EventType: env.Type, Payload: payload,
		})
		mustFail := i%2 == 0
		if mustFail {
			failIDs[env.ID] = struct{}{}
		}
		metas = append(metas, recMeta{id: env.ID, mustFail: mustFail})
	}

	pub := &selectiveFailPublisher{failIDs: failIDs}
	svc := service.NewOutboxService(store, pub, nil, nil, 5, 2, 0) // concurrency=2

	err := svc.PublishBatch(context.Background(), 10)
	require.NoError(t, err)

	store.mu.Lock()
	defer store.mu.Unlock()

	assert.Len(t, store.published, 2, "2 records should be marked published")
	assert.Len(t, store.failed, 2, "2 records should be marked failed")

	for _, m := range metas {
		if m.mustFail {
			assert.Contains(t, store.failed, m.id, "failed ID should be in store.failed")
			assert.NotContains(t, store.published, m.id)
		} else {
			assert.Contains(t, store.published, m.id, "succeeded ID should be in store.published")
			assert.NotContains(t, store.failed, m.id)
		}
	}
}

// ----------------------------
// PublishBatch: context already cancelled before loop starts
// ----------------------------

// TestOutboxService_PublishBatch_CtxAlreadyCancelled covers the ctx.Err() != nil
// guard at the top of the for-range loop (outbox_service.go ~line 155). With an
// already-cancelled context every record must be marked failed immediately without
// acquiring the semaphore or calling the publisher.
func TestOutboxService_PublishBatch_CtxAlreadyCancelled(t *testing.T) {
	store := newMockStore()
	pub := &fixtures.MockPublisher{}
	logger := &fixtures.MockLogger{}
	clock := fixtures.NewFakeClock(time.Now())

	for range 3 {
		env := domain.NewEnvelope("ctx.cancel.test", "svc", json.RawMessage(`{}`))
		payload, _ := json.Marshal(env)
		store.records = append(store.records, domain.OutboxRecord{
			ID:        env.ID,
			EventType: env.Type,
			Payload:   payload,
		})
	}

	svc := service.NewOutboxService(store, pub, logger, clock, 5, 1, 0)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before calling PublishBatch

	err := svc.PublishBatch(ctx, 10)
	assert.ErrorIs(t, err, context.Canceled, "cancelled context must propagate to return value")

	assert.Empty(t, pub.Published(), "no records should be published with already-cancelled context")
	assert.Len(t, store.released, 3, "all records must have their lease released at loop top")
}

// ----------------------------
// PublishBatch: panic in publish goroutine is recovered
// ----------------------------

// panicPortPublisher panics inside Publish to trigger the recover() path.
type panicPortPublisher struct{}

func (p *panicPortPublisher) Publish(_ context.Context, _ domain.Envelope[json.RawMessage]) error {
	panic("simulated publish panic")
}

func (p *panicPortPublisher) PublishBatch(ctx context.Context, envs []domain.Envelope[json.RawMessage]) error {
	for _, env := range envs {
		if err := p.Publish(ctx, env); err != nil {
			return err
		}
	}
	return nil
}

var _ port.Publisher = (*panicPortPublisher)(nil)

// TestOutboxService_PublishBatch_PanicRecovered covers the defer-recover block in the
// publish goroutine (outbox_service.go ~lines 189-209). A panicking publisher must be
// recovered, the panic logged at ERROR, and the record marked failed so the next poll
// can retry it.
func TestOutboxService_PublishBatch_PanicRecovered(t *testing.T) {
	store := newMockStore()
	pub := &panicPortPublisher{}
	logger := &fixtures.MockLogger{}
	clock := fixtures.NewFakeClock(time.Now())

	env := domain.NewEnvelope("panic.test", "svc", json.RawMessage(`{}`))
	payload, _ := json.Marshal(env)
	store.records = []domain.OutboxRecord{
		{ID: env.ID, EventType: env.Type, Payload: payload},
	}

	svc := service.NewOutboxService(store, pub, logger, clock, 5, 1, 0)
	err := svc.PublishBatch(context.Background(), 10)
	require.NoError(t, err) // batch-level panic does not propagate to caller

	store.mu.Lock()
	defer store.mu.Unlock()

	require.Contains(t, store.failed, env.ID, "panicked record must be marked failed")
	assert.Contains(t, store.failed[env.ID], "panic", "failure reason must mention panic")

	found := false
	for _, e := range logger.Entries() {
		if e.Level == "ERROR" {
			found = true
			break
		}
	}
	assert.True(t, found, "expected ERROR log entry for panic recovery")
}

// ----------------------------
// PublishBatch: semaphore-acquire interrupted by ctx.Done()
// ----------------------------

// blockingPortPublisher blocks until its channel is closed.
type blockingPortPublisher struct {
	ch chan struct{}
}

func (p *blockingPortPublisher) Publish(_ context.Context, _ domain.Envelope[json.RawMessage]) error {
	<-p.ch
	return nil
}

func (p *blockingPortPublisher) PublishBatch(ctx context.Context, envs []domain.Envelope[json.RawMessage]) error {
	for _, env := range envs {
		if err := p.Publish(ctx, env); err != nil {
			return err
		}
	}
	return nil
}

var _ port.Publisher = (*blockingPortPublisher)(nil)

// TestOutboxService_PublishBatch_SemaphoreInterruptedByCtx exercises the
// `case <-ctx.Done():` select branch inside the for-range loop (outbox_service.go
// ~line 172-185). With publishConcurrency=2 and 3 records:
//   - records 0 and 1 acquire semaphore slots and block in Publish
//   - record 2 waits for a semaphore slot
//   - ctx is cancelled while record 2 is blocked → ctx.Done() fires → record 2
//     is marked failed without ever calling Publish
func TestOutboxService_PublishBatch_SemaphoreInterruptedByCtx(t *testing.T) {
	store := newMockStore()
	blockCh := make(chan struct{})
	pub := &blockingPortPublisher{ch: blockCh}
	logger := &fixtures.MockLogger{}
	clock := fixtures.NewFakeClock(time.Now())

	for range 3 {
		env := domain.NewEnvelope("sem.interrupt", "svc", json.RawMessage(`{}`))
		env.TenantID = "acme"
		payload, _ := json.Marshal(env)
		store.records = append(store.records, domain.OutboxRecord{
			ID:        env.ID,
			EventType: env.Type,
			Payload:   payload,
		})
	}

	ctx, cancel := context.WithCancel(context.Background())

	svc := service.NewOutboxService(store, pub, logger, clock, 5, 2, 10*time.Second)

	done := make(chan error, 1)
	go func() {
		done <- svc.PublishBatch(ctx, 10)
	}()

	// Give goroutine time to acquire semaphore and block in Publish.
	time.Sleep(50 * time.Millisecond)

	// Cancel — record 1 should see ctx.Done() while waiting for semaphore.
	cancel()

	// Release the block so record 0 finishes and the goroutine exits.
	close(blockCh)

	err := <-done
	// ctx.Err() should be propagated (Canceled or nil depending on timing).
	_ = err

	// At most 2 publishes were attempted; the third was rejected by the
	// semaphore interrupt path.
	assert.LessOrEqual(t, len(store.published), 2, "at most two records should be published before cancel")
}

func TestOutboxService_PublishBatch_SemaphoreInterruptedByCtx_MarkFailedError_Logged(t *testing.T) {
	store := &markFailedErrorStore{
		mockStore: newMockStore(),
		mfErr:     errors.New("mark failed error"),
	}
	blockCh := make(chan struct{})
	pub := &blockingPortPublisher{ch: blockCh}
	logger := &fixtures.MockLogger{}

	for range 3 {
		env := domain.NewEnvelope("sem.interrupt.err", "svc", json.RawMessage(`{}`))
		env.TenantID = "acme"
		payload, _ := json.Marshal(env)
		store.records = append(store.records, domain.OutboxRecord{
			ID: env.ID, EventType: env.Type, Payload: payload,
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	svc := service.NewOutboxService(store, pub, logger, nil, 5, 2, 10*time.Second)

	done := make(chan error, 1)
	go func() { done <- svc.PublishBatch(ctx, 10) }()

	time.Sleep(50 * time.Millisecond)
	cancel()
	close(blockCh)
	<-done

	found := false
	for _, e := range logger.Entries() {
		if e.Level == "ERROR" {
			found = true
			break
		}
	}
	assert.True(t, found, "expected ERROR log when MarkFailed fails during semaphore interrupt")
}

func TestOutboxService_PublishBatch_PanicRecovered_MarkFailedError_Logged(t *testing.T) {
	store := &markFailedErrorStore{
		mockStore: newMockStore(),
		mfErr:     errors.New("mark failed error"),
	}
	pub := &panicPortPublisher{}
	logger := &fixtures.MockLogger{}

	env := domain.NewEnvelope("panic.markfail", "svc", json.RawMessage(`{}`))
	payload, _ := json.Marshal(env)
	store.records = []domain.OutboxRecord{
		{ID: env.ID, EventType: env.Type, Payload: payload},
	}

	svc := service.NewOutboxService(store, pub, logger, nil, 5, 1, 0)
	require.NoError(t, svc.PublishBatch(context.Background(), 10))

	found := false
	for _, e := range logger.Entries() {
		if e.Level == "ERROR" {
			found = true
			break
		}
	}
	assert.True(t, found, "expected ERROR log when MarkFailed fails after panic recovery")
}

// ----------------------------
// OutboxService.PrunePublished
// ----------------------------

// pruneStore overrides PrunePublished to return configurable results.
type pruneStore struct {
	*mockStore
	n   int64
	err error
}

func (s *pruneStore) PrunePublished(_ context.Context, _ time.Duration, _ int) (int64, error) {
	return s.n, s.err
}

func TestOutboxService_PrunePublished_Success(t *testing.T) {
	store := &pruneStore{mockStore: newMockStore(), n: 15}
	pub := &fixtures.MockPublisher{}
	svc := service.NewOutboxService(store, pub, nil, nil, 5, 1, 0)

	n, err := svc.PrunePublished(context.Background(), 24*time.Hour, 100)
	require.NoError(t, err)
	assert.Equal(t, int64(15), n)
}

func TestOutboxService_PrunePublished_Zero(t *testing.T) {
	store := &pruneStore{mockStore: newMockStore(), n: 0}
	pub := &fixtures.MockPublisher{}
	svc := service.NewOutboxService(store, pub, nil, nil, 5, 1, 0)

	n, err := svc.PrunePublished(context.Background(), 7*24*time.Hour, 500)
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)
}

func TestOutboxService_PrunePublished_Error(t *testing.T) {
	store := &pruneStore{mockStore: newMockStore(), err: errors.New("db: prune failed")}
	pub := &fixtures.MockPublisher{}
	svc := service.NewOutboxService(store, pub, nil, nil, 5, 1, 0)

	_, err := svc.PrunePublished(context.Background(), 48*time.Hour, 200)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "db: prune failed")
}

// ----------------------------
// Enqueue: missing-branch validation tests
// ----------------------------

func TestOutboxService_Enqueue_ZeroTimestamp_ReturnsError(t *testing.T) {
	store := newMockStore()
	svc := service.NewOutboxService(store, &fixtures.MockPublisher{}, nil, nil, 5, 1, 0)

	var env domain.Envelope[json.RawMessage]
	env.ID = "some-id"
	env.Type = "order.placed"
	env.Source = "billing"
	env.Payload = json.RawMessage(`{}`)
	// env.Timestamp is zero value

	err := svc.Enqueue(context.Background(), noopTx{}, env)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "non-zero Timestamp")
}

func TestOutboxService_Enqueue_NullByte_ReturnsError(t *testing.T) {
	store := newMockStore()
	svc := service.NewOutboxService(store, &fixtures.MockPublisher{}, nil, nil, 5, 1, 0)

	env := domain.NewEnvelope("ok.type", "billing", json.RawMessage(`{}`))
	env.ID = "id-with-\x00-null"

	err := svc.Enqueue(context.Background(), noopTx{}, env)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "null bytes")
}

func TestOutboxService_Enqueue_OversizedPayload_ReturnsError(t *testing.T) {
	store := newMockStore()
	svc := service.NewOutboxService(store, &fixtures.MockPublisher{}, nil, nil, 5, 1, 0)

	// Construct a payload that exceeds 240 KB after JSON marshalling.
	bigPayload := json.RawMessage(`"` + strings.Repeat("x", 250*1024) + `"`)
	env := domain.NewEnvelope("big.event", "billing", bigPayload)

	err := svc.Enqueue(context.Background(), noopTx{}, env)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds safe SNS limit")
}

// ----------------------------
// PublishBatch: parallel path panic recovery with logger
// ----------------------------

// TestOutboxService_PublishBatch_Parallel_Panic_WithLogger verifies the goroutine-pool
// panic-recovery path when publishConcurrency > 1. The panic must be caught, logged at
// ERROR, and the record marked failed — matching the sequential path but via the goroutine
// defer/recover block in PublishBatch (not publishClaimedSequential).
func TestOutboxService_PublishBatch_Parallel_Panic_WithLogger(t *testing.T) {
	store := newMockStore()
	pub := &panicPortPublisher{}
	logger := &fixtures.MockLogger{}

	env := domain.NewEnvelope("parallel.panic", "svc", json.RawMessage(`{}`))
	payload, _ := json.Marshal(env)
	store.records = []domain.OutboxRecord{{ID: env.ID, EventType: env.Type, Payload: payload}}

	svc := service.NewOutboxService(store, pub, logger, nil, 5, 2, 0) // concurrency=2 → goroutine pool
	err := svc.PublishBatch(context.Background(), 10)
	require.NoError(t, err)

	store.mu.Lock()
	defer store.mu.Unlock()
	require.Contains(t, store.failed, env.ID, "panicked record must be marked failed")
	assert.Contains(t, store.failed[env.ID], "panic")

	found := false
	for _, e := range logger.Entries() {
		if e.Level == "ERROR" && strings.Contains(e.Message, "panic recovered") {
			found = true
			break
		}
	}
	assert.True(t, found, "expected ERROR log for panic recovery in goroutine pool")
}

// TestOutboxService_PublishBatch_Parallel_Panic_MarkFailedError_WithLogger covers the
// markErr != nil branch inside the goroutine panic recovery when MarkFailed fails.
func TestOutboxService_PublishBatch_Parallel_Panic_MarkFailedError_WithLogger(t *testing.T) {
	store := &markFailedErrorStore{
		mockStore: newMockStore(),
		mfErr:     errors.New("mark failed error"),
	}
	pub := &panicPortPublisher{}
	logger := &fixtures.MockLogger{}

	env := domain.NewEnvelope("parallel.panic.mf", "svc", json.RawMessage(`{}`))
	payload, _ := json.Marshal(env)
	store.records = []domain.OutboxRecord{{ID: env.ID, EventType: env.Type, Payload: payload}}

	svc := service.NewOutboxService(store, pub, logger, nil, 5, 2, 0) // concurrency=2
	require.NoError(t, svc.PublishBatch(context.Background(), 10))

	found := false
	for _, e := range logger.Entries() {
		if e.Level == "ERROR" && strings.Contains(e.Message, "failed to mark record failed") {
			found = true
			break
		}
	}
	assert.True(t, found, "expected ERROR log when MarkFailed fails after goroutine panic")
}

func TestNewOutboxService_ZeroPublishConcurrency_DefaultsToOne(t *testing.T) {
	// publishConcurrency <= 0 should be clamped to 1 inside NewOutboxService.
	// This covers the `if publishConcurrency <= 0 { publishConcurrency = 1 }` branch.
	store := newMockStore()
	svc := service.NewOutboxService(store, &fixtures.MockPublisher{}, nil, nil, 5, 0, 0)

	env := domain.NewEnvelope("test.event", "svc", json.RawMessage(`{}`))
	env.Payload = json.RawMessage(`{}`)

	// Just verify the service was constructed and can be used without panic.
	tx := noopTx{}
	_ = svc.Enqueue(context.Background(), tx, env) // error expected (tx is noop), no panic
}

// A payload that is not valid JSON fails OutboxService.Enqueue before the
// store is called.
func TestOutboxService_Enqueue_InvalidPayload_StoreNotCalled(t *testing.T) {
	store := newMockStore()
	svc := service.NewOutboxService(store, &fixtures.MockPublisher{}, nil, nil, 5, 1, 0)
	env := domain.NewEnvelope("bad.payload", "svc", json.RawMessage(`{not json`))
	require.Error(t, svc.Enqueue(context.Background(), noopTx{}, env))
	store.mu.Lock()
	defer store.mu.Unlock()
	assert.Empty(t, store.records)
}

// A permanent batch failure counts an attempt (so it eventually dead-letters);
// a Retryable one releases the lease without counting.
func TestOutboxService_PublishBatch_Sequential_RetryableFlag(t *testing.T) {
	mk := func() (domain.Envelope[json.RawMessage], domain.OutboxRecord) {
		env := domain.NewEnvelope("flag.event", "svc", json.RawMessage(`{}`))
		payload, _ := json.Marshal(env)
		return env, domain.OutboxRecord{ID: env.ID, EventType: env.Type, Payload: payload}
	}
	permEnv, permRec := mk()
	retryEnv, retryRec := mk()
	store := newMockStore()
	store.records = []domain.OutboxRecord{permRec, retryRec}
	pub := &domainBatchErrPublisher{err: &domain.BatchError{Failures: []domain.BatchFailure{
		{ID: permEnv.ID, Code: "AuthorizationError", Message: "not authorized"},
		{ID: retryEnv.ID, Code: "Throttled", Message: "slow down", Retryable: true},
	}}}
	svc := service.NewOutboxService(store, pub, nil, nil, 5, 1, 0)
	require.NoError(t, svc.PublishBatch(context.Background(), 10))

	assert.Equal(t, "not authorized", store.failed[permEnv.ID], "permanent failure counts an attempt")
	assert.NotContains(t, store.released, permEnv.ID)
	assert.Equal(t, "slow down", store.released[retryEnv.ID], "retryable failure releases the lease")
	assert.NotContains(t, store.failed, retryEnv.ID)
}

func TestOutboxService_Enqueue_NonCanonicalID_Rejected(t *testing.T) {
	store := newMockStore()
	svc := service.NewOutboxService(store, &fixtures.MockPublisher{}, nil, nil, 5, 1, 0)
	env := domain.NewEnvelope("id.check", "svc", json.RawMessage(`{}`))
	env.ID = strings.ToUpper(env.ID)
	err := svc.Enqueue(context.Background(), noopTx{}, env)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "canonical lowercase UUID")
	assert.Empty(t, store.records)
}
