package outbox_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/test/fixtures"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// noopTx is a minimal pgx.Tx stub used to avoid nil-tx panics when testing
// OutboxService.Enqueue with a store that returns an error immediately.
type noopTx struct{}

func (noopTx) Begin(_ context.Context) (pgx.Tx, error) { return nil, errors.New("noop") }
func (noopTx) Commit(_ context.Context) error          { return nil }
func (noopTx) Rollback(_ context.Context) error        { return nil }
func (noopTx) CopyFrom(_ context.Context, _ pgx.Identifier, _ []string, _ pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("noop")
}
func (noopTx) SendBatch(_ context.Context, _ *pgx.Batch) pgx.BatchResults { return nil }
func (noopTx) LargeObjects() pgx.LargeObjects                             { return pgx.LargeObjects{} }
func (noopTx) Prepare(_ context.Context, _, _ string) (*pgconn.StatementDescription, error) {
	return nil, errors.New("noop")
}
func (noopTx) Exec(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}
func (noopTx) Query(_ context.Context, _ string, _ ...any) (pgx.Rows, error) {
	return nil, errors.New("noop")
}
func (noopTx) QueryRow(_ context.Context, _ string, _ ...any) pgx.Row { return nil }
func (noopTx) Conn() *pgx.Conn                                        { return nil }

var _ pgx.Tx = noopTx{}

// mockStore is a thread-safe in-memory OutboxStore for testing.
// The mutex protects published and failed maps which are written by parallel
// goroutines in tests that use publishConcurrency > 1.
type mockStore struct {
	mu        sync.Mutex
	records   []domain.OutboxRecord
	published map[string]bool
	failed    map[string]string
	err       error
}

func newMockStore() *mockStore {
	return &mockStore{
		published: make(map[string]bool),
		failed:    make(map[string]string),
	}
}

func (s *mockStore) Enqueue(_ context.Context, _ pgx.Tx, rec domain.OutboxRecord) error {
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

func (s *mockStore) MarkFailed(_ context.Context, id, lastError string, _ int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failed[id] = lastError
	return nil
}

func (s *mockStore) PendingCount(_ context.Context) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return int64(len(s.records)), nil
}

// Ensure mockStore satisfies port.OutboxStore at compile time.
var _ port.OutboxStore = (*mockStore)(nil)

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

func TestOutboxService_Enqueue_StoreError(t *testing.T) {
	store := newMockStore()
	store.err = errors.New("store failure")
	pub := &fixtures.MockPublisher{}
	svc := service.NewOutboxService(store, pub, nil, nil, 5, 1, 0)

	env := domain.NewEnvelope("x.y", "svc", json.RawMessage(`{}`))
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

func (s *markFailedErrorStore) MarkFailed(_ context.Context, id, lastError string, maxAttempts int) error {
	_ = s.mockStore.MarkFailed(context.Background(), id, lastError, maxAttempts)
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
	assert.Len(t, store.failed, 3, "all stranded records should have MarkFailed called")
	for id, reason := range store.failed {
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
	assert.Len(t, store.failed, 4, "all records should have MarkFailed called")
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

	// Record should be marked failed because the publish timed out.
	assert.Contains(t, store.failed, env.ID, "timed-out record should be marked failed")
}

// ----------------------------
// Shutdown does not dead-letter at-maxAttempts-1 records (maxAttempts+1 fix)
// ----------------------------

func TestOutboxService_CtxCancel_DoesNotDeadLetterAtMaxAttemptsMinusOne(t *testing.T) {
	// Custom store that captures the maxAttempts ceiling passed to MarkFailed.
	type failCall struct{ id string; maxAttempts int }
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

	require.Len(t, calls, 1)
	// Cancellation must pass maxAttempts+1 (=6) so attempts=4 is never dead-lettered by shutdown alone.
	assert.Equal(t, 6, calls[0].maxAttempts, "cancellation path must use maxAttempts+1")
}

type captureMaxAttemptsStore struct {
	*mockStore
	onFail func(id string, maxAttempts int)
}

func (s *captureMaxAttemptsStore) MarkFailed(_ context.Context, id, _ string, maxAttempts int) error {
	s.onFail(id, maxAttempts)
	s.failed[id] = "captured"
	return nil
}

// ----------------------------
// TenantID warning
// ----------------------------

func TestOutboxService_Enqueue_EmptyTenantID_LogsWarning(t *testing.T) {
	store := newMockStore()
	logger := &fixtures.MockLogger{}
	svc := service.NewOutboxService(store, &fixtures.MockPublisher{}, logger, nil, 5, 1, 0)

	env := domain.NewEnvelope("evt.type", "svc", json.RawMessage(`{}`))
	// env.TenantID intentionally left empty

	err := svc.Enqueue(context.Background(), noopTx{}, env)
	require.NoError(t, err)

	entries := logger.Entries()
	found := false
	for _, e := range entries {
		if e.Level == "WARN" {
			found = true
			break
		}
	}
	assert.True(t, found, "expected WARN for empty TenantID")
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
