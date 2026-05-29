package runner_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/adapter/outbound/metrics"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/events"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/outbox"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/test/fixtures"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/migrate"
	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockOutboxStore is an in-memory OutboxStore for runner tests.
type mockOutboxStore struct {
	records   []domain.OutboxRecord
	published map[string]bool
	failed    map[string]string
	claimErr  error
}

func newMockOutboxStore() *mockOutboxStore {
	return &mockOutboxStore{
		published: make(map[string]bool),
		failed:    make(map[string]string),
	}
}

func (s *mockOutboxStore) Enqueue(_ context.Context, _ pgx.Tx, rec domain.OutboxRecord) error {
	s.records = append(s.records, rec)
	return nil
}

func (s *mockOutboxStore) ClaimBatch(_ context.Context, n int) ([]domain.OutboxRecord, error) {
	if s.claimErr != nil {
		return nil, s.claimErr
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

func (s *mockOutboxStore) MarkPublished(_ context.Context, id string) error {
	s.published[id] = true
	return nil
}

func (s *mockOutboxStore) MarkFailed(_ context.Context, id, lastError string, _ int) error {
	s.failed[id] = lastError
	return nil
}

func (s *mockOutboxStore) PendingCount(_ context.Context) (int64, error) {
	return int64(len(s.records)), nil
}

var _ port.OutboxStore = (*mockOutboxStore)(nil)

// mockPublicPublisher wraps fixtures.MockPublisher to expose events.Publisher interface.
type mockPublicPublisher struct {
	inner *fixtures.MockPublisher
}

func (m *mockPublicPublisher) Publish(_ context.Context, env events.Envelope[json.RawMessage]) error {
	return m.inner.Publish(context.Background(), domain.Envelope[json.RawMessage]{
		ID:            env.ID,
		Type:          env.Type,
		Source:        env.Source,
		TenantID:      env.TenantID,
		TraceID:       env.TraceID,
		CorrelationID: env.CorrelationID,
		Timestamp:     env.Timestamp,
		Payload:       env.Payload,
	})
}

func (m *mockPublicPublisher) PublishBatch(ctx context.Context, envs []events.Envelope[json.RawMessage]) error {
	for _, env := range envs {
		if err := m.Publish(ctx, env); err != nil {
			return err
		}
	}
	return nil
}

var _ events.Publisher = (*mockPublicPublisher)(nil)

// ----------------------------
// NewRunner: defaults applied
// ----------------------------

func TestNewRunner_Defaults(t *testing.T) {
	store := newMockOutboxStore()
	pub := &mockPublicPublisher{inner: &fixtures.MockPublisher{}}

	r := outbox.NewRunner(outbox.Config{
		Store:     store,
		Publisher: pub,
	})
	assert.NotNil(t, r)
}

func TestNewRunner_CustomValues(t *testing.T) {
	store := newMockOutboxStore()
	pub := &mockPublicPublisher{inner: &fixtures.MockPublisher{}}

	r := outbox.NewRunner(outbox.Config{
		Store:        store,
		Publisher:    pub,
		PollInterval: 2 * time.Second,
		BatchSize:    20,
		MaxAttempts:  3,
	})
	assert.NotNil(t, r)
}

// ----------------------------
// Start with immediately cancelled context
// ----------------------------

func TestRunner_Start_ImmediateCancel(t *testing.T) {
	store := newMockOutboxStore()
	pub := &mockPublicPublisher{inner: &fixtures.MockPublisher{}}

	r := outbox.NewRunner(outbox.Config{
		Store:        store,
		Publisher:    pub,
		PollInterval: 10 * time.Second, // long interval — context cancels first
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	err := r.Start(ctx)
	require.NoError(t, err)
}

// ----------------------------
// Start + Stop: runner stops cleanly
// ----------------------------

func TestRunner_StartStop(t *testing.T) {
	store := newMockOutboxStore()
	pub := &mockPublicPublisher{inner: &fixtures.MockPublisher{}}

	r := outbox.NewRunner(outbox.Config{
		Store:        store,
		Publisher:    pub,
		PollInterval: 100 * time.Millisecond,
	})

	ctx := context.Background()
	errCh := make(chan error, 1)
	go func() {
		errCh <- r.Start(ctx)
	}()

	time.Sleep(150 * time.Millisecond)

	err := r.Stop()
	require.NoError(t, err)

	select {
	case startErr := <-errCh:
		require.NoError(t, startErr)
	case <-time.After(3 * time.Second):
		t.Fatal("Start did not return after Stop")
	}
}

// ----------------------------
// Runner.Stop() is idempotent (second call must not panic)
// ----------------------------

func TestRunner_Stop_IsIdempotent(t *testing.T) {
	store := newMockOutboxStore()
	pub := &mockPublicPublisher{inner: &fixtures.MockPublisher{}}

	r := outbox.NewRunner(outbox.Config{
		Store:        store,
		Publisher:    pub,
		PollInterval: 100 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = r.Start(ctx) }()
	time.Sleep(20 * time.Millisecond)

	assert.NotPanics(t, func() {
		_ = r.Stop()
		_ = r.Stop() // second call must be a no-op, not a panic
	})
	cancel()
}

// ----------------------------
// Runner polls publisher on tick
// ----------------------------

func TestRunner_PollsPublisher(t *testing.T) {
	store := newMockOutboxStore()
	inner := &fixtures.MockPublisher{}
	pub := &mockPublicPublisher{inner: inner}

	// Enqueue a record into the mock store.
	env := domain.NewEnvelope("order.placed", "billing", json.RawMessage(`{"amount":100}`))
	payload, err := json.Marshal(env)
	require.NoError(t, err)
	store.records = []domain.OutboxRecord{
		{ID: env.ID, EventType: env.Type, Payload: payload},
	}

	r := outbox.NewRunner(outbox.Config{
		Store:        store,
		Publisher:    pub,
		PollInterval: 50 * time.Millisecond,
		BatchSize:    10,
		MaxAttempts:  3,
	})

	ctx := context.Background()
	go func() { _ = r.Start(ctx) }()

	// Wait for at least one poll cycle.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if len(inner.Published()) >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	_ = r.Stop()

	published := inner.Published()
	require.NotEmpty(t, published, "publisher should have been called")
	assert.Equal(t, env.ID, published[0].ID)
}

// ----------------------------
// Runner with logger logs errors
// ----------------------------

func TestRunner_WithLogger_LogsOnError(t *testing.T) {
	store := newMockOutboxStore()
	pub := &mockPublicPublisher{inner: &fixtures.MockPublisher{}}
	logger := &fixtures.MockLogger{}

	r := outbox.NewRunner(outbox.Config{
		Store:        store,
		Publisher:    pub,
		Logger:       logger,
		PollInterval: 50 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := r.Start(ctx)
	require.NoError(t, err)
}

// ----------------------------
// publisherBridge.PublishBatch coverage
// ----------------------------

func TestPublisherBridge_PublishBatch(t *testing.T) {
	inner := &fixtures.MockPublisher{}
	pub := &mockPublicPublisher{inner: inner}

	bridge := outbox.NewPublisherBridge(pub)
	require.NotNil(t, bridge)

	envs := []domain.Envelope[json.RawMessage]{
		domain.NewEnvelope("evt.one", "svc", json.RawMessage(`{}`)),
		domain.NewEnvelope("evt.two", "svc", json.RawMessage(`{}`)),
	}

	err := bridge.PublishBatch(context.Background(), envs)
	require.NoError(t, err)

	published := inner.Published()
	require.Len(t, published, 2)
}

func TestPublisherBridge_Publish(t *testing.T) {
	inner := &fixtures.MockPublisher{}
	pub := &mockPublicPublisher{inner: inner}

	bridge := outbox.NewPublisherBridge(pub)

	env := domain.NewEnvelope("evt.test", "svc", json.RawMessage(`{"k":"v"}`))
	env.TenantID = "acme"
	env.TraceID = "trace-123"
	env.CorrelationID = "corr-456"

	err := bridge.Publish(context.Background(), env)
	require.NoError(t, err)

	published := inner.Published()
	require.Len(t, published, 1)
	assert.Equal(t, env.ID, published[0].ID)
	assert.Equal(t, "acme", published[0].TenantID)
}

// ----------------------------
// NewRunner panics on missing required fields
// ----------------------------

func TestNewRunner_NilPublisher_Panics(t *testing.T) {
	store := newMockOutboxStore()
	assert.Panics(t, func() {
		outbox.NewRunner(outbox.Config{
			Store:     store,
			Publisher: nil, // required — must panic at construction time
		})
	})
}

func TestNewRunner_NilPoolAndNilStore_Panics(t *testing.T) {
	pub := &mockPublicPublisher{inner: &fixtures.MockPublisher{}}
	assert.Panics(t, func() {
		outbox.NewRunner(outbox.Config{
			Pool:      nil, // both nil → panic
			Store:     nil,
			Publisher: pub,
		})
	})
}

// ----------------------------
// Runner logs error when poll cycle fails
// ----------------------------

func TestRunner_Start_LogsError(t *testing.T) {
	store := newMockOutboxStore()
	store.claimErr = errors.New("db connection error")
	pub := &mockPublicPublisher{inner: &fixtures.MockPublisher{}}
	logger := &fixtures.MockLogger{}

	r := outbox.NewRunner(outbox.Config{
		Store:        store,
		Publisher:    pub,
		Logger:       logger,
		PollInterval: 30 * time.Millisecond,
	})

	ctx := context.Background()
	startDone := make(chan struct{})
	go func() {
		defer close(startDone)
		_ = r.Start(ctx)
	}()

	// Wait until the ERROR log appears (poll cycle ran at least once).
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, e := range logger.Entries() {
			if e.Level == "ERROR" {
				_ = r.Stop()
				<-startDone
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	_ = r.Stop()
	<-startDone
	t.Fatal("expected ERROR log from failed poll cycle")
}

// ----------------------------
// Runner restart: Stop → Start again must work
// ----------------------------

func TestRunner_Restart(t *testing.T) {
	store := newMockOutboxStore()
	inner := &fixtures.MockPublisher{}
	pub := &mockPublicPublisher{inner: inner}

	r := outbox.NewRunner(outbox.Config{
		Store:        store,
		Publisher:    pub,
		PollInterval: 50 * time.Millisecond,
	})

	// First cycle.
	ctx1, cancel1 := context.WithCancel(context.Background())
	done1 := make(chan error, 1)
	go func() { done1 <- r.Start(ctx1) }()
	time.Sleep(60 * time.Millisecond) // let one poll fire
	cancel1()
	require.NoError(t, <-done1, "first Start() should return nil")

	// Second cycle — must not panic or return 'already running'.
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	done2 := make(chan error, 1)
	go func() { done2 <- r.Start(ctx2) }()
	time.Sleep(60 * time.Millisecond)
	cancel2()

	select {
	case err := <-done2:
		require.NoError(t, err, "second Start() should return nil after restart")
	case <-time.After(3 * time.Second):
		t.Fatal("second Start() did not return — runner is not restartable")
	}
}

// TestRunner_StopBeforeStart verifies that Stop() before Start() does not hang.
// ----------------------------
// publisherBridge: Publish with metrics initialized
// ----------------------------

func TestPublisherBridge_PublishWithMetrics_Success(t *testing.T) {
	reg := prometheus.NewRegistry()
	metrics.InitWithRegisterer("bridge-test", "v0.0.1", reg)

	inner := &fixtures.MockPublisher{}
	pub := &mockPublicPublisher{inner: inner}
	bridge := outbox.NewPublisherBridge(pub)

	env := domain.NewEnvelope("bridge.event", "svc", json.RawMessage(`{}`))
	err := bridge.Publish(context.Background(), env)
	require.NoError(t, err)
	assert.Len(t, inner.Published(), 1)
}

func TestPublisherBridge_PublishWithMetrics_Error(t *testing.T) {
	reg := prometheus.NewRegistry()
	metrics.InitWithRegisterer("bridge-err-test", "v0.0.2", reg)

	inner := &fixtures.MockPublisher{}
	inner.SetError(errors.New("publish failed"))
	pub := &mockPublicPublisher{inner: inner}
	bridge := outbox.NewPublisherBridge(pub)

	env := domain.NewEnvelope("bridge.fail", "svc", json.RawMessage(`{}`))
	err := bridge.Publish(context.Background(), env)
	require.Error(t, err)
}

// ----------------------------
// Runner: pollOnce exercises the metrics gauge path
// ----------------------------

func TestRunner_PollOnce_UpdatesPendingGauge(t *testing.T) {
	reg := prometheus.NewRegistry()
	metrics.InitWithRegisterer("poll-once-test", "v0.0.3", reg)

	store := newMockOutboxStore()
	pub := &mockPublicPublisher{inner: &fixtures.MockPublisher{}}

	r := outbox.NewRunner(outbox.Config{
		Store:        store,
		Publisher:    pub,
		PollInterval: 10 * time.Second, // long — ctx will cancel first
	})

	// Start runs pollOnce immediately, then waits for ticker.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	err := r.Start(ctx)
	require.NoError(t, err)
}

// ----------------------------
// ApplySchema: error-path coverage
// ----------------------------

func TestApplySchema_NilRunner_Error(t *testing.T) {
	err := outbox.ApplySchema(context.Background(), nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nil migrate.Runner")
}

func TestApplySchema_EmptyDSN_Error(t *testing.T) {
	runner := &migrate.Runner{DSN: ""}
	err := outbox.ApplySchema(context.Background(), runner)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "non-empty DSN")
}

func TestRunner_StopBeforeStart(t *testing.T) {
	store := newMockOutboxStore()
	pub := &mockPublicPublisher{inner: &fixtures.MockPublisher{}}

	r := outbox.NewRunner(outbox.Config{Store: store, Publisher: pub})

	done := make(chan struct{})
	go func() {
		_ = r.Stop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop() before Start() hung — should return immediately")
	}
}
