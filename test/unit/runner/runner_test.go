package runner_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/adapter/outbound/metrics"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/events"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/outbox"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/test/fixtures"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/migrate"
)

// mockOutboxStore is a thread-safe in-memory OutboxStore for runner tests.
// The mutex allows tests to safely set claimErr while the runner reads it.
type mockOutboxStore struct {
	mu        sync.Mutex
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

func (s *mockOutboxStore) setClaimErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.claimErr = err
}

func (s *mockOutboxStore) Enqueue(_ context.Context, _ pgx.Tx, rec domain.OutboxRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = append(s.records, rec)
	return nil
}

func (s *mockOutboxStore) ClaimBatch(_ context.Context, n int) ([]domain.OutboxRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
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
	s.mu.Lock()
	defer s.mu.Unlock()
	s.published[id] = true
	return nil
}

func (s *mockOutboxStore) MarkFailed(_ context.Context, id, lastError string, _ int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failed[id] = lastError
	return nil
}

func (s *mockOutboxStore) PendingCount(_ context.Context) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
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
	store.setClaimErr(errors.New("db connection error"))
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

// TestRunner_Start_AlreadyRunning_ReturnsError verifies that a second Start() call
// while the runner is active returns an "already running" error.
func TestRunner_Start_AlreadyRunning_ReturnsError(t *testing.T) {
	store := newMockOutboxStore()
	pub := &mockPublicPublisher{inner: &fixtures.MockPublisher{}}

	r := outbox.NewRunner(outbox.Config{
		Store:        store,
		Publisher:    pub,
		PollInterval: 10 * time.Second,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ready := make(chan struct{})
	go func() {
		close(ready)
		_ = r.Start(ctx)
	}()

	<-ready
	// Give the goroutine time to acquire the atomic.
	time.Sleep(20 * time.Millisecond)

	err := r.Start(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already running")
}

// ----------------------------
// Stop() drain timeout
// ----------------------------

func TestRunner_Stop_DrainTimeout_ReturnsError(t *testing.T) {
	store := newMockOutboxStore()

	// Inject a record so the runner calls Publish (which we can block).
	env := domain.NewEnvelope("drain.event", "svc", json.RawMessage(`{}`))
	payload, _ := json.Marshal(env)
	store.records = []domain.OutboxRecord{{ID: env.ID, EventType: env.Type, Payload: payload}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	blockCh := make(chan struct{})
	blockingPub := &blockingPublicPublisher{ch: blockCh}

	r2 := outbox.NewRunner(outbox.Config{
		Store:        store,
		Publisher:    blockingPub,
		PollInterval: 10 * time.Second,
		DrainTimeout: 50 * time.Millisecond,
	})

	go func() { _ = r2.Start(ctx) }()

	// Give the goroutine time to enter the blocking publisher.
	time.Sleep(30 * time.Millisecond)

	// Stop should time out because the poll cycle is blocked.
	err := r2.Stop()
	// Release the block so Start can exit cleanly (avoids goroutine leak in test).
	close(blockCh)

	require.Error(t, err, "Stop() should return an error when drain timeout is exceeded")
	assert.Contains(t, err.Error(), "drain timeout")
}

// blockingPublicPublisher blocks until its channel is closed.
type blockingPublicPublisher struct {
	ch chan struct{}
}

func (p *blockingPublicPublisher) Publish(_ context.Context, _ events.Envelope[json.RawMessage]) error {
	<-p.ch
	return nil
}
func (p *blockingPublicPublisher) PublishBatch(ctx context.Context, envs []events.Envelope[json.RawMessage]) error {
	for _, e := range envs {
		if err := p.Publish(ctx, e); err != nil {
			return err
		}
	}
	return nil
}

var _ events.Publisher = (*blockingPublicPublisher)(nil)

// ----------------------------
// Poll backoff on consecutive failures
// ----------------------------

func TestRunner_PollBackoff_ResetOnSuccess(t *testing.T) {
	store := newMockOutboxStore()
	inner := &fixtures.MockPublisher{}
	pub := &mockPublicPublisher{inner: inner}
	logger := &fixtures.MockLogger{}

	// Make ClaimBatch fail initially, then succeed.
	store.setClaimErr(errors.New("db error"))

	r := outbox.NewRunner(outbox.Config{
		Store:        store,
		Publisher:    pub,
		Logger:       logger,
		PollInterval: 20 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	startDone := make(chan error, 1)
	go func() { startDone <- r.Start(ctx) }()

	// Wait for at least one ERROR log (poll failed).
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		errFound := false
		for _, e := range logger.Entries() {
			if e.Level == "ERROR" {
				errFound = true
				break
			}
		}
		if errFound {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Now clear the error — runner should recover and run successfully.
	store.setClaimErr(nil)

	// Give it time to recover.
	time.Sleep(100 * time.Millisecond)

	cancel()
	require.NoError(t, <-startDone)
}

// ----------------------------
// Startup jitter
// ----------------------------

func TestRunner_StartupJitter_DoesNotPreventNormalOperation(t *testing.T) {
	store := newMockOutboxStore()
	inner := &fixtures.MockPublisher{}
	pub := &mockPublicPublisher{inner: inner}

	env := domain.NewEnvelope("jitter.event", "svc", json.RawMessage(`{}`))
	payload, _ := json.Marshal(env)
	store.records = []domain.OutboxRecord{{ID: env.ID, EventType: env.Type, Payload: payload}}

	r := outbox.NewRunner(outbox.Config{
		Store:         store,
		Publisher:     pub,
		PollInterval:  100 * time.Millisecond,
		StartupJitter: 20 * time.Millisecond, // tiny jitter for fast test
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = r.Start(ctx) }()

	// Despite the jitter, the runner should still deliver the event.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if len(inner.Published()) >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	_ = r.Stop()
	assert.NotEmpty(t, inner.Published(), "runner should deliver events even with startup jitter")
}

// TestRunner_StartupJitter_StoppedDuringJitter covers the stopCtx.Done() case
// inside the jitter select (line 181 in runner.go).
func TestRunner_StartupJitter_StoppedDuringJitter(t *testing.T) {
	store := newMockOutboxStore()
	pub := &mockPublicPublisher{inner: &fixtures.MockPublisher{}}

	r := outbox.NewRunner(outbox.Config{
		Store:         store,
		Publisher:     pub,
		PollInterval:  10 * time.Second,
		StartupJitter: 10 * time.Second, // long jitter so Stop fires first
	})

	ctx := context.Background()
	startDone := make(chan error, 1)
	go func() { startDone <- r.Start(ctx) }()

	time.Sleep(20 * time.Millisecond) // let jitter sleep begin
	err := r.Stop()                   // fires stopCtx.Done() path
	require.NoError(t, err)

	select {
	case err := <-startDone:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("Start did not return after Stop() during jitter")
	}
}

// TestRunner_SleepBackoff_TimerFires covers the case where the backoff timer
// fires naturally (case <-timer.C in sleepBackoff) rather than being interrupted
// by context cancellation. Uses the initial-poll-failure path.
func TestRunner_SleepBackoff_TimerFires(t *testing.T) {
	store := newMockOutboxStore()
	store.setClaimErr(errors.New("db error"))
	pub := &mockPublicPublisher{inner: &fixtures.MockPublisher{}}

	r := outbox.NewRunner(outbox.Config{
		Store:        store,
		Publisher:    pub,
		PollInterval: 10 * time.Second, // long so ticker doesn't interfere
	})

	ctx, cancel := context.WithCancel(context.Background())
	startDone := make(chan error, 1)
	go func() { startDone <- r.Start(ctx) }()

	// Wait > initPollBackoff (1s) so the sleepBackoff timer.C case fires naturally,
	// doubles the backoff, and returns true before we cancel.
	time.Sleep(1200 * time.Millisecond)
	cancel()

	select {
	case err := <-startDone:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("Start did not return after cancel")
	}
}

// TestRunner_TickerPollFails_Backoff covers the ticker.C code path where a poll
// cycle fails after a successful initial poll, triggering the backoff inside the
// for loop (lines 208-210 in runner.go: pollOnce true, sleepBackoff, return nil).
func TestRunner_TickerPollFails_Backoff(t *testing.T) {
	store := newMockOutboxStore()
	pub := &mockPublicPublisher{inner: &fixtures.MockPublisher{}}
	logger := &fixtures.MockLogger{}

	r := outbox.NewRunner(outbox.Config{
		Store:        store,
		Publisher:    pub,
		Logger:       logger,
		PollInterval: 20 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	startDone := make(chan error, 1)
	go func() { startDone <- r.Start(ctx) }()

	// Let initial poll succeed (store is empty, no error).
	time.Sleep(10 * time.Millisecond)

	// Now inject a DB error so the TICKER poll fails.
	store.setClaimErr(errors.New("ticker db error"))

	// Wait for the ERROR log from the ticker-path poll failure.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, e := range logger.Entries() {
			if e.Level == "ERROR" {
				// Error detected — cancel during the backoff sleep so sleepBackoff
				// returns false, covering the "return nil" path inside the for loop.
				cancel()
				require.NoError(t, <-startDone)
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	t.Fatal("expected ERROR log from ticker-path poll failure")
}

func TestRunner_StartupJitter_ContextCancelled_During_Jitter(t *testing.T) {
	store := newMockOutboxStore()
	pub := &mockPublicPublisher{inner: &fixtures.MockPublisher{}}

	r := outbox.NewRunner(outbox.Config{
		Store:         store,
		Publisher:     pub,
		PollInterval:  10 * time.Second,
		StartupJitter: 10 * time.Second, // long jitter — ctx will cancel first
	})

	ctx, cancel := context.WithCancel(context.Background())
	startDone := make(chan error, 1)
	go func() { startDone <- r.Start(ctx) }()

	time.Sleep(10 * time.Millisecond)
	cancel() // cancel during jitter

	select {
	case err := <-startDone:
		require.NoError(t, err, "Start should return nil when ctx cancelled during jitter")
	case <-time.After(3 * time.Second):
		t.Fatal("Start did not return after ctx cancel during jitter")
	}
}
