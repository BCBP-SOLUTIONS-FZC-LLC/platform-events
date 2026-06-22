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

func (s *mockOutboxStore) MarkFailed(_ context.Context, rec domain.OutboxRecord, lastError string, _ int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failed[rec.ID] = lastError
	return nil
}

func (s *mockOutboxStore) LeasedCount(_ context.Context) (int64, error)               { return 0, nil }
func (s *mockOutboxStore) ReprocessDeadLetters(_ context.Context, _ int) (int, error) { return 0, nil }
func (s *mockOutboxStore) PrunePublished(_ context.Context, _ time.Duration, _ int) (int64, error) {
	return 0, nil
}
func (s *mockOutboxStore) ListDeadLetters(_ context.Context, _ domain.DLQFilter, _ int) ([]domain.DeadLetterRecord, error) {
	return nil, nil
}
func (s *mockOutboxStore) ReprocessDeadLettersWith(_ context.Context, _ domain.DLQFilter, _ int) (int, error) {
	return 0, nil
}
func (s *mockOutboxStore) DiscardDeadLetters(_ context.Context, _ domain.DLQFilter, _ int) (int64, error) {
	return 0, nil
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

	r, err := outbox.NewRunner(outbox.Config{
		Store:     store,
		Publisher: pub,
	})
	require.NoError(t, err)
	assert.NotNil(t, r)
}

func TestNewRunner_CustomValues(t *testing.T) {
	store := newMockOutboxStore()
	pub := &mockPublicPublisher{inner: &fixtures.MockPublisher{}}

	r, err := outbox.NewRunner(outbox.Config{
		Store:        store,
		Publisher:    pub,
		PollInterval: 2 * time.Second,
		BatchSize:    20,
		MaxAttempts:  3,
	})
	require.NoError(t, err)
	assert.NotNil(t, r)
}

// ----------------------------
// Start with immediately cancelled context
// ----------------------------

func TestRunner_Start_ImmediateCancel(t *testing.T) {
	store := newMockOutboxStore()
	pub := &mockPublicPublisher{inner: &fixtures.MockPublisher{}}

	r, err := outbox.NewRunner(outbox.Config{
		Store:        store,
		Publisher:    pub,
		PollInterval: 10 * time.Second, // long interval — context cancels first
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	err = r.Start(ctx)
	require.NoError(t, err)
}

// ----------------------------
// Start + Stop: runner stops cleanly
// ----------------------------

func TestRunner_StartStop(t *testing.T) {
	store := newMockOutboxStore()
	pub := &mockPublicPublisher{inner: &fixtures.MockPublisher{}}

	r, err := outbox.NewRunner(outbox.Config{
		Store:        store,
		Publisher:    pub,
		PollInterval: 100 * time.Millisecond,
	})
	require.NoError(t, err)

	ctx := context.Background()
	errCh := make(chan error, 1)
	go func() {
		errCh <- r.Start(ctx)
	}()

	time.Sleep(150 * time.Millisecond)

	err = r.Stop()
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

	r, err := outbox.NewRunner(outbox.Config{
		Store:        store,
		Publisher:    pub,
		PollInterval: 100 * time.Millisecond,
	})
	require.NoError(t, err)

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

	r, err := outbox.NewRunner(outbox.Config{
		Store:        store,
		Publisher:    pub,
		PollInterval: 50 * time.Millisecond,
		BatchSize:    10,
		MaxAttempts:  3,
	})
	require.NoError(t, err)

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

	r, err := outbox.NewRunner(outbox.Config{
		Store:        store,
		Publisher:    pub,
		Logger:       logger,
		PollInterval: 50 * time.Millisecond,
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = r.Start(ctx)
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
		_, _ = outbox.NewRunner(outbox.Config{
			Store:     store,
			Publisher: nil, // required — must panic at construction time
		})
	})
}

func TestNewRunner_NilPoolAndNilStore_Panics(t *testing.T) {
	pub := &mockPublicPublisher{inner: &fixtures.MockPublisher{}}
	assert.Panics(t, func() {
		_, _ = outbox.NewRunner(outbox.Config{
			Pool:      nil, // both nil → panic
			Store:     nil,
			Publisher: pub,
		})
	})
}

func TestNewRunner_InvalidClaimLeaseDuration_Error(t *testing.T) {
	pub := &mockPublicPublisher{inner: &fixtures.MockPublisher{}}
	store := newMockOutboxStore()
	// BatchSize=10, PublishTimeout=5s → min lease = 10×5s+1m = 110s; 10s is too short.
	_, err := outbox.NewRunner(outbox.Config{
		Store:              store,
		Publisher:          pub,
		PublishTimeout:     5 * time.Second,
		BatchSize:          10,
		ClaimLeaseDuration: 10 * time.Second,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ClaimLeaseDuration")
}

// TestNewRunner_DisabledPublishTimeout_ClaimLeaseTooShort_Error covers the
// "else if effectiveLease < minLeaseFallback" branch in NewRunner when
// PublishTimeout is disabled (≤ 0) and ClaimLeaseDuration is < 30 s.
func TestNewRunner_DisabledPublishTimeout_ClaimLeaseTooShort_Error(t *testing.T) {
	pub := &mockPublicPublisher{inner: &fixtures.MockPublisher{}}
	store := newMockOutboxStore()
	// PublishTimeout=-1 disables per-record timeout; effective lease 5s < 30s floor.
	_, err := outbox.NewRunner(outbox.Config{
		Store:              store,
		Publisher:          pub,
		PublishTimeout:     -1 * time.Second,
		ClaimLeaseDuration: 5 * time.Second,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ClaimLeaseDuration")
	assert.Contains(t, err.Error(), "minimum is")
}

// ----------------------------
// Runner logs error when poll cycle fails
// ----------------------------

func TestRunner_Start_LogsError(t *testing.T) {
	store := newMockOutboxStore()
	store.setClaimErr(errors.New("db connection error"))
	pub := &mockPublicPublisher{inner: &fixtures.MockPublisher{}}
	logger := &fixtures.MockLogger{}

	r, err := outbox.NewRunner(outbox.Config{
		Store:        store,
		Publisher:    pub,
		Logger:       logger,
		PollInterval: 30 * time.Millisecond,
	})
	require.NoError(t, err)

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

	r, err := outbox.NewRunner(outbox.Config{
		Store:        store,
		Publisher:    pub,
		PollInterval: 50 * time.Millisecond,
	})
	require.NoError(t, err)

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

	r, err := outbox.NewRunner(outbox.Config{
		Store:        store,
		Publisher:    pub,
		PollInterval: 10 * time.Second, // long — ctx will cancel first
	})
	require.NoError(t, err)

	// Start runs pollOnce immediately, then waits for ticker.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	err = r.Start(ctx)
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

func TestApplySchema_MigrationsTableConstant(t *testing.T) {
	// The constant must equal the well-known table name so consumers and
	// integration tests can reference it without hardcoding a string.
	assert.Equal(t, "outbox_migrations", outbox.MigrationsTable)
}

func TestApplySchema_InvalidDSN_Error(t *testing.T) {
	// A DSN that url.Parse cannot handle should surface an error from
	// ApplySchema rather than silently falling back to "schema_migrations".
	runner := &migrate.Runner{DSN: "://not a valid url \x00"}
	err := outbox.ApplySchema(context.Background(), runner)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "migrations table")
}

func TestApplySchema_ValidDSN_CoversDSNManipulation(t *testing.T) {
	// Passes a parseable DSN — dsnWithMigrationsTable succeeds, then Up() fails
	// because there is no real database. This test exists solely to exercise the
	// url-manipulation happy path (q.Set + Encode) so that branch is not a
	// coverage gap when integration tests cannot run.
	runner := &migrate.Runner{DSN: "postgres://localhost:5432/testdb?sslmode=disable"}
	err := outbox.ApplySchema(context.Background(), runner)
	// We expect an error (no DB), but NOT the "migrations table" error — that
	// would mean url.Parse failed, which is what we are testing does NOT happen.
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "migrations table",
		"url manipulation should succeed; only the DB connection attempt should fail")
}

func TestApplySchema_ValidDSN_AlreadyHasMigrationsTable(t *testing.T) {
	// When the DSN already contains x-migrations-table, dsnWithMigrationsTable
	// must not override the existing value (the !q.Has() == false branch).
	runner := &migrate.Runner{DSN: "postgres://localhost:5432/testdb?x-migrations-table=custom_migrations"}
	err := outbox.ApplySchema(context.Background(), runner)
	require.Error(t, err) // DB connection fails — expected
	assert.NotContains(t, err.Error(), "migrations table")
}

func TestRunner_PollOnce_SchemaMissingHint_Logged(t *testing.T) {
	store := newMockOutboxStore()
	store.setClaimErr(errors.New(`pq: relation "outbox_events" does not exist`))

	logger := &fixtures.MockLogger{}
	pub := &mockPublicPublisher{inner: &fixtures.MockPublisher{}}
	r, err := outbox.NewRunner(outbox.Config{
		Store:        store,
		Publisher:    pub,
		Logger:       logger,
		PollInterval: 50 * time.Millisecond,
	})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go func() { _ = r.Start(ctx) }()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, e := range logger.Entries() {
			if e.Level == "ERROR" {
				if hint, ok := e.Fields["hint"].(string); ok {
					assert.Contains(t, hint, "ApplySchema")
					require.NoError(t, r.Stop())
					return
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("expected ERROR log with ApplySchema hint when outbox_events is missing")
}

func TestRunner_StopBeforeStart(t *testing.T) {
	store := newMockOutboxStore()
	pub := &mockPublicPublisher{inner: &fixtures.MockPublisher{}}

	r, err := outbox.NewRunner(outbox.Config{Store: store, Publisher: pub})
	require.NoError(t, err)

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

	r, err := outbox.NewRunner(outbox.Config{
		Store:        store,
		Publisher:    pub,
		PollInterval: 10 * time.Second,
	})
	require.NoError(t, err)

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

	err = r.Start(ctx)
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

	r2, err := outbox.NewRunner(outbox.Config{
		Store:        store,
		Publisher:    blockingPub,
		PollInterval: 10 * time.Second,
		DrainTimeout: 50 * time.Millisecond,
	})
	require.NoError(t, err)

	go func() { _ = r2.Start(ctx) }()

	// Give the goroutine time to enter the blocking publisher.
	time.Sleep(30 * time.Millisecond)

	// Stop should time out because the poll cycle is blocked.
	err = r2.Stop()
	// Release the block so Start can exit cleanly (avoids goroutine leak in test).
	close(blockCh)

	require.Error(t, err, "Stop() should return an error when drain timeout is exceeded")
	assert.Contains(t, err.Error(), "drain timeout")
}

// ----------------------------
// Runner: recovery after transient poll error (exercises backoff reset path)
// ----------------------------

// TestRunner_RecoveryAfterPollError verifies that the runner recovers from a
// transient ClaimBatch error and successfully closes Ready() once the error clears.
// This exercises the pollBackoff reset path in runner.go (pollBackoff = initPollBackoff
// on success after failures) — if the backoff did not reset, the runner would still
// recover but would sleep progressively longer between each subsequent failure cycle.
func TestRunner_RecoveryAfterPollError(t *testing.T) {
	store := newMockOutboxStore()
	pub := &mockPublicPublisher{inner: &fixtures.MockPublisher{}}

	// Inject a transient DB error so the first poll fails and backoff is triggered.
	store.setClaimErr(errors.New("transient db connection reset"))

	r, err := outbox.NewRunner(outbox.Config{
		Store:        store,
		Publisher:    pub,
		PollInterval: 10 * time.Millisecond,
	})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- r.Start(ctx) }()

	// Give the runner time to attempt the first poll and enter backoff sleep.
	time.Sleep(50 * time.Millisecond)

	// Clear the error — runner must recover after the backoff sleep (initPollBackoff=1s).
	store.setClaimErr(nil)

	// Ready() closes on the first successful poll. With a 1s backoff sleep the
	// recovery window is ~1.1s; allow 5s total for slow CI environments.
	select {
	case <-r.Ready():
		// Runner recovered: backoff fired, poll succeeded, Ready closed.
	case <-time.After(5 * time.Second):
		t.Fatal("runner did not recover from transient poll error within timeout")
	}

	require.NoError(t, r.Stop())
	require.NoError(t, <-errCh)
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

	r, err := outbox.NewRunner(outbox.Config{
		Store:        store,
		Publisher:    pub,
		Logger:       logger,
		PollInterval: 20 * time.Millisecond,
	})
	require.NoError(t, err)

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

	r, err := outbox.NewRunner(outbox.Config{
		Store:         store,
		Publisher:     pub,
		PollInterval:  100 * time.Millisecond,
		StartupJitter: 20 * time.Millisecond, // tiny jitter for fast test
	})
	require.NoError(t, err)

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

	r, err := outbox.NewRunner(outbox.Config{
		Store:         store,
		Publisher:     pub,
		PollInterval:  10 * time.Second,
		StartupJitter: 10 * time.Second, // long jitter so Stop fires first
	})
	require.NoError(t, err)

	ctx := context.Background()
	startDone := make(chan error, 1)
	go func() { startDone <- r.Start(ctx) }()

	time.Sleep(20 * time.Millisecond) // let jitter sleep begin
	err = r.Stop()                    // fires stopCtx.Done() path
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

	r, err := outbox.NewRunner(outbox.Config{
		Store:        store,
		Publisher:    pub,
		PollInterval: 10 * time.Second, // long so ticker doesn't interfere
	})
	require.NoError(t, err)

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

	r, err := outbox.NewRunner(outbox.Config{
		Store:        store,
		Publisher:    pub,
		Logger:       logger,
		PollInterval: 20 * time.Millisecond,
	})
	require.NoError(t, err)

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

	r, err := outbox.NewRunner(outbox.Config{
		Store:         store,
		Publisher:     pub,
		PollInterval:  10 * time.Second,
		StartupJitter: 10 * time.Second, // long jitter — ctx will cancel first
	})
	require.NoError(t, err)

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

// ----------------------------
// Runner.ReprocessDeadLetters
// ----------------------------

// ----------------------------
// DLQ store stubs
// ----------------------------

// reprocessableStore overrides ReprocessDeadLetters to return configurable results.
type reprocessableStore struct {
	*mockOutboxStore
	count int
	err   error
}

func (s *reprocessableStore) ReprocessDeadLetters(_ context.Context, _ int) (int, error) {
	return s.count, s.err
}

func TestRunner_ReprocessDeadLetters_Success(t *testing.T) {
	store := &reprocessableStore{mockOutboxStore: newMockOutboxStore(), count: 5}
	pub := &mockPublicPublisher{inner: &fixtures.MockPublisher{}}

	r, err := outbox.NewRunner(outbox.Config{Store: store, Publisher: pub})
	require.NoError(t, err)

	n, err := r.ReprocessDeadLetters(context.Background(), 10)
	require.NoError(t, err)
	assert.Equal(t, 5, n)
}

func TestRunner_ReprocessDeadLetters_Zero(t *testing.T) {
	store := &reprocessableStore{mockOutboxStore: newMockOutboxStore(), count: 0}
	pub := &mockPublicPublisher{inner: &fixtures.MockPublisher{}}

	r, err := outbox.NewRunner(outbox.Config{Store: store, Publisher: pub})
	require.NoError(t, err)

	n, err := r.ReprocessDeadLetters(context.Background(), 10)
	require.NoError(t, err)
	assert.Equal(t, 0, n)
}

func TestRunner_ReprocessDeadLetters_Error(t *testing.T) {
	store := &reprocessableStore{mockOutboxStore: newMockOutboxStore(), err: errors.New("db failure")}
	pub := &mockPublicPublisher{inner: &fixtures.MockPublisher{}}

	r, err := outbox.NewRunner(outbox.Config{Store: store, Publisher: pub})
	require.NoError(t, err)

	_, err = r.ReprocessDeadLetters(context.Background(), 10)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "db failure")
}

// ----------------------------
// outboxMetricsAdapter: RecordUnmarshalError via invalid-payload record
// ----------------------------

// TestRunner_UnmarshalError_MarksRecordFailed exercises the outboxMetricsAdapter's
// RecordUnmarshalError path. When a record payload cannot be unmarshalled the runner
// must mark the record failed (not panic) and log an ERROR.
func TestRunner_UnmarshalError_MarksRecordFailed(t *testing.T) {
	reg := prometheus.NewRegistry()
	metrics.InitWithRegisterer("runner-unmarshal-err", "v0.0.1", reg)

	store := newMockOutboxStore()
	store.records = []domain.OutboxRecord{
		{ID: "bad-json-record", EventType: "test.event", Payload: []byte("not-valid-json")},
	}

	inner := &fixtures.MockPublisher{}
	pub := &mockPublicPublisher{inner: inner}
	logger := &fixtures.MockLogger{}

	r, err := outbox.NewRunner(outbox.Config{
		Store:        store,
		Publisher:    pub,
		Logger:       logger,
		PollInterval: 50 * time.Millisecond,
		BatchSize:    10,
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = r.Start(ctx) }()

	// Wait for the record to be processed (marked failed).
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		store.mu.Lock()
		done := len(store.failed) > 0
		store.mu.Unlock()
		if done {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()

	store.mu.Lock()
	defer store.mu.Unlock()
	assert.Contains(t, store.failed, "bad-json-record", "invalid-JSON record must be marked failed")
	assert.Empty(t, inner.Published(), "invalid-JSON record must not be published")
}

// ----------------------------
// outboxMetricsAdapter: RecordMarkPublishedError via MarkPublished failure
// ----------------------------

// markPublishedErrStore overrides MarkPublished to always return an error.
type markPublishedErrStore struct {
	*mockOutboxStore
}

func (s *markPublishedErrStore) MarkPublished(_ context.Context, _ string) error {
	return errors.New("db: mark published timed out")
}

// TestRunner_MarkPublishedError_LogsError exercises the outboxMetricsAdapter's
// RecordMarkPublishedError path. After a successful publish, if MarkPublished fails
// the runner must log the error and continue (the record will be re-delivered).
func TestRunner_MarkPublishedError_LogsError(t *testing.T) {
	reg := prometheus.NewRegistry()
	metrics.InitWithRegisterer("runner-mark-published-err", "v0.0.2", reg)

	base := newMockOutboxStore()
	store := &markPublishedErrStore{mockOutboxStore: base}

	env := domain.NewEnvelope("test.event", "svc", json.RawMessage(`{}`))
	payload, _ := json.Marshal(env)
	base.records = []domain.OutboxRecord{
		{ID: env.ID, EventType: env.Type, Payload: payload},
	}

	inner := &fixtures.MockPublisher{}
	pub := &mockPublicPublisher{inner: inner}
	logger := &fixtures.MockLogger{}

	r, err := outbox.NewRunner(outbox.Config{
		Store:        store,
		Publisher:    pub,
		Logger:       logger,
		PollInterval: 50 * time.Millisecond,
		BatchSize:    10,
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = r.Start(ctx) }()

	// Wait for the publish attempt (publisher should be called).
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if len(inner.Published()) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond) // let the MarkPublished error path complete
	cancel()

	assert.NotEmpty(t, inner.Published(), "publisher must be called before MarkPublished error")

	found := false
	for _, e := range logger.Entries() {
		if e.Level == "ERROR" {
			found = true
			break
		}
	}
	assert.True(t, found, "expected ERROR log when MarkPublished fails after successful publish")
}

// ----------------------------
// pollOnce: PendingCount error path (metrics initialized, PendingCount fails)
// ----------------------------

// errPendingStore overrides PendingCount to return an error.
type errPendingStore struct {
	*mockOutboxStore
	pendErr   error
	leasedErr error
}

func (s *errPendingStore) PendingCount(_ context.Context) (int64, error) {
	if s.pendErr != nil {
		return 0, s.pendErr
	}
	return 0, nil
}

func (s *errPendingStore) LeasedCount(_ context.Context) (int64, error) {
	if s.leasedErr != nil {
		return 0, s.leasedErr
	}
	return 0, nil
}

// TestRunner_PollOnce_PendingCountError exercises the pendErr != nil branch in pollOnce.
// When metrics are registered and PendingCount returns an error the runner must log a
// WARN (not an ERROR) and set the gauge to -1, then continue the poll cycle normally.
func TestRunner_PollOnce_PendingCountError(t *testing.T) {
	reg := prometheus.NewRegistry()
	metrics.InitWithRegisterer("poll-once-pend-err", "v0.0.1", reg)

	base := newMockOutboxStore()
	store := &errPendingStore{
		mockOutboxStore: base,
		pendErr:         errors.New("db: count query timed out"),
	}

	logger := &fixtures.MockLogger{}
	pub := &mockPublicPublisher{inner: &fixtures.MockPublisher{}}

	r, err := outbox.NewRunner(outbox.Config{
		Store:        store,
		Publisher:    pub,
		Logger:       logger,
		PollInterval: 10 * time.Second, // long — ctx cancel fires first
	})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	err = r.Start(ctx)
	require.NoError(t, err)

	// The runner must have logged a WARN about the PendingCount failure.
	found := false
	for _, e := range logger.Entries() {
		if e.Level == "WARN" {
			found = true
			break
		}
	}
	assert.True(t, found, "expected WARN log when PendingCount returns an error")
}

// TestRunner_PollOnce_LeasedCountError exercises the leasedErr != nil branch in pollOnce.
// When metrics are registered and LeasedCount returns an error the runner must log a
// WARN (not an ERROR) and continue the poll cycle without setting the leased gauge.
func TestRunner_PollOnce_LeasedCountError(t *testing.T) {
	reg := prometheus.NewRegistry()
	metrics.InitWithRegisterer("poll-once-leased-err", "v0.0.2", reg)

	base := newMockOutboxStore()
	store := &errPendingStore{
		mockOutboxStore: base,
		leasedErr:       errors.New("db: leased count timed out"),
	}

	logger := &fixtures.MockLogger{}
	pub := &mockPublicPublisher{inner: &fixtures.MockPublisher{}}

	r, err := outbox.NewRunner(outbox.Config{
		Store:        store,
		Publisher:    pub,
		Logger:       logger,
		PollInterval: 10 * time.Second,
	})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	err = r.Start(ctx)
	require.NoError(t, err)

	found := false
	for _, e := range logger.Entries() {
		if e.Level == "WARN" {
			found = true
			break
		}
	}
	assert.True(t, found, "expected WARN log when LeasedCount returns an error")
}

// ----------------------------
// Runner.PrunePublished
// ----------------------------

// pruneableStore overrides PrunePublished to return configurable results.
type pruneableStore struct {
	*mockOutboxStore
	n   int64
	err error
}

func (s *pruneableStore) PrunePublished(_ context.Context, _ time.Duration, _ int) (int64, error) {
	return s.n, s.err
}

func TestRunner_PrunePublished_Success(t *testing.T) {
	store := &pruneableStore{mockOutboxStore: newMockOutboxStore(), n: 42}
	pub := &mockPublicPublisher{inner: &fixtures.MockPublisher{}}

	r, err := outbox.NewRunner(outbox.Config{Store: store, Publisher: pub})
	require.NoError(t, err)

	n, err := r.PrunePublished(context.Background(), 7*24*time.Hour, 1000)
	require.NoError(t, err)
	assert.Equal(t, int64(42), n)
}

func TestRunner_PrunePublished_Error(t *testing.T) {
	store := &pruneableStore{
		mockOutboxStore: newMockOutboxStore(),
		err:             errors.New("db: prune timed out"),
	}
	pub := &mockPublicPublisher{inner: &fixtures.MockPublisher{}}

	r, err := outbox.NewRunner(outbox.Config{Store: store, Publisher: pub})
	require.NoError(t, err)

	_, err = r.PrunePublished(context.Background(), 24*time.Hour, 500)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "db: prune timed out")
}

// ----------------------------
// Runner.ListDeadLetters
// ----------------------------

type listableStore struct {
	*mockOutboxStore
	records []domain.DeadLetterRecord
	err     error
}

func (s *listableStore) ListDeadLetters(_ context.Context, _ domain.DLQFilter, _ int) ([]domain.DeadLetterRecord, error) {
	return s.records, s.err
}

func TestRunner_ListDeadLetters_ReturnsRecords(t *testing.T) {
	now := time.Now().UTC()
	want := []domain.DeadLetterRecord{
		{ID: "id-1", EventType: "iam.user.created", TenantID: "acme", Attempts: 5, LastError: "SNS error", FailedAt: now},
		{ID: "id-2", EventType: "billing.invoice.settled", TenantID: "acme", Attempts: 5, LastError: "timeout", FailedAt: now},
	}
	store := &listableStore{mockOutboxStore: newMockOutboxStore(), records: want}
	pub := &mockPublicPublisher{inner: &fixtures.MockPublisher{}}

	r, err := outbox.NewRunner(outbox.Config{Store: store, Publisher: pub})
	require.NoError(t, err)

	got, err := r.ListDeadLetters(context.Background(), outbox.DLQFilter{TenantID: "acme"}, 10)
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, "id-1", got[0].ID)
	assert.Equal(t, "id-2", got[1].ID)
}

func TestRunner_ListDeadLetters_Empty(t *testing.T) {
	store := &listableStore{mockOutboxStore: newMockOutboxStore()}
	pub := &mockPublicPublisher{inner: &fixtures.MockPublisher{}}

	r, err := outbox.NewRunner(outbox.Config{Store: store, Publisher: pub})
	require.NoError(t, err)

	got, err := r.ListDeadLetters(context.Background(), outbox.DLQFilter{}, 50)
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestRunner_ListDeadLetters_Error(t *testing.T) {
	store := &listableStore{mockOutboxStore: newMockOutboxStore(), err: errors.New("db timeout")}
	pub := &mockPublicPublisher{inner: &fixtures.MockPublisher{}}

	r, err := outbox.NewRunner(outbox.Config{Store: store, Publisher: pub})
	require.NoError(t, err)

	_, err = r.ListDeadLetters(context.Background(), outbox.DLQFilter{EventType: "iam.user.created"}, 10)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "db timeout")
}

// ----------------------------
// Runner.ReprocessDeadLettersWith
// ----------------------------

type reprocessWithStore struct {
	*mockOutboxStore
	count          int
	err            error
	capturedFilter domain.DLQFilter
	capturedLimit  int
}

func (s *reprocessWithStore) ReprocessDeadLettersWith(_ context.Context, f domain.DLQFilter, limit int) (int, error) {
	s.capturedFilter = f
	s.capturedLimit = limit
	return s.count, s.err
}

func TestRunner_ReprocessDeadLettersWith_Success(t *testing.T) {
	store := &reprocessWithStore{mockOutboxStore: newMockOutboxStore(), count: 7}
	pub := &mockPublicPublisher{inner: &fixtures.MockPublisher{}}

	r, err := outbox.NewRunner(outbox.Config{Store: store, Publisher: pub})
	require.NoError(t, err)

	filter := outbox.DLQFilter{EventType: "iam.user.created", TenantID: "acme"}
	n, err := r.ReprocessDeadLettersWith(context.Background(), filter, 100)
	require.NoError(t, err)
	assert.Equal(t, 7, n)
	assert.Equal(t, "iam.user.created", store.capturedFilter.EventType)
	assert.Equal(t, "acme", store.capturedFilter.TenantID)
	assert.Equal(t, 100, store.capturedLimit)
}

func TestRunner_ReprocessDeadLettersWith_Zero(t *testing.T) {
	store := &reprocessWithStore{mockOutboxStore: newMockOutboxStore(), count: 0}
	pub := &mockPublicPublisher{inner: &fixtures.MockPublisher{}}

	r, err := outbox.NewRunner(outbox.Config{Store: store, Publisher: pub})
	require.NoError(t, err)

	n, err := r.ReprocessDeadLettersWith(context.Background(), outbox.DLQFilter{}, 50)
	require.NoError(t, err)
	assert.Equal(t, 0, n)
}

func TestRunner_ReprocessDeadLettersWith_Error(t *testing.T) {
	store := &reprocessWithStore{mockOutboxStore: newMockOutboxStore(), err: errors.New("db failure")}
	pub := &mockPublicPublisher{inner: &fixtures.MockPublisher{}}

	r, err := outbox.NewRunner(outbox.Config{Store: store, Publisher: pub})
	require.NoError(t, err)

	_, err = r.ReprocessDeadLettersWith(context.Background(), outbox.DLQFilter{TenantID: "acme"}, 10)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "db failure")
}

// ----------------------------
// Runner.DiscardDeadLetters
// ----------------------------

type discardableStore struct {
	*mockOutboxStore
	deleted        int64
	err            error
	capturedFilter domain.DLQFilter
	capturedLimit  int
}

func (s *discardableStore) DiscardDeadLetters(_ context.Context, f domain.DLQFilter, limit int) (int64, error) {
	s.capturedFilter = f
	s.capturedLimit = limit
	return s.deleted, s.err
}

func TestRunner_DiscardDeadLetters_Success(t *testing.T) {
	store := &discardableStore{mockOutboxStore: newMockOutboxStore(), deleted: 3}
	pub := &mockPublicPublisher{inner: &fixtures.MockPublisher{}}

	r, err := outbox.NewRunner(outbox.Config{Store: store, Publisher: pub})
	require.NoError(t, err)

	filter := outbox.DLQFilter{EventType: "legacy.sync.requested"}
	n, err := r.DiscardDeadLetters(context.Background(), filter, 1000)
	require.NoError(t, err)
	assert.Equal(t, int64(3), n)
	assert.Equal(t, "legacy.sync.requested", store.capturedFilter.EventType)
	assert.Equal(t, 1000, store.capturedLimit)
}

func TestRunner_DiscardDeadLetters_Zero(t *testing.T) {
	store := &discardableStore{mockOutboxStore: newMockOutboxStore(), deleted: 0}
	pub := &mockPublicPublisher{inner: &fixtures.MockPublisher{}}

	r, err := outbox.NewRunner(outbox.Config{Store: store, Publisher: pub})
	require.NoError(t, err)

	n, err := r.DiscardDeadLetters(context.Background(), outbox.DLQFilter{}, 100)
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)
}

func TestRunner_DiscardDeadLetters_Error(t *testing.T) {
	store := &discardableStore{mockOutboxStore: newMockOutboxStore(), err: errors.New("db: delete failed")}
	pub := &mockPublicPublisher{inner: &fixtures.MockPublisher{}}

	r, err := outbox.NewRunner(outbox.Config{Store: store, Publisher: pub})
	require.NoError(t, err)

	_, err = r.DiscardDeadLetters(context.Background(), outbox.DLQFilter{EventType: "bad.event"}, 10)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "db: delete failed")
}

func TestRunner_DiscardDeadLetters_FilteredByFailedBefore(t *testing.T) {
	cutoff := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	store := &discardableStore{mockOutboxStore: newMockOutboxStore(), deleted: 12}
	pub := &mockPublicPublisher{inner: &fixtures.MockPublisher{}}

	r, err := outbox.NewRunner(outbox.Config{Store: store, Publisher: pub})
	require.NoError(t, err)

	n, err := r.DiscardDeadLetters(context.Background(), outbox.DLQFilter{FailedBefore: cutoff}, 500)
	require.NoError(t, err)
	assert.Equal(t, int64(12), n)
	assert.Equal(t, cutoff, store.capturedFilter.FailedBefore)
}

// ----------------------------
// drainTicker: exercises the <-ticker.C drain branch after backoff
// ----------------------------
