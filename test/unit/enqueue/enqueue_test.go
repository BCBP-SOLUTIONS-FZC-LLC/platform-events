package enqueue_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/events"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/outbox"
)

// stubTx implements pgx.Tx for testing — only Exec is meaningful.
type stubTx struct {
	execSQL  string
	execArgs []interface{}
	execErr  error
}

func (s *stubTx) Begin(_ context.Context) (pgx.Tx, error) {
	return nil, errors.New("not implemented")
}

func (s *stubTx) Commit(_ context.Context) error {
	return nil
}

func (s *stubTx) Rollback(_ context.Context) error {
	return nil
}

func (s *stubTx) CopyFrom(_ context.Context, _ pgx.Identifier, _ []string, _ pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("not implemented")
}

func (s *stubTx) SendBatch(_ context.Context, _ *pgx.Batch) pgx.BatchResults {
	return nil
}

func (s *stubTx) LargeObjects() pgx.LargeObjects {
	return pgx.LargeObjects{}
}

func (s *stubTx) Prepare(_ context.Context, _, _ string) (*pgconn.StatementDescription, error) {
	return nil, errors.New("not implemented")
}

func (s *stubTx) Exec(_ context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	s.execSQL = sql
	s.execArgs = append(s.execArgs, arguments...)
	return pgconn.CommandTag{}, s.execErr
}

func (s *stubTx) Query(_ context.Context, _ string, _ ...any) (pgx.Rows, error) {
	return nil, errors.New("not implemented")
}

func (s *stubTx) QueryRow(_ context.Context, _ string, _ ...any) pgx.Row {
	return nil
}

func (s *stubTx) Conn() *pgx.Conn {
	return nil
}

var _ pgx.Tx = (*stubTx)(nil)

// ----------------------------
// Enqueue: success
// ----------------------------

func TestEnqueue_Success(t *testing.T) {
	tx := &stubTx{}
	env := events.NewEnvelope("order.placed", "billing", json.RawMessage(`{"amount":100}`),
		events.WithTenantID("acme"),
		events.WithTraceID("trace-001"),
	)

	err := outbox.Enqueue(context.Background(), tx, env)
	require.NoError(t, err)

	// Exec should have been called.
	assert.Contains(t, tx.execSQL, "INSERT INTO outbox_events")
	// First arg is the envelope ID.
	require.NotEmpty(t, tx.execArgs)
	assert.Equal(t, env.ID, tx.execArgs[0])
}

// ----------------------------
// Enqueue: tx.Exec returns error
// ----------------------------

func TestEnqueue_ExecError(t *testing.T) {
	tx := &stubTx{execErr: errors.New("db error")}
	env := events.NewEnvelope("x.y", "svc", json.RawMessage(`{}`), events.WithTenantID("acme"))

	err := outbox.Enqueue(context.Background(), tx, env)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "db error")
}

// ----------------------------
// Enqueue: correct fields forwarded to SQL
// ----------------------------

// ----------------------------
// Enqueue: required field validation
// ----------------------------

func TestEnqueue_NilTx_ReturnsError(t *testing.T) {
	env := events.NewEnvelope("x.y", "svc", json.RawMessage(`{}`))
	err := outbox.Enqueue(context.Background(), nil, env)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "transaction must not be nil")
}

func TestEnqueue_EmptyID_ReturnsError(t *testing.T) {
	tx := &stubTx{}
	env := events.Envelope[json.RawMessage]{Type: "x.y", Source: "svc", Payload: json.RawMessage(`{}`)}
	err := outbox.Enqueue(context.Background(), tx, env)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ID")
}

func TestEnqueue_EmptyType_ReturnsError(t *testing.T) {
	tx := &stubTx{}
	env := events.Envelope[json.RawMessage]{ID: "01926e4f-dead-7000-beef-000000000001", Source: "svc", Payload: json.RawMessage(`{}`)}
	err := outbox.Enqueue(context.Background(), tx, env)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Type")
}

func TestEnqueue_EmptySource_ReturnsError(t *testing.T) {
	tx := &stubTx{}
	env := events.Envelope[json.RawMessage]{ID: "01926e4f-dead-7000-beef-000000000001", Type: "x.y", Payload: json.RawMessage(`{}`)}
	err := outbox.Enqueue(context.Background(), tx, env)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Source")
}

func TestEnqueue_ValidEnvelope_NoExecOnValidationPass(t *testing.T) {
	tx := &stubTx{}
	env := events.NewEnvelope("x.y", "svc", json.RawMessage(`{}`), events.WithTenantID("acme"))
	err := outbox.Enqueue(context.Background(), tx, env)
	require.NoError(t, err)
	assert.Contains(t, tx.execSQL, "INSERT INTO outbox_events")
}

// ----------------------------
// Enqueue: correct fields forwarded to SQL
// ----------------------------

func TestEnqueue_FieldsForwardedCorrectly(t *testing.T) {
	tx := &stubTx{}
	env := events.NewEnvelope("user.created", "iam", json.RawMessage(`{"name":"alice"}`),
		events.WithTenantID("tenant123"),
		events.WithTraceID("trace-xyz"),
	)

	err := outbox.Enqueue(context.Background(), tx, env)
	require.NoError(t, err)

	args := tx.execArgs
	// SQL params: $1=ID, $2=Type, $3=payload_json, $4=TenantID, $5=TraceID
	// created_at and scheduled_at use DB-side NOW() — not passed as parameters.
	require.GreaterOrEqual(t, len(args), 5)
	assert.Equal(t, env.ID, args[0])
	assert.Equal(t, env.Type, args[1])
	// args[2] is payload JSON
	assert.Equal(t, "tenant123", args[3])
	assert.Equal(t, "trace-xyz", args[4])
}

func TestEnqueue_EmptyTenantID_Succeeds(t *testing.T) {
	tx := &stubTx{}
	env := events.Envelope[json.RawMessage]{
		ID:        "01926e4f-dead-7000-beef-000000000001",
		Type:      "x.y",
		Source:    "svc",
		Payload:   json.RawMessage(`{}`),
		Timestamp: time.Now(),
	}
	err := outbox.Enqueue(context.Background(), tx, env)
	require.NoError(t, err)
	require.NotEmpty(t, tx.execSQL)
}

func TestEnqueue_SystemTenantID_Succeeds(t *testing.T) {
	tx := &stubTx{}
	env := events.NewEnvelope("system.gc", "scheduler", json.RawMessage(`{}`), events.WithSystemTenant())
	err := outbox.Enqueue(context.Background(), tx, env)
	require.NoError(t, err)
	assert.Contains(t, tx.execSQL, "INSERT INTO outbox_events")
}

func TestEnqueue_ZeroTimestamp_ReturnsError(t *testing.T) {
	tx := &stubTx{}
	env := events.Envelope[json.RawMessage]{
		ID:     "01926e4f-dead-7000-beef-000000000001",
		Type:   "x.y",
		Source: "svc",
		// Timestamp intentionally zero (not set) to simulate manually-constructed envelope.
	}
	err := outbox.Enqueue(context.Background(), tx, env)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Timestamp")
}

func TestEnqueue_NullByteInID_ReturnsError(t *testing.T) {
	tx := &stubTx{}
	env := events.Envelope[json.RawMessage]{
		ID:        "bad\x00id",
		Type:      "x.y",
		Source:    "svc",
		Timestamp: time.Now(),
	}
	err := outbox.Enqueue(context.Background(), tx, env)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "null bytes")
}

func TestEnqueue_NullByteInType_ReturnsError(t *testing.T) {
	tx := &stubTx{}
	env := events.Envelope[json.RawMessage]{
		ID:        "01926e4f-dead-7000-beef-000000000001",
		Type:      "x\x00y",
		Source:    "svc",
		Timestamp: time.Now(),
	}
	err := outbox.Enqueue(context.Background(), tx, env)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "null bytes")
}

func TestEnqueue_OversizedPayload_ReturnsError(t *testing.T) {
	tx := &stubTx{}
	// Build a payload that exceeds 240 KB after JSON serialisation.
	bigPayload := make([]byte, 250*1024)
	for i := range bigPayload {
		bigPayload[i] = 'x'
	}
	env := events.NewEnvelope("big.event", "svc",
		json.RawMessage(`"`+string(bigPayload)+`"`),
		events.WithTenantID("t"),
	)
	err := outbox.Enqueue(context.Background(), tx, env)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds safe SNS limit")
}
