package enqueue_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/events"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/outbox"
)

// stubTx is a pgcommon.Tx for Enqueue tests: it records Exec and returns
// execErr. The embedded interface (left nil) provides every other method —
// Enqueue must not call them, and doing so panics. T is Exec's result type
// (pgx's CommandTag), inferred by newStubTx from pgcommon.Tx itself, so the
// stub never names a pgx package: database access in this repository goes
// through platform-pgcommon only, tests included.
type stubTx[T any] struct {
	pgcommon.Tx
	execSQL  string
	execArgs []any
	execErr  error
}

func (s *stubTx[T]) Exec(_ context.Context, sql string, arguments ...any) (T, error) {
	s.execSQL = sql
	s.execArgs = append(s.execArgs, arguments...)
	var tag T
	return tag, s.execErr
}

// newStubTx returns an empty stubTx. Pass pgcommon.Tx.Exec: its method
// expression fixes T to the interface's Exec result type.
func newStubTx[T any](_ func(pgcommon.Tx, context.Context, string, ...any) (T, error)) *stubTx[T] {
	return &stubTx[T]{}
}

var _ pgcommon.Tx = newStubTx(pgcommon.Tx.Exec)

// ----------------------------
// Enqueue: success
// ----------------------------

func TestEnqueue_Success(t *testing.T) {
	tx := newStubTx(pgcommon.Tx.Exec)
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
	tx := newStubTx(pgcommon.Tx.Exec)
	tx.execErr = errors.New("db error")
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
	tx := newStubTx(pgcommon.Tx.Exec)
	env := events.Envelope[json.RawMessage]{Type: "x.y", Source: "svc", Payload: json.RawMessage(`{}`)}
	err := outbox.Enqueue(context.Background(), tx, env)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ID")
}

func TestEnqueue_EmptyType_ReturnsError(t *testing.T) {
	tx := newStubTx(pgcommon.Tx.Exec)
	env := events.Envelope[json.RawMessage]{ID: "01926e4f-dead-7000-beef-000000000001", Source: "svc", Payload: json.RawMessage(`{}`)}
	err := outbox.Enqueue(context.Background(), tx, env)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Type")
}

func TestEnqueue_EmptySource_ReturnsError(t *testing.T) {
	tx := newStubTx(pgcommon.Tx.Exec)
	env := events.Envelope[json.RawMessage]{ID: "01926e4f-dead-7000-beef-000000000001", Type: "x.y", Payload: json.RawMessage(`{}`)}
	err := outbox.Enqueue(context.Background(), tx, env)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Source")
}

func TestEnqueue_ValidEnvelope_NoExecOnValidationPass(t *testing.T) {
	tx := newStubTx(pgcommon.Tx.Exec)
	env := events.NewEnvelope("x.y", "svc", json.RawMessage(`{}`), events.WithTenantID("acme"))
	err := outbox.Enqueue(context.Background(), tx, env)
	require.NoError(t, err)
	assert.Contains(t, tx.execSQL, "INSERT INTO outbox_events")
}

// ----------------------------
// Enqueue: correct fields forwarded to SQL
// ----------------------------

func TestEnqueue_FieldsForwardedCorrectly(t *testing.T) {
	tx := newStubTx(pgcommon.Tx.Exec)
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
	tx := newStubTx(pgcommon.Tx.Exec)
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
	tx := newStubTx(pgcommon.Tx.Exec)
	env := events.NewEnvelope("system.gc", "scheduler", json.RawMessage(`{}`), events.WithSystemTenant())
	err := outbox.Enqueue(context.Background(), tx, env)
	require.NoError(t, err)
	assert.Contains(t, tx.execSQL, "INSERT INTO outbox_events")
}

func TestEnqueue_ZeroTimestamp_ReturnsError(t *testing.T) {
	tx := newStubTx(pgcommon.Tx.Exec)
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
	tx := newStubTx(pgcommon.Tx.Exec)
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
	tx := newStubTx(pgcommon.Tx.Exec)
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
	tx := newStubTx(pgcommon.Tx.Exec)
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

// A payload that is not valid JSON fails Enqueue before any SQL runs.
func TestEnqueue_InvalidPayload_NoInsert(t *testing.T) {
	tx := newStubTx(pgcommon.Tx.Exec)
	env := events.NewEnvelope("bad.payload", "svc", json.RawMessage(`{not json`))
	require.Error(t, outbox.Enqueue(context.Background(), tx, env))
	assert.Empty(t, tx.execSQL, "nothing is inserted")
}

// Only the canonical UUID spelling is accepted: Postgres reads the id back
// canonicalised, and a mismatch with the payload's ID would make a failed
// publish look delivered.
func TestEnqueue_NonCanonicalID_Rejected(t *testing.T) {
	base := events.NewEnvelope("id.check", "svc", json.RawMessage(`{}`))
	for _, id := range []string{
		strings.ToUpper(base.ID),
		"{" + base.ID + "}",
		strings.ReplaceAll(base.ID, "-", ""),
		"not-a-uuid",
	} {
		tx := newStubTx(pgcommon.Tx.Exec)
		env := base
		env.ID = id
		err := outbox.Enqueue(context.Background(), tx, env)
		require.Error(t, err, id)
		assert.Contains(t, err.Error(), "canonical lowercase UUID")
		assert.Empty(t, tx.execSQL, "nothing is inserted for %q", id)
	}
	tx := newStubTx(pgcommon.Tx.Exec)
	require.NoError(t, outbox.Enqueue(context.Background(), tx, base))
}
