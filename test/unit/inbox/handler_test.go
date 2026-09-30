package inbox_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/adapter/outbound/metrics"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/events"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/inbox"
)

type fakeLedger struct {
	seen           map[uuid.UUID]bool
	isErr, markErr error
	marks, isCalls int
}

func newLedger() *fakeLedger { return &fakeLedger{seen: map[uuid.UUID]bool{}} }

func (f *fakeLedger) IsProcessed(_ context.Context, id uuid.UUID) (bool, error) {
	f.isCalls++
	return f.seen[id], f.isErr
}
func (f *fakeLedger) MarkProcessed(_ context.Context, id uuid.UUID) error {
	f.marks++
	if f.markErr != nil {
		return f.markErr
	}
	f.seen[id] = true
	return nil
}
func (f *fakeLedger) Consumer() string { return "test_consumer" }

func env(id string) events.Envelope[json.RawMessage] {
	return events.Envelope[json.RawMessage]{ID: id, Type: "T"}
}

func TestHandler_ProcessesThenDedups(t *testing.T) {
	metrics.InitWithRegisterer("inbox-test", "v1", prometheus.NewRegistry())
	ledger, calls := newLedger(), 0
	h := inbox.Handler(ledger, func(context.Context, events.Envelope[json.RawMessage]) error { calls++; return nil })
	id := uuid.NewString()

	require.NoError(t, h(context.Background(), env(id)))
	require.NoError(t, h(context.Background(), env(id)))
	assert.Equal(t, 1, calls, "a redelivery must not reach the handler")
	assert.Equal(t, 1, ledger.marks)
	assert.Equal(t, 1.0, testutil.ToFloat64(metrics.InboxDuplicatesTotal.WithLabelValues("test_consumer")))
}

func TestHandler_FailureNotRecorded(t *testing.T) {
	ledger := newLedger()
	boom := errors.New("boom")
	h := inbox.Handler(ledger, func(context.Context, events.Envelope[json.RawMessage]) error { return boom })
	assert.ErrorIs(t, h(context.Background(), env(uuid.NewString())), boom)
	assert.Equal(t, 0, ledger.marks, "a failed handler must not be recorded, so SQS retries it")
}

func TestHandler_InvalidID(t *testing.T) {
	ledger, calls := newLedger(), 0
	h := inbox.Handler(ledger, func(context.Context, events.Envelope[json.RawMessage]) error { calls++; return nil })
	for _, id := range []string{"", "not-a-uuid", uuid.Nil.String()} {
		assert.Error(t, h(context.Background(), env(id)), "id %q", id)
	}
	assert.Zero(t, calls)
	assert.Zero(t, ledger.isCalls)
}

func TestHandler_LedgerErrors(t *testing.T) {
	ledger := newLedger()
	ledger.isErr = errors.New("db down")
	h := inbox.Handler(ledger, func(context.Context, events.Envelope[json.RawMessage]) error { return nil })
	assert.ErrorIs(t, h(context.Background(), env(uuid.NewString())), ledger.isErr)

	ledger = newLedger()
	ledger.markErr = errors.New("mark failed")
	h = inbox.Handler(ledger, func(context.Context, events.Envelope[json.RawMessage]) error { return nil })
	assert.ErrorIs(t, h(context.Background(), env(uuid.NewString())), ledger.markErr)
}

func TestNewStore_Validation(t *testing.T) {
	_, err := inbox.NewStore(nil, "c")
	assert.Error(t, err)
}

func TestApplySchema_Validation(t *testing.T) {
	assert.Error(t, inbox.ApplySchema(context.Background(), nil))
}
