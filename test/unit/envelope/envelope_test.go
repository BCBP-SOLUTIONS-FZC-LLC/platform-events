package envelope_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/events"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewEnvelope_Defaults(t *testing.T) {
	type Payload struct{ Name string }
	env := events.NewEnvelope("user.created", "platform-iam", Payload{Name: "Alice"})

	assert.NotEmpty(t, env.ID, "ID should be set to UUID v7")
	assert.Equal(t, "user.created", env.Type)
	assert.Equal(t, "platform-iam", env.Source)
	assert.False(t, env.Timestamp.IsZero(), "Timestamp should be set")
	assert.WithinDuration(t, time.Now().UTC(), env.Timestamp, 5*time.Second)
	assert.Empty(t, env.TenantID)
	assert.Empty(t, env.TraceID)
	assert.Empty(t, env.CorrelationID)
}

func TestNewEnvelope_WithOptions(t *testing.T) {
	env := events.NewEnvelope("order.placed", "billing", json.RawMessage(`{}`),
		events.WithTenantID("acme"),
		events.WithTraceID("abc123"),
		events.WithCorrelationID("corr-001"),
	)

	assert.Equal(t, "acme", env.TenantID)
	assert.Equal(t, "abc123", env.TraceID)
	assert.Equal(t, "corr-001", env.CorrelationID)
}

func TestEnvelope_JSON_RoundTrip(t *testing.T) {
	original := events.NewEnvelope("test.event", "test-svc", json.RawMessage(`{"foo":"bar"}`),
		events.WithTenantID("tenant1"),
		events.WithTraceID("trace-abc"),
	)

	b, err := original.JSON()
	require.NoError(t, err)

	parsed, err := events.ParseEnvelope[json.RawMessage](b)
	require.NoError(t, err)

	assert.Equal(t, original.ID, parsed.ID)
	assert.Equal(t, original.Type, parsed.Type)
	assert.Equal(t, original.Source, parsed.Source)
	assert.Equal(t, original.TenantID, parsed.TenantID)
	assert.Equal(t, original.TraceID, parsed.TraceID)
	assert.JSONEq(t, `{"foo":"bar"}`, string(parsed.Payload))
}

func TestParseEnvelope_MissingID(t *testing.T) {
	data := `{"type":"test.event","source":"svc","timestamp":"2026-01-01T00:00:00Z","payload":{}}`
	_, err := events.ParseEnvelope[json.RawMessage]([]byte(data))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "id")
}

func TestParseEnvelope_MissingType(t *testing.T) {
	data := `{"id":"01926e4f-1234-7abc-8def-000000000001","source":"svc","timestamp":"2026-01-01T00:00:00Z","payload":{}}`
	_, err := events.ParseEnvelope[json.RawMessage]([]byte(data))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "type")
}

func TestParseEnvelope_MissingSource(t *testing.T) {
	data := `{"id":"01926e4f-1234-7abc-8def-000000000001","type":"test.event","timestamp":"2026-01-01T00:00:00Z","payload":{}}`
	_, err := events.ParseEnvelope[json.RawMessage]([]byte(data))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "source")
}

func TestParseEnvelope_InvalidJSON(t *testing.T) {
	_, err := events.ParseEnvelope[json.RawMessage]([]byte("not-json"))
	require.Error(t, err)
}

func TestEnvelope_UniqueIDs(t *testing.T) {
	ids := make(map[string]bool)
	for i := 0; i < 100; i++ {
		env := events.NewEnvelope("test", "svc", json.RawMessage(`{}`))
		assert.False(t, ids[env.ID], "duplicate ID generated: %s", env.ID)
		ids[env.ID] = true
	}
}
