package envelope_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/events"
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
	data := `{"type":"test.event","source":"svc","time":"2026-01-01T00:00:00Z","data":{}}`
	_, err := events.ParseEnvelope[json.RawMessage]([]byte(data))
	require.Error(t, err)
	assert.ErrorIs(t, err, events.ErrEnvelopeIDRequired)
}

func TestParseEnvelope_MissingType(t *testing.T) {
	data := `{"id":"01926e4f-1234-7abc-8def-000000000001","source":"svc","time":"2026-01-01T00:00:00Z","data":{}}`
	_, err := events.ParseEnvelope[json.RawMessage]([]byte(data))
	require.Error(t, err)
	assert.ErrorIs(t, err, events.ErrEnvelopeTypeRequired)
}

func TestParseEnvelope_MissingSource(t *testing.T) {
	data := `{"id":"01926e4f-1234-7abc-8def-000000000001","type":"test.event","time":"2026-01-01T00:00:00Z","data":{}}`
	_, err := events.ParseEnvelope[json.RawMessage]([]byte(data))
	require.Error(t, err)
	assert.ErrorIs(t, err, events.ErrEnvelopeSourceRequired)
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

// ----------------------------
// WithSystemTenant
// ----------------------------

func TestWithSystemTenant_SetsTenantID(t *testing.T) {
	env := events.NewEnvelope("sys.event", "worker", json.RawMessage(`{}`), events.WithSystemTenant())
	assert.Equal(t, events.SystemTenantID, env.TenantID)
}

func TestSystemTenantID_Constant(t *testing.T) {
	assert.Equal(t, "system", events.SystemTenantID)
}

// ----------------------------
// TraceIDFromContext
// ----------------------------

func TestTraceIDFromContext_Missing(t *testing.T) {
	got := events.TraceIDFromContext(context.Background())
	assert.Equal(t, "", got)
}

func TestTraceIDFromContext_WithInjectedValue(t *testing.T) {
	ctx := port.WithEnvelopeTraceID(context.Background(), "trace-xyz-789")
	got := events.TraceIDFromContext(ctx)
	assert.Equal(t, "trace-xyz-789", got)
}

// ----------------------------
// WithSchemaVersion
// ----------------------------

func TestWithSchemaVersion_SetsField(t *testing.T) {
	env := events.NewEnvelope("iam.user.created.v2", "platform-iam", json.RawMessage(`{}`),
		events.WithSchemaVersion("2"),
	)
	assert.Equal(t, "2", env.SchemaVersion)
}

func TestNewEnvelope_DefaultSchemaVersion_Empty(t *testing.T) {
	env := events.NewEnvelope("iam.user.created", "platform-iam", json.RawMessage(`{}`))
	assert.Empty(t, env.SchemaVersion)
}

func TestEnvelope_SchemaVersion_RoundTrip(t *testing.T) {
	original := events.NewEnvelope("billing.invoice.settled.v2", "billing-svc", json.RawMessage(`{"amount":100}`),
		events.WithSchemaVersion("2"),
		events.WithTenantID("acme"),
	)

	b, err := original.JSON()
	require.NoError(t, err)
	assert.Contains(t, string(b), `"specversion":"2"`)

	parsed, err := events.ParseEnvelope[json.RawMessage](b)
	require.NoError(t, err)
	assert.Equal(t, "2", parsed.SchemaVersion)
	assert.Equal(t, "acme", parsed.TenantID)
}

func TestEnvelope_SchemaVersion_OmittedFromJSON_WhenEmpty(t *testing.T) {
	env := events.NewEnvelope("iam.user.created", "platform-iam", json.RawMessage(`{}`))
	b, err := env.JSON()
	require.NoError(t, err)
	assert.NotContains(t, string(b), "specversion")
}

// ----------------------------
// WithSubject / WithActor
// ----------------------------

func TestWithSubject_SetsField(t *testing.T) {
	env := events.NewEnvelope("iam.user.created", "platform-iam", json.RawMessage(`{}`),
		events.WithSubject("users/01926e4f-1234-7abc-8def-000000000001"),
	)
	assert.Equal(t, "users/01926e4f-1234-7abc-8def-000000000001", env.Subject)
}

func TestWithActor_SetsField(t *testing.T) {
	env := events.NewEnvelope("iam.user.created", "platform-iam", json.RawMessage(`{}`),
		events.WithActor("admin@acme.com"),
	)
	assert.Equal(t, "admin@acme.com", env.Actor)
}

func TestEnvelope_SubjectActor_RoundTrip(t *testing.T) {
	original := events.NewEnvelope("iam.user.created", "platform-iam", json.RawMessage(`{"id":"u1"}`),
		events.WithTenantID("acme"),
		events.WithSubject("users/u1"),
		events.WithActor("svc-account"),
	)

	b, err := original.JSON()
	require.NoError(t, err)
	assert.Contains(t, string(b), `"subject":"users/u1"`)
	assert.Contains(t, string(b), `"actor":"svc-account"`)

	parsed, err := events.ParseEnvelope[json.RawMessage](b)
	require.NoError(t, err)
	assert.Equal(t, "users/u1", parsed.Subject)
	assert.Equal(t, "svc-account", parsed.Actor)
}

func TestEnvelope_SubjectActor_OmittedFromJSON_WhenEmpty(t *testing.T) {
	env := events.NewEnvelope("iam.user.created", "platform-iam", json.RawMessage(`{}`))
	b, err := env.JSON()
	require.NoError(t, err)
	assert.NotContains(t, string(b), "subject")
	assert.NotContains(t, string(b), "actor")
}

// ----------------------------
// WithSchemaID
// ----------------------------

func TestWithSchemaID_SetsField(t *testing.T) {
	env := events.NewEnvelope("iam.user.created", "platform-iam", json.RawMessage(`{}`),
		events.WithSchemaID("550e8400-e29b-41d4-a716-446655440000"),
	)
	assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", env.SchemaID)
}

func TestEnvelope_SchemaID_RoundTrip(t *testing.T) {
	original := events.NewEnvelope("iam.user.created", "platform-iam", json.RawMessage(`{"id":"u1"}`),
		events.WithSchemaVersion("1"),
		events.WithSchemaID("550e8400-e29b-41d4-a716-446655440000"),
	)

	b, err := original.JSON()
	require.NoError(t, err)
	assert.Contains(t, string(b), `"specversion":"1"`)
	assert.Contains(t, string(b), `"dataschema":"550e8400-e29b-41d4-a716-446655440000"`)

	parsed, err := events.ParseEnvelope[json.RawMessage](b)
	require.NoError(t, err)
	assert.Equal(t, "1", parsed.SchemaVersion)
	assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", parsed.SchemaID)
}

func TestEnvelope_SchemaID_OmittedFromJSON_WhenEmpty(t *testing.T) {
	env := events.NewEnvelope("iam.user.created", "platform-iam", json.RawMessage(`{}`),
		events.WithSchemaVersion("1"),
	)
	b, err := env.JSON()
	require.NoError(t, err)
	assert.NotContains(t, string(b), "dataschema")
}

func TestEnvelope_SchemaVersion_And_SchemaID_AreIndependent(t *testing.T) {
	// schema_version is for consumer logic gating ("1", "2")
	// schema_id is for the registry pointer (Glue UUID)
	// setting one must not affect the other
	env := events.NewEnvelope("iam.user.created", "platform-iam", json.RawMessage(`{}`),
		events.WithSchemaVersion("2"),
		events.WithSchemaID("glue-uuid-abc"),
	)
	assert.Equal(t, "2", env.SchemaVersion)
	assert.Equal(t, "glue-uuid-abc", env.SchemaID)
}

// ----------------------------
// ParseEnvelope — timestamp zero check
// ----------------------------

func TestParseEnvelope_MissingTime(t *testing.T) {
	// Valid id, type, source, but no time field
	data := `{"id":"01926e4f-1234-7abc-8def-000000000001","type":"test.event","source":"svc","tenant_id":"acme","data":{}}`
	_, err := events.ParseEnvelope[json.RawMessage]([]byte(data))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "'time'")
}

// ----------------------------
// WithIPAddress / WithUserAgent
// ----------------------------

func TestWithIPAddress_SetsField(t *testing.T) {
	env := events.NewEnvelope("iam.user.created", "platform-iam", json.RawMessage(`{}`),
		events.WithIPAddress("203.0.113.42"),
	)
	assert.Equal(t, "203.0.113.42", env.IPAddress)
}

func TestWithUserAgent_SetsField(t *testing.T) {
	env := events.NewEnvelope("iam.user.created", "platform-iam", json.RawMessage(`{}`),
		events.WithUserAgent("Mozilla/5.0 (compatible; XPert/1.0)"),
	)
	assert.Equal(t, "Mozilla/5.0 (compatible; XPert/1.0)", env.UserAgent)
}

func TestEnvelope_IPAddressUserAgent_RoundTrip(t *testing.T) {
	original := events.NewEnvelope("iam.user.login", "platform-iam", json.RawMessage(`{"ok":true}`),
		events.WithTenantID("acme"),
		events.WithIPAddress("203.0.113.42"),
		events.WithUserAgent("Go-http-client/2.0"),
	)

	b, err := original.JSON()
	require.NoError(t, err)
	assert.Contains(t, string(b), `"ip_address":"203.0.113.42"`)
	assert.Contains(t, string(b), `"user_agent":"Go-http-client/2.0"`)

	parsed, err := events.ParseEnvelope[json.RawMessage](b)
	require.NoError(t, err)
	assert.Equal(t, "203.0.113.42", parsed.IPAddress)
	assert.Equal(t, "Go-http-client/2.0", parsed.UserAgent)
}

func TestEnvelope_IPAddressUserAgent_OmittedFromJSON_WhenEmpty(t *testing.T) {
	env := events.NewEnvelope("iam.user.created", "platform-iam", json.RawMessage(`{}`))
	b, err := env.JSON()
	require.NoError(t, err)
	assert.NotContains(t, string(b), "ip_address")
	assert.NotContains(t, string(b), "user_agent")
}

// ----------------------------
// CloudEvents "data" field rename
// ----------------------------

func TestEnvelope_JSON_UsesDataKey(t *testing.T) {
	env := events.NewEnvelope("test.event", "svc", json.RawMessage(`{"x":1}`))
	b, err := env.JSON()
	require.NoError(t, err)
	assert.Contains(t, string(b), `"data":`)
	assert.NotContains(t, string(b), `"payload":`)
}
