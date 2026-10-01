// Package events provides the public API for platform-events consumers.
package events

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/port"
)

// Envelope is the canonical wire format for all inter-service events.
// T is the payload type; use json.RawMessage for generic handling.
type Envelope[T any] struct {
	ID            string    `json:"id"`
	Type          string    `json:"type"`
	Source        string    `json:"source"`
	SchemaVersion string    `json:"specversion,omitempty"`
	TenantID      string    `json:"tenant_id,omitempty"`
	TraceID       string    `json:"trace_id,omitempty"`
	CorrelationID string    `json:"correlation_id,omitempty"`
	Subject       string    `json:"subject,omitempty"`
	Actor         string    `json:"actor,omitempty"`
	IPAddress     string    `json:"ip_address,omitempty"`
	UserAgent     string    `json:"user_agent,omitempty"`
	SchemaID      string    `json:"dataschema,omitempty"`
	Timestamp     time.Time `json:"time"`
	Payload       T         `json:"data"`
}

// UnmarshalJSON decodes an Envelope from its canonical JSON representation.
func (e *Envelope[T]) UnmarshalJSON(b []byte) error {
	var raw struct {
		ID            string          `json:"id"`
		Type          string          `json:"type"`
		Source        string          `json:"source"`
		SchemaVersion string          `json:"specversion"`
		TenantID      string          `json:"tenant_id"`
		TraceID       string          `json:"trace_id"`
		CorrelationID string          `json:"correlation_id"`
		Subject       string          `json:"subject"`
		Actor         string          `json:"actor"`
		IPAddress     string          `json:"ip_address"`
		UserAgent     string          `json:"user_agent"`
		SchemaID      string          `json:"dataschema"`
		Timestamp     time.Time       `json:"time"`
		Data          json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	e.ID = raw.ID
	e.Type = raw.Type
	e.Source = raw.Source
	e.SchemaVersion = raw.SchemaVersion
	e.TenantID = raw.TenantID
	e.TraceID = raw.TraceID
	e.CorrelationID = raw.CorrelationID
	e.Subject = raw.Subject
	e.Actor = raw.Actor
	e.IPAddress = raw.IPAddress
	e.UserAgent = raw.UserAgent
	e.SchemaID = raw.SchemaID
	e.Timestamp = raw.Timestamp
	// Reset first: decoding into a reused Envelope whose JSON has no data
	// must not keep the previous message's payload.
	var zero T
	e.Payload = zero
	if len(raw.Data) > 0 {
		return json.Unmarshal(raw.Data, &e.Payload)
	}
	return nil
}

// envelopeConfig collects all optional envelope fields set via EnvelopeOpt.
// Keeping this internal means adding new fields never changes the EnvelopeOpt
// function signature and is always a backward-compatible MINOR bump.
type envelopeConfig struct {
	tenantID      string
	traceID       string
	correlationID string
	schemaVersion string
	subject       string
	actor         string
	ipAddress     string
	userAgent     string
	schemaID      string
}

// EnvelopeOpt is applied to a new Envelope at construction time.
type EnvelopeOpt func(*envelopeConfig)

// WithTenantID sets the TenantID field on the envelope.
func WithTenantID(id string) EnvelopeOpt {
	return func(c *envelopeConfig) { c.tenantID = id }
}

// WithTraceID sets the TraceID field on the envelope.
func WithTraceID(id string) EnvelopeOpt {
	return func(c *envelopeConfig) { c.traceID = id }
}

// WithCorrelationID sets the CorrelationID field on the envelope.
func WithCorrelationID(id string) EnvelopeOpt {
	return func(c *envelopeConfig) { c.correlationID = id }
}

// WithSchemaVersion records the payload schema version in the envelope.
// Use "1" for the initial schema. Increment to "2", "3", etc. when a
// breaking payload change is released alongside a versioned event_type
// (e.g. "iam.user.created.v2"). Consumers that receive an unrecognised
// version should reject the message rather than silently misparse it.
func WithSchemaVersion(v string) EnvelopeOpt {
	return func(c *envelopeConfig) { c.schemaVersion = v }
}

// SystemTenantID is the canonical sentinel for events not scoped to any tenant.
// Defined in internal/core/domain and re-exported here for public API stability.
const SystemTenantID = domain.SystemTenantID

// WithSystemTenant marks the envelope as a system-level event with no tenant
// scope. Use for background jobs and scheduled tasks that publish across tenants
// or are not associated with any specific tenant.
//
// Consumers receive TenantID "system" and the SQS consumer sets it as the RLS
// tenant (app.tenant_id = 'system'): RLS is not disabled, so handlers of such
// events need a pool or role that bypasses RLS, or policies admitting 'system'.
func WithSystemTenant() EnvelopeOpt {
	return WithTenantID(SystemTenantID)
}

// WithSubject sets the Subject field on the envelope — a resource URI or
// identifier that the event is about (e.g. "users/01926e4f-...").
// Consumers may use this for fine-grained filtering without deserialising the payload.
func WithSubject(subject string) EnvelopeOpt {
	return func(c *envelopeConfig) { c.subject = subject }
}

// WithActor sets the Actor field on the envelope — the user or service identity
// that caused the event (e.g. a user UUID, a service account name).
// Populated from domain.DomainEvent.Actor at the outbox enqueue site.
func WithActor(actor string) EnvelopeOpt {
	return func(c *envelopeConfig) { c.actor = actor }
}

// WithIPAddress sets the IPAddress field — the client IP at the time the event
// was triggered. Pass r.RemoteAddr (or the X-Forwarded-For value after
// validation) from the HTTP handler. Omit for background/system events.
func WithIPAddress(ip string) EnvelopeOpt {
	return func(c *envelopeConfig) { c.ipAddress = ip }
}

// WithUserAgent sets the UserAgent field — the HTTP User-Agent header value
// from the request that triggered the event. Omit for background/system events.
func WithUserAgent(ua string) EnvelopeOpt {
	return func(c *envelopeConfig) { c.userAgent = ua }
}

// WithSchemaID sets the SchemaID field on the envelope — the schema registry
// version identifier for the encoded payload (e.g. a Glue Schema Registry UUID).
// This is distinct from SchemaVersion: SchemaID is the authoritative registry
// pointer used by the codec for Avro/JSON deserialization; SchemaVersion ("1",
// "2", …) is the human-readable semantic version consumers use to gate business
// logic. When using AWS Glue Schema Registry, pass the UUID returned by the
// codec's Encode call. Absent for NoopCodec (dev/test).
//
// When a codec-enabled publisher is used (see WithCodec, pkg/events/codec.go),
// SchemaID is normally set automatically by the SNS publisher from the
// codec's Encode return value — do not call WithSchemaID yourself in that
// case, since the encode step overwrites it at publish time. WithSchemaID
// remains useful for manual/advanced construction, e.g. republishing an
// already-encoded payload (a base64 JSON string).
//
// Do not use it as an informational tag on a plain-JSON payload: consumers
// treat a dataschema with a JSON-string payload as codec-encoded. (Since
// v1.6.1 the SQS consumer passes a non-string payload through undecoded, but
// older consumers dead-letter it.)
func WithSchemaID(id string) EnvelopeOpt {
	return func(c *envelopeConfig) { c.schemaID = id }
}

// NewEnvelope creates a new Envelope with UUID v7 ID and current UTC timestamp.
// Pass WithTenantID and WithTraceID from the gincommon.RequestContext when
// publishing from an HTTP handler.
//
// ID generation uses uuid.Must — it panics only if the OS random source is
// exhausted, which is an unrecoverable system-level failure.
func NewEnvelope[T any](eventType, source string, payload T, opts ...EnvelopeOpt) Envelope[T] {
	env := Envelope[T]{
		ID:        uuid.Must(uuid.NewV7()).String(),
		Type:      eventType,
		Source:    source,
		Timestamp: time.Now().UTC(),
		Payload:   payload,
	}
	var cfg envelopeConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	env.TenantID = cfg.tenantID
	env.TraceID = cfg.traceID
	env.CorrelationID = cfg.correlationID
	env.SchemaVersion = cfg.schemaVersion
	env.Subject = cfg.subject
	env.Actor = cfg.actor
	env.IPAddress = cfg.ipAddress
	env.UserAgent = cfg.userAgent
	env.SchemaID = cfg.schemaID
	return env
}

// JSON serialises the envelope to canonical JSON.
func (e Envelope[T]) JSON() ([]byte, error) {
	return json.Marshal(e)
}

// TraceIDFromContext returns the envelope TraceID injected by the SQS consumer
// into the handler context. Returns an empty string if no TraceID is present.
// Use this inside a consumer handler to access the originating trace ID when
// gincommon.RequestContext is not available.
func TraceIDFromContext(ctx context.Context) string {
	return port.EnvelopeTraceIDFromContext(ctx)
}

// ParseEnvelope deserialises and validates a JSON-encoded envelope.
// Returns ErrEnvelopeIDRequired / ErrEnvelopeTypeRequired /
// ErrEnvelopeSourceRequired for a missing id / type / source, and an error for
// a missing time.
//
// It is deliberately stricter than the SQS consumer, which requires only id,
// type and source and passes an envelope without time to the handler (time
// only feeds the propagation metric) — so a producer in another language that
// omits time still has its events delivered. Producers must always set time
// (NewEnvelope does).
func ParseEnvelope[T any](data []byte) (Envelope[T], error) {
	var env Envelope[T]
	if err := json.Unmarshal(data, &env); err != nil {
		return env, err
	}
	if env.ID == "" {
		return env, ErrEnvelopeIDRequired
	}
	if env.Type == "" {
		return env, ErrEnvelopeTypeRequired
	}
	if env.Source == "" {
		return env, ErrEnvelopeSourceRequired
	}
	if env.Timestamp.IsZero() {
		return env, fmt.Errorf("events: envelope missing required field 'time'")
	}
	return env, nil
}
