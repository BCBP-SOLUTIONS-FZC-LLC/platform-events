// Package events provides the public API for platform-events consumers.
package events

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Envelope is the canonical wire format for all inter-service events.
// T is the payload type; use json.RawMessage for generic handling.
type Envelope[T any] struct {
	ID            string    `json:"id"`
	Type          string    `json:"type"`
	Source        string    `json:"source"`
	TenantID      string    `json:"tenant_id,omitempty"`
	TraceID       string    `json:"trace_id,omitempty"`
	CorrelationID string    `json:"correlation_id,omitempty"`
	Timestamp     time.Time `json:"timestamp"`
	Payload       T         `json:"payload"`
}

// EnvelopeOpt is applied to an envelope after construction.
// Using a typed closure approach avoids the any-typed option.
type EnvelopeOpt func(tenantID, traceID, correlationID *string)

// WithTenantID sets the TenantID field on the envelope.
func WithTenantID(id string) EnvelopeOpt {
	return func(tenantID, _, _ *string) { *tenantID = id }
}

// WithTraceID sets the TraceID field on the envelope.
func WithTraceID(id string) EnvelopeOpt {
	return func(_, traceID, _ *string) { *traceID = id }
}

// WithCorrelationID sets the CorrelationID field on the envelope.
func WithCorrelationID(id string) EnvelopeOpt {
	return func(_, _, correlationID *string) { *correlationID = id }
}

// NewEnvelope creates a new Envelope with UUID v7 ID and current UTC timestamp.
// Pass WithTenantID and WithTraceID from the gincommon.RequestContext when
// publishing from an HTTP handler.
func NewEnvelope[T any](eventType, source string, payload T, opts ...EnvelopeOpt) Envelope[T] {
	env := Envelope[T]{
		ID:        uuid.Must(uuid.NewV7()).String(),
		Type:      eventType,
		Source:    source,
		Timestamp: time.Now().UTC(),
		Payload:   payload,
	}
	for _, opt := range opts {
		opt(&env.TenantID, &env.TraceID, &env.CorrelationID)
	}
	return env
}

// JSON serialises the envelope to canonical JSON.
func (e Envelope[T]) JSON() ([]byte, error) {
	return json.Marshal(e)
}

// ParseEnvelope deserialises and validates a JSON-encoded envelope.
// Returns errors for missing required fields (ID, Type, Source).
func ParseEnvelope[T any](data []byte) (Envelope[T], error) {
	var env Envelope[T]
	if err := json.Unmarshal(data, &env); err != nil {
		return env, err
	}
	if env.ID == "" {
		return env, fmt.Errorf("events: envelope missing required field 'id'")
	}
	if env.Type == "" {
		return env, fmt.Errorf("events: envelope missing required field 'type'")
	}
	if env.Source == "" {
		return env, fmt.Errorf("events: envelope missing required field 'source'")
	}
	return env, nil
}
