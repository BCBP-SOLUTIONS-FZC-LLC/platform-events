// Package domain contains core entities with no external dependencies.
package domain

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// SystemTenantID is the well-known tenant identifier for events that apply
// globally across all tenants. Use WithSystemTenant() (pkg/events) when
// publishing; receiving a SystemTenantID on the consumer side disables per-tenant
// RLS scoping, which is the intended behaviour for global events.
const SystemTenantID = "system"

// Envelope is the canonical wire format for all inter-service events.
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
	if len(raw.Data) > 0 {
		return json.Unmarshal(raw.Data, &e.Payload)
	}
	return nil
}

// NewEnvelope creates a new Envelope with UUID v7 ID and current UTC timestamp.
func NewEnvelope[T any](eventType, source string, payload T) Envelope[T] {
	return Envelope[T]{
		ID:        uuid.Must(uuid.NewV7()).String(),
		Type:      eventType,
		Source:    source,
		Timestamp: time.Now().UTC(),
		Payload:   payload,
	}
}
