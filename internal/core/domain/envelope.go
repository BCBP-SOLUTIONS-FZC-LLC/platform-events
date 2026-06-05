// Package domain contains core entities with no external dependencies.
package domain

import (
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
	SchemaVersion string    `json:"schema_version,omitempty"`
	TenantID      string    `json:"tenant_id,omitempty"`
	TraceID       string    `json:"trace_id,omitempty"`
	CorrelationID string    `json:"correlation_id,omitempty"`
	Timestamp     time.Time `json:"timestamp"`
	Payload       T         `json:"payload"`
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
