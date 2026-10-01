package domain

import (
	"encoding/json"
	"time"
)

// OutboxRecord represents an event persisted to the outbox_events table.
type OutboxRecord struct {
	ID        string
	EventType string
	// Payload must be json.RawMessage, not []byte: pgx picks its wire codec by
	// Go type when it cannot ask Postgres for the parameter's column type (see
	// pgcommon.Config.PGBouncerMode, which forces QueryExecModeSimpleProtocol).
	// A plain []byte defaults to the bytea codec; binding that to the payload
	// JSONB column fails with "invalid input syntax for type json" (22P02).
	Payload     json.RawMessage
	TenantID    string
	TraceID     string
	Attempts    int
	LastError   string
	CreatedAt   time.Time
	ScheduledAt time.Time
	PublishedAt *time.Time
	// OrderingKey groups records that are published one at a time, in
	// enqueue order ("" = unordered). Set by outbox.EnqueueOrdered.
	OrderingKey string
}

// DeadLetterRecord is a row from outbox_dead_letters — an event that exhausted
// MaxAttempts without a successful SNS publish.
type DeadLetterRecord struct {
	ID        string
	EventType string
	TenantID  string
	TraceID   string
	Attempts  int
	LastError string
	CreatedAt time.Time
	FailedAt  time.Time
}

// DLQFilter selects records in outbox_dead_letters for List, Reprocess, and
// Discard operations. All fields are optional; a zero-value filter matches
// every record in the table.
type DLQFilter struct {
	// EventType restricts the operation to records with this exact event type.
	// Empty string means all event types.
	EventType string

	// TenantID restricts the operation to records for this tenant.
	// Empty string means all tenants.
	TenantID string

	// FailedBefore restricts the operation to records where failed_at is
	// strictly before this time. Zero value means no upper-bound on failed_at.
	FailedBefore time.Time
}
