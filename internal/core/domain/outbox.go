package domain

import "time"

// OutboxRecord represents an event persisted to the outbox_events table.
type OutboxRecord struct {
	ID          string
	EventType   string
	Payload     []byte
	TenantID    string
	TraceID     string
	Attempts    int
	LastError   string
	CreatedAt   time.Time
	ScheduledAt time.Time
	PublishedAt *time.Time
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
