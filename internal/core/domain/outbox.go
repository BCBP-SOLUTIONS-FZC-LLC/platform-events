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
