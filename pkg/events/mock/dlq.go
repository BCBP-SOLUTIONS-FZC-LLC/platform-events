package mock

import (
	"context"
	"maps"
	"sync"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/events"
)

// DLQMessage is a message recorded by DLQPublisher.
type DLQMessage struct {
	SourceQueueURL string
	Body           []byte
	Attrs          map[string]string
	Reason         string
}

// DLQPublisher is a thread-safe in-memory DLQPublisher for use in unit tests.
// ResolveDLQ returns DLQURL (default "mock://dlq").
type DLQPublisher struct {
	DLQURL string

	mu   sync.Mutex
	sent []DLQMessage
	err  error
}

// SendToDLQ records the message and returns any configured error.
func (m *DLQPublisher) SendToDLQ(_ context.Context, sourceQueueURL string, body []byte, attrs map[string]string, reason string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	m.sent = append(m.sent, DLQMessage{
		SourceQueueURL: sourceQueueURL,
		Body:           append([]byte(nil), body...),
		Attrs:          maps.Clone(attrs),
		Reason:         reason,
	})
	return nil
}

// ResolveDLQ returns DLQURL, or any configured error.
func (m *DLQPublisher) ResolveDLQ(_ context.Context, _ string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return "", m.err
	}
	if m.DLQURL == "" {
		return "mock://dlq", nil
	}
	return m.DLQURL, nil
}

// Sent returns a copy of all recorded messages.
func (m *DLQPublisher) Sent() []DLQMessage {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]DLQMessage, len(m.sent))
	copy(out, m.sent)
	return out
}

// SetError configures the error returned by subsequent calls.
func (m *DLQPublisher) SetError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.err = err
}

// Reset clears recorded messages and any configured error.
func (m *DLQPublisher) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sent = nil
	m.err = nil
}

// Ensure DLQPublisher satisfies events.DLQPublisher at compile time.
var _ events.DLQPublisher = (*DLQPublisher)(nil)
