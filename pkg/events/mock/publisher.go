// Package mock provides in-memory test doubles for Publisher, Consumer and DLQPublisher.
package mock

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/events"
)

// Publisher is a thread-safe in-memory Publisher for use in unit tests.
type Publisher struct {
	mu        sync.Mutex
	published []events.Envelope[json.RawMessage]
	err       error
}

// Publish records the envelope and returns any configured error.
func (m *Publisher) Publish(_ context.Context, env events.Envelope[json.RawMessage]) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	m.published = append(m.published, env)
	return nil
}

// PublishBatch calls Publish for each envelope.
func (m *Publisher) PublishBatch(ctx context.Context, envs []events.Envelope[json.RawMessage]) error {
	for _, env := range envs {
		if err := m.Publish(ctx, env); err != nil {
			return err
		}
	}
	return nil
}

// Published returns a copy of all published envelopes.
func (m *Publisher) Published() []events.Envelope[json.RawMessage] {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]events.Envelope[json.RawMessage], len(m.published))
	copy(out, m.published)
	return out
}

// SetError configures the error returned by the next Publish call(s).
func (m *Publisher) SetError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.err = err
}

// Reset clears recorded envelopes and any configured error.
func (m *Publisher) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.published = nil
	m.err = nil
}

// Ensure Publisher satisfies events.Publisher at compile time.
var _ events.Publisher = (*Publisher)(nil)
