package fixtures

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/port"
)

// MockPublisher implements port.Publisher and records published envelopes.
type MockPublisher struct {
	mu        sync.Mutex
	published []domain.Envelope[json.RawMessage]
	err       error
}

// Publish records the envelope and returns any configured error.
func (m *MockPublisher) Publish(_ context.Context, env domain.Envelope[json.RawMessage]) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	m.published = append(m.published, env)
	return nil
}

// PublishBatch calls Publish for each envelope.
func (m *MockPublisher) PublishBatch(ctx context.Context, envs []domain.Envelope[json.RawMessage]) error {
	for _, env := range envs {
		if err := m.Publish(ctx, env); err != nil {
			return err
		}
	}
	return nil
}

// Published returns a copy of all published envelopes.
func (m *MockPublisher) Published() []domain.Envelope[json.RawMessage] {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]domain.Envelope[json.RawMessage], len(m.published))
	copy(out, m.published)
	return out
}

// SetError configures the error returned by Publish.
func (m *MockPublisher) SetError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.err = err
}

// Reset clears recorded envelopes and any configured error.
func (m *MockPublisher) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.published = nil
	m.err = nil
}

// Ensure MockPublisher satisfies port.Publisher at compile time.
var _ port.Publisher = (*MockPublisher)(nil)
