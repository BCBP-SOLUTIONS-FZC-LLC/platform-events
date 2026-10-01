package fixtures

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/internal/core/port"
)

// MockPublisher implements port.Publisher and records published envelopes.
type MockPublisher struct {
	mu        sync.Mutex
	published []domain.Envelope[json.RawMessage]
	err       error
	failOnNth int // if > 0, fail on the Nth Publish call (1-indexed)
	callCount int
}

// Publish records the envelope and returns any configured error.
func (m *MockPublisher) Publish(_ context.Context, env domain.Envelope[json.RawMessage]) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.callCount++
	if m.err != nil {
		return m.err
	}
	if m.failOnNth > 0 && m.callCount == m.failOnNth {
		return fmt.Errorf("mock: simulated failure on call %d", m.callCount)
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

// FailOnNth configures the publisher to return an error on the Nth Publish call
// (1-indexed). Used to simulate partial batch failures in tests.
func (m *MockPublisher) FailOnNth(n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failOnNth = n
}

// Reset clears recorded envelopes, any configured error, and the call counter.
func (m *MockPublisher) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.published = nil
	m.err = nil
	m.failOnNth = 0
	m.callCount = 0
}

// Ensure MockPublisher satisfies port.Publisher at compile time.
var _ port.Publisher = (*MockPublisher)(nil)
