package fixtures

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/internal/core/port"
)

// MockConsumer implements port.Consumer for unit tests.
type MockConsumer struct {
	mu      sync.Mutex
	handler port.Handler
	running bool
}

// Start records that the consumer is running.
func (m *MockConsumer) Start(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.running = true
	return nil
}

// Stop marks the consumer as stopped.
func (m *MockConsumer) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.running = false
	return nil
}

// SetHandler configures the handler called by Inject.
func (m *MockConsumer) SetHandler(h port.Handler) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.handler = h
}

// Inject delivers an envelope synchronously to the registered handler.
func (m *MockConsumer) Inject(env domain.Envelope[json.RawMessage]) error {
	m.mu.Lock()
	h := m.handler
	m.mu.Unlock()
	if h == nil {
		return nil
	}
	return h(context.Background(), env)
}

// IsRunning reports whether Start has been called without a matching Stop.
func (m *MockConsumer) IsRunning() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.running
}

// Ensure MockConsumer satisfies port.Consumer at compile time.
var _ port.Consumer = (*MockConsumer)(nil)
