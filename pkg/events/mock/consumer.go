package mock

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/events"
)

// Consumer is an in-memory Consumer that delivers injected messages synchronously.
type Consumer struct {
	mu      sync.Mutex
	handler events.Handler
	running bool
}

// Start records that the consumer is running and stores the context.
// This mock does not poll — use Inject to deliver messages.
func (m *Consumer) Start(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.running = true
	return nil
}

// Stop marks the consumer as stopped.
func (m *Consumer) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.running = false
	return nil
}

// SetHandler configures the handler called by Inject.
func (m *Consumer) SetHandler(h events.Handler) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.handler = h
}

// Inject delivers an envelope synchronously to the registered handler.
// Returns an error if no handler is set or the handler returns an error.
func (m *Consumer) Inject(env events.Envelope[json.RawMessage]) error {
	m.mu.Lock()
	h := m.handler
	m.mu.Unlock()
	if h == nil {
		return nil
	}
	return h(context.Background(), env)
}

// IsRunning reports whether Start has been called without a matching Stop.
func (m *Consumer) IsRunning() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.running
}

// Ensure Consumer satisfies events.Consumer at compile time.
var _ events.Consumer = (*Consumer)(nil)
