package mock

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/google/uuid"

	internalsqs "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/adapter/outbound/sqs"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/events"
)

// MockQueueURL is the queue URL mock.Consumer reports in the source message
// when Consumer.QueueURL is empty.
const MockQueueURL = "https://sqs.us-east-1.amazonaws.com/000000000000/mock-queue"

// Consumer is an in-memory Consumer that delivers injected messages synchronously.
//
// Inject gives the handler the same context the SQS consumer does: the
// envelope's tenant as platform-pgcommon's GUC set (RLS-scoped pool queries),
// its trace ID (events.TraceIDFromContext), the source message
// (events.SourceMessageFromContext: the envelope's JSON as the body, with
// QueueURL) and the dead-letter attribution that makes a handler's SendToDLQ
// count as dead-lettered — so inbox.Handler and Store.Process skip it, as in
// production.
type Consumer struct {
	// QueueURL is reported as the source message's queue (MockQueueURL when empty).
	QueueURL string

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

// Inject delivers an envelope synchronously to the registered handler, with
// the handler context described on Consumer, and returns the handler's
// error. Without a handler it does nothing and returns nil.
func (m *Consumer) Inject(env events.Envelope[json.RawMessage]) error {
	m.mu.Lock()
	h := m.handler
	queueURL := m.QueueURL
	m.mu.Unlock()
	if h == nil {
		return nil
	}
	if queueURL == "" {
		queueURL = MockQueueURL
	}
	body, err := env.JSON()
	if err != nil {
		return err
	}
	messageID := uuid.NewString()
	ctx := internalsqs.HandlerContext(context.Background(), env.TenantID, env.TraceID, func() port.SourceMessage {
		return port.SourceMessage{
			QueueURL:  queueURL,
			MessageID: messageID,
			Body:      append([]byte(nil), body...),
			Attributes: map[string]string{
				"EventType": env.Type, "EventID": env.ID, "Source": env.Source, "TenantID": env.TenantID,
			},
			ReceiveCount: 1,
		}
	})
	ctx, _ = port.WithDLQAttribution(ctx, "explicit")
	return h(ctx, env)
}

// IsRunning reports whether Start has been called without a matching Stop.
func (m *Consumer) IsRunning() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.running
}

// Ensure Consumer satisfies events.Consumer at compile time.
var _ events.Consumer = (*Consumer)(nil)
