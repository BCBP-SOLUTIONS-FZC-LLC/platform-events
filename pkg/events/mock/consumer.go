package mock

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/google/uuid"

	internalsqs "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/adapter/outbound/sqs"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/domain"
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
//
// Like the SQS consumer, Inject never calls the handler for an envelope
// without id, type or source (production deletes or dead-letters it), and
// decodes a codec-encoded payload (dataschema + JSON-string data) with Codec
// first — returning the decode error, as production leaves the message for
// retry. The envelope is round-tripped through JSON as on the wire.
type Consumer struct {
	// QueueURL is reported as the source message's queue (MockQueueURL when empty).
	QueueURL string
	// Codec decodes codec-encoded payloads, as WithConsumerCodec does.
	Codec events.Codec

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
	if env.ID == "" || env.Type == "" || env.Source == "" {
		return fmt.Errorf("%w: id, type and source are required — the SQS consumer deletes or dead-letters such a message without calling the handler", ErrMalformedEnvelope)
	}
	body, err := env.JSON()
	if err != nil {
		return err
	}
	// As on the wire: the handler sees the envelope parsed from its JSON.
	var wire events.Envelope[json.RawMessage]
	if err := json.Unmarshal(body, &wire); err != nil {
		return fmt.Errorf("%w: %w", ErrMalformedEnvelope, err)
	}
	env = wire
	if env.SchemaID != "" && isJSONString(env.Payload) {
		if m.Codec == nil {
			return fmt.Errorf("mock: message has dataschema %q but no Codec is configured — the SQS consumer fails decode and leaves it for retry", env.SchemaID)
		}
		raw, err := domain.UnwrapCodecPayload(env.Payload)
		if err != nil {
			return err
		}
		decoded, err := m.Codec.Decode(context.Background(), env.SchemaID, raw)
		if err != nil {
			return fmt.Errorf("mock: codec Decode failed: %w", err)
		}
		env.Payload = decoded
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

// ErrMalformedEnvelope is returned by Inject for an envelope the SQS consumer
// would treat as malformed (never passed to the handler).
var ErrMalformedEnvelope = errors.New("mock: malformed envelope")

// isJSONString reports whether payload is a JSON string — the codec wire format.
func isJSONString(payload json.RawMessage) bool {
	for _, b := range payload {
		switch b {
		case ' ', '\t', '\n', '\r':
			continue
		}
		return b == '"'
	}
	return false
}

// IsRunning reports whether Start has been called without a matching Stop.
func (m *Consumer) IsRunning() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.running
}

// Ensure Consumer satisfies events.Consumer at compile time.
var _ events.Consumer = (*Consumer)(nil)
