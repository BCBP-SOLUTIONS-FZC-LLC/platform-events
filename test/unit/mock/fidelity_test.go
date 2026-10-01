package mock_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/events"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/events/mock"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/inbox"
)

// Inject gives the handler the same context as the SQS consumer.
func TestMockConsumer_InjectContextMatchesConsumer(t *testing.T) {
	c := &mock.Consumer{}
	env := events.NewEnvelope("iam.user.created", "iam", json.RawMessage(`{"a":1}`),
		events.WithTenantID("acme"), events.WithTraceID("4bf92f3577b34da6a3ce929d0e0e4736"))
	var got struct {
		tenant, trace string
		src           events.SourceMessage
		srcOK         bool
	}
	c.SetHandler(func(ctx context.Context, _ events.Envelope[json.RawMessage]) error {
		g, _ := pgcommon.GUCSetFromContext(ctx)
		got.tenant = g.TenantID
		got.trace = events.TraceIDFromContext(ctx)
		got.src, got.srcOK = events.SourceMessageFromContext(ctx)
		return nil
	})
	require.NoError(t, c.Inject(env))
	assert.Equal(t, "acme", got.tenant, "RLS tenant GUC")
	assert.Equal(t, env.TraceID, got.trace)
	require.True(t, got.srcOK)
	assert.Equal(t, mock.MockQueueURL, got.src.QueueURL)
	parsed, err := events.ParseEnvelope[json.RawMessage](got.src.Body)
	require.NoError(t, err)
	assert.Equal(t, env.ID, parsed.ID)
	assert.Equal(t, "iam.user.created", got.src.Attributes["EventType"])
}

// A handler that dead-letters through the mock DLQ publisher is not recorded
// by inbox.Handler — same as production — so a redrive is processed.
func TestMockDLQ_DeadLetterSkipsInbox(t *testing.T) {
	ledger := &memLedger{seen: map[uuid.UUID]bool{}}
	dlq := &mock.DLQPublisher{}
	c := &mock.Consumer{QueueURL: "https://sqs.us-east-1.amazonaws.com/1/orders"}
	c.SetHandler(inbox.Handler(ledger, func(ctx context.Context, _ events.Envelope[json.RawMessage]) error {
		src, _ := events.SourceMessageFromContext(ctx)
		return dlq.SendToDLQ(ctx, src.QueueURL, src.Body, src.Attributes, "unsupported tenant")
	}))
	require.NoError(t, c.Inject(events.NewEnvelope("iam.user.created", "iam", json.RawMessage(`{}`))))
	require.Len(t, dlq.Sent(), 1)
	assert.Equal(t, "https://sqs.us-east-1.amazonaws.com/1/orders", dlq.Sent()[0].SourceQueueURL)
	assert.Zero(t, ledger.marks, "dead-lettered message not recorded")
}

func TestMockPublisher_ValidatesAndPartialBatch(t *testing.T) {
	m := &mock.Publisher{}
	require.Error(t, m.Publish(context.Background(), events.Envelope[json.RawMessage]{ID: "x"}), "missing type/source rejected")

	ok := events.NewEnvelope("a.b.c", "svc", json.RawMessage(`{}`))
	throttled := events.NewEnvelope("a.b.c", "svc", json.RawMessage(`{}`))
	m.SetBatchError(&events.BatchError{Failures: []events.BatchFailure{{ID: throttled.ID, Code: "Throttled", Retryable: true}}})
	err := m.PublishBatch(context.Background(), []events.Envelope[json.RawMessage]{ok, throttled, {ID: "bad"}})
	var be *events.BatchError
	require.ErrorAs(t, err, &be)
	require.Len(t, be.Failures, 2)
	assert.True(t, be.Failures[0].Retryable)
	assert.Equal(t, "InvalidEnvelope", be.Failures[1].Code)
	require.Len(t, m.Published(), 1)
	assert.Equal(t, ok.ID, m.Published()[0].ID)

	m.Reset()
	require.NoError(t, m.PublishBatch(context.Background(), []events.Envelope[json.RawMessage]{throttled}))
}

func TestNewSQSConsumer_NilHandlerRejected(t *testing.T) {
	_, err := events.NewSQSConsumer(events.SQSConfig{QueueURL: "https://sqs.us-east-1.amazonaws.com/1/q"}, nil)
	require.Error(t, err)
	_, err = events.NewSQSConsumerWithClient(events.SQSConfig{QueueURL: "https://sqs.us-east-1.amazonaws.com/1/q"}, nil, nil)
	require.Error(t, err)
}

type memLedger struct {
	seen  map[uuid.UUID]bool
	marks int
}

func (l *memLedger) IsProcessed(_ context.Context, id uuid.UUID) (bool, error) {
	return l.seen[id], nil
}
func (l *memLedger) MarkProcessed(_ context.Context, id uuid.UUID) error {
	l.marks++
	l.seen[id] = true
	return nil
}
func (l *memLedger) Consumer() string { return "mem" }

func TestWithDeadLetterHandler_NilIgnored(t *testing.T) {
	_, err := events.NewSQSConsumerWithClient(events.SQSConfig{QueueURL: "https://sqs.us-east-1.amazonaws.com/1/q"}, nil,
		func(context.Context, events.Envelope[json.RawMessage]) error { return nil },
		events.WithDeadLetterHandler(func(context.Context, events.Envelope[json.RawMessage]) error { return nil }),
		events.WithDeadLetterHandler(nil))
	require.NoError(t, err)
}

func TestMockConsumer_InvalidPayload(t *testing.T) {
	c := &mock.Consumer{}
	c.SetHandler(func(context.Context, events.Envelope[json.RawMessage]) error { return nil })
	env := events.NewEnvelope("a.b.c", "svc", json.RawMessage(`{not json`))
	assert.Error(t, c.Inject(env), "an envelope that cannot be serialised fails like on the wire")
}

func TestConsumerOptions_HandlerTimeoutAndBodyLogging(t *testing.T) {
	_, err := events.NewSQSConsumerWithClient(events.SQSConfig{QueueURL: "https://sqs.us-east-1.amazonaws.com/1/q"}, nil,
		func(context.Context, events.Envelope[json.RawMessage]) error { return nil },
		events.WithHandlerTimeout(time.Minute), events.WithMalformedBodyLogging())
	require.NoError(t, err)
}

// upperCodec decodes by upper-casing — enough to see that Inject decoded.
type upperCodec struct{}

func (upperCodec) Encode(context.Context, string, json.RawMessage) ([]byte, string, error) {
	return nil, "", nil
}
func (upperCodec) Decode(_ context.Context, _ string, b []byte) (json.RawMessage, error) {
	return json.RawMessage(`{"decoded":"` + string(b) + `"}`), nil
}

// Inject behaves like the SQS consumer: a malformed envelope never reaches
// the handler, and a codec-encoded payload is decoded first (an error without
// a Codec); a dataschema on a plain JSON object is passed through.
func TestConsumer_Inject_ValidatesAndDecodesLikeProduction(t *testing.T) {
	var got []json.RawMessage
	c := &mock.Consumer{}
	c.SetHandler(func(_ context.Context, env events.Envelope[json.RawMessage]) error {
		got = append(got, env.Payload)
		return nil
	})

	bad := events.NewEnvelope("x.y", "svc", json.RawMessage(`{}`))
	bad.Source = ""
	require.ErrorIs(t, c.Inject(bad), mock.ErrMalformedEnvelope)
	assert.Empty(t, got, "the handler must not see a malformed envelope")

	encoded := events.NewEnvelope("x.y", "svc", json.RawMessage(`"aGVsbG8="`), events.WithSchemaID("schema-1")) // base64("hello")
	require.Error(t, c.Inject(encoded), "no Codec configured: decode fails as in production")
	assert.Empty(t, got)

	c.Codec = upperCodec{}
	require.NoError(t, c.Inject(encoded))
	require.Len(t, got, 1)
	assert.JSONEq(t, `{"decoded":"hello"}`, string(got[0]))

	tagged := events.NewEnvelope("x.y", "svc", json.RawMessage(`{"a":1}`), events.WithSchemaID("schema-1"))
	require.NoError(t, c.Inject(tagged))
	assert.JSONEq(t, `{"a":1}`, string(got[1]), "a dataschema on a JSON object is informational")
}
