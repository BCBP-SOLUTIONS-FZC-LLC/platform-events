package mock_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/events"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/events/mock"
)

func TestMockPublisher_Publish(t *testing.T) {
	m := &mock.Publisher{}
	env := events.NewEnvelope("test.event", "svc", json.RawMessage(`{}`))

	err := m.Publish(context.Background(), env)
	require.NoError(t, err)

	published := m.Published()
	require.Len(t, published, 1)
	assert.Equal(t, env.ID, published[0].ID)
}

func TestMockPublisher_SetError(t *testing.T) {
	m := &mock.Publisher{}
	m.SetError(errors.New("forced error"))

	env := events.NewEnvelope("test.event", "svc", json.RawMessage(`{}`))
	err := m.Publish(context.Background(), env)
	require.Error(t, err)
	assert.Empty(t, m.Published())
}

func TestMockPublisher_Reset(t *testing.T) {
	m := &mock.Publisher{}
	env := events.NewEnvelope("test.event", "svc", json.RawMessage(`{}`))
	_ = m.Publish(context.Background(), env)
	assert.Len(t, m.Published(), 1)

	m.Reset()
	assert.Empty(t, m.Published())
}

func TestMockPublisher_PublishBatch(t *testing.T) {
	m := &mock.Publisher{}
	envs := []events.Envelope[json.RawMessage]{
		events.NewEnvelope("a.event", "svc", json.RawMessage(`{}`)),
		events.NewEnvelope("b.event", "svc", json.RawMessage(`{}`)),
	}

	err := m.PublishBatch(context.Background(), envs)
	require.NoError(t, err)
	assert.Len(t, m.Published(), 2)
}

func TestMockConsumer_StartStop(t *testing.T) {
	c := &mock.Consumer{}
	assert.False(t, c.IsRunning())

	err := c.Start(context.Background())
	require.NoError(t, err)
	assert.True(t, c.IsRunning())

	err = c.Stop()
	require.NoError(t, err)
	assert.False(t, c.IsRunning())
}

func TestMockConsumer_Inject(t *testing.T) {
	c := &mock.Consumer{}
	received := make([]events.Envelope[json.RawMessage], 0)

	c.SetHandler(func(_ context.Context, env events.Envelope[json.RawMessage]) error {
		received = append(received, env)
		return nil
	})

	env := events.NewEnvelope("test.event", "svc", json.RawMessage(`{}`))
	err := c.Inject(env)
	require.NoError(t, err)

	require.Len(t, received, 1)
	assert.Equal(t, env.ID, received[0].ID)
}

func TestMockConsumer_Inject_NoHandler(t *testing.T) {
	c := &mock.Consumer{}
	env := events.NewEnvelope("test.event", "svc", json.RawMessage(`{}`))
	// No handler set — should return nil without panic.
	err := c.Inject(env)
	require.NoError(t, err)
}

func TestMockConsumer_Inject_HandlerError(t *testing.T) {
	c := &mock.Consumer{}
	c.SetHandler(func(_ context.Context, _ events.Envelope[json.RawMessage]) error {
		return errors.New("handler error")
	})
	env := events.NewEnvelope("test.event", "svc", json.RawMessage(`{}`))
	err := c.Inject(env)
	require.Error(t, err)
}

func TestMockPublisher_PublishBatch_Error(t *testing.T) {
	m := &mock.Publisher{}
	m.SetError(errors.New("publish error"))

	envs := []events.Envelope[json.RawMessage]{
		events.NewEnvelope("a", "svc", json.RawMessage(`{}`)),
	}
	err := m.PublishBatch(context.Background(), envs)
	require.Error(t, err)
}

func TestMockDLQPublisher_RecordsAndIsolatesInputs(t *testing.T) {
	m := &mock.DLQPublisher{}
	body := []byte(`{"id":"1"}`)
	attrs := map[string]string{"k": "v"}

	require.NoError(t, m.SendToDLQ(context.Background(), "q", body, attrs, "bad payload"))
	body[0] = 'X'
	attrs["k"] = "changed"

	sent := m.Sent()
	require.Len(t, sent, 1)
	assert.Equal(t, "q", sent[0].SourceQueueURL)
	assert.JSONEq(t, `{"id":"1"}`, string(sent[0].Body), "body must be copied")
	assert.Equal(t, "v", sent[0].Attrs["k"], "attrs must be copied")
	assert.Equal(t, "bad payload", sent[0].Reason)

	url, err := m.ResolveDLQ(context.Background(), "q")
	require.NoError(t, err)
	assert.Equal(t, "mock://dlq", url)
	m.DLQURL = "custom"
	url, _ = m.ResolveDLQ(context.Background(), "q")
	assert.Equal(t, "custom", url)
}

func TestMockDLQPublisher_SetErrorAndReset(t *testing.T) {
	m := &mock.DLQPublisher{}
	m.SetError(events.ErrDLQNotConfigured)
	require.ErrorIs(t, m.SendToDLQ(context.Background(), "q", []byte("x"), nil, "r"), events.ErrDLQNotConfigured)
	_, err := m.ResolveDLQ(context.Background(), "q")
	require.ErrorIs(t, err, events.ErrDLQNotConfigured)
	assert.Empty(t, m.Sent())

	m.Reset()
	require.NoError(t, m.SendToDLQ(context.Background(), "q", []byte("x"), nil, "r"))
	assert.Len(t, m.Sent(), 1)
}

func TestMockDLQPublisher_ValidatesLikeSQSPublisher(t *testing.T) {
	cases := map[string]struct {
		source string
		body   []byte
		attrs  map[string]string
		reason string
	}{
		"empty source":       {"", []byte("x"), nil, "r"},
		"empty body":         {"q", nil, nil, "r"},
		"empty reason":       {"q", []byte("x"), nil, " "},
		"invalid utf8":       {"q", []byte{0xff}, nil, "r"},
		"NUL in body":        {"q", []byte("a\x00"), nil, "r"},
		"reserved attr name": {"q", []byte("x"), map[string]string{"AWS.x": "v"}, "r"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			m := &mock.DLQPublisher{}
			err := m.SendToDLQ(context.Background(), tc.source, tc.body, tc.attrs, tc.reason)
			require.ErrorIs(t, err, events.ErrDLQInvalidMessage)
			var dlqErr *events.DLQError
			require.ErrorAs(t, err, &dlqErr)
			assert.Empty(t, m.Sent(), "rejected messages are not recorded")
		})
	}
}
