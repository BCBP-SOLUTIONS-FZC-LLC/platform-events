package port_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/internal/core/port"
)

var _ port.Codec = port.NoopCodec{}

func TestNoopCodec_Encode_Identity(t *testing.T) {
	payload := json.RawMessage(`{"x":1}`)
	encoded, schemaID, err := port.NoopCodec{}.Encode(context.Background(), "test.event", payload)
	require.NoError(t, err)
	assert.Equal(t, []byte(payload), encoded)
	assert.Empty(t, schemaID)
}

func TestNoopCodec_Decode_Identity(t *testing.T) {
	encoded := []byte(`{"x":1}`)
	decoded, err := port.NoopCodec{}.Decode(context.Background(), "", encoded)
	require.NoError(t, err)
	assert.Equal(t, json.RawMessage(encoded), decoded)
}
