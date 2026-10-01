package envelope_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/events"
)

// Decoding into a reused Envelope whose JSON has no data clears the old payload.
func TestEnvelope_UnmarshalReuse_ResetsPayload(t *testing.T) {
	var env events.Envelope[json.RawMessage]
	require.NoError(t, json.Unmarshal([]byte(`{"id":"a","type":"t","source":"s","data":{"x":1}}`), &env))
	require.NotEmpty(t, env.Payload)
	require.NoError(t, json.Unmarshal([]byte(`{"id":"b","type":"t","source":"s"}`), &env))
	assert.Empty(t, env.Payload, "the previous message's payload must not survive")
}
