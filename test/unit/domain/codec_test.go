package domain_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/internal/core/domain"
)

func TestWrapUnwrapCodecPayload_RoundTrip(t *testing.T) {
	original := []byte{0x00, 0x01, 0xFF, 'h', 'i', 0x00}

	wrapped, err := domain.WrapCodecPayload(original)
	require.NoError(t, err)

	// The wrapped form must be a valid, standalone JSON value (a string).
	var s string
	require.NoError(t, json.Unmarshal(wrapped, &s))

	got, err := domain.UnwrapCodecPayload(wrapped)
	require.NoError(t, err)
	assert.Equal(t, original, got)
}

func TestWrapCodecPayload_EmptyInput(t *testing.T) {
	wrapped, err := domain.WrapCodecPayload(nil)
	require.NoError(t, err)

	got, err := domain.UnwrapCodecPayload(wrapped)
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestUnwrapCodecPayload_NotAJSONString_Error(t *testing.T) {
	_, err := domain.UnwrapCodecPayload(json.RawMessage(`{"x":1}`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "base64 JSON string")
}

func TestUnwrapCodecPayload_InvalidBase64_Error(t *testing.T) {
	// A syntactically valid JSON string, but not valid base64 content.
	notBase64, err := json.Marshal("not-valid-base64!!!")
	require.NoError(t, err)

	_, err = domain.UnwrapCodecPayload(notBase64)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "base64 decode failed")
}
