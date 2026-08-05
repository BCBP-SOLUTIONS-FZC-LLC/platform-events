package domain

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
)

// WrapCodecPayload base64-encodes codec-encoded bytes and marshals the
// result as a JSON string literal, so Envelope.Payload stays valid JSON even
// though the codec's native output may be arbitrary binary. Never assign
// raw codec bytes directly to Payload — that can produce invalid JSON (and
// SNS's Message field requires valid UTF-8).
func WrapCodecPayload(encoded []byte) (json.RawMessage, error) {
	wrapped, err := json.Marshal(base64.StdEncoding.EncodeToString(encoded))
	if err != nil {
		return nil, fmt.Errorf("domain: failed to wrap codec payload as JSON string: %w", err)
	}
	return wrapped, nil
}

// UnwrapCodecPayload reverses WrapCodecPayload: given an Envelope.Payload
// that holds a base64-encoded JSON string, returns the raw codec-encoded
// bytes for Codec.Decode.
func UnwrapCodecPayload(payload json.RawMessage) ([]byte, error) {
	var b64 string
	if err := json.Unmarshal(payload, &b64); err != nil {
		return nil, fmt.Errorf("domain: codec payload is not a base64 JSON string: %w", err)
	}
	raw, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return nil, fmt.Errorf("domain: codec payload base64 decode failed: %w", err)
	}
	return raw, nil
}
