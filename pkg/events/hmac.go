package events

import (
	"encoding/json"
	"fmt"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/service"
)

// Sign returns the hex-encoded HMAC-SHA256 signature of payload using key.
// Returns an error if len(key) < 32.
func Sign(key, payload []byte) (string, error) {
	return service.Sign(key, payload)
}

// Verify uses constant-time comparison to check sig against payload.
// Returns false on any error (short key, malformed hex, mismatch).
func Verify(key, payload []byte, sig string) bool {
	return service.Verify(key, payload, sig)
}

// SignEnvelope signs the canonical JSON serialisation of env.
func SignEnvelope(key []byte, env Envelope[json.RawMessage]) (string, error) {
	b, err := json.Marshal(env)
	if err != nil {
		return "", fmt.Errorf("events: SignEnvelope marshal failed: %w", err)
	}
	return service.Sign(key, b)
}

// VerifyEnvelope verifies the HMAC signature of env's canonical JSON.
// Returns (false, nil) on signature mismatch and (false, err) on malformed input.
// Callers must check both return values.
func VerifyEnvelope(key []byte, env Envelope[json.RawMessage], sig string) (bool, error) {
	b, err := json.Marshal(env)
	if err != nil {
		return false, fmt.Errorf("events: VerifyEnvelope marshal failed: %w", err)
	}
	return service.Verify(key, b, sig), nil
}
