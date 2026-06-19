package events

import (
	"encoding/json"
	"fmt"
	"time"

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

// hmacEnvelope is the canonical wire representation used exclusively for HMAC
// signing and verification. Unlike Envelope, no fields carry omitempty — all
// fields are always emitted in the JSON output regardless of whether they are
// empty. This prevents the omitempty tags on the public Envelope type from
// making the signed bytes non-deterministic when an optional field (TenantID,
// TraceID, CorrelationID) is present on one side of the sign/verify pair but
// absent on the other.
type hmacEnvelope struct {
	ID            string          `json:"id"`
	Type          string          `json:"type"`
	Source        string          `json:"source"`
	SchemaVersion string          `json:"schema_version"`
	TenantID      string          `json:"tenant_id"`
	TraceID       string          `json:"trace_id"`
	CorrelationID string          `json:"correlation_id"`
	Subject       string          `json:"subject"`
	Actor         string          `json:"actor"`
	Timestamp     time.Time       `json:"timestamp"`
	Payload       json.RawMessage `json:"payload"`
}

// canonicalHMACBytes returns a deterministic JSON encoding of env suitable for
// HMAC signing. All fields are always included regardless of their zero-value
// status.
func canonicalHMACBytes(env Envelope[json.RawMessage]) ([]byte, error) {
	return json.Marshal(hmacEnvelope(env))
}

// SignEnvelope signs the canonical JSON serialisation of env.
// Returns an error if len(key) < 32, matching the upfront check in VerifyEnvelope.
func SignEnvelope(key []byte, env Envelope[json.RawMessage]) (string, error) {
	if len(key) < 32 {
		return "", fmt.Errorf("events: HMAC key must be at least 32 bytes (got %d)", len(key))
	}
	b, err := canonicalHMACBytes(env)
	if err != nil {
		return "", fmt.Errorf("events: SignEnvelope marshal failed: %w", err)
	}
	return service.Sign(key, b)
}

// VerifyEnvelope verifies the HMAC signature of env's canonical JSON.
// Returns (false, err) when the key is too short or the input cannot be
// marshalled — callers must check both return values. Returns (false, nil) on
// a valid key with a signature mismatch (treat as authentication failure).
func VerifyEnvelope(key []byte, env Envelope[json.RawMessage], sig string) (bool, error) {
	if len(key) < 32 {
		return false, fmt.Errorf("events: HMAC key must be at least 32 bytes (got %d)", len(key))
	}
	b, err := canonicalHMACBytes(env)
	if err != nil {
		return false, fmt.Errorf("events: VerifyEnvelope marshal failed: %w", err)
	}
	return service.Verify(key, b, sig), nil
}
