package port

import (
	"context"
	"encoding/json"
)

// Codec is a pluggable hook for encoding/decoding an Envelope's plain-JSON
// Payload into a schema-registry-specific wire format (e.g. AWS Glue Schema
// Registry). This library does not implement a concrete Codec or depend on
// any schema-registry SDK — consuming services implement this interface
// with their own registry client and inject it via WithCodec (publisher) /
// WithConsumerCodec (consumer).
//
// Encode is called by the SNS publisher on the envelope's plain-JSON Payload
// bytes only (never the full envelope), immediately before publish. The
// returned bytes are base64-encoded and substituted into Envelope.Payload as
// a JSON string (see domain.WrapCodecPayload) so the envelope's "data" field
// stays valid JSON, and schemaID is written to Envelope.SchemaID so Decode
// can find the right schema on the consumer side. Returning schemaID == ""
// (as NoopCodec does) leaves Payload untouched — no wire format change.
//
// Decode is called by the SQS consumer immediately after a message is
// received, only when the envelope's SchemaID is non-empty — an empty
// SchemaID means the message was never codec-encoded (legacy producer,
// dev/test, or a producer that never configured WithCodec) and Payload is
// treated as already-plain-JSON.
//
// Errors: wrap a transient registry failure (outage, throttling, timeout)
// around events.ErrRetryable with %w — e.g.
// fmt.Errorf("glue: %w: %v", events.ErrRetryable, err) — so the outbox retries
// it without counting toward MaxAttempts; any other Encode error counts as a
// permanent failure and dead-letters after MaxAttempts.
type Codec interface {
	// Encode encodes plain-JSON payload for eventType, returning the codec's
	// native wire bytes and the schema registry ID to record on
	// Envelope.SchemaID.
	Encode(ctx context.Context, eventType string, payload json.RawMessage) (encoded []byte, schemaID string, err error)

	// Decode reverses Encode: given the schemaID recorded on the received
	// envelope and the raw (already base64-decoded) encoded bytes, returns
	// plain JSON bytes suitable for direct use as the envelope's new Payload.
	Decode(ctx context.Context, schemaID string, encoded []byte) (payload json.RawMessage, err error)
}

// NoopCodec is the identity/reference Codec implementation. Encode returns
// the payload unchanged with an empty schemaID (no wire format change);
// Decode returns its input unchanged. WithCodec(nil) already behaves this
// way — NoopCodec exists so callers can be explicit and so tests can
// exercise the WithCodec/WithConsumerCodec plumbing without a real schema
// registry.
type NoopCodec struct{}

func (NoopCodec) Encode(_ context.Context, _ string, payload json.RawMessage) ([]byte, string, error) {
	return payload, "", nil
}

func (NoopCodec) Decode(_ context.Context, _ string, encoded []byte) (json.RawMessage, error) {
	return json.RawMessage(encoded), nil
}
