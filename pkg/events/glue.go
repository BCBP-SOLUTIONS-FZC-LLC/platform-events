package events

import (
	"context"
	"encoding/json"
	"fmt"
)

// AWS Glue Schema Registry wire format: [version][compression][16-byte
// schema version UUID] followed by the payload.
const (
	glueHeaderVersion byte = 0x03
	glueNoCompression byte = 0x00
	glueHeaderSize         = 18
)

// GlueDecodeCodec is a decode-only [Codec] for consumers of events a producer
// published with an AWS Glue Schema Registry codec (the envelope then carries
// a dataschema and a Glue-framed payload). It strips the self-describing
// 18-byte header and returns the JSON payload — no registry client, schema
// lookup or AWS permission is needed. Plain-JSON envelopes (no dataschema)
// never reach a codec, so it is safe for mixed traffic.
//
// Compressed and non-JSON (e.g. Avro-format) payloads are rejected with a
// clear error (they would otherwise fail later as unparseable JSON). Encode always fails: pair it only with a
// consumer, via [WithConsumerCodec].
type GlueDecodeCodec struct{}

var _ Codec = GlueDecodeCodec{}

// Encode always fails — GlueDecodeCodec is decode-only.
func (GlueDecodeCodec) Encode(_ context.Context, eventType string, _ json.RawMessage) (encoded []byte, schemaVersionID string, err error) {
	return nil, "", fmt.Errorf("glue decode codec: Encode(%q) called on a decode-only codec", eventType)
}

// Decode strips the Glue wire-format header and returns the JSON payload.
func (GlueDecodeCodec) Decode(_ context.Context, _ string, encoded []byte) (json.RawMessage, error) {
	if len(encoded) < glueHeaderSize {
		return nil, fmt.Errorf("glue decode codec: encoded payload is %d bytes — shorter than the %d-byte Glue header", len(encoded), glueHeaderSize)
	}
	if encoded[0] != glueHeaderVersion {
		return nil, fmt.Errorf("glue decode codec: unexpected header version byte 0x%02x — want 0x%02x", encoded[0], glueHeaderVersion)
	}
	if encoded[1] != glueNoCompression {
		return nil, fmt.Errorf("glue decode codec: unsupported compression byte 0x%02x — only uncompressed (0x00) payloads are supported", encoded[1])
	}
	payload := encoded[glueHeaderSize:]
	// An Avro-format Glue schema (or an empty payload) is not JSON; reject it
	// here with a clear error instead of handing the handler bytes that fail
	// later in json.Unmarshal / env.JSON().
	if !json.Valid(payload) {
		return nil, fmt.Errorf("glue decode codec: payload after the Glue header is not valid JSON (%d bytes) — only JSON-format Glue schemas are supported", len(payload))
	}
	return json.RawMessage(payload), nil
}
