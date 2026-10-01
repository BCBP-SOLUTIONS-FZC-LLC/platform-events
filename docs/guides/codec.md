# Codec

The optional schema-registry `Codec` hook. One of the detailed guides linked from the [project README](../../README.md#contributing).

---

## Codec (optional schema-registry hook)

`events.Codec` is a pluggable hook for encoding/decoding an envelope's JSON `Payload` into a schema-registry-specific wire format (e.g. AWS Glue Schema Registry). **`platform-events` ships no concrete implementation and adds no schema-registry SDK dependency** — implement `Codec` against your own registry client and inject it via `WithCodec` (publisher) / `WithConsumerCodec` (consumer), the same pattern used for `port.Logger`.

```go
type Codec interface {
    Encode(ctx context.Context, eventType string, payload json.RawMessage) (encoded []byte, schemaID string, err error)
    Decode(ctx context.Context, schemaID string, encoded []byte) (payload json.RawMessage, err error)
}
```

### Implementing a Codec

A minimal AWS Glue Schema Registry codec skeleton — this is the shape a consuming service writes and owns; `platform-events` does not ship it:

```go
type GlueCodec struct {
    client         *glue.Client // aws-sdk-go-v2/service/glue, or your registry's client
    registryName   string
    schemaIDByType sync.Map // event_type -> schema UUID; populate via GetSchemaByDefinition/RegisterSchemaVersion
}

// Encode must be safe for concurrent use — PublishBatch and parallel Publish
// calls invoke it concurrently across goroutines.
func (g *GlueCodec) Encode(ctx context.Context, eventType string, payload json.RawMessage) ([]byte, string, error) {
    ctx, cancel := context.WithTimeout(ctx, 3*time.Second) // Encode/Decode have no internal timeout — set your own
    defer cancel()

    schemaID, avroBytes, err := g.encodeWithGlue(ctx, eventType, payload) // your registry lookup + serialisation
    if err != nil {
        return nil, "", fmt.Errorf("glue encode: %w", err)
    }
    return avroBytes, schemaID, nil
}

// Decode must also be safe for concurrent use — WithConcurrency(n) on the
// SQS consumer invokes it from up to n goroutines simultaneously.
func (g *GlueCodec) Decode(ctx context.Context, schemaID string, encoded []byte) (json.RawMessage, error) {
    ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
    defer cancel()

    plain, err := g.decodeWithGlue(ctx, schemaID, encoded) // your registry lookup + deserialisation
    if err != nil {
        return nil, fmt.Errorf("glue decode: %w", err)
    }
    return plain, nil
}
```

```go
codec := &GlueCodec{client: glueClient, registryName: "platform-events-registry"}

publisher, err := events.NewSNSPublisher(events.SNSConfig{TopicARN: topicARN, Logger: logger},
    events.WithCodec(codec),
)

consumer, err := events.NewSQSConsumer(events.SQSConfig{QueueURL: queueURL, Logger: logger},
    handler,
    events.WithConsumerCodec(codec),
)
```

| Behaviour | Detail |
|---|---|
| Wire format | `Encode`'s output bytes are base64-encoded and marshalled as a JSON *string*, then substituted into `Envelope.Payload`; `SchemaID` is set to the returned `schemaID`. The envelope stays valid JSON regardless of the codec's native binary format — required for SNS's UTF-8-only `Message` field. |
| `Decode`'s `encoded` parameter | Already base64-*decoded* raw bytes — the library strips the JSON-string/base64 wrapping before calling `Decode`. Your implementation only ever sees the codec's own native wire bytes (e.g. the Glue `[18-byte header][data]` framing), never the transport wrapping. |
| Decode signal | `SchemaID` empty ⇒ `Payload` is already plain JSON (no codec configured on the publisher, or `NoopCodec`) — the consumer skips `Decode` entirely. Non-empty with a JSON-string `Payload` (the base64 wire format) ⇒ `Decode` runs before the message reaches the handler or `WithDeadLetterHandler`. Non-empty with a JSON object / array `Payload` is treated as an informational tag and passed through (since v1.6.1; older consumers dead-letter it). |
| Decode failures | Treated like a normal handler error, **not** like malformed JSON — a schema-registry outage can be transient, so the message is left visible for SQS's own `MaxReceiveCount`/redrive-policy retry rather than deleted immediately. |
| Concurrency | `Encode`/`Decode` must be safe for concurrent use. `PublishBatch` invokes `Encode` once per envelope in the batch; `WithConcurrency(n)` on the consumer invokes `Decode` from up to `n` handler goroutines at once. A stateful codec (e.g. a schema-ID cache) must synchronise its own state. |
| Timeouts | Neither method has an internal timeout — same responsibility as `Handler` (see [Handler contract](consuming.md#handler-contract)). If your codec calls an external registry, wrap the call in your own `context.WithTimeout`; an unbounded `Decode` call blocks that consumer goroutine indefinitely. |
| Outbox interaction | None. Encoding happens transiently inside `Publish`/`PublishBatch`; `pkg/outbox` always stores the canonical plain-JSON envelope and is unaffected by `WithCodec`. |
| `SchemaID` vs `SchemaVersion` | `SchemaID` is set automatically by the publisher from the codec's `Encode` return value once `WithCodec` is configured — manual `WithSchemaID` calls are unnecessary (and are overwritten). `SchemaVersion` is the unrelated, human-readable `"1"`/`"2"` semantic version — see [Event envelope](envelope.md#event-envelope). |

`events.NoopCodec` is the identity reference implementation (`Encode` returns the payload unchanged with an empty `schemaID`) — useful in tests to exercise the `WithCodec`/`WithConsumerCodec` plumbing without a real registry.

