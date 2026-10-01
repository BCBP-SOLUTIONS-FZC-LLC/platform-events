package events

import "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/internal/core/port"

// Codec is a pluggable schema-registry codec hook — see port.Codec for the
// full Encode/Decode contract. Implement this to plug in AWS Glue Schema
// Registry (or any other registry); this library ships no concrete
// implementation and adds no schema-registry SDK dependency. Inject via
// WithCodec (SNS publisher) / WithConsumerCodec (SQS consumer).
type Codec = port.Codec

// NoopCodec is the identity/reference Codec — see port.NoopCodec.
type NoopCodec = port.NoopCodec
