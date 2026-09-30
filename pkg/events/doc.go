// Package events provides the public API for platform event publishing, consumption,
// and envelope utilities.
//
// # Event envelope
//
// [Envelope] is the canonical wire format for all inter-service events. Create one
// with [NewEnvelope]; always pass [WithTenantID] and [WithTraceID] when publishing
// from an HTTP handler so that RLS enforcement and trace continuity are preserved
// end-to-end.
//
// # Publishing
//
// [NewSNSPublisher] constructs an AWS SNS publisher. For guaranteed at-least-once
// delivery, enqueue events via [outbox.Enqueue] inside a [pgcommon.RunInTx] callback
// rather than calling [Publisher.Publish] directly.
//
// # Consuming
//
// [NewSQSConsumer] runs a long-poll SQS receive loop. Handlers receive a context
// with [pgcommon.GUCSet] injected for the event's tenant, enabling Row-Level Security
// enforcement in downstream database calls without any extra wiring.
//
// Handlers must be idempotent — SQS delivers messages at least once.
// Use [Envelope.ID] (UUID v7) as the idempotency key for all side effects.
//
// # Dead-letter forwarding
//
// [NewSQSDLQPublisher] returns a [DLQPublisher] that forwards a message a
// consumer has given up on to the DLQ configured on the source queue's
// RedrivePolicy. Errors are [*DLQError]; use errors.Is with the ErrDLQ*
// sentinels and [ErrRetryable] to tell configuration problems from transient
// transport failures.
//
// # HMAC helpers
//
// [Sign], [Verify], [SignEnvelope], and [VerifyEnvelope] provide HMAC-SHA256 signing
// and constant-time verification for webhook receipt and cross-service authentication.
//
// # Metrics
//
// Call [Init] once at service startup to register Prometheus counters and histograms.
// Use [InitWithRegisterer] in tests to isolate registries.
package events
