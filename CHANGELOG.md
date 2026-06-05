# Changelog

All notable changes to `platform-events` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **`pkg/config`** — public env loading (`LoadSNS`, `LoadSQS`, `LoadOutbox`, `LoadOTel`) plus wiring helpers (`RunnerConfigFromEnv`, `SQSConfigFromEnv`, `SQSConsumerOptions`, `LogWarnings`)
- `OUTBOX_PUBLISH_CONCURRENCY`, `OUTBOX_PUBLISH_TIMEOUT`, `OUTBOX_DRAIN_TIMEOUT` env vars for outbox runner tuning
- `SQS_MAX_RECEIVE_COUNT` env var — map to `events.WithMaxReceiveCount` when wiring `WithDeadLetterHandler`
- Sequential outbox publish path (`PublishConcurrency=1`) uses SNS `PublishBatch` (up to 10 per API call) for higher throughput

### Changed

- `events.SystemTenantID` re-exported from `internal/core/domain` — single canonical definition
- Reference CLI (`cmd/platform-events`) calls `events.Init`, logs config `Warnings`, and prints production wiring reminders; `-strict` exits non-zero on missing required env vars
- `internal/config` is a deprecated alias of `pkg/config` — import `pkg/config` in new code

### Fixed

- `CHANGELOG` claim-lease default corrected: `OUTBOX_CLAIM_LEASE_DURATION` unset → **10 minutes** (store default), not 30s
- `.env-example` documents `OUTBOX_CLAIM_LEASE_DURATION`, `OUTBOX_STARTUP_JITTER`, runner tuning vars, and `RawMessageDelivery` requirement

---

## [1.1.0] - 2026-06-05

### Added

**Public API (`pkg/`)**

- `events.SystemTenantID` constant and `events.WithSystemTenant()` envelope option — use for background jobs that publish cross-tenant events without an HTTP request context
- `events.TraceIDFromContext(ctx)` — retrieves the trace ID injected by `NewSQSConsumer` into the handler context; use in downstream log lines and spans to preserve the publishing service's trace
- `events.SQSClientLike` interface in `pkg/events` — allows consuming services to mock the SQS client without importing internal packages; satisfies `aws-sdk-go-v2/service/sqs` automatically
- `outbox.Runner.ReprocessDeadLetters(ctx, limit int) (int, error)` — moves up to `limit` dead-letter records back to `outbox_events` for re-delivery; safe to call in a scheduled job or admin endpoint
- `events.WithSchemaVersion(v string)` — new `EnvelopeOpt` that sets `Envelope.SchemaVersion`; use `"1"` at inception and increment on additive payload changes. Absent field is `omitempty` — fully backward-compatible with v1.0 publishers and consumers.
- `Envelope.SchemaVersion string` — new optional wire field (`json:"schema_version,omitempty"`); carries the payload contract version across the SNS/SQS boundary so consumers can assert and reject unrecognised versions without silent misparse

**Core domain / ports**

- `domain.RetryableError` struct and `domain.ErrRetryable` sentinel — wraps transient AWS errors; callers use `errors.Is(err, domain.ErrRetryable)` to distinguish throttling from permanent failures without importing AWS SDK types
- `port.WithEnvelopeTraceID(ctx, traceID)` / `port.EnvelopeTraceIDFromContext(ctx)` — internal context key used by `NewSQSConsumer` to thread the publishing service's OTel trace ID through the handler call chain
- `port.OutboxStore.LeasedCount(ctx) (int, error)` — returns the count of records currently held under a lease; surfaced as the `outbox_leased_total` gauge
- `port.OutboxStore.ReprocessDeadLetters(ctx, limit int) (int, error)` — moves dead-letter records back into `outbox_events` for retry

**SQS Consumer hardening**

- Per-call `ReceiveMessage` timeout: each poll wraps a `context.WithTimeout(WaitSeconds + 5s)` to prevent indefinitely hung receive calls from stalling the loop
- `VisibilityTimeout > 12h` validation — `NewSQSConsumer` returns an error at construction time if the configured timeout exceeds the SQS maximum; previously resulted in a silent AWS error at runtime
- Malformed message handling — if a message body cannot be unmarshalled into an `Envelope`, the message is immediately deleted and counted as `events_consumed_total{status=malformed}` rather than retried forever
- Panic recovery in handler goroutines — a panicking handler logs the stack trace and leaves the message visible for retry instead of crashing the consumer loop
- `WithEnvelopeTraceID(ctx, env.TraceID)` injection — the handler context now carries the publishing trace ID, accessible via `events.TraceIDFromContext(ctx)`, so consumer-side spans link back to the publisher's trace without relying on message attributes

**SNS Publisher hardening**

- `wrapIfRetryable` — SNS adapter inspects `smithy.APIError.ErrorCode()` for `ThrottlingException`, `ServiceUnavailable`, `InternalFailure`, and `RequestTimeout`; wraps matching errors in `domain.RetryableError` so the outbox runner does not dead-letter on transient failures
- Envelope field validation at publish time — `Publish` returns an error immediately if `env.ID`, `env.Type`, or `env.Source` is empty, preventing invalid Prometheus label cardinality

**Outbox Runner hardening**

- Lease-based concurrency (`ClaimLeaseDuration`) — the claim step updates `scheduled_at = NOW() + ClaimLeaseDuration` so a record cannot be claimed by a second runner while in flight; replaces the prior optimistic locking approach
- `StartupJitter` — on `Runner.Start`, sleep a random duration between `0` and `StartupJitter` before the first poll to desync horizontally scaled replicas and reduce thundering-herd contention on startup
- Retryable-error threshold — `OutboxService` uses `threshold = MaxAttempts + 1` when a retryable error is detected, so transient throttling never advances the attempt counter toward dead-letter; the record is rescheduled without penalty

**Metrics**

- `outbox_leased_total` (`GaugeVec`, label `service`) — tracks records currently claimed by a runner; combine with `outbox_pending_total` for a complete in-flight picture
- `outbox_dead_letters_total` (`Counter`, label `service`) — incremented each time a record is moved to `outbox_dead_letters`; alert on `rate() > 0`
- `events_consumed_total` now includes `status=malformed` for envelope-parse failures

**Config**

- `OutboxConfigEnv.ClaimLeaseDuration` — loaded from `OUTBOX_CLAIM_LEASE_DURATION` (unset → **10 minutes** store default); controls how long a leased record is held before it becomes reclaimable
- `OutboxConfigEnv.StartupJitter` — loaded from `OUTBOX_STARTUP_JITTER` (default `0s`); set to `5s`–`10s` in scaled deployments
- `OutboxConfigEnv.String()` — safe, credential-redacted log representation; masks passwords in both URL (`postgres://user:***@host/db`) and key-value (`password=***`) DSN formats

**Event schema governance**

- `EVENT_SCHEMA_GOVERNANCE.md` — new document covering: event type naming rules (including `.v<N>` versioning for breaking changes), payload evolution rules (additive-only, `omitempty` discipline, forbidden mutations), `schema_version` publisher and consumer contracts, migration window pattern, consumer compatibility contract, event type registry, and deprecation/sunset process
- Updated `event_type` convention to `<domain>.<entity>.<past-tense-verb>[.v<N>]` — `.v<N>` suffix only appears for breaking payload changes; v1 remains implicit (no suffix)
- `CONTRIBUTING.md` PR checklist: added event type registry, payload struct, and breaking-change rules
- `README.md` and `ARCHITECTURE.md` updated with `schema_version` wire field, `WithSchemaVersion` usage examples, and links to `EVENT_SCHEMA_GOVERNANCE.md`

### Changed

- `OutboxService.PublishPending` skips attempt-count increment for retryable errors (uses threshold `MaxAttempts + 1`); records remain in `outbox_events` and are rescheduled rather than progressing toward dead-letter
- `NewSQSConsumer` injects `TraceID` into handler context via `port.WithEnvelopeTraceID` in addition to the existing `pgcommon` GUC injection, giving handlers a single `events.TraceIDFromContext(ctx)` call site
- `EnvelopeOpt` is now `func(*envelopeConfig)` internally (unexported config struct) — all library-provided option functions (`WithTenantID`, `WithTraceID`, `WithCorrelationID`, `WithSystemTenant`) continue to work without change; consumers that wrote raw `EnvelopeOpt` closures must update to the new signature (expected impact: none — the type was not documented for direct construction)

### Fixed

- SQS consumer no longer hangs indefinitely on a `ReceiveMessage` call when the AWS endpoint becomes unresponsive — bounded by `WaitSeconds + 5s` per call
- Outbox runner no longer dead-letters records on transient AWS throttling; they are rescheduled and retried on the next poll cycle

---

## [1.0.0] - 2026-05-29

### Added

- Initial implementation of `platform-events` shared library
- `pkg/events` — typed `Envelope[T]` with UUID v7 IDs, `NewEnvelope`, `ParseEnvelope`, `JSON()`
- `pkg/events` — `Publisher` interface and `NewSNSPublisher` (AWS SNS, FIFO support, batch)
- `pkg/events` — `Consumer` interface and `NewSQSConsumer` (long-poll, concurrency, drain)
- `pkg/events` — HMAC helpers: `Sign`, `Verify`, `SignEnvelope`, `VerifyEnvelope`
- `pkg/events` — Prometheus metrics: `Init`, `InitWithRegisterer`
- `pkg/events/mock` — `MockPublisher` and `MockConsumer` for unit testing
- `pkg/outbox` — transactional outbox `Runner`, `Enqueue`, `ApplySchema`
- `internal/core` — domain entities, port interfaces, HMAC and outbox services
- `internal/adapter/outbound` — SNS publisher, SQS consumer, Postgres outbox store, Zap logger, Prometheus metrics
- `internal/config` — environment-variable loading with defaults
- OTel tracing on SNS publish and SQS receive spans
- GUC injection into SQS handler context for `platform-pgcommon` RLS
- GitHub Actions CI/CD workflows (validate, CI, release)

### Chore

- Bump `actions/upload-artifact` from v4 to v7 in CI workflows
