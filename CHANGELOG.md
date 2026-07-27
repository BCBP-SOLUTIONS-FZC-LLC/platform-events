# Changelog

All notable changes to `platform-events` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- **Outbox writes under `PGBouncerMode: true`** — `OutboxRecord.Payload` was `[]byte`; `platform-pgcommon`'s `PGBouncerMode` sets pgx's `DefaultQueryExecMode` to `SimpleProtocol`, which encodes parameters client-side using pgx's default codec for the Go type with no server round-trip to describe the target column. A plain `[]byte` defaults to the `bytea` codec, and binding that to the `payload` `JSONB` column failed with `invalid input syntax for type json` (`SQLSTATE 22P02`) — even though the underlying bytes were valid JSON. `OutboxRecord.Payload` is now `json.RawMessage`, which pgx encodes with its JSON codec instead. Affects `outbox.Enqueue`, `OutboxService.Enqueue`, `ClaimBatch`, and the dead-letter insert path in `MarkFailed`. No public API changes — `outbox.Enqueue` and `Runner` callers are unaffected.

---

## [1.3.0] - 2026-06-23

### Added

- **`Envelope.Subject`** (`json:"subject,omitempty"`) — resource URI or identifier the event is about (e.g. `"users/01926e4f-..."`). Set via `events.WithSubject(subject string)`. Forwarded as an SNS message attribute (`Subject`) when non-empty, enabling SQS subscription filter policies without body parsing.
- **`Envelope.Actor`** (`json:"actor,omitempty"`) — identity that caused the event (user UUID, service-account name, etc.). Set via `events.WithActor(actor string)`. Audit trail field — not forwarded as an SNS attribute.
- `events.WithSubject(subject string) EnvelopeOpt` — sets `Subject` on the envelope at construction time.
- `events.WithActor(actor string) EnvelopeOpt` — sets `Actor` on the envelope at construction time.
- **`Envelope.SchemaID`** (`json:"dataschema,omitempty"`) — schema registry version identifier (e.g. AWS Glue Schema Registry UUID). Set via `events.WithSchemaID(id string)`. Technical registry pointer used by the codec for Avro/JSON deserialization; distinct from `specversion`. Not forwarded as an SNS attribute.
- `events.WithSchemaID(id string) EnvelopeOpt` — sets `SchemaID` on the envelope at construction time.
- **`Envelope.IPAddress`** (`json:"ip_address,omitempty"`) — client IP at the time the event was triggered. Set via `events.WithIPAddress(ip string)`. Audit trail field — not forwarded as an SNS attribute.
- **`Envelope.UserAgent`** (`json:"user_agent,omitempty"`) — HTTP `User-Agent` header from the triggering request. Set via `events.WithUserAgent(ua string)`. Audit trail field — not forwarded as an SNS attribute.
- `events.WithIPAddress(ip string) EnvelopeOpt` — sets `IPAddress` on the envelope at construction time.
- `events.WithUserAgent(ua string) EnvelopeOpt` — sets `UserAgent` on the envelope at construction time.
- **`runner.ListDeadLetters(ctx, DLQFilter, limit) ([]DeadLetterRecord, error)`** — returns up to `limit` records from `outbox_dead_letters` matching the filter, ordered by `failed_at` ascending. Returns an empty slice when no records match.
- **`runner.ReprocessDeadLettersWith(ctx, DLQFilter, limit) (int, error)`** — selective replay: moves up to `limit` filtered records from `outbox_dead_letters` back to `outbox_events`, resetting attempts to 0. Supports filtering by `EventType`, `TenantID`, and `FailedBefore`.
- **`runner.DiscardDeadLetters(ctx, DLQFilter, limit) (int64, error)`** — permanently deletes up to `limit` filtered records from `outbox_dead_letters`. Always call `ListDeadLetters` first to confirm the selection.
- **`outbox.DLQFilter`** — public type with fields `EventType string`, `TenantID string`, `FailedBefore time.Time`. All fields optional; zero value matches all records.
- **`outbox.DeadLetterRecord`** — public type returned by `ListDeadLetters` with fields `ID`, `EventType`, `TenantID`, `TraceID`, `Attempts`, `LastError`, `CreatedAt`, `FailedAt`.
- **`outbox_dead_letters_discarded_total`** Prometheus counter — incremented by `DiscardDeadLetters`.
- **Migration 008** — composite index `idx_outbox_dead_letters_event_type_tenant_id` on `(event_type, tenant_id)` for efficient DLQ filter queries.
- **`pkg/outbox/dlq.go`** — documents the public DLQ API surface with usage examples.

### Changed

- **`Envelope` wire format — full CloudEvents alignment (JSON key renames; Go field names unchanged):**
  - `payload` → `data` (CloudEvents `data`)
  - `timestamp` → `time` (CloudEvents `time`)
  - `schema_version` → `specversion` (CloudEvents `specversion`)
  - `schema_id` → `dataschema` (CloudEvents `dataschema`)
  - `hmacEnvelope` canonical bytes updated for all renamed fields — `SignEnvelope`/`VerifyEnvelope` callers must redeploy signer and verifier together.
- `buildMessageAttributes` (SNS adapter) emits `Subject` as an SNS message attribute when non-empty.
- `hmacEnvelope` (internal) updated to include `Subject`, `Actor`, `IPAddress`, and `UserAgent` in the canonical HMAC payload.
- `publicToDomain` / `domainToPublic` carry all envelope fields including `Subject`, `Actor`, `IPAddress`, `UserAgent`, `SchemaID` without loss.
- `outbox-poll-cycle.mmd` updated to reference DLQ management operations.
- `ARCHITECTURE.md` and `README.md` updated to reflect all new fields and the CloudEvents wire format.

---

## [1.2.0] - 2026-06-12

### Added

- **`pkg/config`** — public env loading (`LoadSNS`, `LoadSQS`, `LoadOutbox`, `LoadOTel`) plus wiring helpers (`RunnerConfigFromEnv`, `SQSConfigFromEnv`, `SQSConsumerOptions`, `LogWarnings`, `LogWarningsTo`)
- `config.LogWarningsTo(logger port.Logger, warnings []string)` — emits configuration warnings via a structured logger (Warn level); falls back to stderr when logger is nil. Prefer over `LogWarnings` when a structured logger is available so warnings reach log aggregators (Loki, CloudWatch)
- `outbox.Runner.PrunePublished(ctx, olderThan time.Duration, limit int) (int64, error)` — removes published `outbox_events` rows older than `olderThan`; call periodically from a scheduled job to prevent unbounded table growth. Delegates to `port.OutboxStore.PrunePublished`
- `port.OutboxStore.PrunePublished(ctx, olderThan time.Duration, limit int) (int64, error)` — **breaking interface change**: all `OutboxStore` implementations must add this method. The Postgres implementation in `internal/adapter/outbound/outboxstore` applies a 30 s internal timeout and uses the `idx_outbox_events_published_at` partial index (migration 007) for efficient batch deletes
- Migration `007_add_prune_index` — partial index `idx_outbox_events_published_at ON outbox_events(published_at) WHERE published_at IS NOT NULL`; makes `PrunePublished` an index scan instead of a sequential scan as the table grows
- `metrics.OversizedEventTypeLabelTotal` counter (`events_oversized_event_type_label_total`) — incremented by `SanitizeEventType` whenever an `event_type` value exceeds 128 bytes and is replaced with `"__oversized__"`; alert when non-zero to detect misconfigured or adversarial producers
- `OUTBOX_PUBLISH_CONCURRENCY`, `OUTBOX_PUBLISH_TIMEOUT`, `OUTBOX_DRAIN_TIMEOUT` env vars for outbox runner tuning
- `SQS_MAX_RECEIVE_COUNT` env var — map to `events.WithMaxReceiveCount` when wiring `WithDeadLetterHandler`
- Sequential outbox publish path (`PublishConcurrency=1`) uses SNS `PublishBatch` (up to 10 per API call) for higher throughput
- `outbox.MigrationsTable` — exported constant (`"outbox_migrations"`); `ApplySchema` injects `x-migrations-table=outbox_migrations` into the DSN so the outbox migration history is tracked in its own table, isolated from the consuming service's `schema_migrations` and `pgcommon_migrations` tables

### Breaking Changes

- **`port.OutboxStore` interface** has a new required method: `PrunePublished(ctx context.Context, olderThan time.Duration, limit int) (int64, error)`. Any code that implements `OutboxStore` (e.g. test mocks) must add this method. The no-op implementation is `func (s *myStore) PrunePublished(_ context.Context, _ time.Duration, _ int) (int64, error) { return 0, nil }`.

### Changed

- **Breaking:** `outbox.NewRunner` now returns `(*Runner, error)` instead of `*Runner` — returns an error when `ClaimLeaseDuration` is too short for the configured `BatchSize × PublishTimeout`; panics are reserved for nil `Publisher` / nil `Store+Pool` (programming errors). Update all call sites: `runner, err := outbox.NewRunner(cfg)`.
- `ClaimLeaseDuration` validation: when `PublishTimeout` is disabled (≤0), a minimum floor of 30 s is enforced so leases never expire instantly, preventing duplicate delivery across concurrent runners
- Panicking SQS handlers now record the panic as an OTel span error (via `span.RecordError` + `span.SetStatus(codes.Error)`) before re-panicking, giving end-to-end trace visibility even when handlers crash
- `maskDSN` (internal) now also masks `password=` / `passwd=` values that appear in URL query parameters (e.g. `postgres://host/db?password=secret`), preventing credential leaks in `OutboxConfigEnv.String()` log output
- `publishClaimedSequential` (sequential outbox batch path): per-failure `Code == "TransportError"` in a `domain.BatchError` now uses `threshold = MaxAttempts+1`, matching the concurrent single-record path — SNS transport failures (ThrottlingException, ServiceUnavailable) no longer consume a retry slot and will not prematurely dead-letter healthy records
- `outboxstore.MarkPublished` and `outboxstore.MarkFailed` now apply a 5 s per-call DB timeout (`defaultStoreQueryTimeout`), consistent with `ClaimBatch`, `PendingCount`, and `LeasedCount` — a saturated DB can no longer stall the entire batch bookkeeping loop
- `outboxstore.ReprocessDeadLetters` now applies a 30 s internal DB timeout (`defaultPruneTimeout`) so a saturated DB does not block the caller indefinitely
- `WithDrainTimeout(0)` is now documented: a zero drain timeout disables the drain window — in-flight handler contexts are cancelled immediately when `Stop` is called. Negative values are rejected (default 30 s preserved). Use zero only in tests
- `bridge.PublishBatch`: `RecordOutboxAttempt` and `RecordOutboxPublished` are now recorded post-call so a panicking publisher leaves both counters at 0 rather than creating a permanent `attempts > published{success+error}` mismatch in dashboards
- `events.SystemTenantID` re-exported from `internal/core/domain` — single canonical definition
- Reference CLI (`cmd/platform-events`) calls `events.Init`, logs config `Warnings`, and prints production wiring reminders; `-strict` exits non-zero on missing required env vars
- `internal/config` is a deprecated alias of `pkg/config` — import `pkg/config` in new code
- `logger.NewLogger` default encoder corrected: `"dev"`, `"development"`, and `"local"` use a colored console encoder; all other values (including `"staging"`, unknown, empty) use a JSON production encoder. Previously any env value other than `"production"`/`"prod"` triggered dev mode, causing staging deployments to emit colored console output.
- SQS consumer receive-error backoff now includes ±25% random jitter so concurrent consumer replicas do not retry in lock-step after a shared SQS error.
- Handler `ctx` uses `context.WithoutCancel` — `ctx.Deadline()` always returns a zero time. Handlers must not rely on the parent deadline for timeouts; use `context.WithTimeout` explicitly instead. Cancellation is only delivered once the consumer's drain timeout expires.

### Fixed

- `CHANGELOG` claim-lease default corrected: `OUTBOX_CLAIM_LEASE_DURATION` unset → **10 minutes** (store default), not 30s
- `.env-example` documents `OUTBOX_CLAIM_LEASE_DURATION`, `OUTBOX_STARTUP_JITTER`, runner tuning vars, and `RawMessageDelivery` requirement
- `outbox_service.truncateError`: truncation marker (`"…[truncated]"`, 12 runes) is now counted within the `maxLastErrorLen` cap rather than appended after it, preventing error strings of up to `maxLastErrorLen+12` runes from reaching the database column.
- `metrics.Init` now acquires `metricsMu` inside the `sync.Once` callback, closing a race window with concurrent `InitWithRegisterer` calls.
- `outboxstore.MarkPublished` doc comment corrected: returns `nil` (not an error) when 0 rows are affected and logs a `WARN` instead — the concurrent-update case (record published/removed by another runner between claim and mark) is an expected non-error condition.
- `publishChunk` (SNS adapter sequential batch path): transport-level errors are now wrapped with `wrapIfRetryable` before returning so the outbox service's `failureThreshold` correctly identifies retryable SNS errors (ThrottlingException, ServiceUnavailable) and uses `MaxAttempts+1`

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
