# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Repo Is

`platform-events` is a **private Go shared library** (module: `github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events`, `go 1.26.0` + `toolchain go1.26.8`) that provides reusable SNS publisher and SQS consumer primitives for platform services. It lives in a **private GitHub repository** and is consumed as a Go module dependency by internal platform services — it is never deployed as a standalone server.

Core capabilities:
- `Publisher` interface + AWS SNS implementation
- `Consumer` interface + AWS SQS implementation (long-poll loop, parallel dispatch)
- **Outbox runner** — transactional outbox pattern over a Postgres table; guarantees at-least-once delivery without 2PC
- **Inbox** (`pkg/inbox`) — consumer-side dedup: `processed_events` ledger (`ApplySchema`, `Store`, `Handler` wrapper, `Prune`), metric `events_inbox_duplicates_total`. `Store.Process(ctx, env, fn(ctx, tx))` claims the ID inside the handler's transaction for exactly-once Postgres writes; `Handler` is best-effort (separate transactions). Neither records a message the handler dead-lettered, so DLQ redrives are processed
- **`GlueDecodeCodec`** — decode-only codec stripping the Glue Schema Registry wire header
- **`DLQPublisher`** — forwards failed messages to the source queue's `RedrivePolicy` DLQ (`SendToDLQ`, `ResolveDLQ`); the only sanctioned SQS path for consumer services (they must not import the SQS SDK). Typed `*DLQError` + `ErrRetryable`; `mock.DLQPublisher` (validates input like the real one); metrics `platform_dlq_messages_total` (counted once via `port.DLQAttribution`) + `platform_dependency_request_seconds{dependency="sqs"}` (legacy `events_dlq_forwarded_total`); span `sqs.dlq_forward`. Consumer option `WithDLQForwarding(dlq)` forwards malformed / over-`WithMaxReceiveCount` / decode-poison messages automatically (raw body + attributes); handlers get the raw message via `events.SourceMessageFromContext(ctx)` — never forward `env.JSON()`
- **Event-envelope types** — versioned, typed `Envelope[T]` carrying metadata (event ID, type, source, tenant, trace ID, timestamp) plus JSON-serialised payload
- **HMAC helpers** — SHA-256 HMAC signing and verification for webhook and cross-service event authentication
- **Retry classification** — transient publish failures (throttling, SNS 5xx/429, timeouts, network, a codec wrapping `ErrRetryable`) never count toward the outbox's `MaxAttempts`; permanent ones back off per record (`RetryBackoff`) and dead-letter
- **Per-key outbox ordering (opt-in)** — `outbox.EnqueueOrdered(ctx, tx, env, key)` publishes a key's records one at a time in enqueue order (migration 010)
- **Observability** — Tier 1 `platform_*` metrics per the Enterprise Platform Observability Standard (`events.InitMetrics`), reference alert rules, dashboard and KEDA example in `monitoring/`
- **Design reference** — the low-level design is `docs/lld/platform-events-lld.md`

**Consuming services** must set `GOPRIVATE=github.com/BCBP-SOLUTIONS-FZC-LLC/*` (or `GONOSUMDB`/`GOFLAGS` equivalents) to fetch private module versions.

### Shared Library Dependencies

This library builds on two other BCBP platform libraries:

| Library | Module | Role in platform-events |
|---------|--------|--------------------------|
| [`platform-gincommon`](https://github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon) | `github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon` | OTel tracing initialisation (`InitTracingFromEnv`, `EnsureTracing`); `port.Logger` interface (compatible — gincommon's `ZapLogger` can be injected directly); `RequestContext` carries `TraceID` / `TenantID` / `UserID` that populate `Envelope` fields |
| [`platform-pgcommon`](https://github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon) | `github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon` | Connection pool (`pgcommon.Pool`) used by the outbox store; `pgcommon.RunInTx` composes business logic + `outbox.Enqueue` atomically; `migrate.Runner` applies the outbox schema (`outbox_events`, `outbox_dead_letters`); `SlowQueryTracer` surfaces slow outbox queries |

See [`ARCHITECTURE.md`](../ARCHITECTURE.md) for detailed flow diagrams and invariant tables, and [`docs/lld/platform-events-lld.md`](../docs/lld/platform-events-lld.md) for the low-level design (data model, API contract, flows, retry classification, configuration). Keep the LLD's revision history and §16 "Deployment stage" current when behaviour or release state changes.

## Common Commands

```bash
make setup           # Copy .env-example → .env + install .githooks/pre-commit (run once before anything else)
make install-hooks   # Re-install .githooks/pre-commit (tidy + drift check, fmt-check, lint)
make tidy            # go mod tidy
make fmt             # go fmt ./...
make vet             # go vet ./...
make lint            # golangci-lint
make metrics-lint    # Observability standard gate (metric conformance, rule files, inventory drift)
make metrics-doc     # Regenerate docs/observability/metrics-registry.md from the registry
make rules-check     # promtool check + alert unit tests for monitoring/prometheus (Docker)
make docs-check      # every docs/architecture/mermaid/*.mmd embedded verbatim in ARCHITECTURE.md
make test            # All tests (unit + integration), excludes smoke
make test-ci         # All tests with race detector (used in CI)
make test-unit       # Unit tests only
make test-int        # Integration tests (floci + Postgres via testcontainers)
make test-smoke      # Smoke tests (requires live AWS resources at SNS_TOPIC_ARN / SQS_QUEUE_URL)
make race            # All tests with -race flag
make build           # Compile reference CLI to bin/platform-events
make cover           # Coverage HTML report (measures ./internal/... ./pkg/...)
make cover-func      # Coverage summary by function (terminal)
make ci              # tidy + mod-verify + fmt-check + vet + lint + docs-check + metrics-lint + rules-check + dashboards-check + test-ci + build (the same gates as CI)
make docker-up       # Start floci (SNS/SQS, :4574) + floci-ui (http://localhost:4505) + Postgres (:5538); demo topology via scripts/init-floci.sh
make docker-down     # Stop the local containers
make clean           # Remove bin/ artefacts
```

To run a single test:
```bash
cd test   # the suites are their own module
go test ./unit/envelope/...   -run TestEnvelopeSign -v
go test ./integration/...     -tags=integration -run TestSNSPublishRoundTrip -v
```

**Module layout:** Three Go modules, the same layout as platform-pgcommon, so consuming services inherit only what the library itself imports: `.` (the library), `test/` (every suite and fixture; `replace …platform-events => ../`) and `tools/` (golangci-lint, run through `go tool -modfile=tools/go.mod`). The library's `go.mod` went from 289 to 55 lines, and testcontainers and the linter's dependency tree are gone from it. White-box tests beside the sources (`internal/core/service/*_test.go`) stay in the root module. `make tidy`, `vet`, `lint`, `mod-verify` and every `test-*` target cover all three modules.

**Coverage note:** tests live in the separate `test/` module. Always use `-coverpkg=./internal/...,./pkg/...` to get meaningful numbers; running `go test ./...` without it shows 0% for source packages. `make cover` and `make cover-func` handle this correctly.

**Testcontainers note:** each test package shares one floci container (topics/queues deleted on test cleanup) and one Postgres container (a fresh database per `NewTestDB`, dropped on cleanup) — torn down by the package's `TestMain`. Integration tests spin up floci (`floci/floci:2.1.0` — open-source, always-free AWS emulator, the platform's LocalStack replacement, same as iam-org-membership; fixture `test/fixtures/floci.go`) and Postgres via `testcontainers-go`. Docker must be running locally. Pass `-short` to skip integration tests without a Docker daemon.

## Architecture

The library follows **Clean Architecture** — dependencies point inward; outer layers depend on inner layers, never the reverse.

```
pkg/                       ← public API surface (consumers import these)
  events/                  ← Envelope, Publisher/Consumer + options, DLQPublisher, Codec (+ GlueDecodeCodec),
                             HMAC helpers, InitMetrics / MetricsIdentity / MetricsRegistry
    mock/                  ← mock.Publisher, mock.Consumer, mock.DLQPublisher (production-faithful)
  outbox/                  ← Runner (NewRunner/Config), Enqueue, EnqueueOrdered, ApplySchema, DLQ management
    migrations/            ← embedded 001–010 (outbox_events, outbox_dead_letters, ordering_key/ordering_seq)
  inbox/                   ← processed_events ledger: Store (Process, IsProcessed, MarkProcessed, Prune), Handler
    migrations/            ← embedded 001 (processed_events)
  config/                  ← env loading (LoadSNS/LoadSQS/LoadOutbox), RunnerConfigFromEnv, SQSConsumerOptions
internal/
  core/
    domain/                ← Envelope, OutboxRecord (OrderingKey), BatchError/BatchFailure, DLQ errors, ErrRetryable
    port/                  ← OutboxStore, Publisher, Consumer, Logger, Clock, Codec, DLQPublisher,
                             DLQAttribution, SourceMessage (owned by the use-case layer)
    service/               ← OutboxService (publish cycle, retry/backoff classification), HMACService
  adapter/outbound/
    sns/                   ← SNS publisher (batching by count + 256 KiB, failure classification)
    sqs/                   ← SQS consumer (consumer.go), DLQPublisher (dlq.go), queue-depth sampler
    outboxstore/           ← Postgres outbox store (platform-pgcommon Pool + RunInTx only)
    metrics/               ← metrics registry (registry.go), identity, Tier 1 + legacy collectors
cmd/platform-events/       ← reference CLI (prints resolved config; Docker image for Trivy/smoke)
monitoring/                ← prometheus rules (+ promtool tests), grafana dashboard, KEDA example
docs/                      ← guides/, observability/ (README, runbook, generated metrics-registry),
                             architecture/mermaid/, lld/platform-events-lld.md
test/                      ← separate Go module (replace … => ../)
  unit/                    ← isolated unit tests per package
  integration/             ← floci + Postgres containers (build tag integration)
  e2e/                     ← end-to-end publish → consume, outbox (build tag e2e)
  smoke/                   ← live AWS (build tag smoke; make test-smoke)
  fixtures/                ← shared floci/Postgres containers, MockLogger, FakeClock
  testenv/                 ← loads .env-example for tests
tools/                     ← separate Go module: golangci-lint via `go tool -modfile=tools/go.mod`

External dependencies (private modules):
  platform-pgcommon v1.4.2 → Pool, RunInTx, ConfigFromEnv, migrate.Runner, Tx/Conn/Rows aliases
  platform-gincommon       → not imported: port.Logger matches its ZapLogger; tracing initialised by the service
```

**Dependency rule:** `domain` ← `port` ← `service` ← `adapter` ← `pkg`. The `core/` layers never import `adapter/` or AWS SDK packages. Interfaces in `core/port/` are implemented in `adapter/outbound/` and injected inward.

### Public API (`pkg/`)

**`pkg/events`** — what consuming services import:

- **Envelope**
  - `Envelope[T any]` — typed event wrapper: `ID`, `Type`, `Source`, `TenantID`, `TraceID`, `Timestamp`, `Payload T`
  - `NewEnvelope[T](eventType, source string, payload T, opts ...EnvelopeOpt) Envelope[T]` — generates `ID` (UUID v7), sets `Timestamp` to `time.Now()`. Options: `WithTenantID`, `WithTraceID`, `WithCorrelationID`, `WithSystemTenant`, `WithSchemaVersion`, `WithSubject`, `WithActor`, `WithSchemaID`. When publishing from an HTTP handler, pass `WithTenantID(rc.TenantID)`, `WithTraceID(rc.TraceID)`, and `WithSchemaVersion("1")` where `rc` is the `gincommon.RequestContext` extracted via `gincommon.GetRequestContext(c)`. Pass `WithSubject` to identify the resource the event is about (e.g. `"users/<id>"`); pass `WithActor` to record who triggered it.
  - `Envelope.JSON() ([]byte, error)` — canonical JSON serialisation (payload marshalled inline)
  - `ParseEnvelope[T](data []byte) (Envelope[T], error)` — deserialise and validate required fields

- **Publisher**
  - `Publisher` interface — `Publish(ctx, Envelope[json.RawMessage]) error`; `PublishBatch(ctx, []Envelope[json.RawMessage]) error`
  - `NewSNSPublisher(cfg SNSConfig, opts ...PublisherOption) (Publisher, error)` — constructs the SNS implementation; returns an error if `TopicARN` is empty (prevents invalid label cardinality in metrics)
  - `SNSConfig{TopicARN, Region, EndpointURL, Logger}` — `TopicARN` required. `Logger` accepts any `port.Logger` implementation — pass the `ZapLogger` from `platform-gincommon` directly.
  - `PublisherOption` — `WithMessageGroupID(fn)`, `WithMessageDeduplicationID(fn)` (FIFO topics), `WithAttributes(map)`, `WithCodec(codec)` (schema-registry hook — see "Codec" below)
  - `MockPublisher` (in `pkg/events/mock`) — in-memory, thread-safe; `Published() []Envelope[json.RawMessage]`

- **Consumer**
  - `Consumer` interface — `Start(ctx) error`; `Stop() error`
  - `NewSQSConsumer(cfg SQSConfig, handler Handler, opts ...ConsumerOption) (Consumer, error)` — constructs the SQS long-poll loop
  - `SQSConfig{QueueURL, Region, EndpointURL, MaxMessages, WaitSeconds, Logger}` — `QueueURL` required; `MaxMessages` default 10; `WaitSeconds` default 20. `Logger` accepts any `port.Logger` — pass `platform-gincommon`'s `ZapLogger` directly.
  - `Handler` — `func(ctx context.Context, env Envelope[json.RawMessage]) error`; returning a non-nil error skips deletion (message becomes visible again after visibility timeout). The `ctx` passed to each handler has a `platform-gincommon`-compatible `RequestContext` injected (populated from `env.TenantID`, `env.TraceID`) so downstream calls to pgcommon pool helpers (e.g. `pool.WithTx`) pick up the correct GUC values automatically.
  - `ConsumerOption` — `WithConcurrency(n)` (default 1), `WithVisibilityTimeout(d)`, `WithDeadLetterHandler(fn)` (nil ignored), `WithMaxReceiveCount(n)`, `WithDrainTimeout(d)`, `WithConsumerCodec(codec)` (schema-registry hook — see "Codec" below), `WithDLQForwarding(dlq)`, `WithHandlerTimeout(d)`, `WithQueueDepthMetrics(interval)`, `WithMalformedBodyLogging()`. `NewSQSConsumer` returns an error for a nil handler.
  - `MockConsumer` (in `pkg/events/mock`) — in-memory queue; `Inject(env)` delivers messages synchronously

- **HMAC helpers**
  - `Sign(key []byte, payload []byte) (string, error)` — returns hex-encoded HMAC-SHA256 signature; `ErrKeyTooShort` for keys < 32 bytes
  - `Verify(key []byte, payload []byte, sig string) bool` — constant-time comparison; returns false on any parse/length error rather than panicking
  - `SignEnvelope(key []byte, env Envelope[json.RawMessage]) (string, error)` — signs canonical JSON of envelope
  - `VerifyEnvelope(key []byte, env Envelope[json.RawMessage], sig string) (bool, error)` — deserialises and verifies; safe for webhook receipt handlers

**`pkg/outbox`** — transactional outbox:

- `NewRunner(outbox.Config{Pool, Publisher, Logger, PollInterval, BatchSize, MaxAttempts, ClaimLeaseDuration, PublishConcurrency, PublishTimeout, DrainTimeout, StartupJitter, RetryBackoff, MaxRetryBackoff, GaugeInterval}) (*Runner, error)` — configure the outbox runner (`Runner`'s fields are unexported). `Pool` (a `*pgcommon.Pool` from `platform-pgcommon`) and `Publisher` are **required**; `config.RunnerConfigFromEnv` maps the `OUTBOX_*` env vars onto it.
- `Runner.Start(ctx) error` — start the polling loop; blocks until `ctx` is cancelled.
- `Runner.Stop() error` — graceful drain; waits for in-flight batch to complete before returning.
- `Runner.PrunePublished(ctx, olderThan time.Duration, limit int) (int64, error)` — deletes published records older than `olderThan` from `outbox_events` (up to `limit` rows per call) to prevent unbounded table growth. Call periodically from a scheduled job. Applies a 30 s internal DB timeout.
- `Runner.ListDeadLetters(ctx, filter DLQFilter, limit int) ([]DeadLetterRecord, error)` — returns up to `limit` dead-letter records matching filter (by `EventType`, `TenantID`, `FailedBefore`), oldest first; safe to call repeatedly as an inspection step; applies a 30 s internal DB timeout.
- `Runner.ReprocessDeadLetters(ctx, limit int) (int, error)` — moves up to `limit` records from `outbox_dead_letters` back to `outbox_events` for retry; applies a 30 s internal DB timeout.
- `Runner.ReprocessDeadLettersWith(ctx, filter DLQFilter, limit int) (int, error)` — same as `ReprocessDeadLetters` but filters by `DLQFilter`; use for targeted replay without touching unrelated failures.
- `Runner.DiscardDeadLetters(ctx, filter DLQFilter, limit int) (int64, error)` — permanently deletes up to `limit` matching dead-letter records; always call `ListDeadLetters` first to confirm selection; applies a 30 s internal DB timeout.
- `Enqueue(ctx, tx pgcommon.Tx, env Envelope[json.RawMessage]) error` — insert a serialised envelope into the `outbox_events` table within the caller's transaction. No publish happens at insert time — the runner delivers asynchronously. Callers should pass the `pgcommon.Tx` obtained from `pgcommon.RunInTx` so the enqueue and the business-logic write commit or roll back as a single unit.
- `ApplySchema(ctx, runner *migrate.Runner) error` — convenience wrapper that calls `platform-pgcommon`'s `migrate.Runner` to apply the embedded `pkg/outbox/migrations/` SQL files (`001`–`010`). Call once at service startup before `Runner.Start`.
- Schema: `outbox_events(id UUID PK, event_type TEXT, payload JSONB, tenant_id TEXT, trace_id TEXT, attempts INT DEFAULT 0, last_error TEXT, created_at TIMESTAMPTZ, scheduled_at TIMESTAMPTZ, published_at TIMESTAMPTZ)`

**Typical wiring with platform-pgcommon:**
```go
// 0. Logger and tracing are owned by the service (platform-gincommon), never
//    by this library: gincommon.InitTracingFromEnv() reads OTEL_*, and its
//    ZapLogger satisfies port.Logger — pass it to every Config.Logger below.
logger := newServiceLogger() // e.g. platform-gincommon's ZapLogger; call Sync() on shutdown

// 1. Wire OS signals so SIGTERM/SIGINT cancel ctx (graceful shutdown).
ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
defer stop()

// 2. Apply the outbox schema via pgcommon's migrate runner (or run this as a
//    separate migration job — see ApplySchema's rollback note).
// migrate.Runner.Logger is pgcommon's domain.Logger (variadic Field
// arguments), not port.Logger — pass a pgcommon-compatible logger or omit it.
migrateRunner := &migrate.Runner{DSN: pgcommon.MigrationDSNFromEnv()}
if err := outbox.ApplySchema(ctx, migrateRunner); err != nil {
    log.Fatal(err)
}

// 3. Load config and log any warnings via the structured logger.
outboxEnv := config.LoadOutbox()
config.LogWarningsTo(logger, outboxEnv.Warnings) // falls back to stderr when logger is nil

// 4. Construct the runner on a pgcommon pool. DB config is owned by
//    platform-pgcommon: outboxEnv.DB = pgcommon.ConfigFromEnv().
pool, err := pgcommon.NewPool(ctx, outboxEnv.DB)
if err != nil {
    log.Fatal(err)
}
runner, err := outbox.NewRunner(config.RunnerConfigFromEnv(outboxEnv, pool, snsPublisher, logger))
if err != nil {
    log.Fatal(err) // e.g. a ClaimLeaseDuration too short for BatchSize × PublishTimeout
}
go func() { _ = runner.Start(ctx) }()

// 5. While serving: enqueue inside the business transaction (pgcommon.RunInTx).
err = pgcommon.RunInTx(ctx, pool, pgcommon.TxOptions{}, func(ctx context.Context, tx pgcommon.Tx) error {
    if err := repo.SaveUser(ctx, tx, user); err != nil { // business write
        return err
    }
    return outbox.Enqueue(ctx, tx, envelope) // event write — same transaction
})

// 6. On termination, drain the in-flight batch, then close the pool.
<-ctx.Done()
_ = runner.Stop()
pool.Close()
```

### Event Envelope Design

`Envelope[T]` is the canonical wire format for all inter-service events:

```json
{
  "id":             "01926e4f-...",     // UUID v7 — sortable, unique per event
  "type":           "iam.user.created", // <domain>.<entity>.<past-tense-verb>[.v<N>]
  "source":         "platform-iam",    // emitting service name
  "specversion":    "1",               // SchemaVersion — optional; omitted = treat as "1"
  "tenant_id":      "acme",
  "trace_id":       "4bf92f3577...",   // OTel trace ID (hex, 32 chars) or empty
  "correlation_id": "...",             // optional: ties events in a saga/workflow
  "subject":        "users/01926e4f-...", // optional: resource the event is about; also an SNS filter attribute
  "actor":          "admin@acme.com",  // optional: identity that caused the event (audit trail)
  "dataschema":     "550e8400-...",    // SchemaID — optional: Glue Schema Registry UUID, distinct from specversion
  "time":           "2026-05-27T...",  // Timestamp — RFC3339Nano, UTC
  "data":           { ... }            // Payload — typed T, inlined (not base64)
}
```

The JSON keys follow CloudEvents naming (`specversion`, `dataschema`, `time`, `data`); the Go fields keep their descriptive names (`SchemaVersion`, `SchemaID`, `Timestamp`, `Payload`). `ip_address` / `user_agent` are optional audit fields.

`event_type` convention: `<domain>.<entity>.<past-tense-verb>[.v<N>]` (e.g. `iam.user.created`, `billing.invoice.settled`). The `.v<N>` suffix only appears for breaking payload changes — v1 is implicit (no suffix). Consumers match on prefix with `strings.HasPrefix` or exact equality — no glob or regex routing in the base library.

`schema_version` field: set via `WithSchemaVersion("1")` on every new event type at inception. **This is a semantic consumer-branching version — always a small integer string (`"1"`, `"2"`).** Do NOT put a Glue Schema Registry UUID here; use `WithSchemaID` for that instead (see below).

`schema_id` field: set via `WithSchemaID(schemaVersionID)` where `schemaVersionID` is the UUID returned by the Glue codec's `Encode` call. This is the authoritative registry pointer the codec uses for Avro/JSON deserialization. It is distinct from `schema_version` and must never be confused with it. The Glue UUID is also embedded in the encoded payload's 18-byte wire-format header, so `schema_id` in the envelope is for observability and traceability only. Increment on additive-only field additions. For breaking changes, mint a new event type (`.v2`) and reset `schema_version` back to `"1"`. See `EVENT_SCHEMA_GOVERNANCE.md` for full rules, migration window pattern, and the cross-service event type registry. As of the `Codec`/`WithCodec` addition (see "Codec" below), `schema_id` is set automatically by the SNS publisher when a codec is configured — manual `WithSchemaID` calls are unnecessary (and will be overwritten) once a codec is wired in.

### SNS Publisher

`NewSNSPublisher` wraps `aws-sdk-go-v2/service/sns`. Key behaviours:

- **Message attributes** — `EventType`, `TenantID`, `Source`, `EventID`, and `Subject` (when non-empty) are set as SNS message attributes to enable SQS subscription filter policies without deserialising the body. `Actor` is not forwarded as an attribute — it is an audit-trail field, not a routing field.
- **FIFO topics** — if `TopicARN` ends in `.fifo`, the publisher requires `MessageGroupID`; `MessageDeduplicationID` defaults to `Envelope.ID` (content-based deduplication must be disabled at the topic level).
- **Batching** — `PublishBatch` uses `sns:PublishBatch` (max 10 per call); batches larger than 10 are automatically split.
- **Retry** — caller is responsible for retry (the outbox runner handles this); the SNS adapter does not retry internally. `Publish` returns the AWS error, wrapped in `RetryableError` (`errors.Is(err, events.ErrRetryable)`) when transient: SNS throttling / internal codes (`Throttled`, `InternalError`, `KMSThrottling`, …) and any failure without an AWS API error (network, DNS, TLS, timeouts, credentials). `errors.As(err, &smithy.APIError)` still reaches the underlying error. `PublishBatch` failures carry `BatchFailure.Retryable` (and Code `TransportError` for a transient whole-request failure; a permanent one keeps its AWS code). Chunks are split by count (10) **and** by SNS's 256 KiB request size.
- **Outbox retry classification** — retryable failures release the record without counting an attempt (shared backoff); everything else counts toward `MaxAttempts` and dead-letters. A custom `events.Publisher` should set `BatchFailure.Retryable` (or wrap single-publish errors with `ErrRetryable`) only for failures that say nothing about the message.
- **OTel** — each `Publish` call creates a child span `sns.publish` with attributes `messaging.system=aws_sns`, `messaging.destination`, `messaging.message_id`. OTel must be initialised by the consuming service before publishing; call `gincommon.InitTracingFromEnv()` (from `platform-gincommon`) at startup — `platform-events` calls `otel.Tracer(...)` and will produce no-op spans if the provider is not yet set.

### SQS Consumer

`NewSQSConsumer` runs a long-poll loop with configurable concurrency:

```
Start() →
  loop:
    ReceiveMessage (WaitSeconds, MaxMessages)
    for each message:
      go handler(ctx, envelope)   # bounded by semaphore (WithConcurrency)
        if err == nil → DeleteMessage
        if err != nil → log warn, increment retry metric, leave visible
    back to top
```

- **Batch receive, extended from receipt** — each `ReceiveMessage` takes up to `MaxMessages`; every received message's visibility is extended from receipt (while it waits for a worker, then while processed), so a batch queued behind slow handlers never reappears and is processed twice. Without a visibility timeout (no extension) a receive asks only for `min(MaxMessages, free workers)`. With `WithHandlerTimeout`, a message still waiting when the timeout passes is handed back (visibility 0); `Stop` hands back undispatched messages at once.
- **Envelope validation** — a body that is not JSON, or JSON without `id` / `type` / `source` (e.g. an SNS notification wrapper from a subscription without `RawMessageDelivery`), is malformed: forwarded to the DLQ with `WithDLQForwarding`, else deleted; never passed to the handler. Logged with `body_bytes` / `body_sha256` only (`WithMalformedBodyLogging` adds an excerpt — bodies may carry PII).
- **Visibility extension** — from receipt until dispatch ends (decode, dead-letter handler, DLQ forward, handler), the consumer calls `ChangeMessageVisibility` every `max(VisibilityTimeout/2, 1s)`. `WithHandlerTimeout(d)` (`SQS_HANDLER_TIMEOUT`) is one deadline from when a worker picks the message up, for decode, dead-letter handler and handler contexts and for the extension, so a hung handler's message is redelivered.
- **Dead-letter handler** — with `WithDeadLetterHandler` and/or `WithDLQForwarding`, a message whose `ApproximateReceiveCount` exceeds `WithMaxReceiveCount` (default 5; keep it below the queue's RedrivePolicy `maxReceiveCount`) goes to the handler, then — unless the handler already forwarded it with `SendToDLQ` — is forwarded to the DLQ, and is deleted once both succeed. Without either option there is no consumer-side threshold: SQS's own redrive policy moves the message.
- **Graceful shutdown** — `Stop()` cancels the receive loop, waits for all in-flight handlers to complete (up to `DrainTimeout`, default 30 s), then returns.
- **OTel** — each message dispatch creates a child span `sqs.receive` with `messaging.system=aws_sqs`, `messaging.destination`, `messaging.message_id`, `messaging.operation=process`. The span starts a new trace with a span **link** to the producer span, extracted from the W3C `traceparent` message attribute the SNS publisher injects (baggage is propagated into the handler ctx too), giving end-to-end visibility across the SNS/SQS boundary in Tempo/Grafana. OTel must be initialised by the consuming service via `gincommon.InitTracingFromEnv()` before starting the consumer.
- **RLS GUC propagation** — the handler `ctx` has `env.TenantID` and `env.TraceID` injected so that `pgcommon.Pool` GUC injection (via `platform-pgcommon`'s `RLSMiddleware` / `GUCProvider`) correctly scopes all DB queries inside the handler to the event's tenant without any extra wiring by the caller.

### Codec (optional schema-registry hook)

`events.Codec` (aliased from `port.Codec`) is a pluggable hook for encoding/decoding an envelope's JSON `Payload` into a schema-registry-specific wire format (e.g. AWS Glue Schema Registry). **`platform-events` ships no concrete implementation and adds no schema-registry SDK dependency** — a consuming service implements `Codec` against its own registry client and injects it via `WithCodec` (publisher) / `WithConsumerCodec` (consumer), the same pattern used for `port.Logger`.

```go
type Codec interface {
    Encode(ctx context.Context, eventType string, payload json.RawMessage) (encoded []byte, schemaID string, err error)
    Decode(ctx context.Context, schemaID string, encoded []byte) (payload json.RawMessage, err error)
}
```

- **Wire format** — `Encode`'s output bytes are base64-encoded and marshalled as a JSON *string*, then substituted into `Envelope.Payload`; `SchemaID` is set to the returned `schemaID`. This keeps the envelope always-valid JSON (required for SNS's UTF-8-only `Message` field) regardless of the codec's native binary format.
- **`SchemaID` is the decode signal** — empty means `Payload` is already plain JSON (legacy producer, `NoopCodec`, or `WithCodec` never configured on the publisher); the SQS consumer skips `Decode` entirely in that case. Non-empty means `Decode` runs before the message reaches the handler (and before `WithDeadLetterHandler` routing).
- **Decode failures are treated like handler errors, not malformed JSON** — a schema-registry outage can be transient, so a decode failure leaves the message visible for SQS's own `MaxReceiveCount`/redrive-policy retry rather than deleting it immediately.
- **No outbox involvement** — encoding happens transiently inside `Publish`/`PublishBatch`; `pkg/outbox` always stores the canonical plain-JSON envelope and is unaffected by `WithCodec`.
- `NoopCodec` (in `pkg/events`, aliased from `port.NoopCodec`) is the identity reference implementation — `Encode` returns the payload unchanged with an empty `schemaID`.

### Outbox Runner

The outbox pattern eliminates the dual-write problem: services write the event *inside their business transaction* (via `outbox.Enqueue`), and the runner publishes asynchronously with at-least-once delivery.

**Poll cycle:**
1. `SELECT ... FOR UPDATE SKIP LOCKED` — claim up to `BatchSize` unpublished records with no published_at.
2. For each record: call `Publisher.Publish`; on success set `published_at = NOW()`.
3. On a permanent failure: increment `attempts`; set `last_error`; retry after `RetryBackoff·2^(attempts-1)` (default 1s, capped at `MaxRetryBackoff` 5m, jittered); if `attempts >= MaxAttempts` move to dead-letter table (`outbox_dead_letters`). Transient failures (transport, throttling, timeouts) and shutdown use `ReleaseLease` — no attempt counted; transient ones back off on one shared schedule that resets on the next successful publish, so an SNS outage builds a backlog instead of dead-lettering it.
4. Commit; sleep `PollInterval` (default `5s`).

**Ordering** — none by default, not even per aggregate on a FIFO topic: a failed / backed-off record is published after later records and replicas publish concurrently. Opt-in per record: `outbox.EnqueueOrdered(ctx, tx, env, key)` (migration 010) — records of a key publish one at a time in INSERT order (`ordering_seq`, a sequence; callers take the aggregate row lock before enqueueing so insert order = commit order). A record behind an unpublished one of its key waits at `scheduled_at = 'infinity'` and is promoted after its head is marked published (best-effort, in a follow-up transaction) or inside the transaction that dead-letters it ( the `PromoteWaiting` sweep with the gauge refresh — every max(GaugeInterval, PollInterval) — covers a failed promotion and the enqueue-during-publish race); the claim query keeps a cheap NOT EXISTS guard. A failing head holds its key until published or dead-lettered (`platform_outbox_ordering_blocked_events`). The runner re-polls at once while batches publish, stopping on a batch that published nothing or hit a transient failure.

**Idempotency** — `Envelope.ID` (UUID v7) is forwarded as the SNS `MessageDeduplicationID` on FIFO topics and as a message attribute on standard topics. Consumers should use `Envelope.ID` as their idempotency key.

### HMAC Helpers

`Sign` / `Verify` operate on raw bytes. `SignEnvelope` / `VerifyEnvelope` serialise the envelope to canonical JSON before signing, ensuring field-ordering is deterministic (sorted keys via `encoding/json` + a stable marshaller).

`Verify` uses `hmac.Equal` (constant-time) — not `==`. Never replace with string comparison. `VerifyEnvelope` returns `(false, nil)` on signature mismatch and `(false, err)` on malformed input — callers must check both return values.

HMAC keys must be ≥ 32 bytes; `Sign` returns an error (not a panic) if the key is shorter.

### Metrics — Enterprise Platform Observability Standard

All metrics are **Tier 1 `platform_*`**. platform-events is a cross-domain platform library, so Tier 2 (`<domain>_*`) and Tier 3 (`<domain>_<service>_*`) belong to the services. This is the same model as platform-pgcommon's `platform_db_*`. Full docs: `docs/observability/README.md`. Generated inventory: `docs/observability/metrics-registry.md`.

- **Init.** `events.InitMetrics(events.MetricsIdentity{Domain, Service, Environment, Version}, registerer, ...events.MetricsOption) ([]RegistrationWarning, error)`.
  - Injects `domain`/`service`/`environment` as const labels (rule 8). An empty Environment falls back to `APP_ENV` → `ENVIRONMENT` → `dev`.
  - Registers the legacy metrics in parallel unless `events.WithoutLegacyMetrics()` is given.
  - Error = invalid identity or legacy registration failure (nothing changes).
  - Warning = a `platform_*` metric the registry refused (fail-soft, metric disabled). IAM services already own `platform_retry_total` / `platform_dependency_request_seconds` with other label sets.
  - Registerer wrappers that inject identity labels are handled; each label is applied once and the wrapper wins.
  - Helpers: `MetricsIdentityFromEnv`, `MetricsIdentityFromLabels` (gincommon interop), `MetricsEnvironmentFromEnv`, `MetricsRegistry()`.
- **Deprecated entry points.** `events.Init` / `InitWithRegisterer` register legacy metrics only (SA1019).
- **Registry = source of truth.** `internal/adapter/outbound/metrics/registry.go` holds tier, status (Canonical / Proposed / Deprecated), semantic definition, approved labels + values, cardinality, aggregation, `Supersedes` ↔ `SupersededBy`, and sunset. Change a metric → update the registry → `make metrics-doc`.
- **Canonical:** `platform_messages_received_total{queue}`, `platform_messages_processed_total{queue,event_type}`, `platform_messages_failed_total{queue,event_type,reason}`, `platform_retry_total{operation,event_type}`, `platform_dlq_messages_total{operation,event_type,reason}`.
- **Proposed** (shadow-emitted, never in alerts/SLO/HPA until ratified): `platform_queue_depth` / `platform_dlq_depth` (opt-in `WithQueueDepthMetrics` / `SQS_QUEUE_DEPTH_INTERVAL`; polls `sqs:GetQueueAttributes`, DLQ URL derived from the RedrivePolicy ARN; client capability checked by type assertion so `SQSClientAPI` is unchanged), `platform_duplicate_messages_total`, `platform_dependency_request_seconds{dependency,operation,outcome}`, `platform_event_propagation_seconds`, `platform_messages_published_total`, `platform_message_processing_duration_seconds`, `platform_outbox_{pending,leased}_events`, `platform_outbox_publish_attempts_total`, `platform_outbox_errors_total`, `platform_outbox_dead_letter_operations_total`, `platform_telemetry_label_overflow_total`, `platform_library_info`, `platform_messages_in_flight{queue}` (work in progress per replica), `platform_outbox_oldest_pending_age` (seconds; the delivery-stall signal — its alert is commented out until ratified), `platform_outbox_ordering_blocked_events` (ordered records waiting behind their key's head), `platform_message_timeouts_total{queue,event_type,operation}` (WithHandlerTimeout expiries by stage: decode / dead_letter_handler / handler — keeps the Canonical failed_total vocabulary unchanged).
- **Deprecated legacy:** `events_*`, `outbox_*`, `sqs_*`, `platform_events_build_info`. Still authoritative where the successor is Proposed.
- **Consumer semantics.**
  - Every delivery is received once and ends processed, failed (+ retry), or dead-lettered.
  - A dead-letter is counted **once**. `port.DLQAttribution` in the handler ctx carries the reason (`malformed` / `decode_error` / `max_receive_count` / `explicit`), and the DLQ publisher marks it recorded.
  - A handler that calls `SendToDLQ` and returns nil is not counted as processed.
- **Label rules.**
  - `queue` / `topic` = name, never URL/ARN (`metrics.QueueName` / `TopicName`).
  - `event_type` goes through `SanitizeEventType`: ≤200 distinct values per process (`WithEventTypeLimit`, then `__other__`), ≤128 bytes (`__oversized__`), empty → `unknown`, invalid UTF-8 repaired; replacements counted in `platform_telemetry_label_overflow_total`. Slots are first come, first served (consumed types come from message bodies), so services should pre-register their known types with `events.WithEventTypes(...)`.
  - Propagation is observed on the first receipt only (`ApproximateReceiveCount ≤ 1`). SQS `receive_message` latency includes long-poll wait; exclude it from latency views.
  - A leftover legacy `Init` after `InitMetrics` is a no-op; `InitWithRegisterer` is the test reset (clears Tier 1).
  - Prohibited: `tenant_id`, `event_id`, `user_id`, `email`, `request_id`, `session_id`, `message_id`, `trace_id`, `span_id`, `correlation_id`, `subject`, `actor`.
- **CI.**
  - `make metrics-lint` (in `Validate / Quality` and `make ci`) registers the real collectors and enforces tiers, naming (`_total` / `_seconds`), required labels, vocabulary and registry parity.
  - The same target also checks the rule files (no Proposed metric outside comments, labels in vocabulary, runbook anchors) and inventory drift.
  - `make rules-check` runs promtool on `monitoring/prometheus/platform-events.rules.yml` (+ `.test.yml`).
  - The same lint covers `monitoring/grafana/*.json` (registered metrics and labels; Proposed panels titled "(Proposed)", legacy ones "(legacy)") and `monitoring/kubernetes/*.yaml` (no scaling on Proposed metrics).
- **Adding a metric.** Add the registry entry (Proposed + full packet), register it in `registerPlatform`, and add a recording function that nil-checks `platform.Load()`. Exercise it in `test/unit/metrics/standard_test.go` `exerciseAll`, then run `make metrics-doc`.

### Key Configuration Defaults

| Variable | Default | Notes |
|----------|---------|-------|
| `AWS_REGION` | `us-east-1` | Applies to both SNS and SQS clients |
| `SNS_TOPIC_ARN` | — | Required for SNS publisher |
| `SQS_QUEUE_URL` | — | Required for SQS consumer |
| `SQS_MAX_MESSAGES` | `10` | 1–10; SQS hard limit |
| `SQS_WAIT_SECONDS` | `20` | Long-poll duration |
| `SQS_VISIBILITY_TIMEOUT` | `30s` | Parsed as `time.Duration` |
| `SQS_CONCURRENCY` | `1` | Parallel handler goroutines |
| `SQS_QUEUE_DEPTH_INTERVAL` | — (off) | Enables `platform_queue_depth` / `platform_dlq_depth` sampling (min 10s) |
| `SQS_HANDLER_TIMEOUT` | — (off) | `WithHandlerTimeout`: handler ctx deadline + stop extending visibility, so a hung handler's message is redelivered |
| `OUTBOX_GAUGE_INTERVAL` | `15s` | Backlog-gauge refresh interval; counts capped at `outboxstore.MaxCountedRows` (100k) |
| `OUTBOX_POLL_INTERVAL` | `5s` | Parsed as `time.Duration` |
| `OUTBOX_BATCH_SIZE` | `50` | Records per poll cycle |
| `OUTBOX_MAX_ATTEMPTS` | `5` | Before moving to dead-letter |
| `OUTBOX_RETRY_BACKOFF` / `OUTBOX_MAX_RETRY_BACKOFF` | `1s` / `5m` | Per-record retry delay base·2^(n-1), capped; also the shared transient-failure backoff |
| `APP_ENV` → `ENVIRONMENT` | `dev` | Metrics `environment` label when `MetricsIdentity.Environment` is empty (same precedence as platform-pgcommon) |
| `APP_NAME` | — | Metrics `service` via `events.MetricsIdentityFromEnv` |
| `OTEL_*` | — | **Not read by this library.** Read by platform-gincommon's `InitTracingFromEnv` in the consuming service; platform-events only uses the global tracer provider/propagator it installs. `config.LoadOTel` is deprecated (its parsing diverges from gincommon's). |

Logging has no env configuration here either: every component logs through the `port.Logger` the service injects (e.g. gincommon's `ZapLogger`), nil-safe.

## Key Environment Variables

Defined in `.env-example` (copy to `.env` via `make setup`):

```
APP_NAME, APP_ENV, BUILD_VERSION
AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY  # use instance role / IRSA in production
AWS_ENDPOINT_URL                                       # http://localhost:4574 for the local floci stack (make docker-up)
SNS_TOPIC_ARN
SQS_QUEUE_URL
SQS_MAX_MESSAGES, SQS_WAIT_SECONDS, SQS_VISIBILITY_TIMEOUT, SQS_CONCURRENCY
OUTBOX_POLL_INTERVAL, OUTBOX_BATCH_SIZE, OUTBOX_MAX_ATTEMPTS
DATABASE_URL                                           # outbox runner DSN — read by platform-pgcommon's ConfigFromEnv (or PG_HOST/PG_PORT/PG_USER/PG_PASSWORD/PG_DBNAME/PG_SSLMODE; plus PG_MAX_CONNS, PG_STATEMENT_TIMEOUT, PG_LOCK_TIMEOUT, PG_BOUNCER_MODE, …)
MIGRATION_DATABASE_URL                                 # optional DDL-role DSN for ApplySchema (pgcommon.MigrationDSNFromEnv)
# No OTEL_* / log variables: tracing and logging are configured by the consuming service (platform-gincommon).
SMOKE_SNS_TOPIC_ARN, SMOKE_SQS_QUEUE_URL               # for smoke tests against real AWS
```

## Test Layout

- `test/unit/` — isolated unit tests per package: clock, config, domain, enqueue, envelope, glue, hmac, inbox, metrics (incl. the `make metrics-lint` conformance tests and inventory drift), mock (production fidelity), outbox, port, publisher, runner, sns (incl. failure classification), sqs (consumer, DLQ publisher, queue depth, visibility / timeout / shutdown paths)
- `test/integration/` — floci (SNS round-trip, SQS consume loop, DLQ forwarding, codec) and Postgres (outbox store incl. per-key ordering and commit order, inbox `Store.Process`, error paths) — build tag `integration`
- `test/e2e/` — publish → consume and outbox runner end to end — build tag `e2e`
- `test/smoke/` — optional; requires live AWS resources (`SMOKE_SNS_TOPIC_ARN`, `SMOKE_SQS_QUEUE_URL`)
- `test/fixtures/` — shared floci and Postgres containers (one per package; fresh database per `NewTestDB`, `NewEmptyTestDB` for schema tests), `MockLogger`, `FakeClock`
- `test/testenv/` — loads `.env-example` for tests
- White-box tests stay beside the sources in the root module (`internal/core/service/*_test.go`).
- Merged coverage (root + unit + integration + e2e, `-race`) is **99.0%**; the CI gate is 97%.

## Key Design Decisions

**`Publisher` and `Consumer` are interfaces, not concrete types** — consumers of this library inject the interface, enabling `MockPublisher`/`MockConsumer` in unit tests without an AWS emulator (floci). SNS/SQS constructors return the interface, not a pointer to a struct.

**Envelope ID is UUID v7** — UUIDs v7 are time-ordered and collation-friendly in Postgres B-tree indexes. The outbox `SELECT ... ORDER BY id` scan is therefore an index scan, not a seq scan, as the table grows.

**`Verify` uses constant-time comparison** — `hmac.Equal` prevents timing attacks. Never replace with string equality. The function signature returns `bool` (not error) for the happy path so call sites read naturally: `if !events.Verify(...) { return ErrBadSig }`.

**Outbox uses `SKIP LOCKED`** — multiple outbox runner instances (e.g. in a horizontally scaled deployment) do not contend. Each runner claims its own batch atomically. No distributed lock is needed.

**SNS message attributes enable zero-body filtering** — SQS subscription filter policies match on `EventType` and `TenantID` message attributes. Consumers that only handle a subset of event types do not deserialise envelopes they will discard.

**Dead-letter is a Postgres table, not SQS DLQ** — the outbox dead-letter table (`outbox_dead_letters`) is queryable, retryable, and auditable from standard SQL tooling. SQS DLQs are recommended for consumption-side failures; the outbox handles publish-side failures.

**`Consumer` graceful drain on `Stop()`** — in-flight handlers are given `DrainTimeout` (default 30 s) to complete before `Stop()` returns; received-but-undispatched messages are handed back to the queue (visibility 0). This matches `net/http.Server.Shutdown` semantics and ensures clean pod termination in Kubernetes without losing partially-processed messages. `Runner.Stop()` likewise ends the re-poll loop and drains the in-flight batch.

**Nil-safe logger** — every component that accepts a `port.Logger` checks for `nil` before calling it. `SQSConsumer` with a nil logger runs silently. `OutboxRunner` with a nil logger skips per-record log lines but still updates metrics.

**Metrics identity validated, registration fail-soft** — `events.InitMetrics` returns an error (changing nothing) for an invalid `MetricsIdentity` (empty or malformed `domain` / `service` / `environment`), and a `RegistrationWarning` — not a failure — for a `platform_*` metric the registry refuses (e.g. one an IAM service already registered with other labels). The deprecated `metrics.Init` still panics on an empty service name.

**HMAC key length enforced at call time** — `Sign` returns `("", ErrKeyTooShort)` for keys < 32 bytes rather than silently using a weak key. Callers that ignore the error emit an empty signature, which `Verify` will reject (constant-time) — the system degrades safely.

**OTel trace context propagated via message attributes** — the SNS publisher injects the W3C `traceparent` (and baggage) as message attributes. The consumer links its `sqs.receive` span to that producer span (async consumers start a new trace with a link rather than a parent) and puts the baggage in the handler ctx. `Envelope.TraceID` is carried separately for log correlation and RLS-style context (`events.TraceIDFromContext`).

**`PublishBatch` splits automatically at 10 entries and 256 KiB** — SNS's hard limits per `PublishBatch` request. The adapter splits silently by count and by request size, so callers can pass arbitrarily-sized slices and one large event cannot fail its neighbours. Partial failures return a `BatchError` listing per-message errors with `Retryable` set for transient ones; successful messages within the same batch are not retried.

**Idempotent Prometheus registration** — `InitMetrics` reuses already-registered collectors (also through `prometheus.WrapRegistererWith`); the deprecated `Init` is guarded by `sync.Once` and is a no-op once `InitMetrics` ran; `InitWithRegisterer` is the test reset (clears the Tier 1 set).

**Retry classification is the outbox's safety valve** — a failure that says nothing about the message (throttling, SNS 5xx/429, timeouts, network/credentials, a codec wrapping `ErrRetryable`) releases the lease without counting an attempt and backs off on one shared schedule; anything else counts an attempt and backs off per record. Without this split a short SNS outage dead-letters the backlog. Custom publishers and codecs must follow it (`BatchFailure.Retryable` / `ErrRetryable`).

**`port.Logger` is interface-compatible with platform-gincommon's `ZapLogger`** — `port.Logger` is `Debug` / `Info` / `Warn` / `Error`, each taking `(msg string, fields map[string]interface{})` — the shape of platform-gincommon's `ZapLogger`. A consuming service that already constructs a `ZapLogger` passes it directly to `SNSConfig.Logger`, `SQSConfig.Logger`, `DLQConfig.Logger` and `outbox.Config.Logger` without any adapter. Do not introduce a second logger abstraction.

**OTel is always initialised by the consuming service, never by this library** — `platform-events` calls standard `otel.Tracer(...)` / `otel.GetTracerProvider()` and produces no-op spans if no provider is set. The consuming service is responsible for calling `gincommon.InitTracingFromEnv()` (from `platform-gincommon`) at startup. This avoids double-initialisation when both `platform-gincommon` and `platform-events` are imported together.

**`Envelope.TraceID` is sourced from `gincommon.RequestContext`** — when an HTTP handler publishes an event, `rc.TraceID` (from `gincommon.GetRequestContext(c)`) must be passed as `WithTraceID(rc.TraceID)`. This threads the HTTP trace through SNS/SQS and into the consumer's OTel span, making the full request→event→handler path visible in a single Tempo trace without manual W3C header forwarding across queues.

**`Envelope.TenantID` drives RLS on the consumer side** — `platform-pgcommon`'s `GUCProvider` reads `tenant_id` from the handler context (injected by `NewSQSConsumer` from `env.TenantID`). Postgres RLS policies (`current_setting('app.tenant_id', true)`) therefore scope all DB queries inside a consumer handler to the correct tenant automatically, matching the HTTP path's behaviour via `platform-gincommon`'s `ContextMiddleware`.

**Outbox schema lifecycle is owned by `platform-pgcommon`'s migrate runner** — `outbox.ApplySchema` is a thin wrapper that passes the embedded `pkg/outbox/migrations/` FS to `platform-pgcommon`'s `migrate.Runner`. Services that already call `migrateRunner.Up(ctx)` for their own schema can run outbox migrations in the same step. The outbox schema is versioned separately so consuming services can upgrade `platform-events` without conflating it with their domain migrations.

**All database access goes through platform-pgcommon** — connections (`pgcommon.Pool`), configuration (`config.LoadOutbox().DB` is `pgcommon.ConfigFromEnv()`, `MigrationDatabaseURL` is `pgcommon.MigrationDSNFromEnv()`), transactions (`pgcommon.RunInTx` — never `conn.Begin`, so pgcommon's PgBouncer GUC injection and per-transaction `StatementTimeout`/`LockTimeout` always apply) and types (`pgcommon.Tx`/`TxOptions`/`Conn`/`Rows`/`Row`/`ErrNoRows` aliases). A depguard rule (`pgcommon-only` in `.golangci.yml`) rejects `github.com/jackc/pgx`, `database/sql` and `golang-migrate` imports in **every** file — library, CLI, fixtures and tests. pgx is only an indirect dependency (through pgcommon). Test fakes embed `pgcommon.Tx`. A fake that must implement a method with a pgx-only type, such as `Exec`'s `CommandTag`, infers it from `pgcommon.Tx` with the generic `newStubTx(pgcommon.Tx.Exec)` pattern in `test/unit/enqueue/enqueue_test.go`, so it never imports pgx.

**Outbox transactions compose with `pgcommon.RunInTx`** — `outbox.Enqueue` accepts a `pgcommon.Tx` (an alias of `pgx.Tx`) rather than a pool so callers control the transaction boundary. The idiomatic pattern is `pgcommon.RunInTx(ctx, pool, opts, fn)` where `fn` performs the business write and calls `outbox.Enqueue(ctx, tx, env)` — both commit or both roll back. This avoids a second `BEGIN` inside `Enqueue` and keeps the dual-write window at zero.

**`Codec` is optional and pluggable, not implemented in this library** — mirrors `port.Logger`: the interface and a `NoopCodec` identity reference live here; a consuming service implements it against its own schema-registry client (e.g. AWS Glue) and injects it via `WithCodec`/`WithConsumerCodec`. Absent, behaviour is byte-for-byte identical to pre-`Codec` releases — the encode/decode hooks are gated on `codec != nil` and `SchemaID != ""` respectively, both unreachable no-ops for existing callers.

## CI/CD

GitHub Actions mirrors `iam-org-membership`'s pipeline — the org ruleset on `main` requires its job names (`Validate / Test / test`, `Validate / Quality / quality`, `Build image (cache)`, `Trivy CVE scan`, `Smoke tests`, `PR summary`), so **do not rename those jobs**. This is a **private module** with no production deployment; the Docker image is the reference CLI (`cmd/platform-events`), built only so CI can Trivy-scan and smoke-test the compiled binary.

- **`validate-test.yml`** (reusable) — `make test-ci` (unit + integration + e2e in parallel, `-race`, merged coverage) → `.github/scripts/coverage-gate.sh` (≥ 97%).
- **`validate-quality.yml`** (reusable) — `go mod verify`, HTML-entity check, RLS-6 grep, `gofmt`, tidy drift, `make vet` / `make lint` (each also with `-tags=integration,e2e`), `make metrics-lint` + `make rules-check` + `make dashboards-check` (Observability Standard), `make docs-check` (diagram sync), `make vuln-check`, Dockerfile digest-pinning check. `golangci-lint` runs via `go tool` (the `tool` directive in `go.mod` is not propagated to consumers).
- **`docs.yml`** — docs-only changes (`ARCHITECTURE.md`, `docs/architecture/**`, skipped by `ci.yml`): `make docs-check`. **Edit a diagram in both places** — the `.mmd` source and its embedded copy in `ARCHITECTURE.md` must stay byte-identical.
- **`ci.yml`** (push/PR to main) — the two gates + `Build image (cache)` in parallel → `Trivy CVE scan` / `Smoke tests` → `Cross-language compatibility` → `PR summary`; on push, `Push image → GHCR` (Cosign-signed).
- **`changelog-check.yml`** — PRs touching `internal/`, `pkg/`, `cmd/` must update `CHANGELOG.md`.
- **`release.yml`** (`v*` tags) — **the same job graph as `ci.yml`** at the tag, behind a fail-fast `verify` job (tag on `main`, dispatch only from `main` or the tag, CHANGELOG section — a prerelease may use its base version's; floating image tags `X.Y`/`X`/`latest` only move forward, via `release-image-tags.sh`, same scripts as platform-pgcommon v1.4.1), plus 5-platform CLI binaries; the image is pushed (semver tags, signed, provenance) only after every gate passes, then the GitHub Release is published. Change a gate in `ci.yml` → change it in `release.yml` too. The Git tag is the **Go module release** consuming services pin with `go get github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events@vX.Y.Z`.

Standard-library `govulncheck` findings are fixed by bumping the `toolchain` line (in all three `go.mod` files) and the Dockerfile builder image together. The `go` directive stays at `1.26.0` so consumers are not pinned to a patch release; CI reads the toolchain via `go-version-file`, the same as platform-pgcommon. Every image this repository runs is digest-pinned and re-pinned with `make pin-base-images`: the Dockerfile, promtool, docker-compose and the testcontainers fixtures, all recorded in `.docker-digests` and checked in CI.

## Extending the Library

- **New event type:** define a Go struct and use `NewEnvelope[YourType](...)`. No changes to the library itself — types are generic. Always pass `WithTenantID(rc.TenantID)`, `WithTraceID(rc.TraceID)`, and `WithSchemaVersion("1")` from the `gincommon.RequestContext` when publishing from an HTTP handler. Register the new type in `EVENT_SCHEMA_GOVERNANCE.md`. For breaking payload changes, mint a new versioned type (e.g. `iam.user.created.v2`) and reset `WithSchemaVersion("1")` — never mutate the existing type's payload in an incompatible way.
- **New Publisher backend** (e.g. EventBridge): implement `port.Publisher` in `internal/adapter/outbound/eventbridge/`, expose a constructor in `pkg/events/`. Follow the SNS adapter as a template — call `otel.Tracer(...)` (not gincommon directly) + Prometheus metrics in the adapter.
- **New Consumer backend** (e.g. Kinesis): implement `port.Consumer` in `internal/adapter/outbound/kinesis/`, expose via `pkg/events/`. `Handler` signature is shared — no changes to calling code. Inject tenant+trace into handler `ctx` using the same helper as `NewSQSConsumer` so `pgcommon` GUC injection works transparently.
- **New outbox store backend** (e.g. DynamoDB): implement `port.OutboxStore` in `internal/core/port/outboxstore.go`, place in `internal/adapter/outbound/dynamooutbox/`, inject via `Runner.Store`. The Postgres implementation should remain the default — only swap if `platform-pgcommon` is not available in the consuming service.
- **New outbox migration:** add `NNN_description.up.sql` / `NNN_description.down.sql` to `pkg/outbox/migrations/`. The embedded FS is recompiled on next build; `outbox.ApplySchema` picks it up automatically via `platform-pgcommon`'s `migrate.Runner`.
- **Replace logger:** the `port.Logger` interface is deliberately kept identical to `platform-gincommon`'s. Do not change method signatures — consuming services pass a single `ZapLogger` instance to both libraries.
- **New Codec implementation** (e.g. AWS Glue Schema Registry): implement `port.Codec` (aliased as `events.Codec`) in the consuming service — this library does not implement one or depend on `aws-sdk-go-v2/service/glue`. Inject via `WithCodec` (publisher) / `WithConsumerCodec` (consumer).
- **New metrics:** follow "Adding a metric" under Metrics above — registry entry first (`internal/adapter/outbound/metrics/registry.go`), then `registerPlatform`, a recording function, `exerciseAll` in `test/unit/metrics/standard_test.go`, and `make metrics-doc`. Never invent a Canonical `platform_*` name; new names start as Proposed (rules 11/12).
- **New use case:** add to `internal/core/service/`, depending only on `domain/` types and `port/` interfaces.
