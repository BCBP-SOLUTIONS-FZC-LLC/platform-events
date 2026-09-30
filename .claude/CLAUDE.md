# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Repo Is

`platform-events` is a **private Go shared library** (module: `github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events`, Go 1.26.6) that provides reusable SNS publisher and SQS consumer primitives for platform services. It lives in a **private GitHub repository** and is consumed as a Go module dependency by internal platform services — it is never deployed as a standalone server.

Core capabilities:
- `Publisher` interface + AWS SNS implementation
- `Consumer` interface + AWS SQS implementation (long-poll loop, parallel dispatch)
- **Outbox runner** — transactional outbox pattern over a Postgres table; guarantees at-least-once delivery without 2PC
- **Inbox** (`pkg/inbox`) — consumer-side dedup: `processed_events` ledger (`ApplySchema`, `Store`, `Handler` wrapper, `Prune`), metric `events_inbox_duplicates_total`
- **`GlueDecodeCodec`** — decode-only codec stripping the Glue Schema Registry wire header
- **`DLQPublisher`** — forwards failed messages to the source queue's `RedrivePolicy` DLQ (`SendToDLQ`, `ResolveDLQ`); the only sanctioned SQS path for consumer services (they must not import the SQS SDK). Typed `*DLQError` + `ErrRetryable`; `mock.DLQPublisher`; metric `events_dlq_forwarded_total`
- **Event-envelope types** — versioned, typed `Envelope[T]` carrying metadata (event ID, type, source, tenant, trace ID, timestamp) plus JSON-serialised payload
- **HMAC helpers** — SHA-256 HMAC signing and verification for webhook and cross-service event authentication

**Consuming services** must set `GOPRIVATE=github.com/BCBP-SOLUTIONS-FZC-LLC/*` (or `GONOSUMDB`/`GOFLAGS` equivalents) to fetch private module versions.

### Shared Library Dependencies

This library builds on two other BCBP platform libraries:

| Library | Module | Role in platform-events |
|---------|--------|--------------------------|
| [`platform-gincommon`](https://github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon) | `github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon` | OTel tracing initialisation (`InitTracingFromEnv`, `EnsureTracing`); `port.Logger` interface (compatible — gincommon's `ZapLogger` can be injected directly); `RequestContext` carries `TraceID` / `TenantID` / `UserID` that populate `Envelope` fields |
| [`platform-pgcommon`](https://github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon) | `github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon` | Connection pool (`pgcommon.Pool`) used by the outbox store; `pgcommon.RunInTx` composes business logic + `outbox.Enqueue` atomically; `migrate.Runner` applies the outbox schema (`outbox_events`, `outbox_dead_letters`); `SlowQueryTracer` surfaces slow outbox queries |

See [`ARCHITECTURE.md`](../ARCHITECTURE.md) for detailed flow diagrams and invariant tables.

## Common Commands

```bash
make setup           # Copy .env-example → .env + install .githooks/pre-commit (run once before anything else)
make install-hooks   # Re-install .githooks/pre-commit (tidy + drift check, fmt-check, lint)
make tidy            # go mod tidy
make fmt             # go fmt ./...
make vet             # go vet ./...
make lint            # golangci-lint
make test            # All tests (unit + integration), excludes smoke
make test-ci         # All tests with race detector (used in CI)
make test-unit       # Unit tests only
make test-int        # Integration tests (requires LocalStack via testcontainers)
make test-smoke      # Smoke tests (requires live AWS resources at SNS_TOPIC_ARN / SQS_QUEUE_URL)
make race            # All tests with -race flag
make build           # Compile reference CLI to bin/platform-events
make cover           # Coverage HTML report (measures ./internal/... ./pkg/...)
make cover-func      # Coverage summary by function (terminal)
make ci              # tidy + vet + lint + test-ci + build (full CI pipeline)
make docker-up       # Start LocalStack (SNS + SQS + Postgres for outbox)
make docker-down     # Stop LocalStack
make clean           # Remove bin/ artefacts
```

To run a single test:
```bash
go test ./test/unit/envelope/...   -run TestEnvelopeSign -v
go test ./test/integration/...     -run TestSNSPublishRoundTrip -v
```

**Coverage note:** tests live under `test/` (a separate package tree from sources). Always use `-coverpkg=./internal/...,./pkg/...` to get meaningful numbers; running `go test ./...` without it shows 0% for source packages. `make cover` and `make cover-func` handle this correctly.

**Testcontainers note:** integration tests spin up LocalStack (SNS + SQS) and Postgres via `testcontainers-go`. Docker must be running locally. Pass `-short` to skip integration tests without a Docker daemon.

## Architecture

The library follows **Clean Architecture** — dependencies point inward; outer layers depend on inner layers, never the reverse.

```
pkg/               ← public API surface (consumers import these)
  events/          ← Envelope types, Publisher/Consumer interfaces, HMAC helpers
  outbox/          ← Outbox runner (Postgres-backed, uses platform-pgcommon pool)
  inbox/           ← Consumer dedup ledger (processed_events; Store, Handler, Prune)
internal/
  core/
    domain/        ← Entities: Envelope, OutboxRecord, domain errors (no external deps)
    port/          ← Interfaces: Publisher, Consumer, Logger, Clock (owned by use-case layer)
                      port.Logger is interface-compatible with platform-gincommon's ZapLogger
    service/       ← Use Cases: OutboxService, HMACService
  adapter/
    outbound/
      sns/         ← SNS Publisher implementation (aws-sdk-go-v2)
      sqs/         ← SQS Consumer implementation (long-poll loop)
      outboxstore/ ← Postgres outbox store (pgx via platform-pgcommon Pool + RunInTx)
      metrics/     ← Prometheus counters + OTel spans
      logger/      ← Zap logger adapter (same as platform-gincommon's adapter)
  config/          ← Environment variable loading
test/
  unit/            ← Isolated unit tests per package
  integration/     ← Tests against LocalStack + Postgres containers
  fixtures/        ← Shared helpers (MockPublisher, MockConsumer, MockLogger, fake clock)
  testenv/         ← Loads .env-example for tests

External dependencies (private modules):
  platform-gincommon → OTel init, port.Logger interface, RequestContext (TraceID/TenantID source)
  platform-pgcommon  → Pool, RunInTx, migrate.Runner (outbox schema), SlowQueryTracer
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
  - `ConsumerOption` — `WithConcurrency(n)` (default 1), `WithVisibilityTimeout(d)`, `WithDeadLetterHandler(fn)`, `WithConsumerCodec(codec)` (schema-registry hook — see "Codec" below)
  - `MockConsumer` (in `pkg/events/mock`) — in-memory queue; `Inject(env)` delivers messages synchronously

- **HMAC helpers**
  - `Sign(key []byte, payload []byte) string` — returns hex-encoded HMAC-SHA256 signature
  - `Verify(key []byte, payload []byte, sig string) bool` — constant-time comparison; returns false on any parse/length error rather than panicking
  - `SignEnvelope(key []byte, env Envelope[json.RawMessage]) (string, error)` — signs canonical JSON of envelope
  - `VerifyEnvelope(key []byte, env Envelope[json.RawMessage], sig string) (bool, error)` — deserialises and verifies; safe for webhook receipt handlers

**`pkg/outbox`** — transactional outbox:

- `Runner{Pool, Publisher, Logger, PollInterval, BatchSize, MaxAttempts}` — configure the outbox runner. `Pool` is a `*pgcommon.Pool` from `platform-pgcommon`; `Publisher` are **required**.
- `Runner.Start(ctx) error` — start the polling loop; blocks until `ctx` is cancelled.
- `Runner.Stop() error` — graceful drain; waits for in-flight batch to complete before returning.
- `Runner.PrunePublished(ctx, olderThan time.Duration, limit int) (int64, error)` — deletes published records older than `olderThan` from `outbox_events` (up to `limit` rows per call) to prevent unbounded table growth. Call periodically from a scheduled job. Applies a 30 s internal DB timeout.
- `Runner.ListDeadLetters(ctx, filter DLQFilter, limit int) ([]DeadLetterRecord, error)` — returns up to `limit` dead-letter records matching filter (by `EventType`, `TenantID`, `FailedBefore`), oldest first; safe to call repeatedly as an inspection step; applies a 30 s internal DB timeout.
- `Runner.ReprocessDeadLetters(ctx, limit int) (int, error)` — moves up to `limit` records from `outbox_dead_letters` back to `outbox_events` for retry; applies a 30 s internal DB timeout.
- `Runner.ReprocessDeadLettersWith(ctx, filter DLQFilter, limit int) (int, error)` — same as `ReprocessDeadLetters` but filters by `DLQFilter`; use for targeted replay without touching unrelated failures.
- `Runner.DiscardDeadLetters(ctx, filter DLQFilter, limit int) (int64, error)` — permanently deletes up to `limit` matching dead-letter records; always call `ListDeadLetters` first to confirm selection; applies a 30 s internal DB timeout.
- `Enqueue(ctx, tx pgcommon.Tx, env Envelope[json.RawMessage]) error` — insert a serialised envelope into the `outbox_events` table within the caller's transaction. No publish happens at insert time — the runner delivers asynchronously. Callers should pass the `pgcommon.Tx` obtained from `pgcommon.RunInTx` so the enqueue and the business-logic write commit or roll back as a single unit.
- `ApplySchema(ctx, runner *migrate.Runner) error` — convenience wrapper that calls `platform-pgcommon`'s `migrate.Runner` to apply the embedded `pkg/outbox/migrations/` SQL files (`001`–`008`). Call once at service startup before `Runner.Start`.
- Schema: `outbox_events(id UUID PK, event_type TEXT, payload JSONB, tenant_id TEXT, trace_id TEXT, attempts INT DEFAULT 0, last_error TEXT, created_at TIMESTAMPTZ, scheduled_at TIMESTAMPTZ, published_at TIMESTAMPTZ)`

**Typical wiring with platform-pgcommon:**
```go
// 0. Logger — call Sync() on shutdown to flush buffered entries.
logger, _ := logger.NewLogger(os.Getenv("APP_ENV"))
defer func() { _ = logger.Sync() }()

// 1. Apply outbox schema via pgcommon migrate runner
migrateRunner := &migrate.Runner{DSN: pgcommon.MigrationDSNFromEnv(), Logger: logger}
outbox.ApplySchema(ctx, migrateRunner)

// 2. Load config and log any warnings via the structured logger (preferred over LogWarnings).
outboxEnv := config.LoadOutbox()
config.LogWarningsTo(logger, outboxEnv.Warnings) // falls back to stderr when logger is nil

// 3. Construct outbox runner with pgcommon pool.
// NewRunner returns an error for misconfigured ClaimLeaseDuration.
// DB config is owned by platform-pgcommon: outboxEnv.DB = pgcommon.ConfigFromEnv().
pool, _ := pgcommon.NewPool(ctx, outboxEnv.DB)
runner, err := outbox.NewRunner(outbox.Config{Pool: pool, Publisher: snsPublisher, Logger: logger})
if err != nil {
    log.Fatal(err)
}

// 3. Wire OS signals so SIGTERM/SIGINT trigger graceful shutdown.
ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
defer stop()

go func() { _ = runner.Start(ctx) }()

// 4. Wait for termination, then drain in-flight messages.
<-ctx.Done()
_ = runner.Stop()

// 5. Enqueue inside a business transaction (pgcommon.RunInTx)
pgcommon.RunInTx(ctx, pool, pgcommon.TxOptions{}, func(ctx context.Context, tx pgcommon.Tx) error {
    _ = repo.SaveUser(ctx, tx, user)           // business write
    return outbox.Enqueue(ctx, tx, envelope)   // event write — same transaction
})
```

### Event Envelope Design

`Envelope[T]` is the canonical wire format for all inter-service events:

```json
{
  "id":             "01926e4f-...",     // UUID v7 — sortable, unique per event
  "type":           "iam.user.created", // <domain>.<entity>.<past-tense-verb>[.v<N>]
  "source":         "platform-iam",    // emitting service name
  "schema_version": "1",               // optional; omitted = treat as "1"
  "tenant_id":      "acme",
  "trace_id":       "4bf92f3577...",   // OTel trace ID (hex, 32 chars) or empty
  "correlation_id": "...",             // optional: ties events in a saga/workflow
  "subject":        "users/01926e4f-...", // optional: resource the event is about; also an SNS filter attribute
  "actor":          "admin@acme.com",  // optional: identity that caused the event (audit trail)
  "schema_id":      "550e8400-...",    // optional: Glue Schema Registry UUID — distinct from schema_version
  "timestamp":      "2026-05-27T...",  // RFC3339Nano, UTC
  "payload":        { ... }            // typed T, inlined (not base64)
}
```

`event_type` convention: `<domain>.<entity>.<past-tense-verb>[.v<N>]` (e.g. `iam.user.created`, `billing.invoice.settled`). The `.v<N>` suffix only appears for breaking payload changes — v1 is implicit (no suffix). Consumers match on prefix with `strings.HasPrefix` or exact equality — no glob or regex routing in the base library.

`schema_version` field: set via `WithSchemaVersion("1")` on every new event type at inception. **This is a semantic consumer-branching version — always a small integer string (`"1"`, `"2"`).** Do NOT put a Glue Schema Registry UUID here; use `WithSchemaID` for that instead (see below).

`schema_id` field: set via `WithSchemaID(schemaVersionID)` where `schemaVersionID` is the UUID returned by the Glue codec's `Encode` call. This is the authoritative registry pointer the codec uses for Avro/JSON deserialization. It is distinct from `schema_version` and must never be confused with it. The Glue UUID is also embedded in the encoded payload's 18-byte wire-format header, so `schema_id` in the envelope is for observability and traceability only. Increment on additive-only field additions. For breaking changes, mint a new event type (`.v2`) and reset `schema_version` back to `"1"`. See `EVENT_SCHEMA_GOVERNANCE.md` for full rules, migration window pattern, and the cross-service event type registry. As of the `Codec`/`WithCodec` addition (see "Codec" below), `schema_id` is set automatically by the SNS publisher when a codec is configured — manual `WithSchemaID` calls are unnecessary (and will be overwritten) once a codec is wired in.

### SNS Publisher

`NewSNSPublisher` wraps `aws-sdk-go-v2/service/sns`. Key behaviours:

- **Message attributes** — `EventType`, `TenantID`, `Source`, `EventID`, and `Subject` (when non-empty) are set as SNS message attributes to enable SQS subscription filter policies without deserialising the body. `Actor` is not forwarded as an attribute — it is an audit-trail field, not a routing field.
- **FIFO topics** — if `TopicARN` ends in `.fifo`, the publisher requires `MessageGroupID`; `MessageDeduplicationID` defaults to `Envelope.ID` (content-based deduplication must be disabled at the topic level).
- **Batching** — `PublishBatch` uses `sns:PublishBatch` (max 10 per call); batches larger than 10 are automatically split.
- **Retry** — caller is responsible for retry (the outbox runner handles this); the SNS adapter does not retry internally. `Publish` returns the raw AWS error for callers to inspect (`smithy.APIError`).
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

- **Visibility extension** — if a handler runs longer than `VisibilityTimeout/2`, the consumer automatically calls `ChangeMessageVisibility` to extend by `VisibilityTimeout` until the handler returns.
- **Dead-letter handler** — messages that have exceeded `MaxReceiveCount` (configured at the SQS level) are routed to `WithDeadLetterHandler` if set; otherwise they are logged at `ERROR` and deleted.
- **Graceful shutdown** — `Stop()` cancels the receive loop, waits for all in-flight handlers to complete (up to `DrainTimeout`, default 30 s), then returns.
- **OTel** — each message dispatch creates a child span `sqs.receive` with `messaging.system=aws_sqs`, `messaging.destination`, `messaging.message_id`, `messaging.operation=process`. The span is linked to the publisher's trace via `Envelope.TraceID`, giving end-to-end visibility across the SNS/SQS boundary in Tempo/Grafana. OTel must be initialised by the consuming service via `gincommon.InitTracingFromEnv()` before starting the consumer.
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
3. On failure: increment `attempts`; set `last_error`; if `attempts >= MaxAttempts` move to dead-letter table (`outbox_dead_letters`).
4. Commit; sleep `PollInterval` (default `5s`).

**Idempotency** — `Envelope.ID` (UUID v7) is forwarded as the SNS `MessageDeduplicationID` on FIFO topics and as a message attribute on standard topics. Consumers should use `Envelope.ID` as their idempotency key.

### HMAC Helpers

`Sign` / `Verify` operate on raw bytes. `SignEnvelope` / `VerifyEnvelope` serialise the envelope to canonical JSON before signing, ensuring field-ordering is deterministic (sorted keys via `encoding/json` + a stable marshaller).

`Verify` uses `hmac.Equal` (constant-time) — not `==`. Never replace with string comparison. `VerifyEnvelope` returns `(false, nil)` on signature mismatch and `(false, err)` on malformed input — callers must check both return values.

HMAC keys must be ≥ 32 bytes; `Sign` returns an error (not a panic) if the key is shorter.

### Metrics

`pkg/events/metrics.Init(serviceName, buildVersion string)` registers:
- `events_published_total{service, topic, event_type, status}` — counter
- `events_publish_duration_seconds{service, topic, event_type}` — histogram
- `events_consumed_total{service, queue, event_type, status}` — counter
- `events_consume_duration_seconds{service, queue, event_type}` — histogram
- `outbox_pending_total{service}` — gauge (set each poll cycle)
- `outbox_published_total{service, event_type, status}` — counter
- `outbox_attempts_total{service, event_type}` — counter
- `events_codec_encode_total{service, topic, event_type, status}` — counter (`status`: `success`/`noop`/`error`); only incremented when `WithCodec` is configured
- `events_codec_encode_duration_seconds{service, topic, event_type}` — histogram
- `events_codec_decode_total{service, queue, event_type, status}` — counter (`status`: `success`/`error`); only incremented when `WithConsumerCodec` is configured
- `events_codec_decode_duration_seconds{service, queue, event_type}` — histogram
- `events_oversized_event_type_label_total{service}` — counter; incremented by `SanitizeEventType` when an `event_type` value exceeds 128 bytes and is replaced with `"__oversized__"`; alert on `rate() > 0`

`InitWithRegisterer(serviceName, buildVersion, prometheus.Registerer)` — for isolated test registries; bypasses `sync.Once`.

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
| `OUTBOX_POLL_INTERVAL` | `5s` | Parsed as `time.Duration` |
| `OUTBOX_BATCH_SIZE` | `50` | Records per poll cycle |
| `OUTBOX_MAX_ATTEMPTS` | `5` | Before moving to dead-letter |
| `OTEL_SERVICE_NAME` | — | OTel resource attribute |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | |
| `OTEL_EXPORTER_OTLP_INSECURE` | `false` unless `APP_ENV=dev` | |

`APP_ENV=dev/development/local` sets `OTEL_EXPORTER_OTLP_INSECURE=true` automatically. An unset `APP_ENV` is treated as production (TLS on, 10% trace sampling).

## Key Environment Variables

Defined in `.env-example` (copy to `.env` via `make setup`):

```
APP_NAME, APP_ENV, BUILD_VERSION
AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY  # use instance role / IRSA in production
AWS_ENDPOINT_URL                                       # set to http://localhost:4566 for LocalStack
SNS_TOPIC_ARN
SQS_QUEUE_URL
SQS_MAX_MESSAGES, SQS_WAIT_SECONDS, SQS_VISIBILITY_TIMEOUT, SQS_CONCURRENCY
OUTBOX_POLL_INTERVAL, OUTBOX_BATCH_SIZE, OUTBOX_MAX_ATTEMPTS
DATABASE_URL                                           # outbox runner DSN — read by platform-pgcommon's ConfigFromEnv (or PG_HOST/PG_PORT/PG_USER/PG_PASSWORD/PG_DBNAME/PG_SSLMODE; plus PG_MAX_CONNS, PG_STATEMENT_TIMEOUT, PG_LOCK_TIMEOUT, PG_BOUNCER_MODE, …)
MIGRATION_DATABASE_URL                                 # optional DDL-role DSN for ApplySchema (pgcommon.MigrationDSNFromEnv)
OTEL_SERVICE_NAME, OTEL_EXPORTER_OTLP_ENDPOINT
OTEL_EXPORTER_OTLP_INSECURE
SMOKE_SNS_TOPIC_ARN, SMOKE_SQS_QUEUE_URL               # for smoke tests against real AWS
```

## Test Layout

- `test/unit/` — isolated unit tests per package (envelope, hmac, outbox service, config, metrics, mock publisher/consumer)
- `test/integration/` — tests against LocalStack (SNS publish round-trip, SQS consume loop, outbox runner end-to-end) and Postgres (outbox enqueue/publish/dead-letter)
- `test/smoke/` — optional; requires live AWS resources (`SMOKE_SNS_TOPIC_ARN`, `SMOKE_SQS_QUEUE_URL`)
- `test/fixtures/` — shared helpers (`MockPublisher`, `MockConsumer`, `MockLogger`, `FakeClock`, LocalStack bootstrap)
- `test/testenv/` — loads `.env-example` for tests

## Key Design Decisions

**`Publisher` and `Consumer` are interfaces, not concrete types** — consumers of this library inject the interface, enabling `MockPublisher`/`MockConsumer` in unit tests without LocalStack. SNS/SQS constructors return the interface, not a pointer to a struct.

**Envelope ID is UUID v7** — UUIDs v7 are time-ordered and collation-friendly in Postgres B-tree indexes. The outbox `SELECT ... ORDER BY id` scan is therefore an index scan, not a seq scan, as the table grows.

**`Verify` uses constant-time comparison** — `hmac.Equal` prevents timing attacks. Never replace with string equality. The function signature returns `bool` (not error) for the happy path so call sites read naturally: `if !events.Verify(...) { return ErrBadSig }`.

**Outbox uses `SKIP LOCKED`** — multiple outbox runner instances (e.g. in a horizontally scaled deployment) do not contend. Each runner claims its own batch atomically. No distributed lock is needed.

**SNS message attributes enable zero-body filtering** — SQS subscription filter policies match on `EventType` and `TenantID` message attributes. Consumers that only handle a subset of event types do not deserialise envelopes they will discard.

**Dead-letter is a Postgres table, not SQS DLQ** — the outbox dead-letter table (`outbox_dead_letters`) is queryable, retryable, and auditable from standard SQL tooling. SQS DLQs are recommended for consumption-side failures; the outbox handles publish-side failures.

**`Consumer` graceful drain on `Stop()`** — in-flight handlers are given `DrainTimeout` (default 30 s) to complete before `Stop()` returns. This matches `net/http.Server.Shutdown` semantics and ensures clean pod termination in Kubernetes without losing partially-processed messages.

**Nil-safe logger** — every component that accepts a `port.Logger` checks for `nil` before calling it. `SQSConsumer` with a nil logger runs silently. `OutboxRunner` with a nil logger skips per-record log lines but still updates metrics.

**`ServiceName` required on metrics init** — `metrics.Init` panics on empty `ServiceName` to prevent invalid Prometheus const labels, matching the pattern established in `platform-gincommon`.

**HMAC key length enforced at call time** — `Sign` returns `("", ErrKeyTooShort)` for keys < 32 bytes rather than silently using a weak key. Callers that ignore the error emit an empty signature, which `Verify` will reject (constant-time) — the system degrades safely.

**OTel trace context propagated via Envelope** — `Envelope.TraceID` carries the OTel trace ID from the publishing service. On the consumer side, `NewSQSConsumer` reconstructs a remote span context from `TraceID` and sets it as the parent of the `sqs.receive` span, enabling cross-service trace continuity without relying on SNS/SQS message attributes for propagation.

**`PublishBatch` splits automatically at 10** — SNS hard limit is 10 messages per `PublishBatch` call. The adapter splits silently rather than returning an error, so callers can pass arbitrarily-sized slices. Partial failures return a `BatchError` listing per-message errors; successful messages within the same batch are not retried.

**Idempotent Prometheus registration** — `metrics.Init` is guarded by `sync.Once`. `InitWithRegisterer` bypasses it for test isolation — same pattern as `platform-gincommon` and `platform-pgcommon`.

**`port.Logger` is interface-compatible with platform-gincommon's `ZapLogger`** — both libraries define `Logger` with the same method set (`Info`, `Warn`, `Error`, `With`, `Named`). A consuming service that already constructs a `ZapLogger` from `platform-gincommon` passes it directly to `SNSConfig.Logger`, `SQSConfig.Logger`, and `outbox.Runner.Logger` without any adapter. Do not introduce a second logger abstraction.

**OTel is always initialised by the consuming service, never by this library** — `platform-events` calls standard `otel.Tracer(...)` / `otel.GetTracerProvider()` and produces no-op spans if no provider is set. The consuming service is responsible for calling `gincommon.InitTracingFromEnv()` (from `platform-gincommon`) at startup. This avoids double-initialisation when both `platform-gincommon` and `platform-events` are imported together.

**`Envelope.TraceID` is sourced from `gincommon.RequestContext`** — when an HTTP handler publishes an event, `rc.TraceID` (from `gincommon.GetRequestContext(c)`) must be passed as `WithTraceID(rc.TraceID)`. This threads the HTTP trace through SNS/SQS and into the consumer's OTel span, making the full request→event→handler path visible in a single Tempo trace without manual W3C header forwarding across queues.

**`Envelope.TenantID` drives RLS on the consumer side** — `platform-pgcommon`'s `GUCProvider` reads `tenant_id` from the handler context (injected by `NewSQSConsumer` from `env.TenantID`). Postgres RLS policies (`current_setting('app.tenant_id', true)`) therefore scope all DB queries inside a consumer handler to the correct tenant automatically, matching the HTTP path's behaviour via `platform-gincommon`'s `ContextMiddleware`.

**Outbox schema lifecycle is owned by `platform-pgcommon`'s migrate runner** — `outbox.ApplySchema` is a thin wrapper that passes the embedded `pkg/outbox/migrations/` FS to `platform-pgcommon`'s `migrate.Runner`. Services that already call `migrateRunner.Up(ctx)` for their own schema can run outbox migrations in the same step. The outbox schema is versioned separately so consuming services can upgrade `platform-events` without conflating it with their domain migrations.

**All database access goes through platform-pgcommon** — connections (`pgcommon.Pool`), configuration (`config.LoadOutbox().DB` is `pgcommon.ConfigFromEnv()`, `MigrationDatabaseURL` is `pgcommon.MigrationDSNFromEnv()`), transactions (`pgcommon.RunInTx` — never `conn.Begin`, so pgcommon's PgBouncer GUC injection and per-transaction `StatementTimeout`/`LockTimeout` always apply) and types (`pgcommon.Tx`/`TxOptions`/`Conn`/`Rows`/`Row`/`ErrNoRows` aliases). A depguard rule (`pgcommon-only` in `.golangci.yml`) rejects `github.com/jackc/pgx` and `database/sql` imports in non-test code; tests may import pgx only to fake the full `pgx.Tx` interface.

**Outbox transactions compose with `pgcommon.RunInTx`** — `outbox.Enqueue` accepts a `pgcommon.Tx` (an alias of `pgx.Tx`) rather than a pool so callers control the transaction boundary. The idiomatic pattern is `pgcommon.RunInTx(ctx, pool, opts, fn)` where `fn` performs the business write and calls `outbox.Enqueue(ctx, tx, env)` — both commit or both roll back. This avoids a second `BEGIN` inside `Enqueue` and keeps the dual-write window at zero.

**`Codec` is optional and pluggable, not implemented in this library** — mirrors `port.Logger`: the interface and a `NoopCodec` identity reference live here; a consuming service implements it against its own schema-registry client (e.g. AWS Glue) and injects it via `WithCodec`/`WithConsumerCodec`. Absent, behaviour is byte-for-byte identical to pre-`Codec` releases — the encode/decode hooks are gated on `codec != nil` and `SchemaID != ""` respectively, both unreachable no-ops for existing callers.

## CI/CD

GitHub Actions mirrors `iam-org-membership`'s pipeline — the org ruleset on `main` requires its job names (`Validate / Test / test`, `Validate / Quality / quality`, `Build image (cache)`, `Trivy CVE scan`, `Smoke tests`, `PR summary`), so **do not rename those jobs**. This is a **private module** with no production deployment; the Docker image is the reference CLI (`cmd/platform-events`), built only so CI can Trivy-scan and smoke-test the compiled binary.

- **`validate-test.yml`** (reusable) — `make test-ci` (unit + integration + e2e in parallel, `-race`, merged coverage) → `.github/scripts/coverage-gate.sh` (≥ 95%).
- **`validate-quality.yml`** (reusable) — `go mod verify`, HTML-entity check, RLS-6 grep, `gofmt`, tidy drift, `make vet` / `make lint` (each also with `-tags=integration,e2e`), `make vuln-check`, Dockerfile digest-pinning check. `golangci-lint` runs via `go tool` (the `tool` directive in `go.mod` is not propagated to consumers).
- **`ci.yml`** (push/PR to main) — the two gates + `Build image (cache)` in parallel → `Trivy CVE scan` / `Smoke tests` → `Cross-language compatibility` → `PR summary`; on push, `Push image → GHCR` (Cosign-signed).
- **`changelog-check.yml`** — PRs touching `internal/`, `pkg/`, `cmd/` must update `CHANGELOG.md`.
- **`release.yml`** (`v*` tags) — **the same job graph as `ci.yml`** at the tag, behind a fail-fast tag + CHANGELOG `verify` job, plus 5-platform CLI binaries; the image is pushed (semver tags, signed, provenance) only after every gate passes, then the GitHub Release is published. Change a gate in `ci.yml` → change it in `release.yml` too. The Git tag is the **Go module release** consuming services pin with `go get github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events@vX.Y.Z`.

Standard-library `govulncheck` findings are fixed by bumping the `go` directive in `go.mod` (CI reads the toolchain from it via `go-version-file`). Base images are re-pinned with `make pin-base-images`.

## Extending the Library

- **New event type:** define a Go struct and use `NewEnvelope[YourType](...)`. No changes to the library itself — types are generic. Always pass `WithTenantID(rc.TenantID)`, `WithTraceID(rc.TraceID)`, and `WithSchemaVersion("1")` from the `gincommon.RequestContext` when publishing from an HTTP handler. Register the new type in `EVENT_SCHEMA_GOVERNANCE.md`. For breaking payload changes, mint a new versioned type (e.g. `iam.user.created.v2`) and reset `WithSchemaVersion("1")` — never mutate the existing type's payload in an incompatible way.
- **New Publisher backend** (e.g. EventBridge): implement `port.Publisher` in `internal/adapter/outbound/eventbridge/`, expose a constructor in `pkg/events/`. Follow the SNS adapter as a template — call `otel.Tracer(...)` (not gincommon directly) + Prometheus metrics in the adapter.
- **New Consumer backend** (e.g. Kinesis): implement `port.Consumer` in `internal/adapter/outbound/kinesis/`, expose via `pkg/events/`. `Handler` signature is shared — no changes to calling code. Inject tenant+trace into handler `ctx` using the same helper as `NewSQSConsumer` so `pgcommon` GUC injection works transparently.
- **New outbox store backend** (e.g. DynamoDB): implement `port.OutboxStore` in `internal/core/port/outboxstore.go`, place in `internal/adapter/outbound/dynamooutbox/`, inject via `Runner.Store`. The Postgres implementation should remain the default — only swap if `platform-pgcommon` is not available in the consuming service.
- **New outbox migration:** add `NNN_description.up.sql` / `NNN_description.down.sql` to `pkg/outbox/migrations/`. The embedded FS is recompiled on next build; `outbox.ApplySchema` picks it up automatically via `platform-pgcommon`'s `migrate.Runner`.
- **Replace logger:** the `port.Logger` interface is deliberately kept identical to `platform-gincommon`'s. Do not change method signatures — consuming services pass a single `ZapLogger` instance to both libraries.
- **New Codec implementation** (e.g. AWS Glue Schema Registry): implement `port.Codec` (aliased as `events.Codec`) in the consuming service — this library does not implement one or depend on `aws-sdk-go-v2/service/glue`. Inject via `WithCodec` (publisher) / `WithConsumerCodec` (consumer).
- **New metrics:** add counters/histograms inside `initMetricsWithRegisterer` in `internal/adapter/outbound/metrics/metrics.go`.
- **New use case:** add to `internal/core/service/`, depending only on `domain/` types and `port/` interfaces.
