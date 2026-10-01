# platform-events

The platform's shared **event-driven messaging library** — the single sanctioned path for every service that publishes or consumes domain events. It owns the canonical event envelope, the SNS publisher, the SQS consumer loop, the transactional outbox, consumer-side deduplication (inbox), dead-letter forwarding, HMAC signing, and the messaging-layer observability every service would otherwise re-implement. It is consumed as a private Go module and is **never deployed on its own**.

**Repository:** `github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events`
**Module:** Go 1.26 (`go 1.26.0`, `toolchain go1.26.8`) · private module · library only (`cmd/platform-events` is a reference CLI that validates config and prints version info, not a server — CI builds, scans and smoke-tests it as a container image)
**Design:** Clean Architecture — public API in `pkg/`, AWS/Postgres adapters in `internal/adapter/`, SDK-free core in `internal/core/`. Design narrative, sequence diagrams and invariants: [ARCHITECTURE.md](ARCHITECTURE.md); low-level design (data model, API contract, flows, failure handling): [docs/lld/platform-events-lld.md](docs/lld/platform-events-lld.md). The wire format is byte-compatible with the Python sibling [`platform-eventcommon`](https://github.com/BCBP-SOLUTIONS-FZC-LLC/platform-eventcommon); the `interop` CI job enforces it.

---

## Mental model

| This library owns | It does NOT own |
|---|---|
| `Envelope[T]` — the CloudEvents-aligned wire format (`id` UUID v7, `type`, `source`, `specversion`, `tenant_id`, `trace_id`, `correlation_id`, `subject`, `actor`, `dataschema`, `time`, `data`) | Event type naming and payload schemas — [EVENT_SCHEMA_GOVERNANCE.md](EVENT_SCHEMA_GOVERNANCE.md) and each producing service |
| SNS publishing — message attributes (`EventType`, `TenantID`, `Source`, `EventID`, `Subject`), FIFO group/dedup IDs, batch splitting, retryable-error classification | Topic, queue, subscription and filter-policy provisioning (Terraform/CDK) |
| SQS consumption — long-poll loop, bounded concurrency, visibility extension, graceful drain, RLS `GUCSet` injection, dead-letter-handler routing | SQS `RedrivePolicy` / `maxReceiveCount` values (queue configuration) |
| Transactional outbox — `outbox_events` / `outbox_dead_letters` schema, `Enqueue`, `Runner`, dead-letter inspect/replay/discard, pruning | The business transaction itself (`pgcommon.RunInTx` in the caller) |
| Inbox — `processed_events` dedup ledger and the `inbox.Handler` wrapper | Idempotency of external side effects (payments, emails) — see [Side-effect classification](docs/guides/consuming.md#side-effect-classification) |
| `DLQPublisher` — forwarding a failed message to the queue's already-configured SQS DLQ | Reading, replaying or redriving the SQS DLQ; creating DLQs |
| `Codec` hook + decode-only `GlueDecodeCodec` | A schema-registry client or SDK (services implement `Codec` against their own registry) |
| HMAC-SHA256 `Sign` / `Verify` helpers | Key storage and rotation (Secrets Manager / SSM) |
| Tier 1 `platform_*` Prometheus metrics (Enterprise Platform Observability Standard; legacy `events_*` / `outbox_*` / `sqs_*` in parallel) and OTel spans | Metric/trace exporters and providers (initialised by the consuming service) |

Consuming services never import `github.com/aws/aws-sdk-go-v2/service/sns` or `.../sqs` — depguard rules in service repos forbid it. Every transport operation a service needs is a `platform-events` API; if one is missing, it is added here rather than worked around in the service.

### System invariants

These hold everywhere, always. If a design requires violating one, the design changes — not the invariant.

| Invariant | What it means for you |
|---|---|
| **Delivery is at-least-once** | Every handler may be called more than once for the same `Envelope.ID`. Idempotency is not a nice-to-have. |
| **Outbox ordering is opt-in** | By default the outbox does not preserve publish order (a failed record is published after later ones; replicas publish concurrently), so even FIFO does not give per-aggregate order. For per-aggregate order, enqueue with `outbox.EnqueueOrdered(…, key)`: each key's records are published one at a time, in enqueue order — see [Outbox § Ordering](docs/guides/outbox.md#ordering). |
| **Idempotency is required for all consumers** | No configuration, queue type, or delivery mode removes this requirement. |
| **Events are immutable once published** | An `event_type` and its payload contract are frozen on first production publish. Breaking changes require a new versioned type (`.v2`). |
| **The producer has no knowledge of consumers** | Never check "who is listening" before publishing. Consumers come and go; the event type remains. |
| **Tenant context is always explicit** | Every tenant-scoped event carries `TenantID`. Using `WithSystemTenant()` for a tenant-scoped event is a correctness bug. |
| **Delivery timing is unbounded and not SLA-backed** | Latency is the outbox poll interval + SNS/SQS propagation, and grows under load, backlog drain and replay. Consumers must be correct whether an event arrives in 2 seconds or 2 minutes. |
| **Idempotency must cover all externally observable side effects** | A DB write, an HTTP call, a notification and a cache invalidation are all observable. Protecting only the DB write is incomplete — see [Side-effect classification](docs/guides/consuming.md#side-effect-classification). |

---

## Why this library exists

Every service in the platform needs the same messaging guarantees — no lost events on a crash between the DB commit and the publish, tenant isolation on the consumer side, correct retry/poison handling, and uniform metrics and traces. Centralising them means:

- **One delivery guarantee, implemented once.** The transactional outbox removes the dual-write crash window for every producer; nobody hand-rolls SQL against `outbox_events`.
- **One wire format.** Go and Python services publish to and consume from the same topics and queues; `Envelope` JSON and HMAC canonicalisation are byte-for-byte identical across both libraries.
- **Safe defaults you cannot forget.** The consumer injects `pgcommon.GUCSet` for RLS, extends visibility timeouts on slow handlers, and drains in-flight work on `Stop()`.
- **Business logic free of plumbing.** Handlers receive a typed `Envelope` and a context; retry, visibility extension, metrics and spans are invisible.
- **One version bump propagates.** HMAC, tenant propagation, observability and AWS SDK upgrades land here and reach every service together.

**Design principles:** events are facts, not commands (producers never direct consumers) · fail fast on misconfiguration (`NewSNSPublisher` rejects an empty/invalid `TopicARN`, `NewSQSConsumer` an empty `QueueURL`, `ResolveDLQ` a missing `RedrivePolicy` — at startup, not first use) · make safe usage the default · keep business logic free from messaging plumbing · centralise cross-cutting concerns.

`internal/core/domain` and `internal/core/port` import no AWS SDK (a convention checked in review), and `pkg/*` is the only surface services can import — Go's `internal/` rule blocks the rest.

---

## API overview

Import `pkg/events`, `pkg/outbox`, `pkg/inbox` and `pkg/config`. Never import `internal/`. Godoc (`make godoc`) is the full reference; the tables below are the map.

### `pkg/events` — envelope, publisher, consumer

| Symbol | Purpose |
|---|---|
| `Envelope[T]`, `NewEnvelope(type, source, payload, opts...)` | Typed event; UUID v7 `ID`, UTC `Timestamp`. Opts: `WithTenantID`, `WithTraceID`, `WithCorrelationID`, `WithSystemTenant`, `WithSchemaVersion`, `WithSubject`, `WithActor`, `WithIPAddress`, `WithUserAgent`, `WithSchemaID` |
| `ParseEnvelope[T](data)`, `Envelope.JSON()` | Validate-and-decode / canonical JSON |
| `Publisher`, `NewSNSPublisher(SNSConfig, opts...)` | `Publish` / `PublishBatch`. Opts: `WithMessageGroupID`, `WithMessageDeduplicationID`, `WithAttributes`, `WithCodec` |
| `Consumer`, `Handler`, `NewSQSConsumer(SQSConfig, handler, opts...)` | Long-poll loop (nil handler → error). Opts: `WithConcurrency`, `WithVisibilityTimeout`, `WithDeadLetterHandler`, `WithMaxReceiveCount`, `WithDrainTimeout`, `WithConsumerCodec`, `WithDLQForwarding`, `WithHandlerTimeout`, `WithQueueDepthMetrics`, `WithMalformedBodyLogging` |
| `SourceMessageFromContext(ctx)` | The raw received message (body + attributes) inside a handler — forward this to the DLQ, never `env.JSON()` |
| `NewSQSConsumerWithClient`, `SQSClientLike` | Inject a fake SQS client to test the consumer loop itself |
| `TraceIDFromContext(ctx)` | Envelope `TraceID` inside a handler |
| `Codec`, `NoopCodec`, `GlueDecodeCodec` | Schema-registry hook; decode-only Glue header stripper |
| `Sign`, `Verify`, `SignEnvelope`, `VerifyEnvelope` | HMAC-SHA256, constant-time verify |
| `InitMetrics(MetricsIdentity, registerer, ...MetricsOption)` | Register the Tier 1 `platform_*` metrics (required `domain`/`service`/`environment` labels injected centrally) plus the legacy metrics during the compatibility period; options `WithoutLegacyMetrics()`, `WithEventTypeLimit(n)`, `WithEventTypes(...)`; `MetricsRegistry()`. `Init` / `InitWithRegisterer` are deprecated (legacy only) |

### `pkg/events` — dead-letter forwarding

| Symbol | Purpose |
|---|---|
| `DLQPublisher`, `NewSQSDLQPublisher(DLQConfig)` | `SendToDLQ(ctx, sourceQueueURL, body, attrs, reason)` forwards to the source queue's `RedrivePolicy` DLQ; `ResolveDLQ` fails fast at startup |
| `NewSQSDLQPublisherWithClient`, `DLQClientLike` | Test injection (`GetQueueAttributes`, `GetQueueUrl`, `SendMessage`) |
| `DLQAttrEventType` · `DLQAttrReason` · `DLQAttrOriginalQueue` · `DLQAttrConsumerName` · `DLQAttrFailedAt` | Standard attributes added to every forwarded message |
| `DLQError`, `ErrDLQ*`, `ErrRetryable` | Typed errors — see [Validation and errors](#validation-and-errors). `ErrRetryable` also marks transient publish / codec failures (`BatchFailure.Retryable` in batch errors) that the outbox retries without counting attempts |

### `pkg/events/mock` — test doubles

| Symbol | Purpose |
|---|---|
| `mock.Publisher` | `Published()`, `SetError()`, `SetBatchError()` (partial / `Retryable` batch failures), `Reset()`; rejects envelopes without ID, Type or Source like the SNS publisher |
| `mock.Consumer` | `Inject(env)` delivers synchronously with the real consumer's handler context: tenant GUC (RLS), trace ID, source message, dead-letter attribution; rejects malformed envelopes (`ErrMalformedEnvelope`) and decodes codec payloads with `Codec`, as production does |
| `mock.DLQPublisher` | `Sent()`, `SetError()`, `Reset()`; `ResolveDLQ` returns `DLQURL`; counts the forward and marks the attribution like the SQS publisher (so inbox skips dead-lettered messages in tests too) |

### `pkg/outbox` — transactional outbox

| Symbol | Purpose |
|---|---|
| `Enqueue(ctx, tx, env)` | Insert inside the caller's `pgcommon.Tx` — no SNS call; requires a canonical lowercase UUID `ID`; rejects payloads > 240 KB |
| `EnqueueOrdered(ctx, tx, env, key)` | Same, with an ordering key: a key's records are published one at a time, in enqueue order — [Outbox § Ordering](docs/guides/outbox.md#ordering) |
| `NewRunner(Config)`, `Runner.Start` / `Stop` / `Ready` | Poll → claim (`SKIP LOCKED` + lease) → publish → mark; per-record retry backoff (`RetryBackoff` / `MaxRetryBackoff`), transient failures never count toward `MaxAttempts`; re-polls while batches publish |
| `Runner.ListDeadLetters` / `ReprocessDeadLetters` / `ReprocessDeadLettersWith` / `DiscardDeadLetters` | Inspect / replay / discard `outbox_dead_letters` (`DLQFilter`, `DeadLetterRecord`) |
| `Runner.PrunePublished(ctx, olderThan, limit)` | Batched delete of old published rows |
| `ApplySchema(ctx, migrateRunner)`, `MigrationsTable` | Embedded migrations `001`–`011`, isolated `outbox_migrations` tracking table |

### `pkg/inbox` — consumer-side deduplication

| Symbol | Purpose |
|---|---|
| `Handler(ledger, next)` | Best-effort dedup: skips already-recorded envelope IDs; records an ID only after `next` succeeds (and did not dead-letter it) |
| `NewStore(pool, consumer)`, `Store.Process(ctx, env, fn)` | `processed_events` ledger on a `pgcommon.Pool`; `Process` claims the ID inside one transaction with `fn`'s writes — exactly-once Postgres writes |
| `Store.IsProcessed` / `MarkProcessed` / `Prune` | Ledger primitives and batched retention delete |
| `ApplySchema`, `MigrationsTable` | Embedded schema, own `inbox_migrations` table |

### `pkg/config` — environment wiring

`LoadSNS` / `LoadSQS` / `LoadOutbox` · `SNSConfigFromEnv` · `SQSConfigFromEnv` · `SQSConsumerOptions` · `RunnerConfigFromEnv` · `LogWarnings` / `LogWarningsTo` — see [Environment variables](#environment-variables).

---

## Validation and errors

Invalid configuration is rejected at construction; invalid envelopes and DLQ input are rejected before any AWS call. Branch on errors with `errors.Is` / `errors.As` — never on message strings.

| Error | Returned when |
|---|---|
| `events.ErrEnvelopeIDRequired` / `ErrEnvelopeTypeRequired` / `ErrEnvelopeSourceRequired` | `ParseEnvelope` or `outbox.Enqueue` — missing `id` / `type` / `source` |
| `events.ErrKeyTooShort` | `Sign` / `SignEnvelope` with a key < 32 bytes |
| `events.ErrInvalidSignature` | Exported for forward-compatibility; `Verify` / `VerifyEnvelope` return `bool` |
| `*events.BatchError` | `PublishBatch` partial failure — one `BatchFailure{ID, Code, Message}` per failed message |
| `events.ErrDLQInvalidMessage` | `DLQPublisher` — empty source URL / body / reason, invalid UTF-8, > 10 attributes |
| `events.ErrDLQNotConfigured` | `DLQPublisher` — source queue has no `RedrivePolicy` |
| `events.ErrDLQInvalidRedrivePolicy` | `DLQPublisher` — policy not JSON, no `deadLetterTargetArn`, or not an SQS ARN |
| `events.ErrDLQUnresolved` | `DLQPublisher` — `GetQueueAttributes` / `GetQueueUrl` failed |
| `events.ErrDLQSendFailed` | `DLQPublisher` — `SendMessage` to the DLQ failed |
| `events.ErrRetryable` | Matched by `DLQPublisher` errors whose AWS cause is transient (throttling, service unavailable, network timeout) |

All `DLQPublisher` errors are `*events.DLQError{Kind, SourceQueue, Cause}`; the AWS cause stays reachable with `errors.As`.

```go
if err := dlq.SendToDLQ(ctx, queueURL, body, nil, reason); err != nil {
    if errors.Is(err, events.ErrRetryable) {
        return err // transient — SQS redelivers the original later
    }
    log.Error("dlq forward failed — check the queue's RedrivePolicy / IAM", zap.Error(err))
    return err // original stays on the source queue
}
```

### Notable validation rules

| Rule | Enforcement |
|---|---|
| `TopicARN` must be an SNS ARN | `NewSNSPublisher` rejects empty values and anything without an `arn:aws:sns:` / `arn:aws-cn:sns:` / `arn:aws-us-gov:sns:` prefix |
| `VisibilityTimeout` ≤ 12 h | `NewSQSConsumer` rejects larger values (SQS hard limit) |
| Outbox payload ≤ 240 KB | `outbox.Enqueue` rejects larger envelopes (the SNS limit is 256 KB) and NUL characters anywhere (Postgres `jsonb` cannot store `\u0000`) |
| Malformed message bodies are not retried | Counted as `platform_messages_failed_total{reason="malformed"}` (legacy `events_consumed_total{status="malformed"}`); forwarded verbatim to the queue's DLQ with `WithDLQForwarding`, otherwise deleted immediately |
| `WithMaxReceiveCount(n)` must be **lower** than the queue's `RedrivePolicy` `maxReceiveCount` | Otherwise SQS moves the message before the dead-letter handler runs — see [Forwarding to the SQS DLQ](docs/guides/consuming.md#forwarding-to-the-sqs-dlq) |
| DLQ forwards carry authoritative metadata | `DLQReason`, `OriginalQueue`, `FailedAt`, `ConsumerName` override caller values; `DLQReason` capped at 1 KiB |
| `last_error` is bounded | Truncated to 512 chars in `outbox_events` / `outbox_dead_letters` |

---

## Architecture

Clean Architecture — dependencies point inward; the core never imports an adapter or the AWS SDK. Layer diagrams, sequence diagrams, the failure lifecycle and the invariant table are in **[ARCHITECTURE.md](ARCHITECTURE.md)**; standalone Mermaid sources live in **[`docs/architecture/`](docs/README.md)**.

```
platform-events/
├── cmd/platform-events/               # Reference CLI — prints version + resolved config, -strict validates (make build → bin/platform-events)
├── pkg/                               # Public API — the only packages services may import
│   ├── events/                        # Envelope, Publisher, Consumer, DLQPublisher, Codec, GlueDecodeCodec, HMAC, InitMetrics / MetricsRegistry
│   │   └── mock/                      # mock.Publisher, mock.Consumer, mock.DLQPublisher
│   ├── outbox/                        # Enqueue, EnqueueOrdered, Runner, dead-letter API, ApplySchema + embedded migrations/ (001–011)
│   ├── inbox/                         # processed_events ledger (Store.Process, Handler), ApplySchema + migrations/
│   └── config/                        # Env loaders + wiring helpers
├── internal/
│   ├── core/
│   │   ├── domain/                    # Envelope, OutboxRecord, DLQFilter, DLQError, sentinel errors — no external deps
│   │   ├── port/                      # Publisher, Consumer, Codec, Logger, Clock, OutboxStore, DLQPublisher, DLQAttribution, SourceMessage
│   │   └── service/                   # OutboxService (publish cycle, retry/backoff classification), HMACService
│   └── adapter/outbound/
│       ├── sns/                       # SNS publisher, batch split (10 entries / 256 KiB), failure classification
│       ├── sqs/                       # SQS consumer loop, DLQ publisher (RedrivePolicy resolution + cache), queue-depth sampler
│       ├── outboxstore/               # Postgres outbox store (platform-pgcommon only)
│       └── metrics/                   # Tier 1 platform_* + legacy metrics; registry.go = Platform Observability Registry entry
├── docs/
│   ├── architecture/mermaid/          # 13 × .mmd diagram sources (embedded in ARCHITECTURE.md)
│   ├── guides/                        # Detailed how-to guides (linked throughout this README)
│   ├── lld/                           # Low-level design (platform-events-lld.md)
│   └── observability/                 # Observability standard: model, generated metrics registry, runbook
├── monitoring/                        # Reference bundle: prometheus/ (rules, SLO, alerts, promtool tests), grafana/ (dashboard), kubernetes/ (KEDA)
├── scripts/merge_coverage.py          # Merges per-suite coverage profiles (max-count)
├── .github/workflows/ + scripts/      # CI: ci, validate-test, validate-quality, changelog-check, release
├── Dockerfile · .docker-digests       # Reference-CLI image (digest-pinned) for CI build / Trivy / smoke
├── .githooks/pre-commit               # tidy (+ drift check) + fmt-check + lint; installed via `make setup`
├── test/                              # own module: unit/ (no Docker), integration/ + e2e/ (testcontainers: one shared floci per package), smoke/ (live AWS), fixtures/
└── tools/                             # own module: golangci-lint (go tool -modfile=tools/go.mod)
```

### Dependency rules

| Component | May depend on |
|---|---|
| `internal/core/domain` | Nothing internal; no AWS SDK |
| `internal/core/port` | `domain` only |
| `internal/core/service` | `domain`, `port` |
| `internal/adapter/outbound/*` | `domain`, `port`, `metrics`; the AWS SDK is confined here (pgx is never imported — platform-pgcommon only) |
| `pkg/*` | Adapters and core; re-exports the public surface via type aliases |
| `cmd/`, `test/` | Everything above |

The layer rules are a convention checked in review; CI enforces the depguard rule `pgcommon-only` (no `pgx`, `database/sql` or `golang-migrate` imports anywhere but `test/smoke`), and Go's `internal/` rule keeps services on `pkg/`.

### Storage and messaging

| Concern | Technology | Notes |
|---|---|---|
| **Outbound events** | AWS SNS (standard or FIFO) | Attributes `EventType` · `TenantID` · `Source` · `EventID` · `Subject` for filter policies; `PublishBatch` splits at 10 entries and 256 KiB per request |
| **Inbound events** | AWS SQS (standard or FIFO) | Long poll (≤ 20 s), visibility extended from receipt, `ApproximateReceiveCount`-based dead-letter routing; FIFO message groups processed in order (one worker per group, a failure or failed delete holds the group; one in flight per group per replica); `RawMessageDelivery=true` required on SNS subscriptions |
| **Dead letters (producer)** | Postgres `outbox_dead_letters` | Records that exhausted `MaxAttempts`; managed via the `Runner` DLQ API |
| **Dead letters (consumer)** | The queue's SQS DLQ | Via `RedrivePolicy`, or forwarded explicitly with `DLQPublisher` |
| **Outbox / inbox** | PostgreSQL via `platform-pgcommon` | `outbox_events` (with optional per-key ordering), `outbox_dead_letters`, `processed_events`; own migration tracking tables |
| **Schema registry** | Pluggable `Codec` | No SDK dependency; `GlueDecodeCodec` strips the AWS Glue header on consume |

### Shared library dependencies

| Library | Version | Purpose |
|---|---|---|
| `platform-pgcommon` | v1.4.3 | `pgcommon.Pool`, `RunInTx`, `ConfigFromEnv`, RLS `GUCSet` injection, `migrate.Runner` for the outbox/inbox schemas, `Tx`/`Conn`/`Rows` aliases (pgx is never imported directly) |
| `platform-gincommon` | — (not a dependency) | Interface-compatible only: its `ZapLogger` satisfies `port.Logger`, and its `RequestContext` supplies `TenantID` / `TraceID` for envelopes |

---

## Integrating into a service

### 1. Prerequisites

This is a **private module**. Configure Go before fetching:

```bash
go env -w GOPRIVATE=github.com/BCBP-SOLUTIONS-FZC-LLC/*
git config --global url."ssh://git@github.com/".insteadOf "https://github.com/"   # SSH key registered with the org
go get github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events@v1.6.0
```

**CI / Docker builds:** add a GitHub PAT (classic `repo` scope, or fine-grained Contents: Read on all `BCBP-SOLUTIONS-FZC-LLC/*` repos) as the repository secret `GO_PRIVATE_TOKEN`, and configure git credentials before `go mod download`:

```yaml
- name: Configure private module access
  run: |
    git config --global credential.helper store
    echo "https://x-access-token:${{ secrets.GO_PRIVATE_TOKEN }}@github.com" > ~/.git-credentials
    chmod 600 ~/.git-credentials
```

> Set `persist-credentials: false` on `actions/checkout` so `GITHUB_TOKEN` (current-repo-only) does not override `GO_PRIVATE_TOKEN` when git fetches other private modules.

A complete end-to-end wiring example is in **[Quick start](docs/guides/quick-start.md)**.

### 2. Publishing — always through the outbox

Every domain event tied to a DB write goes through `outbox.Enqueue` inside `pgcommon.RunInTx`; the `outbox.Runner` publishes it. Direct `publisher.Publish` is only for best-effort notifications where loss is acceptable. Decision table, crash-window explanation and anti-patterns: [Publishing](docs/guides/publishing.md). Envelope construction: [Event envelope](docs/guides/envelope.md). Runner wiring, poll cycle, dead letters and pruning: [Transactional outbox](docs/guides/outbox.md).

### 3. Consuming

`NewSQSConsumer` + a `Handler`. Return `nil` to delete, an `error` to retry; return `nil` (and log) for permanent failures. Handler contract, error classification, poison messages, ordering and concurrency: [Consuming](docs/guides/consuming.md).

### 4. Idempotency

Wrap handlers with `inbox.Handler(store, next)` (best-effort: the check, the handler and the record are separate transactions), or — for Postgres writes — run them through `store.Process(ctx, env, func(ctx, tx) error {…})`, which claims the ID inside the handler's transaction so the writes happen exactly once. Neither records a message the handler dead-lettered, so a DLQ redrive is processed — [Implementing idempotency](docs/guides/consuming.md#implementing-idempotency).

### 5. Dead-letter forwarding

To keep a poison message rather than drop it, forward it with `DLQPublisher.SendToDLQ` — never with a direct SQS SDK call. Call `ResolveDLQ` at startup, and set `WithMaxReceiveCount(n)` below the queue's `maxReceiveCount`. The service role needs `sqs:GetQueueAttributes` (source queue) and `sqs:GetQueueUrl` + `sqs:SendMessage` (DLQ). Full guide: [Forwarding to the SQS DLQ](docs/guides/consuming.md#forwarding-to-the-sqs-dlq).

### 6. Schema registry (optional)

Producers pass `WithCodec`; consumers of Glue-encoded events pass `WithConsumerCodec(events.GlueDecodeCodec{})`. Envelopes with an empty `dataschema` (or a `dataschema` on a non-string `data`) never reach a codec, so mixed traffic is safe — [Codec](docs/guides/codec.md).

### 7. Wire format

SNS→SQS subscriptions **must** use `RawMessageDelivery=true` — without it every message is an SNS notification wrapper, which the consumer treats as malformed (forwarded to the DLQ with `WithDLQForwarding`, otherwise deleted) and never passes to the handler. The body is the envelope JSON; with a codec, `data` is a base64 string and `dataschema` holds the schema version ID. Envelope compatibility classes: [ARCHITECTURE.md § Envelope compatibility guarantees](ARCHITECTURE.md#envelope-compatibility-guarantees).

### 8. Handling errors

See [Validation and errors](#validation-and-errors). For what a handler should return: [Error classification](docs/guides/consuming.md#error-classification).

### 9. Testing in your service

Use `pkg/events/mock` — no AWS emulator needed: [Testing in consuming services](docs/guides/testing-in-services.md). To test consumer-loop behaviour itself, inject a fake client with `NewSQSConsumerWithClient` — [Testing with an injected client](docs/guides/consuming.md#testing-with-an-injected-client).

### 10. Background work you should schedule

| Job | How | Why |
|---|---|---|
| Outbox runner | `runner.Start(ctx)` in a goroutine; `runner.Stop()` on SIGTERM; gate readiness on `runner.Ready()` | Publishes enqueued events |
| Outbox pruning | `runner.PrunePublished(ctx, olderThan, limit)` daily; `olderThan` ≥ the longest consumer idempotency window (≥ 7 days) | Bounds `outbox_events` growth |
| Inbox pruning | `store.Prune(ctx, retention, inbox.DefaultPruneBatch)`; retention longer than the 7-day SQS message lifetime | Bounds `processed_events` growth |
| Dead-letter review | `ListDeadLetters` → `ReprocessDeadLettersWith` / `DiscardDeadLetters` (operator action) | Recovers publish failures |

Production defaults, backpressure tuning and the service adoption checklist: [Operations](docs/guides/operations.md).

---

## Local development

### Prerequisites

- Go 1.26+ (the `toolchain go1.26.8` line makes the go command fetch 1.26.8 automatically for this repository's own builds; consumers are not pinned to a patch release)
- Docker — testcontainers-go starts floci (`floci/floci:2.1.0`) + Postgres for the integration and e2e suites; `make docker-up` for manual runs
- `GOPRIVATE=github.com/BCBP-SOLUTIONS-FZC-LLC/*` (`GONOSUMDB` too) and an SSH key registered with the BCBP org — the Makefile exports both

### Setup

```bash
git clone https://github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events
cd platform-events
make setup       # copies .env-example → .env and installs .githooks/pre-commit (run once)
make tidy        # go mod tidy
make test-unit   # no Docker needed
make docker-up   # optional: floci (SNS + SQS) on :4574, floci-ui on :4505, Postgres on :5538
```

### Module layout

Three Go modules, the same layout as platform-pgcommon, so consuming services inherit only what the library itself imports: `.` (the library), `test/` (every suite and fixture; `replace …platform-events => ../`) and `tools/` (golangci-lint, run through `go tool -modfile=tools/go.mod`). The library's `go.mod` went from 289 to 55 lines, and testcontainers and the linter's dependency tree are gone from it. White-box tests beside the sources (`internal/core/service/*_test.go`) stay in the root module. `make tidy`, `vet`, `lint`, `mod-verify` and every `test-*` target cover all three modules.

### Common commands

| Command | Description |
|---|---|
| `make setup` | Copy `.env-example` → `.env` and install `.githooks/pre-commit` |
| `make install-hooks` | (Re)install `.githooks/pre-commit` into `.git/hooks` — tidy (fails on `go.mod`/`go.sum` drift) + `fmt-check` + `lint` before every commit |
| `make tidy` / `make fmt` / `make fmt-check` | Go basics; `fmt-check` mirrors CI and does not modify files |
| `make vet` | `go vet` — default build plus every test build tag (`integration`, `e2e`) |
| `make lint` | `golangci-lint` via `go tool` — default build plus every test build tag |
| `make metrics-lint` | Observability Standard gate: registered-collector conformance (tiers, naming, required labels, vocabulary, registry parity), governance of rule files, dashboards and autoscaling manifests, inventory drift |
| `make metrics-doc` | Regenerate `docs/observability/metrics-registry.md` from the metrics registry |
| `make rules-check` | `promtool check rules` + alert unit tests for `monitoring/prometheus/` (Docker) |
| `make dashboards-check` | PromQL syntax gate for `monitoring/grafana/*.json`: every panel and variable query checked with promtool (Docker, jq) |
| `make toolchain-check` | The Go toolchain is identical in the three `go.mod` files and the Dockerfile builder image |
| `make docs-check` | Diagram drift gate: every `docs/architecture/mermaid/*.mmd` embedded byte-identically in `ARCHITECTURE.md` |
| `make pin-base-images` | Re-pin every image this repository runs by digest: Dockerfile, promtool, docker-compose, testcontainers fixtures (recorded in `.docker-digests`) |
| `make mod-verify` | `go mod verify` |
| `make vuln-check` | `govulncheck` (pinned version) on `./internal/...` + `./pkg/...` |
| `make test` | Unit + integration in parallel (Docker required; e2e is separate) |
| `make test-ci` | Unit + integration + e2e in parallel with `-race`; per-suite profiles in `.coverage/`, merged into `coverage.out` (used in CI) |
| `make test-unit` | Unit tests only, no Docker |
| `make test-integration` (alias `test-int`) | Integration tests — floci + Postgres via testcontainers |
| `make test-e2e` | Full outbox pipeline: Postgres → runner → SNS → SQS → consumer |
| `make test-smoke` | Live AWS (`SMOKE_SNS_TOPIC_ARN` / `SMOKE_SQS_QUEUE_URL`) — manual, never in CI |
| `make race` | All three suites with `-race`, no coverage merge |
| `make build` | Compile `bin/platform-events` and verify library packages compile |
| `make cover` / `make cover-func` | Coverage HTML report / per-function summary (runs `test-ci`) |
| `make ci` | `tidy` + `mod-verify` + `fmt-check` + `vet` + `lint` + `docs-check` + `metrics-lint` + `rules-check` + `dashboards-check` + `test-ci` + `build` — the same gates as CI |
| `make docker-up` / `make docker-down` | Start/stop floci + floci-ui + Postgres (demo topology provisioned) |
| `make docker-build` | Build the reference-CLI image the way CI does (needs `GO_PRIVATE_TOKEN`) |
| `make pin-base-images` | Re-pin the Dockerfile base-image digests (updates `Dockerfile` + `.docker-digests`) |
| `make godoc` | Serve package docs via pkgsite at http://localhost:8080 |
| `make clean` | Remove `bin/`, `.coverage/` and coverage files |

When suites run in parallel their logs interleave; a failing suite re-prints its `--- FAIL:` lines in a summary block at the end.

### Running a single test

```bash
cd test   # the suites are their own module
go test ./unit/sqs/...    -run TestSendToDLQ_Success_PopulatesAttributes -v
go test ./integration/... -tags=integration -run TestDLQPublisher_ForwardsToRedriveTarget -v
go test -short -tags=integration ./integration/...   # -short skips every test that needs Docker
```

### Developer tools

`make godoc` serves the full package documentation (every exported symbol is documented). The `docs/architecture/mermaid/*.mmd` diagrams render natively on GitHub — see [docs/README.md](docs/README.md) for local rendering.

---

## Testing events locally

### How the pipeline works

```
service write ──(same pgcommon.Tx)──▶ outbox_events (Postgres)
                                        │
                              outbox.Runner (PollInterval, default 5 s)
                                        │
                                        ▼
                                  SNS topic ──(RawMessageDelivery=true)──▶ SQS queue
                                                                              │
                                                  events.Consumer → inbox.Handler → your Handler
                                                                              │ (poison)
                                                        events.DLQPublisher → the queue's SQS DLQ
```

### Step 1 — Start infrastructure

```bash
make docker-up    # floci (SNS + SQS) on :4574, floci-ui on http://localhost:4505, Postgres (platform_events_dev) on :5538
```

[floci](https://floci.io) is the open-source (MIT), always-free AWS emulator the platform uses instead of LocalStack, the same as iam-org-membership. Its ready hook, `scripts/init-floci.sh`, provisions a demo topology that `.env-example` already points at:

| Resource | Name |
|---|---|
| SNS topic | `platform-events-demo` (`SNS_TOPIC_ARN`) |
| SQS queue | `platform-events-demo-q` (`SQS_QUEUE_URL`), subscribed with `RawMessageDelivery=true` |
| DLQ | `platform-events-demo-q-dlq`, the demo queue's `RedrivePolicy` target (`maxReceiveCount=5`), for exercising `DLQPublisher` / `WithDLQForwarding` |

The host ports are unique across the org's local stacks, so platform-events runs alongside any IAM service. Override them with `FLOCI_PORT`, `FLOCI_UI_PORT` and `POSTGRES_PORT`.

### Step 2 — Verify the topology

```bash
docker compose exec floci aws --region us-east-1 sns list-topics
docker compose exec floci aws --region us-east-1 sqs list-queues
```

`--region` is required: the CLI in the floci image defaults to `us-east-1`, and floci treats region as an isolation boundary. From the host, use `AWS_ENDPOINT_URL=http://localhost:4574` with credentials `test` / `test`.

### Step 3 — Watch delivery in the browser (floci-ui)

Open **http://localhost:4505** → **Integration → SQS**. The **Messages** column shows live counts per queue. Publish a test event and refresh to see it land on `platform-events-demo-q`:

```bash
docker compose exec floci aws --region us-east-1 sns publish \
  --topic-arn arn:aws:sns:us-east-1:000000000000:platform-events-demo \
  --message '{"id":"demo-1","type":"demo.thing.happened"}' \
  --message-attributes 'EventType={DataType=String,StringValue=demo.thing.happened}'
```

floci-ui shows queue and topic metadata, not message bodies. Use the CLI in Step 4 for those.

### Step 4 — Inspect messages

```bash
Q=http://floci:4566/000000000000/platform-events-demo-q
# Peek without deleting — the message becomes visible again after the visibility timeout.
docker compose exec floci aws --region us-east-1 sqs receive-message --queue-url "$Q" \
  --max-number-of-messages 10 --message-attribute-names All

# Forwarded poison messages carry DLQReason / OriginalQueue / FailedAt / ConsumerName.
docker compose exec floci aws --region us-east-1 sqs receive-message --queue-url "$Q-dlq" --message-attribute-names All
```

### Step 5 — Inspect the outbox

```bash
docker compose exec postgres psql -U postgres -d platform_events_dev -c \
  "SELECT id, event_type, published_at IS NOT NULL AS published, attempts FROM outbox_events ORDER BY created_at DESC LIMIT 20;"
```

### Troubleshooting events

| Symptom | Likely cause | Fix |
|---|---|---|
| Consumer deletes every message as `malformed` | Subscription without `RawMessageDelivery=true` — the body is an SNS notification envelope | Recreate the subscription with raw delivery |
| `outbox_events` row never gets `published_at` | Runner not started, wrong `SNS_TOPIC_ARN`, or SNS unreachable | Check runner logs and `outbox_events.last_error`; retryable errors self-heal |
| Rows land in `outbox_dead_letters` | Permanent publish failure after `MaxAttempts` | `runner.ListDeadLetters` → fix → `ReprocessDeadLettersWith` |
| Messages keep reappearing after `receive-message` | Normal — visibility timeout, not deletion | Use `delete-message` |
| `SendToDLQ` returns `ErrDLQNotConfigured` | Source queue has no `RedrivePolicy` | Step 3 above, or fix the queue's Terraform/CDK |
| Dead-letter handler never fires | `WithMaxReceiveCount(n)` ≥ the queue's `maxReceiveCount` — SQS redrives first | Set `n` below `maxReceiveCount` |

---

## Testing

### Canonical tests (do not break)

- **`test/e2e/outbox_test.go`** — the full outbox pipeline end to end, including `TestOutbox_RollbackDoesNotPublish`: a rolled-back transaction's event must never be published.
- **`test/integration/outboxstore_test.go`** — claiming, attempt counting and dead-lettering against real Postgres.
- **`test/unit/sqs/consumer_test.go`** — consumer-loop semantics: retry-vs-delete, visibility extension, drain, and dead-letter routing on `ApproximateReceiveCount > n`.
- **`test/unit/sqs/dlq_test.go`** + **`test/integration/dlq_test.go`** — `RedrivePolicy` parsing, ARN resolution, caching, error classification, and a real forward against floci.
- **Interop** — `platform-interop-tests` checks `Envelope` JSON and HMAC canonicalisation byte-for-byte against the Python library.

### Coverage

CI (`Validate / Test`: `make test-ci`, then `.github/scripts/coverage-gate.sh`) fails below **97%** total (the same gate as platform-pgcommon), measured over `./internal/...` + `./pkg/...` (`COVER_PKG_LIST`). Tests live in the separate `test/` module, so every run uses `-coverpkg`. `make test-ci` merges the root (white-box) / unit / integration / e2e profiles with `scripts/merge_coverage.py` (max-count). The current merged total is **98.5%** (verified 2026-10-01).

---

## Environment variables

Read by `pkg/config` (`LoadSNS` / `LoadSQS` / `LoadOutbox`; database settings via platform-pgcommon's `ConfigFromEnv`). The library reads nothing implicitly — services opt in by calling the loaders. The two exceptions follow platform-pgcommon exactly: `InitMetrics` fills an empty metrics `environment` from `APP_ENV` → `ENVIRONMENT` → `dev`, and `MetricsIdentityFromEnv` takes `service` from `APP_NAME`. **Tracing and logging are configured by the service, not this library:** the `OTEL_*` variables are read by platform-gincommon's `InitTracingFromEnv` (platform-events only uses the global tracer provider it installs), and every component logs through the `port.Logger` the service injects (e.g. gincommon's `ZapLogger`). `config.LoadOTel` is deprecated.

| Variable | Default | Purpose |
|---|---|---|
| `AWS_REGION` (then `AWS_DEFAULT_REGION`) | `us-east-1` | SNS and SQS clients |
| `AWS_ENDPOINT_URL` | — | `http://localhost:4574` for the local floci stack |
| `SNS_TOPIC_ARN` | — | Required for the SNS publisher |
| `SQS_QUEUE_URL` | — | Required for the SQS consumer |
| `SQS_MAX_MESSAGES` / `SQS_WAIT_SECONDS` | `10` / `20` | Batch size (1–10) / long-poll duration |
| `SQS_VISIBILITY_TIMEOUT` | `30s` | ≥ 2× p99 handler duration; ≤ 12 h |
| `SQS_CONCURRENCY` | `1` | Parallel handler goroutines |
| `SQS_MAX_RECEIVE_COUNT` | `0` (unset) | `WithMaxReceiveCount`; must be **below** the queue's `RedrivePolicy` `maxReceiveCount`. Takes effect with `WithDeadLetterHandler` and/or `WithDLQForwarding`, either of which defaults it to 5 when unset |
| `SQS_DRAIN_TIMEOUT` | `30s` | `WithDrainTimeout`: how long `Stop` waits for in-flight handlers; keep it below the pod's `terminationGracePeriodSeconds` |
| `SQS_HANDLER_TIMEOUT` | — (off) | `WithHandlerTimeout`: cancels a handler's context after this long and stops extending its message's visibility, so a hung handler's message is redelivered instead of held forever |
| `OUTBOX_GAUGE_INTERVAL` | `15s` | How often the runner refreshes the backlog gauges (two `COUNT` queries capped at 100k rows), independent of `OUTBOX_POLL_INTERVAL` |
| `SQS_QUEUE_DEPTH_INTERVAL` | — (off) | `WithQueueDepthMetrics`: samples `platform_queue_depth` / `platform_dlq_depth` every interval (min 10s). Needs `sqs:GetQueueAttributes` on the queue and its DLQ; skipped when `InitMetrics` hasn't run |
| `OUTBOX_POLL_INTERVAL` / `OUTBOX_BATCH_SIZE` / `OUTBOX_MAX_ATTEMPTS` | `5s` / `50` / `5` | Runner cadence, batch size, attempts before dead-letter |
| `OUTBOX_CLAIM_LEASE_DURATION` | `10m` (store default when unset) | How long a claimed record is hidden from other runners |
| `OUTBOX_STARTUP_JITTER` | `0` | Use `5s`–`10s` with multiple replicas |
| `OUTBOX_PUBLISH_CONCURRENCY` / `OUTBOX_PUBLISH_TIMEOUT` / `OUTBOX_DRAIN_TIMEOUT` | `1` / `10s` / `30s` | `1` uses SNS `PublishBatch`; per-record timeout; `Stop()` wait bound |
| `OUTBOX_RETRY_BACKOFF` / `OUTBOX_MAX_RETRY_BACKOFF` | `1s` / `5m` | Delay before a failed record's next attempt: base·2^(attempt−1), capped, with jitter. Transport errors, throttling and timeouts never count toward `MAX_ATTEMPTS`; they back off together and reset on the next successful publish |
| `DATABASE_URL` | — | Postgres DSN for the outbox runner. Read by platform-pgcommon's `ConfigFromEnv` (exposed as `config.LoadOutbox().DB`); alternatively `PG_HOST`/`PG_PORT`/`PG_USER`/`PG_PASSWORD`/`PG_DBNAME`/`PG_SSLMODE`. Pool tuning (`PG_MAX_CONNS`, `PG_STATEMENT_TIMEOUT`, `PG_LOCK_TIMEOUT`, `PG_BOUNCER_MODE`, …) per platform-pgcommon |
| `MIGRATION_DATABASE_URL` | `DATABASE_URL` | DDL-role DSN for `ApplySchema`, connecting directly (not via PgBouncer) — `config.LoadOutbox().MigrationDatabaseURL` |
| `APP_ENV` → `ENVIRONMENT` / `APP_NAME` | `dev` / — | Metrics `environment` / `service` labels when not set on `MetricsIdentity` (same as platform-pgcommon) |
| `OTEL_*` | — | Not read by platform-events — platform-gincommon's `InitTracingFromEnv` in the service reads them (`config.LoadOTel` is deprecated) |
| `SMOKE_SNS_TOPIC_ARN` / `SMOKE_SQS_QUEUE_URL` | — | `make test-smoke` only |

`DLQConfig` (`Region`, `EndpointURL`, `ConsumerName`) has no env loader — wire it from the service's own config. Copy `.env-example` to `.env` via `make setup`.

---

## Security

| Topic | Guidance |
|---|---|
| **Transport ownership** | Services never import the SNS/SQS SDK; all transport — including DLQ forwarding — goes through this library, so IAM use and error handling are reviewed in one place |
| **Tenant isolation** | The consumer injects `pgcommon.GUCSet{TenantID}` per message before the handler runs, so downstream pool calls enforce RLS for the event's tenant |
| **HMAC** | For webhook / external-ingress authentication, **not** the SNS/SQS path. Keys ≥ 32 bytes from Secrets Manager / SSM; `Verify` is constant-time (`hmac.Equal`) — [HMAC helpers](docs/guides/hmac.md) |
| **Least-privilege IAM** | Publisher: `sns:Publish`. Consumer: `sqs:ReceiveMessage`, `sqs:DeleteMessage`, `sqs:ChangeMessageVisibility`. DLQ forwarding: `sqs:GetQueueAttributes` (source), `sqs:GetQueueUrl` + `sqs:SendMessage` (DLQ), plus KMS for encrypted queues |
| **Payload data** | Do not forward PII or credentials to audit topics or DLQs without confirming the destination's access controls and retention |
| **Vulnerability reporting** | [SECURITY.md](SECURITY.md) |

---

## Observability

### Metrics

platform-events implements the **Enterprise Platform Observability Standard**. As a cross-domain platform library, all of its metrics are Tier 1 `platform_*`. Call `events.InitMetrics` once at startup, with the same registerer and identity you pass to platform-pgcommon's `pgmetrics.InitWithIdentity`:

```go
warnings, err := events.InitMetrics(events.MetricsIdentity{Domain: "iam", Service: "event-consumer", Version: buildVersion}, registry)
// err: invalid identity / legacy registration failure. warnings: a platform_* metric the
// registry refused (e.g. an existing platform_retry_total with other labels) — disabled, not fatal.
```

- **Consume (Canonical):** `platform_messages_received_total{queue}`, `platform_messages_processed_total{queue,event_type}`, `platform_messages_failed_total{queue,event_type,reason}`, `platform_retry_total{operation,event_type}`, `platform_dlq_messages_total{operation,event_type,reason}`. Each delivery is received once and ends processed, failed or dead-lettered, and a dead-lettered message is counted once whoever forwarded it.
- **Proposed** (shadow-emitted until ratified): `platform_queue_depth` / `platform_dlq_depth` (opt-in: `WithQueueDepthMetrics`, the only way services can get SQS depth into Prometheus), `platform_duplicate_messages_total`, `platform_dependency_request_seconds{dependency,operation,outcome}` (SNS, SQS and codec calls), `platform_event_propagation_seconds`, `platform_messages_published_total`, `platform_message_processing_duration_seconds`, `platform_outbox_{pending,leased}_events`, `platform_outbox_publish_attempts_total`, `platform_outbox_errors_total`, `platform_outbox_dead_letter_operations_total`, `platform_telemetry_label_overflow_total`, `platform_library_info`.
- **Legacy** (Deprecated, emitted in parallel until `WithoutLegacyMetrics()`): `events_*`, `outbox_*`, `sqs_*`, `platform_events_build_info`.
- **Required labels** `domain`, `service`, `environment` on every Tier 1 metric; `queue` / `topic` are names, never URLs or ARNs; `tenant_id`, `event_id` and other unbounded labels are prohibited.

Registry, label vocabulary and ratification packets: [docs/observability/metrics-registry.md](docs/observability/metrics-registry.md) (generated). Model, wiring and migration: [docs/observability/README.md](docs/observability/README.md). Reference rules, SLO and alerts: [`monitoring/prometheus/`](monitoring/prometheus/), with [runbook](docs/observability/runbook.md). Reference dashboard: [`monitoring/grafana/`](monitoring/grafana/). Reference autoscaling: [`monitoring/kubernetes/`](monitoring/kubernetes/). CI enforces all of it (`make metrics-lint`, `make rules-check`).

### Tracing and logs

The publisher starts an `sns.publish` span and injects the OTel propagation headers (W3C `traceparent`, baggage) into message attributes; the consumer starts an `sqs.receive` span **linked** (not parented) to the producer span and propagates baggage into the handler context. OTel is initialised by the consuming service — without a provider the spans are no-ops. Logs go through `port.Logger` (gincommon's `ZapLogger` fits directly); bind `event_id`, `event_type`, `trace_id`, `tenant_id` on every handler log line — [Logging correlation](docs/guides/observability.md#logging-correlation).

---

## Releasing

This is a library — it is never deployed; consuming services pin a Git tag. The release also publishes cross-compiled reference-CLI binaries and a signed CLI image to GHCR, so the exact build that passed the CVE scan is traceable.

| Version bump | When |
|---|---|
| **MAJOR** | Breaking change in the `pkg/*` public API or the envelope wire format |
| **MINOR** | New backward-compatible capability (e.g. `DLQPublisher`) |
| **PATCH** | Bug fix, performance improvement, documentation correction |

```bash
git tag -a v1.6.0 -m "v1.6.0"
git push origin v1.6.0     # triggers release.yml
```

`release.yml` is the **same pipeline as `ci.yml`**, run at the tag: a fast `Verify tag + CHANGELOG` job (tag = checkout, `## [X.Y.Z]` section present), then the identical `Validate / Test`, `Validate / Quality`, `Build image (cache)`, `Trivy CVE scan` (CRITICAL / HIGH / UNKNOWN), `Smoke tests` and `Cross-language compatibility` gates, plus 5-platform CLI binaries. Only after **all** of them pass does `Push image → GHCR` publish the image (`vX.Y.Z`, `vX.Y`, `vX`, `latest`) — a cache hit of the exact image that was scanned and smoke-tested — with SBOM, SLSA provenance and a Cosign keyless signature, and `GitHub Release` publishes the notes from the matching `CHANGELOG.md` section with binaries, checksums, SBOM and provenance attached. SemVer rules and supported versions: [VERSIONING.md](VERSIONING.md).

---

## CI

Five workflow files, mirroring `iam-org-membership` — the org's reference pipeline, whose job names are the required status checks on `main`:

- **`docs.yml`** — on docs-only changes to `ARCHITECTURE.md` / `docs/architecture/**` (which `ci.yml` skips): the diagram sync check.
- **`ci.yml`** — orchestrator on push / PR to `main`. `Validate / Test`, `Validate / Quality` and `Build image (cache)` run in parallel; `Trivy CVE scan` and `Smoke tests` gate on the test job and the image; `Cross-language compatibility` (`platform-interop-tests`) gates on the test job; `PR summary` posts one status comment per PR; on push to `main`, `Push image → GHCR` publishes and Cosign-signs the reference-CLI image. Docs-only commits (`**.md`, `docs/architecture/**`, `docs/guides/**`) skip the pipeline.
- **`validate-test.yml`** (reusable) — `make test-ci` (unit + integration + e2e in parallel, `-race`, merged coverage) → coverage gate (**≥ 97%**, `.github/scripts/coverage-gate.sh`) → uploads `coverage.out`.
- **`validate-quality.yml`** (reusable) — `go mod verify` → HTML-entity check on workflow files → RLS-6 check (no non-`LOCAL` `SET app.tenant_id`) → `gofmt` → `go mod tidy` drift → `make vet` → `make lint` (both with the `integration,e2e` tags) → `make metrics-lint` (Observability Standard conformance) → `make rules-check` (promtool) → `make dashboards-check` (dashboard PromQL) → `make docs-check` (diagram sync) → `make toolchain-check` → `make vuln-check` → digest-pinning check (Dockerfile, docker-compose and the testcontainers images).
- **`changelog-check.yml`** — fails a PR touching `internal/`, `pkg/` or `cmd/` without a `CHANGELOG.md` update.
- **`release.yml`** — on `v*.*.*` tags: the same job graph as `ci.yml` plus verify / binaries / publish — see [Releasing](#releasing).

| Required check (org ruleset) | Job |
|---|---|
| `Validate / Test / test` | `validate-test.yml` via `ci.yml` |
| `Validate / Quality / quality` | `validate-quality.yml` via `ci.yml` |
| `Build image (cache)` | Hadolint → `.dockerignore` check → Buildx build into GHA/registry cache |
| `Trivy CVE scan` | Fails on fixable CRITICAL / HIGH / UNKNOWN in the image (OS layer **and** the Go binary's modules + stdlib); uploads SARIF + CycloneDX SBOM |
| `Smoke tests` | Image ≤ 50 MB; the CLI exits non-zero with no configuration (`-strict`); prints `platform-events <version>` |
| `PR summary` | Posts / updates the PR status comment |

`make test-smoke` (live AWS) is intentionally excluded — run it manually before the first deploy to a new AWS account.

**Required GitHub Actions secret: `GO_PRIVATE_TOKEN`** — fetches `platform-pgcommon` in every Go job and is passed to `docker build` as the `go_private_token` build secret (never written to an image layer). Optional: `CI_REPO_READ_TOKEN` (checkout; falls back to `GITHUB_TOKEN`) and `GH_PRIVATE_TOKEN` (interop; falls back to `GO_PRIVATE_TOKEN`).
---

## Docker

### What the bundled `docker-compose.yml` starts

| Container | Image | Host port | Purpose |
|---|---|---|---|
| `floci` | `floci/floci:2.1.0-compat` | `4574` (`FLOCI_PORT`) | SNS + SQS (`us-east-1`, account `000000000000`, credentials `test` / `test`); `scripts/init-floci.sh` provisions the demo topic, queue and DLQ |
| `floci-ui` | `floci/floci-ui:0.5.0` | `4505` (`FLOCI_UI_PORT`) | Web console for floci: queues, topics, live message counts |
| `postgres` | `postgres:16-alpine` | `5538` (`POSTGRES_PORT`) | Outbox / inbox store (`postgres` / `postgres`, DB `platform_events_dev`) |

`make docker-up` / `make docker-down` start and stop them. Host ports are unique across the org's local stacks, so this runs alongside pgcommon's and every IAM service's. The integration and e2e suites do **not** need them — testcontainers-go starts its own isolated containers per run (the Makefile exports `DOCKER_HOST` for Docker Desktop's user socket).

### The reference-CLI image

`Dockerfile` builds `cmd/platform-events` into a ~6 MB `gcr.io/distroless/static-debian13:nonroot` image (non-root, no shell). It exists so CI can CVE-scan and smoke-test the compiled binary — the library itself is consumed as a Go module, never as an image. Both base images are pinned by digest (recorded in `.docker-digests`; refresh with `make pin-base-images`), and the private-module token is a BuildKit secret, never a layer.

```bash
GO_PRIVATE_TOKEN=$(gh auth token) make docker-build   # → platform-events-ci-test
docker run --rm platform-events-ci-test               # -strict: exits 1 without SNS/SQS/DB config
```

---

## Compatibility and dependencies

### 1. Cross-language compatibility

The Python [`platform-eventcommon`](https://github.com/BCBP-SOLUTIONS-FZC-LLC/platform-eventcommon) library publishes to and consumes from the same topics and queues. Any change to the envelope JSON (`internal/core/domain/envelope.go`) or HMAC canonicalisation must be mirrored there; the `interop` job fails the build on a mismatch.

### 2. Upstream libraries

| Library | Used for | If it changes |
|---|---|---|
| `platform-pgcommon` | Pool, `RunInTx`, `GUCSet`, `migrate.Runner` | Bump here first; the outbox/inbox integration tests cover the contract |
| `aws-sdk-go-v2` (`sns`, `sqs`, `config`) | Transport | Confined to `internal/adapter/outbound/` |
| `prometheus/client_golang`, `go.opentelemetry.io/otel` | Metrics / tracing | Metric names are part of the public contract — renames are breaking |

### 3. Infrastructure the consuming service provides

| System | Requirement |
|---|---|
| **AWS SNS** | A topic per event stream; FIFO topics need `WithMessageGroupID` |
| **AWS SQS** | A queue per consumer, a `RawMessageDelivery=true` subscription, and a `RedrivePolicy` DLQ (required for `DLQPublisher`) |
| **PostgreSQL** | For the outbox and inbox; schemas applied with `outbox.ApplySchema` / `inbox.ApplySchema` |
| **OTel provider / Prometheus registry** | Initialised by the service before the first publish/consume |

---

## Out of scope

| Concern | Where it lives |
|---|---|
| Creating topics, queues, subscriptions, filter policies, DLQs | Terraform / CDK in the service or platform infra repo |
| Reading, replaying or redriving the SQS DLQ | SQS console / `StartMessageMoveTask` |
| Event schemas and the event-type registry | [EVENT_SCHEMA_GOVERNANCE.md](EVENT_SCHEMA_GOVERNANCE.md), `platform-schemagov` |
| Schema-registry clients | The consuming service's `Codec` implementation |
| Non-AWS brokers (EventBridge, Kafka) | Not supported without a new adapter |
| One-off scripts / CLIs | A plain SDK call is simpler — the library targets long-running services |
| Services without Postgres | The outbox needs `platform-pgcommon`; without a DB, direct publish means accepting at-most-once delivery |

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, adding adapters, testing requirements and the PR checklist.

| Document | Description |
|---|---|
| [`.claude/CLAUDE.md`](.claude/CLAUDE.md) | Guidance for Claude Code working in this repo |
| [`ARCHITECTURE.md`](ARCHITECTURE.md) | Layer model, sequence diagrams, failure lifecycle, invariants, performance |
| [`docs/lld/platform-events-lld.md`](docs/lld/platform-events-lld.md) | Low-level design: data model, public API contract, flows, retry classification, configuration, observability |
| [`docs/observability/`](docs/observability/README.md) | Observability standard: tier model, generated metrics registry, runbook |
| [`docs/README.md`](docs/README.md) | Mermaid diagram index and how to keep diagrams in sync |
| [`docs/guides/quick-start.md`](docs/guides/quick-start.md) | End-to-end service wiring |
| [`docs/guides/envelope.md`](docs/guides/envelope.md) | Envelope construction, serialisation, payload typing |
| [`docs/guides/publishing.md`](docs/guides/publishing.md) | Publishing rules, anti-patterns, SNS publisher |
| [`docs/guides/consuming.md`](docs/guides/consuming.md) | Handler contract, errors, poison messages, DLQ forwarding, idempotency |
| [`docs/guides/outbox.md`](docs/guides/outbox.md) | Outbox wiring, retries, ordering, dead letters, pruning, replay |
| [`docs/guides/codec.md`](docs/guides/codec.md) | Schema-registry `Codec` hook |
| [`docs/guides/hmac.md`](docs/guides/hmac.md) | HMAC helpers |
| [`docs/guides/observability.md`](docs/guides/observability.md) | Metrics, tracing, logging correlation |
| [`docs/guides/operations.md`](docs/guides/operations.md) | Backpressure, production defaults, adoption checklist |
| [`docs/guides/testing-in-services.md`](docs/guides/testing-in-services.md) | Mocks for consuming-service tests |
| [`EVENT_SCHEMA_GOVERNANCE.md`](EVENT_SCHEMA_GOVERNANCE.md) | Event naming, payload evolution, consumer compatibility |
| [`VERSIONING.md`](VERSIONING.md) · [`CHANGELOG.md`](CHANGELOG.md) · [`SECURITY.md`](SECURITY.md) | Release policy · per-version changes · vulnerability reporting |

---

## License / ownership

BCBP Solutions FZC LLC — internal platform shared library. Not for external distribution.
