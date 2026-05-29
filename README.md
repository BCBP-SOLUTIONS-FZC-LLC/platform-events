# platform-events

Shared **event-driven messaging** library for platform services. Consumed as a private Go module — not deployed on its own.

**Features:** typed event envelopes · SNS publisher · SQS consumer · transactional outbox (Postgres-backed) · HMAC signing/verification · Prometheus metrics · OpenTelemetry tracing · RLS tenant propagation.

See [ARCHITECTURE.md](ARCHITECTURE.md) for design decisions and layer diagrams.

## Design principles

- **Fail fast on misconfiguration** — `NewSNSPublisher` panics on an empty `TopicARN`; `NewSQSConsumer` returns an error on an empty `QueueURL`. Invalid configurations are rejected at construction time, not at first use.
- **Make safe usage the default** — the SQS consumer automatically injects `pgcommon.GUCSet` for RLS, extends visibility timeouts on slow handlers, and drains in-flight messages on `Stop()`. You cannot forget these by accident when the defaults are wired correctly.
- **Keep business logic free from messaging plumbing** — handlers receive a typed `Envelope` and a context; retry backoff, visibility extension, metric recording, and OTel span creation are invisible to the caller.
- **Centralise cross-cutting concerns** — HMAC signing, tenant propagation, and observability hooks live here, not scattered across every service. A single version bump propagates to all consumers.

---

## Contents

- [TL;DR](#tldr)
- [When NOT to use this](#when-not-to-use-this)
- [Adding to a consumer service](#adding-to-a-consumer-service)
- [Quick start](#quick-start)
- [Event envelope](#event-envelope)
- [SNS Publisher](#sns-publisher)
- [SQS Consumer](#sqs-consumer)
- [Transactional outbox](#transactional-outbox)
- [HMAC helpers](#hmac-helpers)
- [Observability — Prometheus and OTel](#observability--prometheus-and-otel)
- [Configuration reference](#configuration-reference)
- [Error reference](#error-reference)
- [Testing](#testing)
- [CI](#ci)
- [Docker](#docker)
- [Versioning and releases](#versioning-and-releases)

---

## TL;DR

- Use `events.NewEnvelope[T]` to create typed events — never construct `Envelope` structs directly.
- Always pass `WithTenantID(rc.TenantID)` and `WithTraceID(rc.TraceID)` when publishing from an HTTP handler — these fields drive RLS enforcement and trace continuity.
- Use `outbox.Enqueue` inside a `pgcommon.RunInTx` callback to guarantee at-least-once delivery without dual-write risk.
- **Never publish domain events directly** — always use `outbox.Enqueue` inside a transaction; direct `publisher.Publish` bypasses the transaction boundary.
- Set `events.Init(serviceName, buildVersion)` once at startup to enable Prometheus metrics.
- OTel tracing is enabled by the consuming service (`gincommon.InitTracingFromEnv()`), not by this library.

---

## When NOT to use this

This library targets **long-running platform services** that publish or consume domain events. It may be more than you need if:

- **One-off scripts or CLIs** — a plain `aws-sdk-go-v2/service/sns` call is simpler without the pool setup overhead.
- **Services that only consume a single event type** — if filtering, retries, and concurrency control are handled entirely by the SQS queue configuration, a thin wrapper is sufficient.
- **Non-AWS message brokers** — the library is SNS/SQS-specific. EventBridge, Kafka, and other backends are not supported without a new adapter.
- **Services that do not use PostgreSQL** — the outbox runner requires `platform-pgcommon`. If your service has no database, use direct SNS publishing and accept at-most-once delivery.

---

## Adding to a consumer service

This is a **private module**. Configure Go to bypass the public proxy and checksum database before fetching:

```bash
go env -w GOPRIVATE=github.com/BCBP-SOLUTIONS-FZC-LLC/*
```

Set the same variable in every CI pipeline that builds a consuming service.

### GitHub authentication

**SSH key (recommended for local dev):**

```bash
git config --global url."ssh://git@github.com/".insteadOf "https://github.com/"
```

**Personal access token (CI / Docker builds):**

```bash
GONOSUMCHECK=github.com/BCBP-SOLUTIONS-FZC-LLC/* \
GOFLAGS=-mod=mod \
go get github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events@v1.0.0
```

### Pin the version

```bash
go get github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events@v1.0.0
```

> Import only `pkg/events` and `pkg/outbox`. Never import `internal/` — Go enforces this boundary for external modules.

---

## Quick start

```go
import (
    "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/events"
    "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/outbox"
    "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/migrate"
    "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/pgcommon"
)

func main() {
    ctx := context.Background()

    // 1. Register Prometheus metrics once.
    events.Init(os.Getenv("APP_NAME"), os.Getenv("BUILD_VERSION"))

    // 2. Open the connection pool for the outbox runner.
    pool, err := pgcommon.NewPool(ctx, pgcommon.Config{
        DSN:         os.Getenv("DATABASE_URL"),
        GUCProvider: pgcommon.GUCSetFromContext,
    })
    if err != nil {
        log.Fatal(err)
    }
    defer pool.Close()

    // 3. Apply the outbox schema migration.
    migrateRunner := &migrate.Runner{DSN: os.Getenv("DATABASE_URL")}
    if err := outbox.ApplySchema(ctx, migrateRunner); err != nil {
        log.Fatal(err)
    }

    // 4. Construct the SNS publisher.
    publisher, err := events.NewSNSPublisher(events.SNSConfig{
        TopicARN: os.Getenv("SNS_TOPIC_ARN"),
        Region:   os.Getenv("AWS_REGION"),
        Logger:   logger, // port.Logger — pass gincommon's ZapLogger directly
    })
    if err != nil {
        log.Fatal(err)
    }

    // 5. Start the outbox runner (delivers events asynchronously).
    runner := outbox.NewRunner(outbox.Config{
        Pool:      pool,
        Publisher: publisher,
        Logger:    logger,
    })
    go runner.Start(ctx)
    defer runner.Stop()

    // 6. Construct and start the SQS consumer.
    consumer, err := events.NewSQSConsumer(
        events.SQSConfig{
            QueueURL: os.Getenv("SQS_QUEUE_URL"),
            Region:   os.Getenv("AWS_REGION"),
            Logger:   logger,
        },
        func(ctx context.Context, env events.Envelope[json.RawMessage]) error {
            // handle event...
            return nil
        },
        events.WithConcurrency(5),
    )
    if err != nil {
        log.Fatal(err)
    }
    go consumer.Start(ctx)
    defer consumer.Stop()
}
```

---

## Event envelope

`Envelope[T]` is the canonical wire format for all inter-service events.

```json
{
  "id":             "01926e4f-...",
  "type":           "iam.user.created",
  "source":         "platform-iam",
  "tenant_id":      "acme",
  "trace_id":       "4bf92f3577b34da6a3ce929d0e0e4736",
  "correlation_id": "...",
  "timestamp":      "2026-05-27T12:00:00Z",
  "payload":        { ... }
}
```

### Creating envelopes

```go
type UserCreatedPayload struct {
    UserID string `json:"user_id"`
    Email  string `json:"email"`
}

// From an HTTP handler — always carry trace and tenant from the RequestContext.
rc, ok := gincommon.RequestContext(c) // returns (*RequestContext, bool) — false for unauthenticated requests
if !ok {
    return errors.New("missing request context")
}
env := events.NewEnvelope("iam.user.created", "platform-iam", UserCreatedPayload{
    UserID: user.ID,
    Email:  user.Email,
},
    events.WithTenantID(rc.TenantID),
    events.WithTraceID(rc.TraceID),
)
```

`event_type` convention: `<domain>.<entity>.<past-tense-verb>` — e.g. `iam.user.created`, `billing.invoice.settled`. Consumers match on prefix with `strings.HasPrefix` or exact equality.

### Serialisation

```go
// Marshal to JSON wire format.
data, err := env.JSON()

// Unmarshal and validate required fields.
env, err := events.ParseEnvelope[UserCreatedPayload](data)
```

### Envelope ID

`Envelope.ID` is a **UUID v7** (time-ordered, collation-friendly in Postgres B-tree indexes). Use it as an idempotency key on the consumer side.

---

## SNS Publisher

```go
publisher, err := events.NewSNSPublisher(events.SNSConfig{
    TopicARN:    os.Getenv("SNS_TOPIC_ARN"), // required — panics if empty
    Region:      os.Getenv("AWS_REGION"),
    EndpointURL: os.Getenv("AWS_ENDPOINT_URL"), // set to http://localhost:4566 for LocalStack
    Logger:      logger,
})
```

### Publishing a single event

```go
raw, _ := json.Marshal(payload)
env := events.NewEnvelope("iam.user.created", "platform-iam",
    json.RawMessage(raw),
    events.WithTenantID(rc.TenantID),
    events.WithTraceID(rc.TraceID),
)
if err := publisher.Publish(ctx, env); err != nil {
    return fmt.Errorf("publish user.created: %w", err)
}
```

### Batch publishing

```go
// PublishBatch splits automatically at the SNS hard limit of 10.
err := publisher.PublishBatch(ctx, envelopes)
// Partial failures return a BatchError listing per-message errors.
```

### FIFO topics

```go
publisher, err := events.NewSNSPublisher(events.SNSConfig{
    TopicARN: "arn:aws:sns:us-east-1:123456789012:my-topic.fifo",
    Region:   "us-east-1",
    Logger:   logger,
},
    events.WithMessageGroupID(func(env events.Envelope[json.RawMessage]) string {
        return env.TenantID // group per tenant for ordered delivery
    }),
)
```

When `TopicARN` ends in `.fifo`, `MessageGroupID` is required; `MessageDeduplicationID` defaults to `Envelope.ID` (requires content-based deduplication disabled at the topic level).

### Message attributes

`EventType`, `TenantID`, `Source`, and `EventID` are always set as SNS message attributes. This enables SQS subscription filter policies that scope queues to specific event types or tenants without deserialising the message body.

---

## SQS Consumer

```go
consumer, err := events.NewSQSConsumer(
    events.SQSConfig{
        QueueURL:    os.Getenv("SQS_QUEUE_URL"), // required
        Region:      os.Getenv("AWS_REGION"),
        EndpointURL: os.Getenv("AWS_ENDPOINT_URL"),
        MaxMessages: 10,    // 1–10; defaults to 10
        WaitSeconds: 20,    // long-poll; defaults to 20
        Logger:      logger,
    },
    func(ctx context.Context, env events.Envelope[json.RawMessage]) error {
        // ctx already has pgcommon.GUCSet{TenantID: env.TenantID} injected —
        // pool.WithConn / RunInTx automatically enforce RLS for this tenant.
        return handleEvent(ctx, env)
    },
    events.WithConcurrency(5),
    events.WithVisibilityTimeout(30*time.Second),
)
if err != nil {
    log.Fatal(err)
}

go consumer.Start(ctx)   // blocks; run in a goroutine
defer consumer.Stop()    // graceful drain — waits up to 30s for in-flight handlers
```

### Handler contract

| Handler return | Consumer behaviour |
|---|---|
| `nil` | Message deleted from queue |
| `non-nil error` | Message left visible; retried after visibility timeout |
| Panic | Recovered; message left visible; panic re-logged |

### Concurrency and graceful shutdown

```go
events.WithConcurrency(n)             // bound goroutines via semaphore; default 1
events.WithVisibilityTimeout(d)       // per-message timeout; default 30s
events.WithDeadLetterHandler(fn)      // receives messages exceeding MaxReceiveCount
```

`Stop()` cancels the receive loop, then waits up to `DrainTimeout` (default 30 s) for in-flight handlers to complete — matching `net/http.Server.Shutdown` semantics for clean Kubernetes pod termination.

### Ordering

Handlers should not rely on message ordering unless FIFO queues with a consistent `MessageGroupID` are used (see `WithMessageGroupID`). Standard SQS queues offer best-effort ordering only.

### RLS tenant propagation

The handler context has `pgcommon.WithGUCSet(ctx, GUCSet{TenantID: env.TenantID})` injected automatically. All downstream pool calls (`pool.WithConn`, `pgcommon.RunInTx`) enforce the event's tenant context without any extra code in the handler.

---

## Transactional outbox

The outbox pattern eliminates dual-write risk: the event is written **inside the business transaction** alongside the domain mutation. If the transaction rolls back, the event is never published. The runner delivers asynchronously with at-least-once guarantee.

### Wiring

```go
// 1. Apply outbox schema migration (once at startup).
migrateRunner := &migrate.Runner{DSN: os.Getenv("DATABASE_URL")}
if err := outbox.ApplySchema(ctx, migrateRunner); err != nil {
    log.Fatal(err)
}

// 2. Construct the outbox runner.
runner := outbox.NewRunner(outbox.Config{
    Pool:         pool,       // *pgcommon.Pool — required
    Publisher:    publisher,  // events.Publisher — required
    Logger:       logger,
    PollInterval: 5 * time.Second,
    BatchSize:    50,
    MaxAttempts:  5,
})
go runner.Start(ctx) // blocks until ctx is cancelled
defer runner.Stop()  // graceful drain of in-flight batch
```

### Enqueueing inside a transaction

```go
err = pgcommon.RunInTx(ctx, pool, pgx.TxOptions{}, func(ctx context.Context, tx pgx.Tx) error {
    // Business write and event enqueue commit or roll back atomically.
    if err := repo.SaveUser(ctx, tx, user); err != nil {
        return err
    }
    env := events.NewEnvelope("iam.user.created", "platform-iam",
        json.RawMessage(mustMarshal(UserCreatedPayload{UserID: user.ID})),
        events.WithTenantID(rc.TenantID),
        events.WithTraceID(rc.TraceID),
    )
    return outbox.Enqueue(ctx, tx, env)
})
```

### Poll cycle

1. `SELECT … FOR UPDATE SKIP LOCKED` — claim up to `BatchSize` unpublished records (safe for horizontal scale; no distributed lock needed).
2. For each record: call `Publisher.Publish`; on success set `published_at = NOW()`.
3. On failure: increment `attempts`, set `last_error`. If `attempts >= MaxAttempts` move to `outbox_dead_letters`.
4. Commit; sleep `PollInterval`.

### Dead letters

Failed records that exhaust `MaxAttempts` move to `outbox_dead_letters` — a queryable Postgres table. Inspect and replay from standard SQL tooling rather than an SQS DLQ.

```sql
SELECT * FROM outbox_dead_letters WHERE tenant_id = 'acme' ORDER BY failed_at DESC;
```

### Idempotency

`Envelope.ID` (UUID v7) is forwarded as the SNS `MessageDeduplicationID` on FIFO topics and as a message attribute on standard topics. Consumers should use `Envelope.ID` as their idempotency key.

---

## HMAC helpers

Use HMAC-SHA256 to sign and verify event envelopes for webhook receipt or cross-service authentication.

```go
key := []byte(os.Getenv("WEBHOOK_SECRET")) // must be ≥ 32 bytes

// Sign
sig, err := events.Sign(key, payload)

// Verify — constant-time; returns false on mismatch, never panics
if !events.Verify(key, payload, sig) {
    return errors.New("invalid signature")
}

// Sign/verify a full envelope (serialises to canonical JSON first)
sig, err := events.SignEnvelope(key, env)

ok, err := events.VerifyEnvelope(key, env, sig)
if err != nil || !ok {
    return errors.New("envelope signature invalid")
}
```

> `Verify` uses `hmac.Equal` (constant-time) — never replace with string `==`. `VerifyEnvelope` returns `(false, nil)` on mismatch and `(false, err)` on malformed input — callers must check both return values.

**Key length:** `Sign` returns `("", ErrKeyTooShort)` for keys < 32 bytes. Callers that ignore the error emit an empty signature, which `Verify` rejects — the system degrades safely.

---

## Observability — Prometheus and OTel

Both Prometheus metrics and OTel tracing are **optional**. The publisher, consumer, and outbox runner work without calling `events.Init` or having an OTel provider registered.

### Prometheus metrics

Call once at service startup (idempotent — first caller wins):

```go
events.Init(os.Getenv("APP_NAME"), os.Getenv("BUILD_VERSION"))
```

For isolated test registries:

```go
events.InitWithRegisterer("test-svc", "v0.0.0", prometheus.NewRegistry())
```

Registered metrics:

| Metric | Type | Labels | Description |
|---|---|---|---|
| `events_published_total` | Counter | `service`, `topic`, `event_type`, `status` | SNS publish attempts |
| `events_publish_duration_seconds` | Histogram | `service`, `topic`, `event_type` | SNS publish latency |
| `events_consumed_total` | Counter | `service`, `queue`, `event_type`, `status` | SQS messages processed |
| `events_consume_duration_seconds` | Histogram | `service`, `queue`, `event_type` | Handler execution latency |
| `outbox_pending_total` | Gauge | `service` | Unpublished records in `outbox_events` |
| `outbox_published_total` | Counter | `service`, `status` | Records published by the runner |
| `outbox_attempts_total` | Counter | `service` | Total publish attempts by the runner |

### OpenTelemetry

OTel is **always initialised by the consuming service**. Call `gincommon.InitTracingFromEnv()` (from `platform-gincommon`) at startup — `platform-events` calls `otel.Tracer("platform-events")` and produces no-op spans if no provider is registered.

**SNS publish span:** `sns.publish` with `messaging.system=aws_sns`, `messaging.destination`, `messaging.message_id`.

**SQS receive span:** `sqs.receive` with `messaging.system=aws_sqs`, `messaging.destination`, `messaging.message_id`, `messaging.operation=process`. The span is **linked to the publisher's trace** via `Envelope.TraceID`, giving end-to-end visibility across the SNS/SQS boundary in Tempo/Grafana.

---

## Configuration reference

| Variable | Default | Notes |
|---|---|---|
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
| `DATABASE_URL` | — | Postgres DSN for outbox runner |
| `AWS_ENDPOINT_URL` | — | Set to `http://localhost:4566` for LocalStack |
| `OTEL_SERVICE_NAME` | — | OTel resource attribute |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | |
| `OTEL_EXPORTER_OTLP_INSECURE` | `false` | Set automatically to `true` when `APP_ENV=dev` |
| `APP_ENV` | — | `dev/development/local` enables OTel insecure mode |

Copy `.env-example` to `.env` via `make setup` for local development.

---

## Error reference

| Error | Package | Returned when |
|---|---|---|
| `domain.ErrEnvelopeIDRequired` | `events` | `ParseEnvelope` called on data with no `id` field |
| `domain.ErrEnvelopeTypeRequired` | `events` | `ParseEnvelope` called on data with no `type` field |
| `domain.ErrEnvelopeSourceRequired` | `events` | `ParseEnvelope` called on data with no `source` field |
| `domain.ErrKeyTooShort` | `events` | `Sign` / `SignEnvelope` called with key < 32 bytes |
| `domain.ErrInvalidSignature` | `events` | Internal parse failure in `Verify` |
| `domain.ErrBatchTooLarge` | `events` | Internal sentinel; `PublishBatch` splits automatically at 10 — this error is never returned to callers |

---

## Testing

```bash
make test-unit   # unit tests — no Docker required
make test-int    # integration tests — spins up LocalStack + Postgres (testcontainers-go)
make test-smoke  # smoke tests — requires live AWS resources at SNS_TOPIC_ARN / SQS_QUEUE_URL
make race        # unit + integration with -race detector
make cover       # HTML coverage report (threshold: ≥95%)
make cover-func  # per-function coverage summary in the terminal
```

Pass `-short` to skip any test that requires Docker:

```bash
go test -short ./test/integration/...
```

Run a single test by name:

```bash
go test ./test/unit/envelope/...     -run TestEnvelopeSign -v
go test ./test/integration/...       -run TestSNSPublishRoundTrip -tags=integration -v
```

Integration tests use `testcontainers-go` — Docker must be running locally. Unit tests have no external dependencies and always run in CI without Docker.

**Coverage note:** tests live under `test/` (a separate package tree from sources). Always use `-coverpkg=./internal/...,./pkg/...` to get meaningful numbers. `make cover` and `make cover-func` handle this correctly.

---

## CI

The GitHub Actions pipeline runs on every push and pull request to `main`:

| Job | What it checks |
|-----|---------------|
| `validate` | `gofmt`, `go mod tidy` drift, `go vet`, `golangci-lint`, `govulncheck`, race-detector tests |
| `coverage` | unit + integration tests with race detector; coverage gate (≥95%) against `./internal/...` + `./pkg/...` |
| `build` | Compiles the library and the `cmd/platform-events` binary |
| `release` | Triggered on `v*` tags — creates a GitHub Release; this is the Go module release consuming services pin to |

All checks must pass before merging. The coverage gate measures `./internal/...` and `./pkg/...` using `-coverpkg`.

---

## Docker

`docker-compose.yml` starts **LocalStack** (SNS + SQS) and **Postgres** for running integration tests locally:

```bash
make docker-up    # start LocalStack on :4566, Postgres on :5432
make docker-down  # stop and remove containers
```

The library itself is never containerised — it is a Go module dependency, not a server.

---

## Versioning and releases

| Version bump | When |
|---|---|
| **MAJOR** | Breaking change in `pkg/*` public API |
| **MINOR** | New backward-compatible capability |
| **PATCH** | Bug fix, performance improvement, documentation correction |

```bash
# Tag and push triggers the release workflow automatically.
git tag -a v1.0.0 -m "v1.0.0"
git push origin v1.0.0
```

## License / ownership

BCBP Solutions FZC LLC — internal platform shared library.

---

## See also

| Document | Description |
|----------|-------------|
| [ARCHITECTURE.md](./ARCHITECTURE.md) | Layer model, sequence diagrams, invariants, performance |
| [VERSIONING.md](./VERSIONING.md) | SemVer rules, supported versions, release process |
| [CHANGELOG.md](./CHANGELOG.md) | Per-version changes |
| [CONTRIBUTING.md](./CONTRIBUTING.md) | Development setup, adding adapters, PR checklist |
| [SECURITY.md](./SECURITY.md) | Vulnerability reporting, trust model, supported versions |

---

All services must use `outbox.Enqueue` inside a `pgcommon.RunInTx` callback for any event that must be delivered reliably. Direct `publisher.Publish` is appropriate only for best-effort notifications where at-most-once delivery is acceptable.

`platform-events` must be the only entry point for event publishing and consumption to preserve delivery guarantees, tenant isolation, and uniform observability.
