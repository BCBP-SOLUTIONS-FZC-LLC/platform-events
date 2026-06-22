# platform-events

Shared **event-driven messaging** library for platform services. Consumed as a private Go module — not deployed on its own.

**Features:** typed event envelopes · SNS publisher · SQS consumer · transactional outbox (Postgres-backed) · HMAC signing/verification · Prometheus metrics · OpenTelemetry tracing · RLS tenant propagation.

See [ARCHITECTURE.md](ARCHITECTURE.md) for design decisions and layer diagrams.

## Design principles

- **Events are facts, not commands.** An event records something that already happened (`user.created`, `invoice.settled`) — it is an immutable statement of past truth, not an instruction to perform an action. Producers do not direct consumers; consumers decide independently how to react. A consequence of this: consumers must be resilient to receiving the same fact more than once (`Envelope.ID` idempotency) and must handle the absence of a reaction gracefully (a consumer can be added or removed without any producer change).
- **Fail fast on misconfiguration** — `NewSNSPublisher` returns an error on an empty or invalid `TopicARN`; `NewSQSConsumer` returns an error on an empty `QueueURL`. Invalid configurations are rejected at construction time, not at first use.
- **Make safe usage the default** — the SQS consumer automatically injects `pgcommon.GUCSet` for RLS, extends visibility timeouts on slow handlers, and drains in-flight messages on `Stop()`. You cannot forget these by accident when the defaults are wired correctly.
- **Keep business logic free from messaging plumbing** — handlers receive a typed `Envelope` and a context; retry backoff, visibility extension, metric recording, and OTel span creation are invisible to the caller.
- **Centralise cross-cutting concerns** — HMAC signing, tenant propagation, and observability hooks live here, not scattered across every service. A single version bump propagates to all consumers.

---

## Contents

- [TL;DR](#tldr)
- [System invariants](#system-invariants)
- [Publishing rules](#publishing-rules)
- [Anti-patterns](#anti-patterns)
- [When NOT to use this](#when-not-to-use-this)
- [Adding to a consumer service](#adding-to-a-consumer-service)
- [Quick start](#quick-start)
- [Event envelope](#event-envelope)
- [SNS Publisher](#sns-publisher)
- [SQS Consumer](#sqs-consumer)
  - [Implementing idempotency](#implementing-idempotency)
- [Transactional outbox](#transactional-outbox)
- [HMAC helpers](#hmac-helpers)
  - [When to use HMAC](#when-to-use-hmac)
- [Observability — Prometheus and OTel](#observability--prometheus-and-otel)
  - [Logging correlation](#logging-correlation)
- [Backpressure considerations](#backpressure-considerations)
- [Recommended production defaults](#recommended-production-defaults)
- [Configuration reference](#configuration-reference)
- [Error reference](#error-reference)
- [Testing in consuming services](#testing-in-consuming-services)
- [Testing](#testing)
- [CI](#ci)
- [Docker](#docker)
- [Service adoption checklist](#service-adoption-checklist)
- [Versioning and releases](#versioning-and-releases)

---

## TL;DR

> ⚠️ **Exactly-once delivery is not possible anywhere in this system.** SNS and SQS provide at-least-once delivery — a message may be delivered more than once under normal operating conditions (visibility timeout expiry, consumer crash mid-handler, SQS internal redelivery). FIFO topics reduce the duplicate window to 5 minutes within a `MessageGroupID` but do not eliminate duplicates end-to-end. **Consumer idempotency is not optional — it is required for correctness regardless of whether FIFO is used.** See [Implementing idempotency](#implementing-idempotency) for the concrete pattern.

- The system provides **at-least-once delivery** — consumers must implement idempotency using `Envelope.ID`. See [Implementing idempotency](#implementing-idempotency) for the recommended `processed_events` Postgres pattern.
- Use `events.NewEnvelope[T]` to create typed events — never construct `Envelope` structs directly.
- Always pass `WithTenantID(rc.TenantID)` and `WithTraceID(rc.TraceID)` when publishing from an HTTP handler — these fields drive RLS enforcement and trace continuity.
- Use `outbox.Enqueue` inside a `pgcommon.RunInTx` callback to guarantee reliable at-least-once delivery without dual-write risk. See [Publishing rules](#publishing-rules) for the full decision table and crash-window explanation.
- **Never use `publisher.Publish` for domain events tied to a DB write** — event loss on process crash is silent and unrecoverable. The outbox is the only safe path.
- Set `events.Init(serviceName, buildVersion)` once at startup to enable Prometheus metrics.
- OTel tracing is enabled by the consuming service (`gincommon.InitTracingFromEnv()`), not by this library.

---

## System invariants

These hold everywhere in the system, always. If your design requires violating any of them, the design needs to change — not the invariant.

| Invariant | What it means for you |
|---|---|
| **Delivery is at-least-once** | Every consumer handler may be called more than once for the same `Envelope.ID`. Idempotency is not a nice-to-have. |
| **Ordering is best-effort unless FIFO per group** | Standard SNS/SQS make no ordering promise. FIFO guarantees order only within a single `MessageGroupID`. Cross-group and cross-service ordering is never guaranteed. |
| **Idempotency is required for all consumers** | A handler that is not idempotent is not correct. There is no configuration, queue type, or delivery mode that removes this requirement. |
| **Events are immutable once published** | An `event_type` string and its payload contract are frozen on first production publish. Breaking changes require a new versioned type (`.v2`). |
| **The producer has no knowledge of consumers** | A service must never check "who is listening" before publishing. Consumers come and go; the event type remains. |
| **Tenant context is always explicit** | Every event scoped to a tenant carries `TenantID`. Using `WithSystemTenant()` for a tenant-scoped event is a correctness bug, not a shortcut. |
| **Delivery timing is unbounded** | Consumers must not rely on when an event arrives — only on what it says. Delivery latency varies under load, during outbox backlog drain, and on replay. A handler that breaks when the event arrives "late" is not correct. |
| **Delivery latency is not SLA-backed** | Event delivery is best-effort within the outbox poll interval + SNS/SQS propagation time. There is no guaranteed maximum latency. Consumers must be correct whether an event arrives in 2 seconds or 2 minutes. Do not design consumers that time out or abort if an expected event has not arrived within a fixed window. |
| **Idempotency must cover all externally observable side effects** | A DB write, an external HTTP call, a notification, and a cache invalidation are all externally observable. Idempotency that protects the DB write but not the Stripe charge or the email is incomplete — the handler is still not safe to run twice. See [Side-effect classification](#side-effect-classification) for per-class guards. |

---

## Publishing rules

> **Violating the outbox rule introduces silent data-loss bugs.** A domain event is permanently lost if the process crashes between the DB commit and the SNS call. There is no retry, no error, no log — the event is simply gone.

### Decision table

| Scenario | Correct method | Delivery guarantee |
|---|---|---|
| Domain event tied to a DB write — user created, invoice settled, order placed, state machine transition | `outbox.Enqueue` inside `pgcommon.RunInTx` | **At-least-once** ✅ |
| Background job publishing across tenants — scheduled reconciliation, batch processing | `outbox.Enqueue` + `WithSystemTenant()` inside `pgcommon.RunInTx` | **At-least-once** ✅ |
| External notification not tied to a DB write — webhook ping after a read-only operation | `publisher.Publish` | At-most-once ⚠️ |
| Fire-and-forget telemetry or audit where loss is explicitly acceptable | `publisher.Publish` | At-most-once ⚠️ |
| Service with no PostgreSQL database (document in your service ADR) | `publisher.Publish` | At-most-once ⚠️ |

**The rule in one sentence:** if a consumer's state would be wrong or incomplete if it never received this event, use the outbox.

### Why the crash window matters

Without the outbox, there is an unrecoverable gap between the DB commit and the SNS publish:

```
WITHOUT OUTBOX:
  1. BEGIN
  2. INSERT users ...       ← domain write
  3. COMMIT                 ← process crashes here (OOM, deploy, network drop)
  4. sns.Publish(...)       ← never executes — event lost permanently
```

With the outbox, the event is durable the moment the transaction commits:

```
WITH OUTBOX:
  1. BEGIN
  2. INSERT users ...         ← domain write
  3. INSERT outbox_events ... ← event write in same transaction
  4. COMMIT                   ← both rows durable; crash here is safe
  5. [outbox runner, async]
       sns.Publish(...)       ← retried up to MaxAttempts on failure
       published_at = NOW()   ← idempotent completion marker
```

If the runner crashes between steps 4 and 5, the row is still in `outbox_events` with `published_at IS NULL`. The next runner instance (or the restarted process) picks it up on the next poll cycle.

### Detecting misuse in code review

Flag any call to `publisher.Publish` or `publisher.PublishBatch` that appears inside a handler that also writes to the database. The correct pattern is:

```go
// ✅ Correct — both writes in one transaction
pgcommon.RunInTx(ctx, pool, pgx.TxOptions{}, func(ctx context.Context, tx pgx.Tx) error {
    if err := repo.SaveUser(ctx, tx, user); err != nil {
        return err
    }
    return outbox.Enqueue(ctx, tx, envelope)
})

// ❌ Wrong — SNS call outside the transaction; event lost on crash
pgcommon.RunInTx(ctx, pool, pgx.TxOptions{}, func(ctx context.Context, tx pgx.Tx) error {
    return repo.SaveUser(ctx, tx, user)
})
publisher.Publish(ctx, envelope) // ← data inconsistency risk
```

---

## Anti-patterns

Common mistakes that cause silent failures, data loss, or broken tenant isolation.

| Anti-pattern | Why it's wrong | What to do instead |
|---|---|---|
| `publisher.Publish(ctx, env)` directly inside an HTTP handler for a transactional event | If the process dies after the DB commit but before `Publish` returns, the event is silently dropped — no retry, no recovery | Use `outbox.Enqueue` inside `pgcommon.RunInTx` alongside the domain write |
| Ignoring `Envelope.ID` in the consumer handler | The outbox runner delivers at-least-once; without an idempotency check, a redelivered event causes duplicate side effects | `INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING` inside the same transaction as the side-effect write |
| `json.Unmarshal(env.Payload, &payload)` without checking `env.Type` first | A handler subscribed to multiple event types will silently misparse the wrong payload into a struct with zero-value fields | Always assert `env.Type` before unmarshalling; route to typed handlers by event type |
| FIFO topic with a non-stable `MessageGroupID` (e.g. random UUID per message) | Defeats ordering — every message lands in its own group; SNS treats them as independent and delivers concurrently | Use a deterministic, stable group ID: `TenantID`, `UserID`, or `AggregateID` — a value that must be ordered relative to itself |
| `WithSystemTenant()` in an HTTP handler context | Bypasses per-tenant RLS on the consumer side; the handler's DB queries run without a tenant GUC, returning wrong row sets silently | Pass `WithTenantID(rc.TenantID)` from the `gincommon.RequestContext`; only use `WithSystemTenant()` for genuine cross-tenant background jobs |
| `json.NewDecoder(r).DisallowUnknownFields()` on event payloads | Turns every non-breaking producer schema addition (Tier 1) into a consumer runtime error | Remove `DisallowUnknownFields`; use `omitempty` discipline on the producer side instead |
| Calling `outbox.Enqueue` outside a transaction (`tx == nil`) | The event is not durably linked to the domain write; partial failures can produce an event with no corresponding domain record | Always call `outbox.Enqueue` inside `pgcommon.RunInTx`; never pass a nil `tx` |

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

Add a GitHub classic PAT (scope: `repo`) or fine-grained PAT (Contents: Read on all `BCBP-SOLUTIONS-FZC-LLC/*` repos) as a repository secret named `GO_PRIVATE_TOKEN`. The workflow configures git credentials before `go mod download`:

```yaml
- name: Configure private module access
  run: |
    git config --global credential.helper store
    echo "https://x-access-token:${{ secrets.GO_PRIVATE_TOKEN }}@github.com" > ~/.git-credentials
    chmod 600 ~/.git-credentials
```

> `persist-credentials: false` must be set on `actions/checkout` so that `GITHUB_TOKEN` (current-repo-only) does not override `GO_PRIVATE_TOKEN` when git fetches other private modules.

### Pin the version

```bash
go get github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events@v1.0.0
```

> Import `pkg/events`, `pkg/outbox`, and `pkg/config`. Never import `internal/` — Go enforces this boundary for external modules. The outbox path requires **`platform-pgcommon`** for connection pools, transactions (`RunInTx`), and schema migrations.

---

## Quick start

```go
import (
    "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/config"
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
    snsEnv := config.LoadSNS()
    publisher, err := events.NewSNSPublisher(config.SNSConfigFromEnv(snsEnv, logger))
    if err != nil {
        log.Fatal(err)
    }

    // 5. Start the outbox runner (delivers events asynchronously).
    outboxEnv := config.LoadOutbox()
    config.LogWarnings(outboxEnv.Warnings)
    runner, err := outbox.NewRunner(config.RunnerConfigFromEnv(outboxEnv, pool, publisher, logger))
    if err != nil {
        log.Fatal(err)
    }
    go runner.Start(ctx)
    defer runner.Stop()

    // 6. Construct and start the SQS consumer.
    sqsEnv := config.LoadSQS()
    config.LogWarnings(sqsEnv.Warnings)
    consumer, err := events.NewSQSConsumer(
        config.SQSConfigFromEnv(sqsEnv, logger),
        func(ctx context.Context, env events.Envelope[json.RawMessage]) error {
            // handle event...
            return nil
        },
        config.SQSConsumerOptions(sqsEnv)...,
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

`Envelope[T]` is the canonical wire format for all inter-service events. Terminology used consistently throughout this document:

| Term | Meaning | Example |
|---|---|---|
| **event** | The logical domain occurrence and the `Envelope` that carries it | "publish an event", "the event's `TenantID`" |
| **message** | The SNS/SQS infrastructure unit — what the broker delivers, retries, and deletes | "SQS message", "DeleteMessage", "`MessageGroupID`" |
| **outbox record** | A row in `outbox_events` or `outbox_dead_letters` — the durable Postgres representation of an event pending publish | "claim outbox records", "failed outbox record" |

```json
{
  "id":             "01926e4f-...",
  "type":           "iam.user.created",
  "source":         "platform-iam",
  "schema_version": "1",
  "tenant_id":      "acme",
  "trace_id":       "4bf92f3577b34da6a3ce929d0e0e4736",
  "correlation_id": "...",
  "subject":        "users/01926e4f-...",
  "actor":          "admin@acme.com",
  "schema_id":      "550e8400-e29b-41d4-a716-446655440000",
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
    events.WithTraceID(rc.TraceID),    // REQUIRED for cross-service trace continuity — see note below
    events.WithSchemaVersion("1"),
)
```

> ⚠️ **Always pass `WithTraceID(rc.TraceID)` from the request context.** If `TraceID` is omitted:
>
> - The OTel `sqs.receive` span on the consumer side has no parent — it starts a new disconnected trace, severing the link back to the originating HTTP request
> - A production incident that begins as an HTTP 500 in service A and manifests as a data anomaly in service B becomes impossible to correlate in Tempo or Grafana without manual log searching
> - The `trace_id` field in handler log lines will be empty, breaking the log-to-trace pivot that structured logging provides
>
> `rc.TraceID` is available via `gincommon.GetRequestContext(c).TraceID` in any HTTP handler. For background jobs where there is no inbound HTTP trace, generate a new trace ID at job startup and pass it consistently across all events emitted in that job run — this at minimum groups all events from the same run together.

**System-level events** (background jobs, scheduled tasks, cross-tenant operations) that are not scoped to any single tenant should use `WithSystemTenant()` instead:

```go
// Background job — not associated with any specific tenant.
env := events.NewEnvelope("billing.invoices.generated", "billing-worker",
    json.RawMessage(payload),
    events.WithSystemTenant(),   // sets TenantID = "system"
)
```

> ⚠️ **Use `WithSystemTenant()` only when the event genuinely has no tenant scope** — scheduled batch jobs, cross-tenant reconciliation, or platform-level operational events. Do not use it to avoid looking up a tenant ID, to simplify a code path, or because the tenant is "unknown at call time" (that last case means the upstream context is missing and should be fixed, not papered over).
> Misuse bypasses the tenant isolation that `platform-pgcommon`'s RLS policies enforce on the consumer side. A consumer handler that receives a `system` tenant ID will execute DB queries without a tenant GUC set, which causes RLS policies to either reject the query or return the wrong row set, depending on your policy definition. The failure is silent at the event level and only surfaces as incorrect data in the consuming service.

**`source` field contract:** identifies the producing service. Rules:

| Rule | Detail |
|---|---|
| Stable and globally unique | Use the canonical service name: `platform-iam`, `billing-service`, `inventory-worker`. It must be unique across all services in the organisation |
| Immutable across deployments | Do not derive it from hostname, pod name, or any runtime variable — it must be the same value in every environment |
| Environment-free | Never append `-dev`, `-staging`, `-prod`, or a region suffix. The environment is implicit in which AWS account/topic the event lands on; embedding it in `source` makes Loki/Tempo queries environment-specific and breaks dashboards when you promote code |
| Set as a constant | Define it once as a package-level constant in the service (`const serviceName = "platform-iam"`) and pass it to `NewEnvelope` and `events.Init` from that single source of truth |

`source` is used for debugging, tracing (`sns.publish` span attribute), Prometheus metric labels, and consumer routing. Changing it severs observability continuity — historical log queries, dashboards, and alert rules that filter on `source` will silently stop matching.

**`event_type` convention:** `<domain>.<entity>.<past-tense-verb>[.v<N>]` — e.g. `iam.user.created`, `billing.invoice.settled`. The `.v<N>` segment is **only added for breaking payload changes** (v1 is implicit).

**Event types are immutable once published.** Additive fields (tagged `json:",omitempty"`) are allowed in the same type. Removing, renaming, changing the type, or changing the meaning of an existing field requires a new event type (e.g. `iam.user.created.v2`). See [EVENT_SCHEMA_GOVERNANCE.md § Event versioning](EVENT_SCHEMA_GOVERNANCE.md#event-versioning) for the full three-tier model and decision guide.

**Consumers ignore unknown fields by default (Go JSON behaviour).** This is a safety guarantee — do not disable it. `json.Decoder.DisallowUnknownFields()` turns every non-breaking producer addition into a consumer runtime failure. If you find yourself reaching for strict decoding to catch typos in field names, write a struct-tag linter instead.

**`WithSchemaVersion`** records the payload contract version in the envelope. Set `"1"` at inception. Increment **only** when both conditions are true: (1) a new optional field was added **and** (2) at least one consumer needs to branch logic based on whether that field is present. Do not increment for fields that are purely additive and whose absence consumers will handle identically to their presence (e.g. a display-only label that is simply rendered or ignored):

```go
env := events.NewEnvelope("iam.user.created.v2", "platform-iam", payload,
    events.WithTenantID(rc.TenantID),
    events.WithTraceID(rc.TraceID),
    events.WithSchemaVersion("1"), // v2 event type starts a new schema series at "1"
)
```

Consumers that receive an unrecognised `schema_version` should log a warning and delete the message rather than silently misparsing it. If `schema_version` is absent, treat it as `"1"` for backward compatibility.

### Serialisation

```go
// Marshal to JSON wire format.
data, err := env.JSON()

// Unmarshal and validate required fields.
env, err := events.ParseEnvelope[UserCreatedPayload](data)
```

### Payload typing

`Envelope[T any]` is generic, but two concrete forms appear throughout the codebase with different purposes:

| Form | When to use |
|------|-------------|
| `Envelope[json.RawMessage]` | Publishing (`Publisher.Publish` requires this form); handler dispatch (the `Handler` func type uses this); routing layers that inspect headers but not the payload; audit loggers; DLQ handlers that forward without parsing |
| `Envelope[YourStruct]` | Direct serialisation via `env.JSON()` in tests; `ParseEnvelope[YourStruct]` when you own the event type and want the payload pre-parsed; mock injection in unit tests with `mock.Inject(env)` |

**The transport boundary always uses `json.RawMessage`.** The `Publisher` and `Handler` interfaces are both fixed to `json.RawMessage` — this is intentional. It keeps the routing layer (SQS consumer, outbox runner) free of business-type imports and avoids a dependency on the payload struct definition.

Typed payload access happens *inside* the handler after the transport layer has delivered the envelope:

```go
// ✅ Correct pattern — transport receives json.RawMessage; handler parses to typed struct
func handleUserCreated(ctx context.Context, env events.Envelope[json.RawMessage]) error {
    var payload UserCreatedPayload
    if err := json.Unmarshal(env.Payload, &payload); err != nil {
        return fmt.Errorf("unmarshal UserCreatedPayload: %w", err)
    }
    // payload is now typed; env.ID / env.TenantID / env.TraceID are always available directly
    return repo.CreateUser(ctx, payload)
}

// ✅ Also correct — ParseEnvelope when consuming outside a Handler context
env, err := events.ParseEnvelope[UserCreatedPayload](rawBytes)
// env.Payload is now UserCreatedPayload (not json.RawMessage)
```

**When publishing**, always marshal to `json.RawMessage` before calling `Publish` or `Enqueue`:

```go
// ✅ Correct
raw, err := json.Marshal(UserCreatedPayload{UserID: user.ID, Email: user.Email})
if err != nil { ... }
env := events.NewEnvelope("iam.user.created", "platform-iam", json.RawMessage(raw),
    events.WithTenantID(rc.TenantID),
    events.WithTraceID(rc.TraceID),
    events.WithSchemaVersion("1"),
)

// ❌ Wrong — NewEnvelope[UserCreatedPayload] produces Envelope[UserCreatedPayload],
// which cannot be passed to Publisher.Publish or outbox.Enqueue without explicit conversion
env := events.NewEnvelope("iam.user.created", "platform-iam", UserCreatedPayload{...})
publisher.Publish(ctx, env) // compile error: Envelope[UserCreatedPayload] ≠ Envelope[json.RawMessage]
```

**Routing handlers** that need to inspect the event type and forward without parsing should stay with `json.RawMessage` throughout — no unmarshal cost, no coupling to payload structs:

```go
// Fan-out router — dispatches by event_type without ever touching the payload
func route(ctx context.Context, env events.Envelope[json.RawMessage]) error {
    switch env.Type {
    case "iam.user.created":
        return iamConsumer.Publish(ctx, env)   // forward as-is
    case "billing.invoice.settled":
        return billingConsumer.Publish(ctx, env)
    default:
        return nil // unknown type; discard
    }
}
```

### Envelope ID

`Envelope.ID` is a **UUID v7** (time-ordered, collation-friendly in Postgres B-tree indexes). Use it as an idempotency key on the consumer side.

### Envelope compatibility guarantees

The `id`, `type`, `source`, and `timestamp` fields are **stable** — always present, never removed or renamed, format frozen within `v1.x`. The remaining fields (`tenant_id`, `trace_id`, `correlation_id`, `schema_version`, `subject`, `actor`, `schema_id`) are **contextual** — present when set, never removed. The library may add new optional fields in MINOR releases; existing consumers are unaffected. See [ARCHITECTURE.md § Envelope compatibility guarantees](ARCHITECTURE.md#envelope-compatibility-guarantees) for the full per-field stability class table and the `v1.x` never-break list.

---

## SNS Publisher

```go
publisher, err := events.NewSNSPublisher(events.SNSConfig{
    TopicARN:    os.Getenv("SNS_TOPIC_ARN"), // required — returns error if empty or invalid ARN format
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

> ⚠️ **`PublishBatch` is not atomic and must never be used for critical domain events.** SNS batch publish is a best-effort call: some messages in the batch may succeed while others fail within the same call. A returned error does not mean the entire batch was rejected. For any event tied to a state change or a database write, use `outbox.Enqueue` — partial batch failure combined with a process crash leaves no recovery path.

Callers must inspect the error to distinguish partial from total failure:

```go
err := publisher.PublishBatch(ctx, envelopes)
if err == nil {
    return nil // all succeeded
}

var batchErr *events.BatchError
if errors.As(err, &batchErr) {
    // Partial failure — some messages published, some did not.
    // batchErr.Failures is []BatchFailure{Index int, Err error}.
    // Successfully published messages must NOT be retried.
    failed := make([]events.Envelope[json.RawMessage], 0, len(batchErr.Failures))
    for _, f := range batchErr.Failures {
        logger.Warn(ctx, "batch publish failure",
            zap.Int("index", f.Index),
            zap.String("event_id", envelopes[f.Index].ID),
            zap.Error(f.Err),
        )
        failed = append(failed, envelopes[f.Index])
    }
    // Retry failed messages individually or hand off to the outbox for durable retry.
    return publisher.PublishBatch(ctx, failed)
}

// Non-BatchError: total failure (network error, auth failure, etc.) — safe to retry the full batch.
return fmt.Errorf("publishBatch: %w", err)
```

**Key rules:**

| Rule | Reason |
|---|---|
| Never retry the full batch on a `BatchError` | Messages that succeeded would be published a second time, creating duplicates |
| Always extract failed indices from `batchErr.Failures` | The batch index is the only link between a failure and the original envelope |
| Prefer the outbox for durable retry | If the caller cannot afford to lose messages on process crash, use `outbox.Enqueue` instead of `PublishBatch` — the outbox handles partial failures and retries automatically |
| `PublishBatch` is appropriate for idempotent or at-most-once scenarios | Background notifications, cache invalidation signals, or any event where a missed or duplicate delivery is acceptable |

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

> ⚠️ **FIFO does not mean exactly-once delivery end-to-end.** This is the most common FIFO misconception:
>
> | Layer | What FIFO guarantees | What it does NOT guarantee |
> |-------|----------------------|---------------------------|
> | SNS | Deduplicates identical `MessageDeduplicationID` values within a **5-minute window** | Deduplication outside that window; delivery to SQS is still at-least-once |
> | SQS | Ordered delivery within a `MessageGroupID`; no duplicate delivery **within a single consumer session** | Protection against redelivery after a visibility timeout expires or a consumer crashes mid-handler |
> | End-to-end | Ordered, deduplicated fan-out from SNS to SQS | That the consumer handler runs exactly once — it will not if the handler crashes after processing but before `DeleteMessage` |
>
> **FIFO gives you ordering. Idempotency still gives you safety.** Use `WithMessageGroupID` to enforce processing order within a group (e.g. per-tenant, per-aggregate). Use `Envelope.ID` + `INSERT ... ON CONFLICT DO NOTHING` to make the handler safe to run twice. The two properties are independent and both are required for correct behaviour. See [Implementing idempotency](#implementing-idempotency) for the concrete pattern.

### Message attributes

`EventType`, `TenantID`, `Source`, `EventID`, and `Subject` (when non-empty) are set as SNS message attributes. This enables SQS subscription filter policies that scope queues to specific event types, tenants, or resource subjects without deserialising the message body. `Actor` is an audit-trail field and is not forwarded as an SNS attribute.

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
| `nil` | SQS message deleted from queue |
| `non-nil error` | SQS message left visible; retried after visibility timeout |
| Panic | Recovered; stack trace logged; SQS message left visible for retry |
| Unmarshal failure | Message deleted immediately; counted as `events_consumed_total{status=malformed}` |

**VisibilityTimeout limit:** SQS enforces a hard maximum of 12 hours. `NewSQSConsumer` returns an error if `VisibilityTimeout > 12h`.

**Context contract:** the `ctx` passed to each handler has its cancellation stripped via `context.WithoutCancel` so handlers run to completion during graceful shutdown. As a consequence, `ctx.Deadline()` always returns a zero time — handlers must set their own timeouts (e.g. `context.WithTimeout(ctx, 5*time.Second)`) instead of relying on the parent deadline. Cancellation is delivered only when the consumer's drain timeout expires.

### Error classification

The handler return value is the only signal the consumer uses to decide retry-or-delete. **Returning the wrong kind determines whether a broken message sits in SQS forever or gets silently discarded.**

| Error class | Examples | Return | Outcome |
|---|---|---|---|
| **Transient** | DB connection timeout, downstream HTTP 503, network blip, lock contention | `error` | Message left visible; SQS retries after visibility timeout; moves to DLQ after `MaxReceiveCount` |
| **Permanent** | Payload fails business validation, unknown `event_type` your handler cannot process, schema version too new to parse | `nil` + log at `WARN`/`ERROR` | Message deleted immediately; no retry; no DLQ pressure |
| **Programming error** | Nil pointer, index out of range, type assertion failure | `error` (let the panic recover do it) | Message retried; surfaced in `events_consumed_total{status=error}`; investigate immediately |
| **Unknown / unexpected** | Error from a dependency you haven't classified | `error` | Retry by default; safe to escalate to DLQ if unresolved |

> ⚠️ **Misclassifying permanent errors as transient is the most common handler mistake.** If a malformed payload is returned as an `error`, SQS retries it `MaxReceiveCount` times, then pushes it to the DLQ — where it will sit indefinitely, consuming DLQ space and alerting on-call with a metric that can never self-heal. Return `nil` (and log) for any message that cannot succeed on retry regardless of how many times it is delivered.

**Classification pattern:**

```go
func handleUserCreated(ctx context.Context, env events.Envelope[json.RawMessage]) error {
    var payload UserCreatedPayload
    if err := json.Unmarshal(env.Payload, &payload); err != nil {
        // Permanent: malformed payload will never unmarshal correctly on retry
        logger.Error(ctx, "failed to unmarshal UserCreatedPayload — discarding",
            zap.String("event_id", env.ID),
            zap.Error(err),
        )
        return nil
    }

    if payload.UserID == "" {
        // Permanent: business validation failure; retrying won't fix a missing UserID
        logger.Warn(ctx, "UserCreatedPayload missing user_id — discarding",
            zap.String("event_id", env.ID),
        )
        return nil
    }

    if err := repo.CreateUser(ctx, payload); err != nil {
        // Transient: DB errors may resolve; let SQS retry
        return fmt.Errorf("createUser: %w", err)
    }

    return nil
}
```

### Poison messages

A **poison message** is structurally valid JSON — it deserialises without error — but cannot be processed successfully regardless of how many times it is retried. It is distinct from a transient error (which may succeed on retry) and from a malformed message (which fails JSON parsing).

Common causes:

| Cause | Example |
|---|---|
| Invalid business state | `user_id` references a user that was deleted before the event arrived |
| Illegal state transition | An `order.shipped` event arrives for an order already in `cancelled` state |
| Missing precondition | A `payment.settled` event arrives but no corresponding `payment.created` exists |
| Schema version too new | `schema_version: "5"` but this consumer only understands up to `"3"` |

**Handling pattern:**

```go
func handleOrderShipped(ctx context.Context, env events.Envelope[json.RawMessage]) error {
    var payload OrderShippedPayload
    if err := json.Unmarshal(env.Payload, &payload); err != nil {
        return nil // malformed — discard (see Error classification)
    }

    order, err := repo.GetOrder(ctx, payload.OrderID)
    if err != nil {
        return fmt.Errorf("getOrder: %w", err) // transient — retry
    }
    if order == nil {
        // Poison: the order doesn't exist and never will on retry.
        // Log with full correlation context so the event can be investigated.
        logger.Error(ctx, "poison message: order not found",
            zap.String("event_id", env.ID),
            zap.String("event_type", env.Type),
            zap.String("order_id", payload.OrderID),
            zap.String("trace_id", env.TraceID),
            zap.String("tenant_id", env.TenantID),
        )
        return nil // drop — do NOT retry
    }
    if order.Status == "cancelled" {
        // Poison: invalid state transition; retrying will never change the order status.
        logger.Warn(ctx, "poison message: illegal state transition — order already cancelled",
            zap.String("event_id", env.ID),
            zap.String("order_id", payload.OrderID),
        )
        // Optional: forward to an audit or failure topic for later investigation.
        _ = auditPublisher.Publish(ctx, events.NewEnvelope(
            "platform.event.poison",
            "order-consumer",
            json.RawMessage(env.Payload), // forward original payload
            events.WithTenantID(env.TenantID),
            events.WithCorrelationID(env.ID), // link to original event
            events.WithSchemaVersion("1"),
        ))
        return nil // drop
    }

    return repo.MarkShipped(ctx, order.ID)
}
```

**Decision rules:**

1. **Return `nil`, not `error`** — returning an error retries the message; a semantically invalid message will never become valid on retry.
2. **Log at `ERROR` or `WARN` with the full correlation set** (`event_id`, `event_type`, `trace_id`, `tenant_id`) — poison messages are silent data anomalies; the log is the only record that something was discarded.
3. **Optionally forward to an audit or failure topic** if the business impact is high enough to warrant investigation or replay tooling. This is preferable to letting the message hit the SQS DLQ, where it mixes with transient-failure messages and is harder to query.
4. **Do not forward sensitive payload data** (PII, credentials) to an audit topic without confirming the destination has appropriate access controls and retention policies.

> The distinction between a **transient error** (DB timeout — retry will probably work) and a **poison message** (missing prerequisite record — retry will never work) is a business judgement, not a technical one. When in doubt, retry once more and log at `WARN`; escalate to `ERROR` + drop after two consecutive failures on the same `event_id`.

### Accessing the trace ID inside a handler

The SQS consumer injects `env.TraceID` into the handler context. Retrieve it without importing the internal port package:

```go
func myHandler(ctx context.Context, env events.Envelope[json.RawMessage]) error {
    traceID := events.TraceIDFromContext(ctx) // "" if not set
    // ...
}
```

Retries are driven by SQS visibility timeouts — not by the library — and follow the queue's redrive policy. The retry limit (`MaxReceiveCount`) is a **queue configuration**, not a library setting. See [ARCHITECTURE.md § Failure lifecycle](ARCHITECTURE.md#failure-lifecycle) for the full consumer-side retry timeline and how it differs from outbox (producer-side) retries.

### Concurrency and graceful shutdown

```go
events.WithConcurrency(n)             // bound goroutines via semaphore; default 1
events.WithVisibilityTimeout(d)       // per SQS message timeout; default 30s
events.WithDeadLetterHandler(fn)      // receives SQS messages exceeding MaxReceiveCount
```

`Stop()` cancels the receive loop, then waits up to `DrainTimeout` (default 30 s) for in-flight handlers to complete — matching `net/http.Server.Shutdown` semantics for clean Kubernetes pod termination.

### Ordering and timestamp semantics

> ⚠️ **Do not rely on `Envelope.Timestamp` for strict ordering across services.** `Envelope.Timestamp` is a wall-clock timestamp recorded by the producing host at `NewEnvelope` call time. Clock skew between service instances — typically sub-millisecond with NTP, but unbounded in theory under network partitions or VM clock drift — means two events from different hosts with the same or adjacent timestamps have no defined order. An event with a later `Timestamp` may have been created on a clock that runs 50 ms fast; the "earlier" event may actually represent a later real-world occurrence.

`Envelope.Timestamp` records **when the producer created the event**, not when it was delivered or processed. Events may reach a consumer:

- **Out of order** — a message published 10 seconds after another may be delivered first (standard SQS) or requeued after a visibility timeout and delivered after a later message
- **Delayed** — outbox poll interval, SQS propagation delay, and visibility extensions all add latency between creation and processing
- **Duplicated** — at-least-once delivery means the same `Envelope.ID` may arrive more than once; the handler must be idempotent regardless

| Use case | Correct approach |
|---|---|
| Detect which of two events from the **same service** is newer | Compare `Envelope.Timestamp` — same-host clock skew is negligible; treat equal-timestamp events as unordered |
| Detect ordering **across different services** | Do not use `Envelope.Timestamp` — use FIFO with a shared `MessageGroupID`, or store a sequence number in the payload from a single authoritative source |
| Enforce strict processing order within an aggregate | FIFO topic + stable `MessageGroupID` (e.g. `AggregateID`) |
| Reconstruct a timeline for audit/display | Sort by `Envelope.Timestamp` after collection — acceptable for human-readable display; document that ±100 ms accuracy is the practical bound |
| React only to the latest state (last-write-wins) | Check stored `processed_at` timestamp before applying; skip if `env.Timestamp` ≤ stored value — valid only when both events originate from the same service |

### RLS tenant propagation

The handler context has `pgcommon.WithGUCSet(ctx, GUCSet{TenantID: env.TenantID})` injected automatically. All downstream pool calls (`pool.WithConn`, `pgcommon.RunInTx`) enforce the event's tenant context without any extra code in the handler.

### Side-effect classification

A handler's idempotency requirements depend on the class of side effect it performs. Different side effects need different guarding strategies — applying the same pattern to all of them either under-protects high-risk operations or over-engineers low-risk ones.

| Side-effect class | Examples | Duplicate risk | Required guard |
|---|---|---|---|
| **DB write** | `INSERT`, `UPDATE`, state machine transition, balance change | High — duplicate rows, double charges, incorrect state | Wrap in `pgcommon.RunInTx` with `processed_events ON CONFLICT DO NOTHING`; both commit atomically or neither does |
| **External HTTP call (mutating)** | Charge a payment, send an API request to a third party, trigger a webhook | High — third party has no knowledge of your idempotency key; duplicate call = duplicate charge/action | Pass `Envelope.ID` as the idempotency key in the downstream request (Stripe, most payment APIs support this); or gate the call with a prior `processed_events` check |
| **Email / SMS / push notification** | Welcome email, OTP, shipping confirmation | Medium — duplicate notifications are visible to the user but not catastrophic | Gate with `processed_events` before sending; or use your notification provider's deduplication key (`Envelope.ID`) if supported |
| **Cache write / invalidation** | Redis `SET`, CDN purge, in-memory state update | Low — a duplicate cache write is idempotent by nature; a duplicate invalidation is harmless | No guard needed for pure cache writes; if cache drives a downstream decision, ensure the underlying DB write is guarded instead |
| **Read-only / observability** | Incrementing a counter, logging, emitting a metric | None — a duplicate log line or metric data point is acceptable | No guard needed |

**Design principle:** identify every side effect in a handler before shipping it, assign it to one of the classes above, and verify the corresponding guard is in place. A handler with multiple side effects in different classes needs different guards for each:

```go
func handlePaymentSettled(ctx context.Context, env events.Envelope[json.RawMessage]) error {
    var payload PaymentSettledPayload
    if err := json.Unmarshal(env.Payload, &payload); err != nil {
        return nil // permanent failure — discard
    }

    return pgcommon.RunInTx(ctx, pool, pgx.TxOptions{}, func(ctx context.Context, tx pgx.Tx) error {
        // ① DB write — guarded by processed_events (atomically)
        tag, err := tx.Exec(ctx,
            `INSERT INTO processed_events (event_id, processed_at) VALUES ($1, NOW()) ON CONFLICT DO NOTHING`,
            env.ID,
        )
        if err != nil { return err }
        if tag.RowsAffected() == 0 {
            return nil // already processed — skip all side effects below
        }

        // ② DB write — safe inside the same transaction; rolls back with ① on error
        if err := repo.MarkInvoicePaid(ctx, tx, payload.InvoiceID); err != nil {
            return err
        }

        // ③ External HTTP call — only reached if ① succeeds (first delivery)
        //    Pass env.ID as idempotency key so the payment provider deduplicates
        //    if this function is somehow called twice despite the guard above.
        if err := paymentProvider.Confirm(ctx, payload.ChargeID, env.ID); err != nil {
            return fmt.Errorf("confirm charge: %w", err) // transient — retry
        }

        return nil
        // ④ Email notification — send AFTER the transaction commits (in a defer or post-commit hook)
        //    so a failed send does not roll back the DB write. Accept the rare duplicate on retry.
    })
}
```

> External HTTP calls that occur *inside* a database transaction hold the transaction open for the duration of the network call, increasing lock contention. For long-running external calls, consider committing the DB write first (using `processed_events` as the gate) and performing the external call in a post-commit step that retries independently.

### Implementing idempotency

SQS delivers messages **at least once**. A handler may be called more than once for the same `Envelope.ID` due to:
- Visibility timeout expiry (handler took too long)
- Network error between handler completion and `DeleteMessage`
- Consumer restart mid-batch

Without idempotency, duplicate delivery causes duplicate side effects: double charges, double emails, double DB rows. Use `Envelope.ID` as the idempotency key.

#### Pattern 1 — Postgres unique constraint (recommended)

Create a `processed_events` table once per service:

```sql
-- migration: add to your service's schema migrations
CREATE TABLE IF NOT EXISTS processed_events (
    event_id    TEXT        PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Optional: prune records older than your SQS message retention period (default 4 days).
-- Run as a nightly job or a Postgres cron extension task.
-- DELETE FROM processed_events WHERE processed_at < NOW() - INTERVAL '5 days';
```

Then guard every handler inside the same transaction as the side-effect write:

```go
func handleUserCreated(ctx context.Context, env events.Envelope[json.RawMessage]) error {
    return pgcommon.RunInTx(ctx, pool, pgx.TxOptions{}, func(ctx context.Context, tx pgx.Tx) error {
        // 1. Claim the event ID — ON CONFLICT DO NOTHING is atomic.
        tag, err := tx.Exec(ctx, `
            INSERT INTO processed_events (event_id)
            VALUES ($1)
            ON CONFLICT (event_id) DO NOTHING
        `, env.ID)
        if err != nil {
            return fmt.Errorf("idempotency check: %w", err)
        }
        if tag.RowsAffected() == 0 {
            // Already processed by a previous delivery — safe no-op.
            // Returning nil causes the SQS message to be deleted without re-running side effects.
            return nil
        }

        // 2. Side-effect writes execute only if the INSERT above succeeded.
        var payload UserCreatedPayload
        if err := json.Unmarshal(env.Payload, &payload); err != nil {
            return fmt.Errorf("unmarshal: %w", err)
        }
        return repo.CreateUser(ctx, tx, payload)
    })
}
```

**Why inside the same transaction?** If the side-effect write succeeds but the transaction rolls back before `processed_events` commits, the next delivery re-inserts the row and re-runs the side effect — correct behaviour. If `processed_events` commits but the side-effect write fails, the transaction rolls back atomically — both are absent, and the next delivery retries both.

#### Pattern 2 — Upsert-based idempotency

When the side effect is a row update (not an insert), use `ON CONFLICT DO UPDATE` with a sentinel column:

```go
tag, err := tx.Exec(ctx, `
    INSERT INTO user_activations (user_id, activated_at, source_event_id)
    VALUES ($1, NOW(), $2)
    ON CONFLICT (user_id) DO UPDATE
        SET activated_at    = EXCLUDED.activated_at,
            source_event_id = EXCLUDED.source_event_id
    WHERE user_activations.source_event_id IS DISTINCT FROM EXCLUDED.source_event_id
`, payload.UserID, env.ID)
if tag.RowsAffected() == 0 {
    return nil // same event_id already applied
}
```

Use this when a separate `processed_events` table adds unacceptable overhead, or when the natural primary key of the affected row provides the idempotency boundary.

#### Pattern 3 — Redis SETNX (for non-Postgres handlers)

For services without a relational database (e.g. sending an email, calling an external API):

```go
func handleWelcomeEmail(ctx context.Context, env events.Envelope[json.RawMessage]) error {
    key := "processed:" + env.ID
    // SET key 1 EX 432000 NX  (5 days TTL matches SQS max retention)
    set, err := redisClient.SetNX(ctx, key, "1", 5*24*time.Hour).Result()
    if err != nil {
        return fmt.Errorf("idempotency redis: %w", err)
    }
    if !set {
        return nil // already processed
    }
    return emailSvc.SendWelcome(ctx, env.Payload)
}
```

**Caveat:** Redis SETNX is not durable across Redis restarts without AOF persistence. For financial or critical events, Pattern 1 is safer.

#### Common mistakes

| Mistake | Consequence | Fix |
|---|---|---|
| Check `processed_events` in a separate query before the transaction | TOCTOU race — two concurrent deliveries both pass the check and both execute | Always check inside the same transaction as the side-effect write |
| No idempotency at all | Duplicate charges, emails, or rows on any redelivery | Use Pattern 1 or 2 |
| Idempotency outside the transaction | DB write succeeds, then `processed_events` insert fails → unguarded on next delivery | Idempotency claim and side-effect write must commit atomically |
| Never pruning `processed_events` | Table grows unboundedly | Nightly job: `DELETE ... WHERE processed_at < NOW() - INTERVAL '5 days'` |

---

## Transactional outbox

The outbox pattern eliminates dual-write risk: the event is written **inside the business transaction** alongside the domain mutation. If the transaction rolls back, the event is never published. The runner delivers asynchronously with at-least-once guarantee.

Outbox does not guarantee global ordering — use FIFO topics with a stable `MessageGroupID` (e.g. `TenantID`) when ordering is required.

```
Legend:  ✅ transaction boundary   🔁 retry point   📦 durable storage   ⚡ async boundary

HTTP Handler / Background Job
  │
  └─► pgcommon.RunInTx ─────────────────────────────────────── ✅ transaction boundary
            │
            ├─► repo.Save(ctx, tx, domainObject)   ← domain write    → 📦 Postgres
            └─► outbox.Enqueue(ctx, tx, envelope)  ← event write     → 📦 outbox_events
            │
            ▼
         COMMIT  (both writes or neither — zero crash window)         ✅ transaction boundary
            │
            ▼  ⚡ async — ≥1 poll interval (default 5s) later
Outbox Runner (polls outbox_events)                                   🔁 retry point
  │           (attempts < MaxAttempts; else → outbox_dead_letters)    📦 dead letters on failure
  └─► Publisher.Publish(ctx, envelope)
            │
            ▼
          AWS SNS ─────────────────────────────────────────────────── 📦 SNS durability
            │
            ▼
          AWS SQS  (fan-out via subscription filter policies)         📦 SQS durability
            │
            ▼  🔁 retry point (visibility timeout + MaxReceiveCount → SQS DLQ on exhaustion)
  Consumer Handler(ctx, envelope)
            │
            ├─► pgcommon.RunInTx ─────────────────────────────────── ✅ transaction boundary
            │         ├─► processed_events INSERT ON CONFLICT DO NOTHING  (idempotency guard)
            │         └─► repo.Apply(ctx, tx, payload)
            │
            ▼
       DeleteMessage  (only on handler success)
```

**Crash safety:** if the process dies between `COMMIT` and `Publish`, the runner rediscovers the undelivered outbox record on the next poll (`🔁`). If it dies between `Publish` and `DeleteMessage`, the consumer's idempotency check prevents the side effect from applying twice — the `✅` on the consumer side guarantees the guard and the side-effect commit atomically.

### Wiring

```go
// 1. Apply outbox schema migration (once at startup).
migrateRunner := &migrate.Runner{DSN: os.Getenv("DATABASE_URL")}
if err := outbox.ApplySchema(ctx, migrateRunner); err != nil {
    log.Fatal(err)
}

// 2. Construct the outbox runner.
runner, err := outbox.NewRunner(outbox.Config{
    Pool:         pool,       // *pgcommon.Pool — required
    Publisher:    publisher,  // events.Publisher — required
    Logger:       logger,
    PollInterval: 5 * time.Second,
    BatchSize:    50,
    MaxAttempts:  5,
    // Production-hardening options (all have safe defaults):
    PublishConcurrency: 1,                 // default — SNS PublishBatch (10/API call); raise only when SNS latency-bound
    PublishTimeout:     10 * time.Second, // per-record publish timeout; sequential batch uses len(batch) × this
    DrainTimeout:       30 * time.Second, // Stop() waits up to this for the in-flight batch
    StartupJitter:      200 * time.Millisecond, // random delay before first poll; desyncs replicas on rolling restart
    // To load these from env: config.RunnerConfigFromEnv(config.LoadOutbox(), pool, publisher, logger)
})
if err != nil {
    log.Fatal(err) // ClaimLeaseDuration too short for configured BatchSize × PublishTimeout
}
go runner.Start(ctx) // blocks until ctx is cancelled
defer func() {
    // Stop() returns an error if the in-flight batch does not drain within DrainTimeout.
    if err := runner.Stop(); err != nil {
        logger.Warn("outbox runner drain timeout — records retry after lease expiry", map[string]any{"error": err.Error()})
    }
}()

// Optional: gate the Kubernetes readiness probe until the first poll succeeds.
// Ready() returns a channel that is closed after the first successful (or empty) poll,
// confirming that the DB connection is live and the outbox schema exists.
//
//   select {
//   case <-runner.Ready():
//       // signal /readyz OK
//   case <-time.After(30 * time.Second):
//       // signal /readyz not ready
//   }
```

**Config defaults:** `PollInterval` 5s · `BatchSize` 50 · `MaxAttempts` 5 · `PublishConcurrency` 1 · `PublishTimeout` 10s · `DrainTimeout` 30s · `ClaimLeaseDuration` 10m · `StartupJitter` 0. When `PublishConcurrency` is `1` (default), the runner publishes via SNS `PublishBatch` (10 messages per API call). Values `> 1` publish records in parallel goroutines with per-record `Publish` calls. When a poll cycle fails (e.g. the DB is unreachable), the runner applies exponential backoff (1s → 30s) before retrying instead of hammering the pool every `PollInterval`.

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

`Enqueue` validates the envelope (non-nil tx; non-empty `ID`/`Type`/`Source`; non-zero `Timestamp`; no null bytes in `ID`/`Type`/`Source`) and rejects payloads whose serialised size exceeds **240 KB** — staying under the SNS 256 KB hard limit so an outbox record that could never publish is never persisted.

### Payload size guidelines

| Tier | Limit | Meaning |
|------|-------|---------|
| **Hard limit** | 256 KB | SNS `Publish` API rejects messages above this — non-negotiable |
| **Enforced limit** | 240 KB | `outbox.Enqueue` rejects at this threshold, leaving a 16 KB headroom for SNS message attributes and envelope wrapper overhead |
| **Recommended** | ≤ 64 KB | Comfortable budget for typical domain events; anything larger warrants a review |

Payloads that approach the enforced limit carry hidden costs beyond the immediate rejection risk:

| Risk | Detail |
|---|---|
| **Latency** | SNS `Publish` and SQS `ReceiveMessage` are synchronous network calls; 240 KB takes measurably longer than 4 KB across the same connection |
| **SNS/SQS cost** | Both services charge per 64 KB chunk — a 240 KB message costs 4× a 60 KB message |
| **Consumer memory pressure** | A concurrency of 10 with 240 KB messages means 2.4 MB held simultaneously per pod just in envelope buffers, before any unmarshalling allocations |
| **HMAC and logging overhead** | `SignEnvelope` marshals the full envelope before hashing; large payloads amplify signing cost and make structured log lines unreadable |

**When the payload is inherently large** (binary content, rendered templates, bulk export rows), store the data in object storage and put only a reference in the envelope:

```go
// ✅ Correct — payload carries a reference, not the data
type ReportGeneratedPayload struct {
    ReportID  string `json:"report_id"`
    S3Bucket  string `json:"s3_bucket"`
    S3Key     string `json:"s3_key"`
    SizeBytes int64  `json:"size_bytes,omitempty"`
}

// ❌ Wrong — embedding a rendered PDF or bulk CSV in the payload
type ReportGeneratedPayload struct {
    ReportID string `json:"report_id"`
    Content  []byte `json:"content"` // base64-encoded; will hit the 240 KB limit
}
```

The consumer fetches the S3 object using the reference after receiving the event. This keeps events cheap, fast, and inspectable while the actual data lives in appropriate storage. Set an S3 presigned URL expiry that outlasts your `SQS_VISIBILITY_TIMEOUT` plus expected handler duration — a reference that expires before the consumer can fetch it turns a delivery into a dead letter.

### Poll cycle

The runner fires one poll immediately on startup, then once per `PollInterval` tick.

1. `SELECT … FOR UPDATE SKIP LOCKED WHERE published_at IS NULL AND scheduled_at <= NOW()` — claim up to `BatchSize` records. Safe for horizontal scale; concurrent runners claim disjoint batches.
2. **Lease:** push `scheduled_at` forward by `ClaimLeaseDuration` (default 10 min) so other runners cannot re-claim the same records while publishing is in progress.
3. For each outbox record: call `Publisher.Publish`; on success set `published_at = NOW()`.
4. On failure: increment `attempts`, reset `scheduled_at = NOW()` (releases the lease for immediate retry), set `last_error`. If `attempts >= MaxAttempts` move to `outbox_dead_letters`.
5. Sleep `PollInterval`, then repeat.

Horizontal scale is achieved by running multiple outbox runners — `FOR UPDATE SKIP LOCKED` ensures work is safely partitioned across instances.

### Dead letters

> **"Dead letters" means two different things** — do not confuse them. Outbox dead letters are **publish failures** stored in Postgres. SQS DLQ entries are **consumer processing failures** stored in a separate SQS queue. They have different causes, different storage, and different recovery paths. See [ARCHITECTURE.md § Failure lifecycle](ARCHITECTURE.md#failure-lifecycle) for the complete side-by-side timeline and operational runbook.

Failed outbox records that exhaust `MaxAttempts` move to `outbox_dead_letters` — a queryable Postgres table. Inspect and replay from standard SQL tooling rather than an SQS DLQ.

```sql
SELECT * FROM outbox_dead_letters WHERE tenant_id = 'acme' ORDER BY failed_at DESC;
```

#### DLQ management API

Three methods on `Runner` give full programmatic control over dead letters without requiring direct SQL access.

**Step 1 — Inspect before acting.**

```go
// List up to 50 failures for a specific tenant, oldest first.
records, err := runner.ListDeadLetters(ctx, outbox.DLQFilter{TenantID: "acme"}, 50)
for _, r := range records {
    log.Printf("id=%s type=%s attempts=%d failed=%s error=%s",
        r.ID, r.EventType, r.Attempts, r.FailedAt.Format(time.RFC3339), r.LastError)
}
```

**Step 2a — Replay after fixing the root cause.**

```go
// Unfiltered: move ALL dead letters back to outbox_events (attempts reset to 0).
n, err := runner.ReprocessDeadLetters(ctx, 100)

// Filtered: replay only a specific event type for one tenant.
n, err := runner.ReprocessDeadLettersWith(ctx, outbox.DLQFilter{
    EventType: "billing.invoice.settled",
    TenantID:  "acme",
}, 100)

// Time-bounded: replay only records that failed before an incident window ended.
n, err := runner.ReprocessDeadLettersWith(ctx, outbox.DLQFilter{
    FailedBefore: incidentEndTime,
}, 500)
```

**Step 2b — Discard poison pills that can never succeed.**

```go
// ⚠️ Always call ListDeadLetters first to confirm the selection.
n, err := runner.DiscardDeadLetters(ctx, outbox.DLQFilter{
    EventType: "legacy.sync.requested", // decommissioned event type
}, 1000)
log.Printf("discarded %d irrecoverable dead letters", n)
```

`DLQFilter` fields are all optional (zero value = match all):

| Field | Type | Meaning |
|-------|------|---------|
| `EventType` | `string` | Exact event type match (`""` = all types) |
| `TenantID` | `string` | Exact tenant match (`""` = all tenants) |
| `FailedBefore` | `time.Time` | Only records where `failed_at < FailedBefore` (zero = no bound) |

**Retryable failures** (SNS throttling: `ThrottlingException`, `ServiceUnavailable`, `InternalFailure`, `RequestTimeout`) do not count toward `MaxAttempts` — the outbox uses `threshold = MaxAttempts+1` for these errors. A period of SNS unavailability will not dead-letter records that are otherwise healthy.

### Idempotency

`Envelope.ID` (UUID v7) is forwarded as the SNS `MessageDeduplicationID` on FIFO topics and as a message attribute on standard topics. Handlers must use `Envelope.ID` as their idempotency key — see [SQS Consumer § Implementing idempotency](#implementing-idempotency) for concrete patterns.

A large backlog in `outbox_events` usually indicates downstream delivery issues (SNS/SQS) or insufficient runner throughput — monitor `outbox_pending_total` and scale runners accordingly.

### Pruning published records

Published records in `outbox_events` are not deleted automatically — the runner marks them with `published_at` but leaves the row in place. Without periodic pruning the table grows unboundedly, degrading `ClaimBatch` index scans over time.

Call `PrunePublished` from a scheduled job (e.g. a Kubernetes `CronJob` or `time.Ticker`):

```go
// Delete published records older than 7 days, up to 1000 per call.
n, err := runner.PrunePublished(ctx, 7*24*time.Hour, 1000)
if err != nil {
    logger.Error("outbox prune failed", map[string]any{"error": err})
}
logger.Info("outbox pruned", map[string]any{"deleted": n})
```

**Guidance:**
- `olderThan` must be long enough that all consumers have processed the event before the row is deleted. 7 days covers most SLA windows; increase for slow consumers.
- `limit` bounds the DELETE batch size (and lock hold time). For large tables, call in a loop until the return value is 0.
- Migration 007 adds a partial index on `(published_at) WHERE published_at IS NOT NULL` to make prune scans efficient. Run `outbox.ApplySchema` to apply it.

### Replay guarantees

Replay (via `ReprocessDeadLetters`, manual SQL reset, or SQS DLQ redrive) carries no stronger delivery guarantees than the original delivery path. Before triggering a replay, consumers must be prepared for all of the following:

| Property | Guarantee |
|---|---|
| **Ordering** | None. Replayed events are inserted at the back of the outbox queue and delivered in poll order, not original publish order. A replayed `user.updated` may arrive before an in-flight `user.created` for the same user. |
| **Idempotency checks triggered again** | The same `Envelope.ID` will be presented to the consumer handler a second time. The `ON CONFLICT DO NOTHING` guard must be in place — this is not a bug, it is the mechanism that makes replay safe. |
| **Timing** | Replayed events re-enter the standard poll cycle. They are not expedited. Under load, a replayed batch may take multiple poll intervals to publish. |
| **Partial replay** | `ReprocessDeadLetters(ctx, n)` moves at most `n` records per call. A large dead-letter backlog requires multiple calls or a loop. There is no atomic "replay all" operation. |
| **Concurrent live traffic** | Replay runs concurrently with live event traffic. A consumer receiving a replayed `order.created` must handle it correctly even if a `order.shipped` for the same order arrived minutes earlier via the live path. |

> **Consumers must be safe to replay at any time, not just during an incident.** Replay is also triggered by: on-call engineers restoring a backlog, a new consumer service bootstrapping from historical dead letters, and automated regression tests. A handler that is not idempotent is not replay-safe — and therefore not production-ready.

---

## HMAC helpers

### When to use HMAC

HMAC-SHA256 provides **message authenticity and integrity** — proof that the payload was produced by a party that holds the shared key and has not been tampered with in transit. It does **not** provide confidentiality (the payload is still plaintext) or replay protection (a captured signature remains valid indefinitely unless you add a nonce or expiry check).

| Scenario | HMAC required | Why |
|----------|:---:|-----|
| Receiving webhooks from external systems (Stripe, GitHub, etc.) | **Yes** | The channel is the public internet; the SNS/SQS IAM boundary does not apply |
| Cross-service call over an internal HTTP endpoint (not SNS/SQS) | **Yes** | Service-to-service HTTP has no built-in message-level auth; HMAC fills that gap |
| Zero-trust internal network where service identity is not enforced at the infra layer | **Yes** | HMAC adds app-layer authentication even when mTLS or IRSA is absent |
| Events flowing exclusively over SNS → SQS within the same AWS account and IAM boundary | **No** | SNS delivery is authenticated by IAM policies; the message cannot be injected or tampered with by an unauthorised caller |
| Events signed by the outbox runner and delivered to an SQS queue you own | **No** | The publisher (outbox runner) is operating under your service's IRSA role; IAM controls who may publish |

**Rules:**
- Always use `VerifyEnvelope` (not `Verify`) when checking a full envelope — it normalises the JSON to canonical form before hashing, preventing hash-mismatch from field reordering.
- Do not verify HMAC in a separate goroutine or after the handler context has branched — a failed verify must reject the message before any side effects occur.
- Rotate keys by accepting both the current and previous key for a short window (one deploy cycle), then dropping the old key.
- Keys must be ≥ 32 bytes. Store them in AWS Secrets Manager or SSM Parameter Store; never in environment variables checked into source control.

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
| `events_consumed_total` | Counter | `service`, `queue`, `event_type`, `status` | SQS messages processed (`status` = `success`/`error`/`malformed`/`dlq_success`/`dlq_error`; `dlq_*` emitted when the dead-letter handler is invoked) |
| `events_consume_duration_seconds` | Histogram | `service`, `queue`, `event_type` | Handler execution latency |
| `outbox_pending_total` | Gauge | `service` | Unpublished records in `outbox_events` |
| `outbox_leased_total` | Gauge | `service` | Records currently claimed (leased) by a runner — combine with `outbox_pending_total` for a complete in-flight picture |
| `outbox_published_total` | Counter | `service`, `event_type`, `status` | Records published by the runner |
| `outbox_attempts_total` | Counter | `service`, `event_type` | Total publish attempts by the runner |
| `outbox_dead_letters_total` | Counter | `service`, `event_type` | Records moved to `outbox_dead_letters` after exhausting `MaxAttempts` — alert on `rate() > 0` |
| `outbox_dead_letters_reprocessed_total` | Counter | `service` | Dead-letter records re-queued via `ReprocessDeadLetters` |
| `sqs_receive_errors_total` | Counter | `service`, `queue` | SQS `ReceiveMessage` errors (excludes context cancellation) — alert on `rate() > 0` |
| `sqs_delete_errors_total` | Counter | `service`, `queue` | SQS `DeleteMessage` errors — a non-zero rate causes duplicate message delivery |
| `sqs_visibility_extension_errors_total` | Counter | `service`, `queue` | SQS `ChangeMessageVisibility` errors — non-zero rate causes duplicate delivery for long-running handlers |
| `outbox_poll_errors_total` | Counter | `service` | Outbox poll cycle errors (ClaimBatch / DB errors) — triggers exponential backoff |
| `outbox_unmarshal_errors_total` | Counter | `service` | Outbox records that failed JSON unmarshal during publish |
| `outbox_mark_published_errors_total` | Counter | `service` | `MarkPublished` failures after a successful SNS delivery — non-zero rate signals potential duplicate delivery on next poll |
| `events_oversized_event_type_label_total` | Counter | `service` | `event_type` values that exceeded 128 bytes and were replaced with `"__oversized__"` — alert on `rate() > 0` to detect misconfigured or adversarial producers |

### OpenTelemetry

OTel is **always initialised by the consuming service**. Call `gincommon.InitTracingFromEnv()` (from `platform-gincommon`) at startup — `platform-events` calls `otel.Tracer("platform-events")` and produces no-op spans if no provider is registered.

**SNS publish span:** `sns.publish` with `messaging.system=aws_sns`, `messaging.destination`, `messaging.message_id`.

**SQS receive span:** `sqs.receive` with `messaging.system=aws_sqs`, `messaging.destination`, `messaging.message_id`, `messaging.operation=process`. The span is **linked to the publisher's trace** via `Envelope.TraceID`, giving end-to-end visibility across the SNS/SQS boundary in Tempo/Grafana.

### Logging correlation

OTel spans cover latency and errors at the infrastructure level. Structured log fields cover business-level debuggability: "which tenant's event failed, and which specific message delivery was it?"

**Always include these four fields on every log line inside a handler or publisher:**

| Field | Source | Why |
|-------|--------|-----|
| `event_id` | `env.ID` | Correlates every log line to the exact message delivery; use it to find all logs for a redelivered message |
| `event_type` | `env.Type` | Filters by domain area in Loki without parsing the payload |
| `trace_id` | `events.TraceIDFromContext(ctx)` | Links logs to the OTel trace in Tempo; empty string if the publisher did not set one |
| `tenant_id` | `env.TenantID` | Scopes to a specific customer; critical for multi-tenant support investigations |

#### Handler pattern

```go
func handleUserCreated(ctx context.Context, env events.Envelope[json.RawMessage]) error {
    log := logger.With(map[string]any{
        "event_id":   env.ID,
        "event_type": env.Type,
        "trace_id":   events.TraceIDFromContext(ctx),
        "tenant_id":  env.TenantID,
    })

    log.Info("handling event", nil)

    var payload UserCreatedPayload
    if err := json.Unmarshal(env.Payload, &payload); err != nil {
        log.Error("unmarshal failed", map[string]any{"error": err.Error()})
        return fmt.Errorf("unmarshal: %w", err)
    }

    if err := repo.CreateUser(ctx, payload); err != nil {
        log.Error("create user failed", map[string]any{"error": err.Error()})
        return fmt.Errorf("create user: %w", err)
    }

    log.Info("event processed", nil)
    return nil
}
```

Using `logger.With(...)` at the top of the handler binds the four fields to every subsequent log call in that invocation. A handler that logs without calling `With` first forces the reader to correlate log lines manually by timestamp — impractical at volume.

#### Publisher pattern

Log the envelope fields at publish time too, so a missing consumer log can be cross-referenced against the publisher log:

```go
env := events.NewEnvelope("iam.user.created", "platform-iam", payload,
    events.WithTenantID(rc.TenantID),
    events.WithTraceID(rc.TraceID),
    events.WithSchemaVersion("1"),
)
if err := outbox.Enqueue(ctx, tx, env); err != nil {
    logger.Error("enqueue failed", map[string]any{
        "event_id":   env.ID,
        "event_type": env.Type,
        "tenant_id":  env.TenantID,
        "error":      err.Error(),
    })
    return err
}
logger.Info("event enqueued", map[string]any{
    "event_id":   env.ID,
    "event_type": env.Type,
    "tenant_id":  env.TenantID,
})
```

#### Optional fields (add when relevant)

```go
map[string]any{
    "schema_version":  env.SchemaVersion, // add when debugging schema evolution issues
    "correlation_id":  env.CorrelationID, // add when tracing saga/workflow flows
    "source":          env.Source,        // add when a consumer handles events from multiple producers
}
```

#### Loki query examples

Once all handlers include these fields, cross-service debugging becomes a single query:

```logql
// All logs for a specific event delivery (publisher + consumer across services):
{service=~".+"} | json | event_id="01926e4f-1234-7abc-8def-000000000001"

// All failed handler invocations for a tenant in the last hour:
{service="notification-svc"} | json | tenant_id="acme" | level="error"

// All events of a specific type processed today:
{service="billing-svc"} | json | event_type="billing.invoice.settled"

// Trace all logs for a cross-service request:
{service=~".+"} | json | trace_id="4bf92f3577b34da6a3ce929d0e0e4736"
```

#### What NOT to log

| Do not log | Reason |
|-----------|--------|
| `env.Payload` (raw or marshalled) | May contain PII, payment data, or credentials — log `event_id` instead and look up the payload in the source database if needed |
| Full error chains that include DSNs or URLs | `DATABASE_URL` and `SNS_TOPIC_ARN` may appear in wrapped errors from AWS SDK and pgx — truncate or sanitise before logging |
| `env.TraceID` as-is in error alerts | Trace IDs are high-cardinality — use them in log lines but not as alert labels |

---

## Backpressure considerations

Under sustained load, the two independent queues — the SQS receive buffer and the outbox `outbox_events` table — will grow if consumers or the outbox runner can't keep up. This section describes the signals to watch and the knobs to turn, in the order to try them.

### Consumer-side (SQS)

The SQS receive loop is gated by `WithConcurrency(n)` (default: `1`). Each received message occupies a slot until the handler returns; messages beyond the concurrency limit stay in SQS and accumulate `ApproximateNumberOfMessages`.

| Signal | Where | What it means |
|--------|-------|----------------|
| `ApproximateNumberOfMessages` rising | CloudWatch / SQS console | Handlers are slower than the publish rate |
| `events_consume_duration_seconds` p99 > `SQS_VISIBILITY_TIMEOUT` | Prometheus | Visibility extensions are firing; handler is at risk of double-delivery |
| `events_consumed_total{status="error"}` rising | Prometheus | Handlers are failing and leaving messages to re-enter the queue |

**Tuning order:**

1. **Increase `WithConcurrency(n)`** — each unit adds one parallel handler goroutine per pod. Start here; it costs only memory and goroutine stack.
2. **Scale pods horizontally** — multiple pods each run their own receive loop. SQS distributes messages across them naturally; no coordination needed. Prefer this over very high per-pod concurrency (> 20) to keep per-handler memory bounded.
3. **Increase `SQS_MAX_MESSAGES`** — up to 10 (SQS hard limit). Increases batch size per receive call; useful when handler latency is dominated by per-message network round trips.
4. **Increase `SQS_VISIBILITY_TIMEOUT`** — if handlers legitimately take longer than the current timeout. A timeout that is too short causes duplicate delivery, which wastes work and stresses the idempotency store.

> **Do not increase `SQS_MAX_MESSAGES` as the first lever.** Larger batches help throughput only when per-message latency is the bottleneck. If your handlers are CPU- or DB-bound, more messages per receive call just means more goroutines contending for the same resource.

### Outbox-side (Postgres → SNS)

The outbox runner publishes `OUTBOX_BATCH_SIZE` records per poll cycle. If the rate of `outbox.Enqueue` calls exceeds the runner's publish throughput, `outbox_events` grows unbounded.

| Signal | Where | What it means |
|--------|-------|----------------|
| `outbox_pending_total` sustained > 0 | Prometheus | Runner is behind; enqueue rate > publish rate |
| `outbox_pending_total` growing monotonically | Prometheus | Runner is falling further behind each cycle |
| `outbox_attempts_total` / `outbox_published_total` ratio rising | Prometheus | SNS publish failures retrying; may be a downstream SNS quota issue |

**Tuning order:**

1. **Decrease `OUTBOX_POLL_INTERVAL`** — shorter sleep between cycles; the runner catches up faster. Default is `5s`; `1s` is reasonable under sustained load.
2. **Increase `OUTBOX_BATCH_SIZE`** — more records per cycle. Each batch is one `SELECT FOR UPDATE SKIP LOCKED` + N `sns:Publish` calls; keep it below `100` to avoid long-held Postgres locks.
3. **Scale runner pods horizontally** — `SKIP LOCKED` ensures multiple runners claim disjoint batches with no coordination. This is the right lever once a single pod is SNS-throughput-bound (each `Publish` call is a network round trip).
4. **Check SNS publish errors** — `outbox_attempts_total - outbox_published_total` counts retries. A rising gap usually indicates SNS throttling or network issues, not a runner configuration problem.

> **Do not increase `OUTBOX_BATCH_SIZE` before scaling pods.** A larger batch holds a Postgres lock for longer, blocking other writers on the same table. Horizontal scaling is almost always preferable.

### Capacity planning summary

```
Consumer falling behind?
├── 1. Raise WithConcurrency(n)
├── 2. Add pods (horizontal scale)
└── 3. Increase SQS_MAX_MESSAGES (only if handler latency is I/O-bound)

Outbox falling behind?
├── 1. Lower OUTBOX_POLL_INTERVAL
├── 2. Add runner pods (SKIP LOCKED handles distribution)
└── 3. Raise OUTBOX_BATCH_SIZE (watch Postgres lock contention)

Both?
└── Check SNS quota limits in CloudWatch first — a publish bottleneck
    manifests in both the outbox retry counter and the SQS dead-letter queue
```

---

## Recommended production defaults

The library ships conservative defaults that are safe for low-traffic workloads. For production services under real load, start with these values and adjust based on your `outbox_pending_total` and `ApproximateNumberOfMessages` metrics.

### SQS Consumer

```bash
SQS_MAX_MESSAGES=10          # Always set to max (SQS hard limit); reduce only for very expensive handlers
SQS_WAIT_SECONDS=20          # Long-poll at max; reduces empty-receive API calls and cost
SQS_VISIBILITY_TIMEOUT=60s   # Comfortably above your p99 handler latency; 30s default is tight
SQS_CONCURRENCY=5            # Start here; scale up to ~10 before considering horizontal pod scaling
```

> **`SQS_VISIBILITY_TIMEOUT` is the most commonly under-set value.** If your handler calls a slow DB query or downstream HTTP endpoint, the default `30s` may expire before the handler finishes, causing the message to re-appear and deliver twice. Set it to 2–3× your p99 handler duration.

### Outbox runner

```bash
OUTBOX_POLL_INTERVAL=2s      # 5s default is conservative; 1–2s is reasonable for production throughput
OUTBOX_BATCH_SIZE=50         # 50–100 is a good range; larger = fewer cycles but longer Postgres locks
OUTBOX_MAX_ATTEMPTS=5        # Default is fine; increase only if your SNS target has known transient outages
```

### Minimal production wiring example

```go
consumer, err := events.NewSQSConsumer(
    events.SQSConfig{
        QueueURL:    os.Getenv("SQS_QUEUE_URL"),
        Region:      os.Getenv("AWS_REGION"),
        MaxMessages: 10,
        WaitSeconds: 20,
        Logger:      logger,
    },
    handler,
    events.WithVisibilityTimeout(60*time.Second),
    events.WithConcurrency(5),
    events.WithDeadLetterHandler(dlqHandler), // always set in production
)
if err != nil {
    return fmt.Errorf("sqs consumer: %w", err)
}

runner, err := outbox.NewRunner(outbox.Config{
    Pool:         pool,
    Publisher:    publisher,
    Logger:       logger,
    PollInterval: 2 * time.Second,
    BatchSize:    50,
    MaxAttempts:  5,
})
if err != nil {
    return fmt.Errorf("outbox runner: %w", err)
}
```

### What to monitor on day one

| Metric | Alert threshold | Action |
|---|---|---|
| `outbox_pending_total` | > 500 sustained for 5 min | Lower `OUTBOX_POLL_INTERVAL`; add runner pods |
| `ApproximateNumberOfMessages` (CloudWatch) | > 1000 sustained | Raise `SQS_CONCURRENCY`; add consumer pods |
| `events_consumed_total{status="error"}` | > 1% error rate | Inspect handler errors; check DLQ depth |
| `outbox_published_total{status="failed"}` / total | > 1% | Check SNS reachability; inspect `outbox_dead_letters` |

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
| `OUTBOX_CLAIM_LEASE_DURATION` | `10m` (store default when unset) | How long a claimed record is hidden from other runners |
| `OUTBOX_STARTUP_JITTER` | `0` | Random delay before first poll; use `5s`–`10s` with multiple replicas |
| `OUTBOX_PUBLISH_CONCURRENCY` | `1` | Parallel publishes per poll cycle; `1` uses SNS `PublishBatch` |
| `OUTBOX_PUBLISH_TIMEOUT` | `10s` | Per-record publish timeout; `0` disables |
| `OUTBOX_DRAIN_TIMEOUT` | `30s` | `runner.Stop()` wait bound |
| `SQS_MAX_RECEIVE_COUNT` | `0` (unset) | Map to `events.WithMaxReceiveCount` when wiring `WithDeadLetterHandler` |
| `DATABASE_URL` | — | Postgres DSN for outbox runner |
| `AWS_ENDPOINT_URL` | — | Set to `http://localhost:4566` for LocalStack |
| `OTEL_SERVICE_NAME` | — | OTel resource attribute |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | |
| `OTEL_EXPORTER_OTLP_INSECURE` | `false` | Set automatically to `true` when `APP_ENV=dev` |
| `APP_ENV` | — | `dev/development/local` enables OTel insecure mode |

**Wiring helpers (`pkg/config`):** `LoadSNS` / `LoadSQS` / `LoadOutbox` / `LoadOTel` · `LogWarnings` · `SNSConfigFromEnv` · `SQSConfigFromEnv` · `SQSConsumerOptions` · `RunnerConfigFromEnv`

Copy `.env-example` to `.env` via `make setup` for local development.

---

## Error reference

| Error | Package | Returned when |
|---|---|---|
| `events.ErrEnvelopeIDRequired` | `events` | `ParseEnvelope` or `outbox.Enqueue` — missing `id` |
| `events.ErrEnvelopeTypeRequired` | `events` | `ParseEnvelope` or `outbox.Enqueue` — missing `type` |
| `events.ErrEnvelopeSourceRequired` | `events` | `ParseEnvelope` or `outbox.Enqueue` — missing `source` |
| `events.ErrKeyTooShort` | `events` | `Sign` / `SignEnvelope` called with key < 32 bytes |
| `events.ErrInvalidSignature` | `events` | Internal parse failure in `Verify` |
| `events.ErrBatchTooLarge` | `events` | Internal sentinel; `PublishBatch` splits automatically at 10 — never returned to callers |

---

## Testing in consuming services

`pkg/events/mock` ships ready-made, thread-safe test doubles so consuming services never need to stand up LocalStack or SNS just to run a unit test.

```go
import "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/events/mock"
```

### Mock Publisher

```go
func TestOrderService_PlaceOrder_PublishesEvent(t *testing.T) {
    pub := &mock.Publisher{}
    svc := NewOrderService(pub)

    err := svc.PlaceOrder(ctx, Order{ID: "ord-1", Amount: 99})
    require.NoError(t, err)

    published := pub.Published()
    require.Len(t, published, 1)
    assert.Equal(t, "order.placed", published[0].Type)
    assert.Equal(t, "acme", published[0].TenantID)
}

func TestOrderService_PlaceOrder_PublishError_IsHandled(t *testing.T) {
    pub := &mock.Publisher{}
    pub.SetError(errors.New("sns unavailable"))

    svc := NewOrderService(pub)
    err := svc.PlaceOrder(ctx, Order{ID: "ord-2", Amount: 50})

    // Your service decides whether to propagate or swallow the publish error.
    require.Error(t, err)
    assert.Empty(t, pub.Published()) // nothing was recorded
}
```

The `mock.Publisher` API:

| Method | What it does |
|--------|-------------|
| `Publish(ctx, env)` | Records the envelope; returns any error set via `SetError` |
| `PublishBatch(ctx, envs)` | Calls `Publish` for each envelope |
| `Published()` | Returns a copy of all recorded envelopes (thread-safe) |
| `SetError(err)` | Makes all subsequent `Publish` calls return `err` |
| `Reset()` | Clears recorded envelopes and any configured error |

### Mock Consumer

```go
func TestNotificationWorker_HandlesUserCreated(t *testing.T) {
    consumer := &mock.Consumer{}
    worker := NewNotificationWorker(consumer)

    // Start the worker (mock Start() is a no-op — no goroutine needed).
    require.NoError(t, consumer.Start(ctx))
    worker.RegisterHandlers()

    // Inject an event directly — no SQS, no network.
    env := events.NewEnvelope("iam.user.created", "platform-iam",
        json.RawMessage(`{"user_id":"u-123","email":"alice@example.com"}`),
        events.WithTenantID("acme"),
    )
    err := consumer.Inject(env)
    require.NoError(t, err)

    // Assert your handler's side effects.
    assert.True(t, worker.EmailWasSentTo("alice@example.com"))
}
```

The `mock.Consumer` API:

| Method | What it does |
|--------|-------------|
| `Start(ctx)` | No-op; marks consumer as running |
| `Stop()` | No-op; marks consumer as stopped |
| `SetHandler(fn)` | Registers the handler called by `Inject` |
| `Inject(env)` | Delivers the envelope synchronously to the registered handler |
| `IsRunning()` | Reports whether `Start` has been called without a matching `Stop` |

### Wiring in tests

Both mocks satisfy the `events.Publisher` and `events.Consumer` interfaces, so they drop in wherever the real implementations are used:

```go
// Production wiring:
publisher, _ := events.NewSNSPublisher(events.SNSConfig{TopicARN: ..., Region: ...})
svc := NewOrderService(publisher)

// Test wiring — identical constructor, no AWS needed:
pub := &mock.Publisher{}
svc := NewOrderService(pub)
```

> **Why not write your own mock?** `events.Publisher` is only two methods, and writing a local stub is perfectly valid Go. The benefit of `pkg/events/mock` is the `SetError` / `Reset` / thread-safety boilerplate that every service would otherwise duplicate — and the guarantee that the mock's signature tracks the library's interface across version bumps.

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

End-to-end tests cover the full outbox pipeline (Postgres → runner → SNS → SQS → consumer):

```bash
go test ./test/e2e/... -tags=e2e -v   # requires Docker (LocalStack + Postgres via testcontainers-go)
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

Integration and e2e tests use `testcontainers-go` — Docker must be running locally. Unit tests have no external dependencies. CI runs unit, integration, and e2e tests (the latter two require Docker on the runner).

**Coverage note:** tests live under `test/` (a separate package tree from sources). Always use `-coverpkg=./internal/...,./pkg/...` to get meaningful numbers. `make cover` and `make cover-func` handle this correctly.

---

## CI

Three GitHub Actions workflows run against this repository:

**`ci.yml`** — triggered on every push and pull request to `main`:

| Job | What it checks |
|-----|---------------|
| `validate` | `make fmt-check`, `go mod tidy` drift, `make vet`, `make lint` (golangci-lint v2), `make vuln-check` (govulncheck), `make test-ci` (unit + integration + e2e with race detector; requires Docker) |
| `coverage` | `make cover-func` — unit + integration + e2e tests with race detector; gate ≥ 95% against `./internal/...` + `./pkg/...` |

**Note:** `make test-smoke` (live AWS) is intentionally excluded from CI — use it manually before first deploy to a new AWS account. LocalStack e2e in `test-ci` covers the full pipeline in CI.
| `build` | `make build` — compiles the library and `cmd/platform-events` binary |

**`release.yml`** — triggered on `v*.*.*` tags:

| Job | What it does |
|-----|--------------|
| `validate` | Same checks as CI |
| `coverage` | Same ≥ 95% gate |
| `build` | Compiles binary at the tagged ref |
| `publish` | Extracts `[X.Y.Z]` section from `CHANGELOG.md` and creates a GitHub Release — this is the Go module version consuming services pin to |

**`validate.yml`** — reusable workflow called by both `ci.yml` and `release.yml`.

All checks must pass before merging. Private module access uses `GO_PRIVATE_TOKEN` via git credential store with `persist-credentials: false` on all checkout steps (see [GitHub authentication](#github-authentication)).

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

## Service adoption checklist

Use this before declaring a service's event integration production-ready. Each item maps to a section of this README or a linked document.

### Publisher checklist

- [ ] `events.Init(appName, buildVersion)` called once at startup ([Prometheus metrics](#prometheus-metrics))
- [ ] Outbox schema applied via `outbox.ApplySchema(ctx, migrateRunner)` on startup ([Wiring](#wiring))
- [ ] `outbox.Runner` started with `go runner.Start(ctx)` and deferred `runner.Stop()` ([Wiring](#wiring))
- [ ] All domain-event publish call sites use `outbox.Enqueue` inside `pgcommon.RunInTx` — no bare `publisher.Publish` for transactional events ([Publishing rules](#publishing-rules))
- [ ] `WithTenantID(rc.TenantID)`, `WithTraceID(rc.TraceID)`, and `WithSchemaVersion("1")` passed to every `NewEnvelope` call in HTTP handler context ([Creating envelopes](#creating-envelopes))
- [ ] Every new event type registered in `EVENT_SCHEMA_GOVERNANCE.md` before the first production publish ([Registry enforcement](./EVENT_SCHEMA_GOVERNANCE.md#registry-enforcement))
- [ ] `OUTBOX_POLL_INTERVAL`, `OUTBOX_BATCH_SIZE`, `OUTBOX_MAX_ATTEMPTS`, `OUTBOX_CLAIM_LEASE_DURATION`, and `OUTBOX_PUBLISH_*` env vars wired via `config.RunnerConfigFromEnv` ([Configuration reference](#configuration-reference))
- [ ] Readiness probe waits for `outbox.Runner.Ready()` before marking the pod ready ([Outbox runner](#outbox-runner))

### Consumer checklist

- [ ] SNS→SQS subscription created with **`RawMessageDelivery=true`** — without it the consumer deletes messages as malformed ([SQS consumer](#sqs-consumer))
- [ ] `SQS_*` env vars wired via `config.SQSConfigFromEnv` + `config.SQSConsumerOptions` ([Configuration reference](#configuration-reference))
- [ ] `NewSQSConsumer` wired with `WithConcurrency(n)` appropriate for handler latency ([Recommended production defaults](#recommended-production-defaults))
- [ ] `SQS_VISIBILITY_TIMEOUT` set to ≥ 2× p99 handler duration — not left at the 30s default if handlers call slow dependencies ([Recommended production defaults](#recommended-production-defaults))
- [ ] `WithDeadLetterHandler(fn)` configured with `WithMaxReceiveCount(n)` aligned to the SQS queue redrive policy — messages exceeding the threshold are not silently discarded ([Handler contract](#handler-contract))
- [ ] Every handler implements idempotency via `INSERT INTO processed_events ... ON CONFLICT DO NOTHING` inside a `pgcommon.RunInTx` ([Implementing idempotency](#implementing-idempotency))
- [ ] Handlers classify errors as transient (return `error`) vs permanent (return `nil` + log) — no permanent errors left as retryable ([Error classification](#error-classification))
- [ ] Handlers check `env.Type` before unmarshalling `env.Payload` — no silent misparse of a wrong event type ([Payload typing](#payload-typing))
- [ ] `DisallowUnknownFields` is NOT used anywhere on event payload structs ([Payload evolution rules](./EVENT_SCHEMA_GOVERNANCE.md#payload-evolution-rules))

### Observability checklist

- [ ] Handler entry logs bind `event_id`, `event_type`, `trace_id`, `tenant_id` via `logger.With(...)` ([Logging correlation](#logging-correlation))
- [ ] OTel provider initialised via `gincommon.InitTracingFromEnv()` before the first `consumer.Start` or `publisher.Publish` call ([OpenTelemetry](#opentelemetry))
- [ ] Alerts configured on `outbox_pending_total`, `events_consumed_total{status="error"}`, and SQS `ApproximateNumberOfMessages` ([Recommended production defaults — what to monitor on day one](#recommended-production-defaults))
- [ ] SQS DLQ depth alert configured at the queue level (CloudWatch) — the library does not alert on DLQ growth

### Security checklist

- [ ] HMAC signing enabled on HTTP/webhook ingress that receives events from external systems — **not** on the SNS/SQS messaging path ([When to use HMAC](#when-to-use-hmac))
- [ ] HMAC keys ≥ 32 bytes, stored in AWS Secrets Manager or SSM — not in source-controlled environment variables ([When to use HMAC](#when-to-use-hmac))
- [ ] `WithSystemTenant()` usage reviewed — no HTTP handler context uses it to avoid a tenant lookup ([WithSystemTenant warning](#creating-envelopes))

---

## See also

| Document | Description |
|----------|-------------|
| [ARCHITECTURE.md](./ARCHITECTURE.md) | Layer model, sequence diagrams, invariants, performance |
| [EVENT_SCHEMA_GOVERNANCE.md](./EVENT_SCHEMA_GOVERNANCE.md) | Event type naming, payload evolution rules, migration window, consumer compatibility contract, event type registry |
| [VERSIONING.md](./VERSIONING.md) | SemVer rules, supported versions, release process |
| [CHANGELOG.md](./CHANGELOG.md) | Per-version changes |
| [CONTRIBUTING.md](./CONTRIBUTING.md) | Development setup, adding adapters, PR checklist |
| [SECURITY.md](./SECURITY.md) | Vulnerability reporting, trust model, supported versions |

---

See [Publishing rules](#publishing-rules) for the full decision table. The short version: `publisher.Publish` is only valid when event loss is explicitly acceptable. For any event that drives downstream state, use `outbox.Enqueue` inside a `pgcommon.RunInTx` callback — no exceptions.

`platform-events` must be the only entry point for event publishing and consumption to preserve delivery guarantees, tenant isolation, and uniform observability.
