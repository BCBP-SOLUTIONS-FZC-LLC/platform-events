# Transactional outbox

Wiring, enqueueing, poll cycle, dead letters, pruning and replay guarantees. One of the detailed guides linked from the [project README](../../README.md#contributing).

---

## Transactional outbox

The outbox pattern eliminates dual-write risk: the event is written **inside the business transaction** alongside the domain mutation. If the transaction rolls back, the event is never published. The runner delivers asynchronously with at-least-once guarantee.

### Ordering

By default the transactional outbox does **not** preserve publish order, even per aggregate on a FIFO topic: when a record fails (or backs off), later records — including the same aggregate's — are still published, and the failed one goes out after them; several runner replicas also publish concurrently. A FIFO `MessageGroupID` keeps the order SNS *receives*, which is then already out of order.

**Per-key ordering (opt-in per record).** Enqueue with `outbox.EnqueueOrdered(ctx, tx, env, key)` — key = the aggregate, e.g. `"user/<id>"`; requires migration `010`. Records with the same key are published one at a time, in enqueue order, across all runner replicas; records enqueued with `Enqueue` are unaffected. Enqueue order is INSERT order (a sequence, `ordering_seq`), so take the aggregate's row lock (the business `UPDATE` of that row, or `SELECT … FOR UPDATE`) **before** `EnqueueOrdered` — two transactions enqueuing for one key then insert in commit order. A record enqueued while an earlier one of its key is unpublished waits (`scheduled_at = 'infinity'`, so claims never scan it) and is promoted once the key's head is marked published (best-effort, in a follow-up transaction) or inside the transaction that dead-letters it; a sweep with the gauge refresh (at the first poll after `GaugeInterval` has elapsed) catches a record enqueued while its head was being published, or whose promotion failed (promotion is best-effort after the publish is marked). Trade-offs: a failing head holds its key until it is published or dead-lettered after `MaxAttempts` (watch `platform_outbox_ordering_blocked_events`); a replayed dead letter joins the back of its key; a key publishes one record per claim, so the runner re-polls at once while batches publish. On a FIFO topic derive `WithMessageGroupID` from the same key so SNS keeps the order. Without ordering keys, consumers that need order use a per-aggregate sequence number in the payload.

FIFO topics are still useful for SNS-side deduplication (`MessageDeduplicationId` = `Envelope.ID`) and for consumer-side serialisation within a group.

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
// DSN from platform-pgcommon: MIGRATION_DATABASE_URL, else DATABASE_URL / PG_*.
migrateRunner := &migrate.Runner{DSN: pgcommon.MigrationDSNFromEnv()}
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
    // Close the pool only after this: after a drain timeout the in-flight
    // batch may still be marking records published on a detached context, and
    // a closed pool turns those records into re-publishes.
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

**Config defaults:** `PollInterval` 5s · `BatchSize` 50 · `MaxAttempts` 5 · `PublishConcurrency` 1 · `PublishTimeout` 10s · `DrainTimeout` 30s · `ClaimLeaseDuration` 10m · `StartupJitter` 0 · `RetryBackoff` 1s · `MaxRetryBackoff` 5m. When `PublishConcurrency` is `1` (default), the runner publishes via SNS `PublishBatch` (10 messages per API call). Values `> 1` publish records in parallel goroutines with per-record `Publish` calls. When a poll cycle fails (e.g. the DB is unreachable), the runner applies exponential backoff (1s → 30s) before retrying instead of hammering the pool every `PollInterval`.

### Enqueueing inside a transaction

```go
err = pgcommon.RunInTx(ctx, pool, pgcommon.TxOptions{}, func(ctx context.Context, tx pgcommon.Tx) error {
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

`Enqueue` validates the envelope (non-nil tx; non-empty `ID`/`Type`/`Source`; non-zero `Timestamp`; no null bytes in `ID`/`Type`/`Source`/`TenantID`/`TraceID` and no NUL (`\u0000`) anywhere in the serialised envelope — Postgres `jsonb` cannot store it, so strip NULs from user input; `ID` a **canonical lowercase UUID**, as `events.NewEnvelope` produces — services that set their own IDs must use `uuid.UUID.String()`) and rejects payloads whose serialised size exceeds **240 KB** — staying under the SNS 256 KB hard limit so an outbox record that could never publish is never persisted.

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
4. On a permanent failure: increment `attempts`, set `last_error` and `scheduled_at = NOW() + RetryBackoff·2^(attempts-1)` (default 1s, capped at `MaxRetryBackoff` = 5m, jittered). If `attempts >= MaxAttempts` move to `outbox_dead_letters`. Transient failures (below) and shutdown release the lease without counting an attempt.
5. Sleep `PollInterval`, then repeat.

Horizontal scale is achieved by running multiple outbox runners — `FOR UPDATE SKIP LOCKED` ensures work is safely partitioned across instances.

### Dead letters

> **"Dead letters" means two different things** — do not confuse them. Outbox dead letters are **publish failures** stored in Postgres. SQS DLQ entries are **consumer processing failures** stored in a separate SQS queue. They have different causes, different storage, and different recovery paths. See [ARCHITECTURE.md § Failure lifecycle](../../ARCHITECTURE.md#failure-lifecycle) for the complete side-by-side timeline and operational runbook.

Failed outbox records that exhaust `MaxAttempts` move to `outbox_dead_letters` — a queryable Postgres table. Inspect and replay from standard SQL tooling rather than an SQS DLQ.

```sql
SELECT * FROM outbox_dead_letters WHERE tenant_id = 'acme' ORDER BY failed_at DESC;
```

#### DLQ management API

Three methods on `Runner` give full programmatic control over dead letters without requiring direct SQL access.

**Step 1 — Inspect before acting.**

```go
// List up to 50 failures for a specific tenant, oldest first (failed_at, id —
// a replay or discard with the same filter and limit selects the same rows).
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

**Retryable failures** do not count toward `MaxAttempts`: SNS throttling and service-side errors (`Throttled`, `InternalError`, `KMSThrottling`, `ThrottlingException`, `ServiceUnavailable`, `InternalFailure`, `RequestTimeout`), any HTTP 5xx or 429 response (including body-less ones the SDK reports as `UnknownError`; a body-less 4xx stays permanent), per-entry batch failures with `SenderFault=false`, publish timeouts, and failures that never got an answer from SNS (network, DNS, TLS, credential resolution). The lease is released and the record retried after a backoff shared by all records (`RetryBackoff`, doubling once per poll cycle up to `MaxRetryBackoff`) that resets on the next successful publish. A period of SNS unavailability builds a backlog (watch `PlatformEventsOutboxBacklog`) but never dead-letters healthy records. Everything else — authorization, a missing topic, invalid parameters, an oversized request — counts an attempt, backs off per record, and dead-letters at `MaxAttempts`.

### Idempotency

`Envelope.ID` (UUID v7) is forwarded as the SNS `MessageDeduplicationID` on FIFO topics and as a message attribute on standard topics. Handlers must use `Envelope.ID` as their idempotency key — see [SQS Consumer § Implementing idempotency](consuming.md#implementing-idempotency) for concrete patterns.

A large backlog in `outbox_events` usually indicates downstream delivery issues (SNS/SQS) or insufficient runner throughput — monitor `platform_outbox_pending_events` (and `platform_outbox_oldest_pending_age`) and scale runners accordingly.

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
| **Ordering** | Unkeyed records: none — replayed events are inserted at the back of the outbox queue and delivered in poll order, not original publish order; a replayed `user.updated` may arrive before an in-flight `user.created` for the same user. Ordered records (`EnqueueOrdered`): a replayed record keeps its key and joins the **back** of it — it is published after the key's records enqueued since, not in its original position. Records replayed together keep their original relative order. |
| **Idempotency checks triggered again** | The same `Envelope.ID` will be presented to the consumer handler a second time. The `ON CONFLICT DO NOTHING` guard must be in place — this is not a bug, it is the mechanism that makes replay safe. |
| **Timing** | Replayed events re-enter the standard poll cycle. They are not expedited. Under load, a replayed batch may take multiple poll intervals to publish. |
| **Partial replay** | `ReprocessDeadLetters(ctx, n)` moves at most `n` records per call. A large dead-letter backlog requires multiple calls or a loop. There is no atomic "replay all" operation. |
| **Concurrent live traffic** | Replay runs concurrently with live event traffic. A consumer receiving a replayed `order.created` must handle it correctly even if a `order.shipped` for the same order arrived minutes earlier via the live path. |

> **Consumers must be safe to replay at any time, not just during an incident.** Replay is also triggered by: on-call engineers restoring a backlog, a new consumer service bootstrapping from historical dead letters, and automated regression tests. A handler that is not idempotent is not replay-safe — and therefore not production-ready.

