# Publishing

When to use the outbox vs. direct publish, anti-patterns, and the SNS publisher API. One of the detailed guides linked from the [project README](../../README.md#contributing).

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
pgcommon.RunInTx(ctx, pool, pgcommon.TxOptions{}, func(ctx context.Context, tx pgcommon.Tx) error {
    if err := repo.SaveUser(ctx, tx, user); err != nil {
        return err
    }
    return outbox.Enqueue(ctx, tx, envelope)
})

// ❌ Wrong — SNS call outside the transaction; event lost on crash
pgcommon.RunInTx(ctx, pool, pgcommon.TxOptions{}, func(ctx context.Context, tx pgcommon.Tx) error {
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
> **FIFO gives you ordering. Idempotency still gives you safety.** Use `WithMessageGroupID` to enforce processing order within a group (e.g. per-tenant, per-aggregate). Use `Envelope.ID` + `INSERT ... ON CONFLICT DO NOTHING` to make the handler safe to run twice. The two properties are independent and both are required for correct behaviour. See [Implementing idempotency](consuming.md#implementing-idempotency) for the concrete pattern.

### Message attributes

`EventType`, `TenantID`, `Source`, `EventID`, and `Subject` (when non-empty) are set as SNS message attributes. This enables SQS subscription filter policies that scope queues to specific event types, tenants, or resource subjects without deserialising the message body. `Actor` is an audit-trail field and is not forwarded as an SNS attribute.

