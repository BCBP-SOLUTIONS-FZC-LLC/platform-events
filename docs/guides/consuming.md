# Consuming

The SQS consumer: handler contract, error classification, poison messages, DLQ forwarding, idempotency and concurrency. One of the detailed guides linked from the [project README](../../README.md#contributing).

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

### Testing with an injected client

`NewSQSConsumerWithClient` builds the same consumer as `NewSQSConsumer` but takes an `events.SQSClientLike` instead of constructing a real `*sqs.Client` — use it to unit-test consumer-loop behaviour (retry, visibility extension, dead-letter routing, concurrency) against a hand-rolled fake, without an AWS emulator (floci) or AWS credentials. This is different from `mock.Consumer` (see [Testing in consuming services](testing-in-services.md#testing-in-consuming-services)), which stubs out the whole `Consumer` interface and skips the SQS loop entirely — reach for `NewSQSConsumerWithClient` when the behaviour under test is the loop itself, and `mock.Consumer` when it's your handler's side effects.

```go
type fakeSQSClient struct {
    receiveCount int
}

func (f *fakeSQSClient) ReceiveMessage(ctx context.Context, in *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
    f.receiveCount++
    if f.receiveCount > 1 {
        <-ctx.Done() // subsequent long-polls just block until Stop()
        return nil, ctx.Err()
    }
    return &sqs.ReceiveMessageOutput{Messages: []types.Message{ /* build via events.NewEnvelope + json.Marshal */ }}, nil
}

func (f *fakeSQSClient) DeleteMessage(ctx context.Context, in *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
    return &sqs.DeleteMessageOutput{}, nil
}

func (f *fakeSQSClient) ChangeMessageVisibility(ctx context.Context, in *sqs.ChangeMessageVisibilityInput, _ ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
    return &sqs.ChangeMessageVisibilityOutput{}, nil
}

func TestConsumer_DeletesOnSuccess(t *testing.T) {
    client := &fakeSQSClient{}
    consumer, err := events.NewSQSConsumerWithClient(
        events.SQSConfig{QueueURL: "https://sqs.test/queue", WaitSeconds: 1},
        client,
        func(ctx context.Context, env events.Envelope[json.RawMessage]) error {
            return nil // assert on env here
        },
    )
    require.NoError(t, err)

    ctx, cancel := context.WithCancel(context.Background())
    go func() { _ = consumer.Start(ctx) }()
    time.Sleep(50 * time.Millisecond)
    cancel()
    _ = consumer.Stop()
}
```

`SQSClientLike` is the minimal subset of the AWS SDK's SQS client used by the consumer — `ReceiveMessage`, `DeleteMessage`, `ChangeMessageVisibility`. `*sqs.Client` from `aws-sdk-go-v2` satisfies it, so production code needs no adapter; only tests implement it directly.

### Handler contract

| Handler return | Consumer behaviour |
|---|---|
| `nil` | SQS message deleted from queue |
| `non-nil error` | SQS message left visible; retried after visibility timeout |
| Panic | Recovered; stack trace logged; SQS message left visible for retry |
| Unmarshal failure | Counted as `platform_messages_failed_total{reason="malformed"}`; forwarded to the DLQ then deleted with `WithDLQForwarding` (left visible if the forward fails), otherwise deleted immediately |

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
| Schema version too new | `specversion: "5"` (`env.SchemaVersion`) but this consumer only understands up to `"3"` |

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
3. **Optionally preserve the message** if the business impact is high enough to warrant investigation or replay: forward it to the queue's SQS DLQ immediately with [`DLQPublisher.SendToDLQ`](#forwarding-to-the-sqs-dlq) (tagged with a `DLQReason`, so it is distinguishable from transient-failure messages), or to an audit/failure topic when you need cross-queue querying.
4. **Do not forward sensitive payload data** (PII, credentials) to an audit topic without confirming the destination has appropriate access controls and retention policies.

> The distinction between a **transient error** (DB timeout — retry will probably work) and a **poison message** (missing prerequisite record — retry will never work) is a business judgement, not a technical one. When in doubt, retry once more and log at `WARN`; escalate to `ERROR` + drop after two consecutive failures on the same `event_id`.

### Forwarding to the SQS DLQ

`events.DLQPublisher` sends a message to the dead-letter queue **already configured** on the source queue's `RedrivePolicy`. It is the supported way to dead-letter explicitly — consumer services must not import `github.com/aws/aws-sdk-go-v2/service/sqs` (depguard), and never need to.

```
Consumer → events.DLQPublisher → SQS GetQueueAttributes(RedrivePolicy)
         → deadLetterTargetArn → GetQueueUrl → SendMessage(DLQ)
```

The lookup is cached per source queue for `DLQConfig.CacheTTL` (default 15 min; negative = never expires) and evicted as soon as `SendMessage` reports the DLQ no longer exists. The body is forwarded verbatim; caller attributes are kept and these are added:

| Attribute | Value |
|---|---|
| `EventType` | Envelope `type` when the body is a full envelope (`id`, `type`, `source`, `time`); else `attrs["EventType"]`; else `unknown` |
| `DLQReason` | The `reason` argument (required; truncated to 1 KiB; characters SQS rejects replaced with U+FFFD) |
| `OriginalQueue` | `sourceQueueURL` |
| `FailedAt` | RFC 3339 UTC timestamp |
| `ConsumerName` | `DLQConfig.ConsumerName`, when set |

Standard attributes override caller values of the same name. SQS allows 10 attributes per message, so callers get at most 6 (5 when `ConsumerName` is set). Excess caller attributes are **dropped** before validation (so a dropped attribute cannot fail the send), lowest priority first (kept first: `TenantID`, `EventID`, `Source`, `Subject`, `traceparent`, `tracestate`, `baggage`, then lexical order) and logged at WARN; set `DLQConfig.StrictAttributes` to reject with `ErrDLQInvalidMessage` instead. A FIFO DLQ (`.fifo`) gets `MessageGroupId` set to the envelope ID (a SHA-256 of the body when it is not an envelope or the ID is not a valid FIFO identifier) and a `MessageDeduplicationId` unique per forward, so forwarding the same event twice (two queues sharing a DLQ, a redrive that fails again) never deduplicates a dead-letter away.

Each forward emits an `sqs.dlq_forward` span (`SpanKindProducer`), counts the message once in `platform_dlq_messages_total{operation="consume",reason}` (legacy `events_dlq_forwarded_total`), and times its SQS calls in `platform_dependency_request_seconds`.

**Wiring:**

```go
dlq, err := events.NewSQSDLQPublisher(events.DLQConfig{
    Region:       cfg.Region,
    ConsumerName: "billing-consumer",
    Logger:       log,
})
if err != nil {
    return err
}
// Fail fast at startup if the queue has no DLQ configured.
if _, err := dlq.ResolveDLQ(ctx, queueURL); err != nil {
    return fmt.Errorf("sqs dlq: %w", err)
}
```

**Automatic — `WithDLQForwarding` (recommended):**

```go
consumer, err := events.NewSQSConsumer(sqsCfg, handle,
    events.WithDLQForwarding(dlq),
    // MUST be lower than the queue's RedrivePolicy maxReceiveCount (5 here):
    // SQS moves the message itself once the count exceeds maxReceiveCount,
    // so with n >= maxReceiveCount the consumer never gets to forward it.
    events.WithMaxReceiveCount(4), // default 5 when omitted
)
```

With `WithDLQForwarding` the consumer forwards the **original raw body and attributes** (never a re-serialised envelope) and deletes the source message only after the forward succeeds — on failure it stays visible and SQS's own redrive remains the backstop. It forwards:

| Case | `DLQReason` |
|---|---|
| Body is not a valid envelope (previously deleted and only logged) | `malformed message body: <json error>` |
| `ApproximateReceiveCount` > `WithMaxReceiveCount` — after `WithDeadLetterHandler`, when set, returns `nil` | `receive count N exceeded consumer max receive count M` |
| Past that threshold **and** `Codec.Decode` fails | `codec decode failed: <error>` |

`Start` resolves the DLQ before polling and **returns an error** (wrapping `ErrDLQNotConfigured` / `ErrDLQInvalidRedrivePolicy`) when the queue has no usable `RedrivePolicy` — otherwise every forward would fail and, with no `RedrivePolicy`, SQS would never move the message either, leaving it redelivered until retention expires. A transient resolution failure is logged at WARN and the consumer starts. Each forward is bounded by 30 s, capped at half of `WithVisibilityTimeout` (minimum 1 s), so the message cannot become visible and be forwarded twice mid-flight.

**Forward in one place only.** With `WithDLQForwarding` on, the consumer already forwards messages that pass the threshold. A dead-letter handler that also calls `SendToDLQ` puts the message in the DLQ **twice**, and it is counted twice. Use the handler for side effects (alerting, compensation), or drop `WithDLQForwarding` and forward from the handler, but don't do both.

When both a dead-letter handler and forwarding are set, the handler runs first; if it fails nothing is forwarded, and if the forward then fails the handler runs again on the next delivery — keep it idempotent.

**From a handler — poison message:** forward the original transport message from `events.SourceMessageFromContext`, not `env.JSON()` — a re-serialised envelope has lost the message attributes and, with `WithConsumerCodec`, holds the *decoded* payload under a still-set `SchemaID`, so a redrive would fail to decode it.

```go
if order == nil {
    src, _ := events.SourceMessageFromContext(ctx) // raw body, String/Number attributes, queue URL, receive count
    if err := dlq.SendToDLQ(ctx, src.QueueURL, src.Body, src.Attributes, "order not found"); err != nil {
        return err // forward failed — keep the original on the queue so it is retried
    }
    return nil // forwarded — let the consumer delete the original
}
```

**Error handling** — every error is a `*events.DLQError`:

```go
switch err := dlq.SendToDLQ(ctx, queueURL, body, nil, reason); {
case err == nil:
    return nil
case errors.Is(err, events.ErrRetryable):
    return err // throttling / timeout — SQS redelivers the original later
case errors.Is(err, events.ErrDLQNotConfigured), errors.Is(err, events.ErrDLQInvalidRedrivePolicy):
    log.Error("queue has no usable DLQ — fix RedrivePolicy", zap.Error(err))
    return err
default:
    return err
}
```

| Sentinel | Cause | Retryable |
|---|---|---|
| `ErrDLQInvalidMessage` | Empty source URL / body / reason; invalid UTF-8 or characters SQS does not allow; invalid attribute name (`AWS.`/`Amazon.` prefix, bad characters, > 256 chars); body + attributes > 1 MiB; too many attributes with `StrictAttributes` — all rejected before any AWS call. Also returned when `SendMessage` rejects the message (`InvalidParameterValue` — e.g. over the queue's `MaximumMessageSize` — `InvalidMessageContents`, `InvalidAttributeName`, `InvalidAttributeValue`) | No |
| `ErrDLQNotConfigured` | Source queue has no `RedrivePolicy` | No |
| `ErrDLQInvalidRedrivePolicy` | Policy not JSON, no `deadLetterTargetArn`, or not an SQS ARN | No |
| `ErrDLQUnresolved` | `GetQueueAttributes` / `GetQueueUrl` failed, or `SendMessage` found the DLQ deleted (cache entry evicted; the next call re-resolves) | Also matches `ErrRetryable` if transient |
| `ErrDLQSendFailed` | `SendMessage` failed | Also matches `ErrRetryable` if transient |

**Queue depth (optional):** `events.WithQueueDepthMetrics(time.Minute)` samples the queue's backlog and its DLQ's into `platform_queue_depth` / `platform_dlq_depth`. It needs `sqs:GetQueueAttributes` on both queues. See [docs/observability](../observability/README.md#tier-classification).

**IAM:** `sqs:GetQueueAttributes` on the source queue; `sqs:GetQueueUrl` and `sqs:SendMessage` on the DLQ; `kms:GenerateDataKey` + `kms:Decrypt` if the DLQ uses a customer-managed KMS key.

**Not in scope:** the library does not create DLQs, read or replay them, or redrive messages — use the SQS console / `StartMessageMoveTask` for redrive. Messages forwarded here are in the **SQS DLQ**, not `outbox_dead_letters`.

### Accessing the trace ID inside a handler

The SQS consumer injects `env.TraceID` into the handler context. Retrieve it without importing the internal port package:

```go
func myHandler(ctx context.Context, env events.Envelope[json.RawMessage]) error {
    traceID := events.TraceIDFromContext(ctx) // "" if not set
    // ...
}
```

Retries are driven by SQS visibility timeouts — not by the library — and follow the queue's redrive policy. The retry limit is the queue's RedrivePolicy `maxReceiveCount`. `WithMaxReceiveCount` adds an optional, lower consumer-side threshold that routes a message to `WithDeadLetterHandler` and/or `WithDLQForwarding` before SQS's redrive would; without either option it has no effect. See [ARCHITECTURE.md § Failure lifecycle](../../ARCHITECTURE.md#failure-lifecycle) for the full consumer-side retry timeline and how it differs from outbox (producer-side) retries.

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
| Enforce strict processing order within an aggregate | FIFO topic + stable `MessageGroupID` (e.g. `AggregateID`), a FIFO queue, and `WithVisibilityTimeout` — the consumer runs each message group on one worker in order and holds the group when a message fails; on the producer side enqueue with `outbox.EnqueueOrdered` |
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

    return pgcommon.RunInTx(ctx, pool, pgcommon.TxOptions{}, func(ctx context.Context, tx pgcommon.Tx) error {
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

#### Pattern 0 — `inbox.Store.Process` (recommended)

`pkg/inbox` packages Pattern 1: apply its schema with `inbox.ApplySchema`, then run the handler's writes through `Store.Process`. It claims `Envelope.ID` in the handler's own transaction (`INSERT … ON CONFLICT DO NOTHING`), so the claim and the writes commit or roll back together, and concurrent copies of a message serialise on the claim:

```go
store, _ := inbox.NewStore(pool, "user_projection")
handler := func(ctx context.Context, env events.Envelope[json.RawMessage]) error {
    return store.Process(ctx, env, func(ctx context.Context, tx pgcommon.Tx) error {
        return repo.ApplyUserCreated(ctx, tx, env) // writes through tx
    })
}
```

A failing `fn` rolls back the claim, so SQS retries; a duplicate returns nil without calling `fn` and is counted in `platform_duplicate_messages_total`. A handler that dead-letters the message (`SendToDLQ`, then nil) is not recorded, so redriving the DLQ processes it. `fn` must not end `tx` itself (`tx.Commit`, `COMMIT`, …): platform-pgcommon (≥ v1.5.1) rolls back and returns `pgcommon.ErrTxEndedInCallback`, and the message is retried. `inbox.Handler(store, next)` is the wrapper for handlers whose effects are not Postgres writes; it uses separate transactions, so the handler must still be idempotent.

#### Pattern 1 — Postgres unique constraint (hand-rolled)

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
    return pgcommon.RunInTx(ctx, pool, pgcommon.TxOptions{}, func(ctx context.Context, tx pgcommon.Tx) error {
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

