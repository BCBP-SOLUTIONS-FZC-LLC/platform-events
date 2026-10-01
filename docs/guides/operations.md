# Operations

Backpressure, recommended production defaults and the service adoption checklist. One of the detailed guides linked from the [project README](../../README.md#contributing).

---

## Backpressure considerations

Under sustained load, the two independent queues — the SQS receive buffer and the outbox `outbox_events` table — will grow if consumers or the outbox runner can't keep up. This section describes the signals to watch and the knobs to turn, in the order to try them.

### Consumer-side (SQS)

The SQS receive loop is gated by `WithConcurrency(n)` (default: `1`). Each received message occupies a slot until the handler returns; messages beyond the concurrency limit stay in SQS and accumulate `ApproximateNumberOfMessages`.

| Signal | Where | What it means |
|--------|-------|----------------|
| `ApproximateNumberOfMessages` rising | CloudWatch / SQS console | Handlers are slower than the publish rate |
| `events_consume_duration_seconds` p99 > `SQS_VISIBILITY_TIMEOUT` | Prometheus | Visibility extensions are firing; handler is at risk of double-delivery |
| `platform_messages_failed_total{reason="handler_error"}` rising | Prometheus | Handlers are failing and leaving messages to re-enter the queue |

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
| `platform_messages_failed_total{reason="handler_error"}` | > 1% error rate | Inspect handler errors; check DLQ depth |
| `outbox_published_total{status="failed"}` / total | > 1% | Check SNS reachability; inspect `outbox_dead_letters` |
| `platform_messages_failed_total{reason="dead_letter_error"}` | > 0 | Check the source queue's `RedrivePolicy` and DLQ IAM permissions |


---

## Service adoption checklist

Use this before declaring a service's event integration production-ready. Each item maps to a section of this README or a linked document.

### Publisher checklist

- [ ] `events.InitMetrics(events.MetricsIdentity{Domain, Service, Version}, registerer)` called once at startup, with the same registerer/identity as `pgmetrics.InitWithIdentity` ([Prometheus metrics](observability.md#prometheus-metrics))
- [ ] Outbox schema applied via `outbox.ApplySchema(ctx, migrateRunner)` on startup ([Wiring](outbox.md#wiring))
- [ ] `outbox.Runner` started with `go runner.Start(ctx)` and deferred `runner.Stop()` ([Wiring](outbox.md#wiring))
- [ ] All domain-event publish call sites use `outbox.Enqueue` inside `pgcommon.RunInTx` — no bare `publisher.Publish` for transactional events ([Publishing rules](publishing.md#publishing-rules))
- [ ] `WithTenantID(rc.TenantID)`, `WithTraceID(rc.TraceID)`, and `WithSchemaVersion("1")` passed to every `NewEnvelope` call in HTTP handler context ([Creating envelopes](envelope.md#creating-envelopes))
- [ ] Every new event type registered in `EVENT_SCHEMA_GOVERNANCE.md` before the first production publish ([Registry enforcement](../../EVENT_SCHEMA_GOVERNANCE.md#registry-enforcement))
- [ ] `OUTBOX_POLL_INTERVAL`, `OUTBOX_BATCH_SIZE`, `OUTBOX_MAX_ATTEMPTS`, `OUTBOX_CLAIM_LEASE_DURATION`, and `OUTBOX_PUBLISH_*` env vars wired via `config.RunnerConfigFromEnv` ([Configuration reference](../../README.md#environment-variables))
- [ ] Readiness probe waits for `outbox.Runner.Ready()` before marking the pod ready ([Outbox runner](#outbox-runner))

### Consumer checklist

- [ ] SNS→SQS subscription created with **`RawMessageDelivery=true`** — without it the consumer deletes messages as malformed ([SQS consumer](#sqs-consumer))
- [ ] `SQS_*` env vars wired via `config.SQSConfigFromEnv` + `config.SQSConsumerOptions` ([Configuration reference](../../README.md#environment-variables))
- [ ] `NewSQSConsumer` wired with `WithConcurrency(n)` appropriate for handler latency ([Recommended production defaults](#recommended-production-defaults))
- [ ] `SQS_VISIBILITY_TIMEOUT` set to ≥ 2× p99 handler duration — not left at the 30s default if handlers call slow dependencies ([Recommended production defaults](#recommended-production-defaults))
- [ ] `WithDeadLetterHandler(fn)` configured with `WithMaxReceiveCount(n)` **strictly lower** than the queue's `RedrivePolicy` `maxReceiveCount` — with `n ≥ maxReceiveCount`, SQS moves the message before the handler ever sees it ([Forwarding to the SQS DLQ](consuming.md#forwarding-to-the-sqs-dlq))
- [ ] Poison messages that must be kept are forwarded with `events.DLQPublisher.SendToDLQ` (not a direct SQS SDK call); `ResolveDLQ` is called at startup so a missing `RedrivePolicy` fails the deploy; the service role has `sqs:GetQueueAttributes`, `sqs:GetQueueUrl` and `sqs:SendMessage` ([Forwarding to the SQS DLQ](consuming.md#forwarding-to-the-sqs-dlq))
- [ ] Every handler implements idempotency via `INSERT INTO processed_events ... ON CONFLICT DO NOTHING` inside a `pgcommon.RunInTx` ([Implementing idempotency](consuming.md#implementing-idempotency))
- [ ] Handlers classify errors as transient (return `error`) vs permanent (return `nil` + log) — no permanent errors left as retryable ([Error classification](consuming.md#error-classification))
- [ ] Handlers check `env.Type` before unmarshalling `env.Payload` — no silent misparse of a wrong event type ([Payload typing](envelope.md#payload-typing))
- [ ] `DisallowUnknownFields` is NOT used anywhere on event payload structs ([Payload evolution rules](../../EVENT_SCHEMA_GOVERNANCE.md#payload-evolution-rules))

### Observability checklist

- [ ] Handler entry logs bind `event_id`, `event_type`, `trace_id`, `tenant_id` via `logger.With(...)` ([Logging correlation](observability.md#logging-correlation))
- [ ] OTel provider initialised via `gincommon.InitTracingFromEnv()` before the first `consumer.Start` or `publisher.Publish` call ([OpenTelemetry](observability.md#opentelemetry))
- [ ] Alerts configured on `outbox_pending_total`, `platform_messages_failed_total{reason="handler_error"}`, and SQS `ApproximateNumberOfMessages` ([Recommended production defaults — what to monitor on day one](#recommended-production-defaults))
- [ ] SQS DLQ depth alert configured at the queue level (CloudWatch) — the library does not alert on DLQ growth
- [ ] Alert on `platform_messages_failed_total{reason="dead_letter_error"}` > 0 — a failing DLQ forward means poison messages are cycling on the source queue

### Security checklist

- [ ] HMAC signing enabled on HTTP/webhook ingress that receives events from external systems — **not** on the SNS/SQS messaging path ([When to use HMAC](hmac.md#when-to-use-hmac))
- [ ] HMAC keys ≥ 32 bytes, stored in AWS Secrets Manager or SSM — not in source-controlled environment variables ([When to use HMAC](hmac.md#when-to-use-hmac))
- [ ] `WithSystemTenant()` usage reviewed — no HTTP handler context uses it to avoid a tenant lookup ([WithSystemTenant warning](envelope.md#creating-envelopes))

