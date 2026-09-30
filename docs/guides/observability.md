# Observability

Prometheus metrics, OpenTelemetry and logging correlation. One of the detailed guides linked from the [project README](../../README.md#contributing).

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
| `outbox_dead_letters_reprocessed_total` | Counter | `service` | Dead-letter records re-queued via `ReprocessDeadLetters` or `ReprocessDeadLettersWith` |
| `outbox_dead_letters_discarded_total` | Counter | `service` | Dead-letter records permanently deleted via `DiscardDeadLetters` |
| `sqs_receive_errors_total` | Counter | `service`, `queue` | SQS `ReceiveMessage` errors (excludes context cancellation) — alert on `rate() > 0` |
| `sqs_delete_errors_total` | Counter | `service`, `queue` | SQS `DeleteMessage` errors — a non-zero rate causes duplicate message delivery |
| `sqs_visibility_extension_errors_total` | Counter | `service`, `queue` | SQS `ChangeMessageVisibility` errors — non-zero rate causes duplicate delivery for long-running handlers |
| `outbox_poll_errors_total` | Counter | `service` | Outbox poll cycle errors (ClaimBatch / DB errors) — triggers exponential backoff |
| `outbox_unmarshal_errors_total` | Counter | `service` | Outbox records that failed JSON unmarshal during publish |
| `outbox_mark_published_errors_total` | Counter | `service` | `MarkPublished` failures after a successful SNS delivery — non-zero rate signals potential duplicate delivery on next poll |
| `events_codec_encode_total` | Counter | `service`, `topic`, `event_type`, `status` | `Codec.Encode` invocations (`status` = `success`/`noop`/`error`); only incremented when `WithCodec` is configured |
| `events_codec_encode_duration_seconds` | Histogram | `service`, `topic`, `event_type` | `Codec.Encode` latency |
| `events_codec_decode_total` | Counter | `service`, `queue`, `event_type`, `status` | `Codec.Decode` invocations (`status` = `success`/`error`); only incremented when `WithConsumerCodec` is configured |
| `events_codec_decode_duration_seconds` | Histogram | `service`, `queue`, `event_type` | `Codec.Decode` latency |
| `events_dlq_forwarded_total` | Counter | `service`, `queue`, `event_type`, `status` | Messages forwarded by `DLQPublisher.SendToDLQ` (`queue` = source queue URL; `status` = `success`/`error`) — alert on `status="error"` > 0 |
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

