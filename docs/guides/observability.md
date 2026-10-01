# Observability

Prometheus metrics, OpenTelemetry and logging correlation. One of the detailed guides linked from the [project README](../../README.md#contributing).

---

## Observability — Prometheus and OTel

Both Prometheus metrics and OTel tracing are **optional**. The publisher, consumer, and outbox runner work without initialising metrics or registering an OTel provider.

### Prometheus metrics

Metrics follow the **Enterprise Platform Observability Standard**. The model, wiring and CI enforcement are described in [docs/observability](../observability/README.md). The full inventory, with every metric's labels, allowed values and ratification packet, is generated into [metrics-registry.md](../observability/metrics-registry.md). This section is a summary.

Call once at startup, with the same registerer and identity as platform-pgcommon:

```go
warnings, err := events.InitMetrics(events.MetricsIdentity{
    Domain: "iam", Service: "event-consumer",
}, registry) // Environment empty → APP_ENV, then ENVIRONMENT, else "dev"
```

For isolated test registries, pass `prometheus.NewRegistry()`. The identity is mandatory — there is no metrics init without one — and these Tier 1 metrics are the only ones platform-events emits.

**Tier 1 metrics** (every one carries `domain`, `service`, `environment`):

| Metric | Type | Labels | Status | Description |
|---|---|---|---|---|
| `platform_messages_received_total` | Counter | `queue` | Canonical | Every SQS delivery, redeliveries included |
| `platform_messages_processed_total` | Counter | `queue`, `event_type` | Canonical | Handler returned `nil`, message deleted (not dead-lettered) |
| `platform_messages_failed_total` | Counter | `queue`, `event_type`, `reason` | Canonical | `malformed` / `decode_error` / `handler_error` / `handler_panic` / `dead_letter_error` |
| `platform_retry_total` | Counter | `operation`, `event_type` | Canonical | Failure left for automatic retry (`consume`, `outbox_publish`) |
| `platform_dlq_messages_total` | Counter | `operation`, `event_type`, `reason` | Canonical | Moved to the SQS DLQ (`consume`) or `outbox_dead_letters` (`outbox_publish`); counted once |
| `platform_duplicate_messages_total` | Counter | `queue`, `event_type` | Proposed | Redelivery acknowledged by the inbox ledger |
| `platform_dependency_request_seconds` | Histogram | `dependency`, `operation`, `outcome` | Proposed | SNS / SQS / codec call latency; `_count` counts calls |
| `platform_event_propagation_seconds` | Histogram | `queue`, `event_type` | Proposed | Envelope `time` → consumer receipt |
| `platform_messages_published_total` | Counter | `topic`, `event_type`, `outcome` | Proposed | Publish attempts per event |
| `platform_message_processing_duration_seconds` | Histogram | `queue`, `event_type` | Proposed | Handler / dead-letter handler time |
| `platform_outbox_pending_events` / `platform_outbox_leased_events` | Gauge | — | Proposed | Outbox backlog / in flight |
| `platform_outbox_publish_attempts_total` | Counter | `event_type`, `outcome` | Proposed | Runner publish attempts |
| `platform_outbox_errors_total` | Counter | `operation` | Proposed | `poll` / `unmarshal` / `mark_published` / `pending_count` / `leased_count` / `oldest_pending` / `blocked_count` |
| `platform_outbox_dead_letter_operations_total` | Counter | `operation` | Proposed | Records reprocessed / discarded |
| `platform_telemetry_label_overflow_total` | Counter | `label` | Proposed | `event_type` values over 128 bytes or beyond the per-process limit |
| `platform_library_info` | Gauge | `library`, `library_version` | Proposed | Library version per service |

- **Using the metrics.** Canonical metrics are safe for alerts, SLOs and HPA. Proposed metrics are emitted the same way but still await governance ratification: the reference alerts that use them live in the `*.proposed` rule groups and carry `metric_status: proposed`, and the reference KEDA trigger declares it with an annotation.
- **No legacy metrics.** The pre-standard metrics were removed without a compatibility period (nothing emitting them was ever deployed); the [CHANGELOG](../../CHANGELOG.md) maps each to its successor.
- **Reference rules.** Recording rules, the consumer SLO (99.9%, multi-window burn rate) and operational alerts are in [`monitoring/prometheus/platform-events.rules.yml`](../../monitoring/prometheus/platform-events.rules.yml), with a [runbook](../observability/runbook.md).

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

