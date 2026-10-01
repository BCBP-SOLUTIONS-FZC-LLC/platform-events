# Observability — Enterprise Platform Observability Standard

How platform-events implements the standard's three-tier metric taxonomy and how a service wires it. platform-events emits **only** Tier 1 `platform_*` metrics: the pre-standard names were removed without a compatibility period, because no release emitting them was ever deployed (see [No legacy metrics](#no-legacy-metrics)).

| File | What it is |
|---|---|
| [metrics-registry.md](metrics-registry.md) | **Generated** inventory and ratification packets for every metric (`make metrics-doc`). The source of truth is [`internal/adapter/outbound/metrics/registry.go`](../../internal/adapter/outbound/metrics/registry.go). |
| [runbook.md](runbook.md) | One section per reference alert. |
| [`monitoring/prometheus/platform-events.rules.yml`](../../monitoring/prometheus/platform-events.rules.yml) | Reference recording rules, SLIs, SLO burn-rate and operational alerts. |
| [`monitoring/prometheus/platform-events.rules.test.yml`](../../monitoring/prometheus/platform-events.rules.test.yml) | promtool alert unit tests (`make rules-check`). |
| [`monitoring/grafana/platform-events.json`](../../monitoring/grafana/platform-events.json) | Reference Grafana dashboard: Canonical panels, then Proposed panels (titled "(Proposed)"). |
| [`monitoring/kubernetes/keda-scaledobject.example.yaml`](../../monitoring/kubernetes/keda-scaledobject.example.yaml) | Reference autoscaling (KEDA): outbox backlog (`platform_outbox_pending_events`, Proposed) and queue backlog. |

---

## Tier classification

platform-events is a **platform library**. Every service in every domain that publishes, consumes, dead-letters or deduplicates events through it emits these metrics with identical semantics, because the library defines them. By the standard's decision tree they are all **Tier 1 (`platform_*`)**.

- **Tier 2 (`<domain>_*`) and Tier 3 (`<domain>_<service>_*`) don't apply to the library.** It has no domain or service of its own. Services add those metrics for their own business signals, e.g. `iam_auth_events_total` or `iam_event_consumer_hmac_failures_total`.
- **Same model as platform-pgcommon.** Its `platform_db_*` metrics work the same way: one identity, one registerer, the same fail-soft behaviour.

| Status | Metrics | May be used in alerts / SLOs / HPA |
|---|---|---|
| **Canonical** (named by the standard) | `platform_messages_received_total`, `platform_messages_processed_total`, `platform_messages_failed_total`, `platform_retry_total`, `platform_dlq_messages_total` | Yes. `platform_retry_total` is the exception; see [Known conflicts](#known-conflicts) |
| **Proposed** (registry-proposed examples) | `platform_duplicate_messages_total`, `platform_dependency_request_seconds`, `platform_event_propagation_seconds`, `platform_queue_depth`, `platform_dlq_depth` | Await governance ratification (rules 11/12). The reference rules use some of them in the `*.proposed` groups, with `metric_status: proposed` — see [Alerts on Proposed metrics](#alerts-on-proposed-metrics) |
| **Proposed** (new names, ratification packets submitted) | `platform_messages_published_total`, `platform_message_processing_duration_seconds`, `platform_outbox_pending_events`, `platform_outbox_leased_events`, `platform_outbox_publish_attempts_total`, `platform_outbox_errors_total`, `platform_outbox_dead_letter_operations_total`, `platform_telemetry_label_overflow_total`, `platform_library_info`, `platform_messages_in_flight`, `platform_message_timeouts_total`, `platform_outbox_oldest_pending_age`, `platform_outbox_ordering_blocked_events` | Same as above |

**Queue depth is opt-in.** `platform_queue_depth` and `platform_dlq_depth` are sampled only by consumers started with `events.WithQueueDepthMetrics(interval)` (or `SQS_QUEUE_DEPTH_INTERVAL` through `config.SQSConsumerOptions`).
- **Why the library emits them:** services may not use the SQS SDK themselves (depguard), so without this option there is no way to get queue depth into Prometheus.
- **Cost:** every replica calls `sqs:GetQueueAttributes` on the queue, and on its RedrivePolicy DLQ, once per interval. Grant that permission on both.
- **Aggregation:** replicas report the same queue, so use `max`, not `sum`.
- **Outbox backlog:** a different quantity, with its own gauge, `platform_outbox_pending_events`.

## Required labels and label governance

Every Tier 1 metric carries `domain`, `service` and `environment`. They are **injected centrally** as const labels from one `events.MetricsIdentity` (rule 8), so no call site can omit or misspell them.

- **Approved labels.** Additional labels come only from each metric's approved vocabulary: `queue`, `topic`, `event_type`, `reason`, `operation`, `dependency`, `outcome`, `label`, `library`, `library_version`. The allowed values and cardinality bounds are in [metrics-registry.md](metrics-registry.md#label-vocabulary).
- **`queue` / `topic` values.** These are the queue or topic *name*, never the URL or ARN (which carry the AWS account ID).
- **Prohibited labels.** `user_id`, `email`, `tenant_id`, `request_id`, `event_id`, `session_id` are banned, as are the equally unbounded `message_id`, `trace_id`, `span_id`, `correlation_id`, `subject` and `actor`.
- **`event_type` values are bounded in-process.** At most 200 distinct values are recorded per process (set with `events.WithEventTypeLimit`); further ones become `__other__`. Values over 128 bytes become `__oversized__`. An `event_type` is taken only from a complete envelope; anything else is `unknown`. Every replacement is counted in `platform_telemetry_label_overflow_total`, so a producer sending unbounded event types cannot explode cardinality. Size the limit above the number of event types the service really handles.

## Wiring

```go
// Once at startup, before publishing or consuming. Use the SAME registerer
// and identity as platform-pgcommon so both libraries report the same labels.
// The identity is mandatory: there is no metrics init path without one.
id := events.MetricsIdentity{Domain: "iam", Service: "event-consumer"}
// Environment empty → APP_ENV, then ENVIRONMENT, else "dev".
// Or: events.MetricsIdentityFromEnv("iam", "") — service from APP_NAME.

warnings, err := events.InitMetrics(id, registry) // nil registry = prometheus.DefaultRegisterer
if err != nil {
    log.Fatal(err) // invalid or missing identity — nothing was registered
}
for _, w := range warnings {
    logger.Warn(w.Error(), nil) // a platform_* metric the registry refused — disabled, not fatal
}

pgWarnings, err := pgmetrics.InitWithIdentity(pgmetrics.Identity(id), registry)
```

- **Registerer wrappers are fine.** A registerer that already injects some identity labels (e.g. `prometheus.WrapRegistererWith`, platform-gincommon's `WrapRegistererWith`) works: each label is applied once and the wrapper's value wins.
- **gincommon interop.** `events.MetricsIdentityFromLabels(gincommon.MetricsConstLabels())` reuses the service's own label values.
- **No build version.** `MetricsIdentity` has no `Version`: a per-build label on Tier 1 metrics would multiply series on every deploy. `platform_library_info{library_version}` reports the platform-events module version instead.

## Semantics that matter for dashboards

- **Every delivery is received once.** Each SQS delivery (redeliveries included) increments `platform_messages_received_total` exactly once. It then ends in exactly one outcome:
  - **processed:** the handler returned `nil` and the message was deleted;
  - **failed:** counted by `reason`, and then either retried (`platform_retry_total{operation="consume"}`) or dead-lettered;
  - **dead-lettered:** `platform_dlq_messages_total{operation="consume"}`.
- **A dead-lettered message is counted once**, whoever forwarded it: `WithDLQForwarding`, the dead-letter handler, or a handler calling `SendToDLQ` and returning `nil`. The consumer passes the reason (`malformed`, `decode_error`, `max_receive_count`, `explicit`) to the DLQ publisher through the handler context. A handler that dead-letters explicitly is **not** also counted as processed.
- **Inbox duplicates are also counted as processed.** `platform_duplicate_messages_total` counts redeliveries acknowledged by the inbox without running the handler. Those deliveries still count as processed, since the wrapper returned `nil`, so the duplicate ratio is `duplicates / received`.
- **Outbox dead letters use the same metric.** `platform_dlq_messages_total{operation="outbox_publish",reason="max_attempts"}` counts events moved to `outbox_dead_letters`, so one dead-letter panel covers both flows.
- **Propagation is measured to the first receipt only.** `platform_event_propagation_seconds` is `first receipt − envelope time`, so redeliveries don't add their retry delay. It includes producer clock skew; negative skew is clamped to 0.
- **SQS receive latency includes long-polling.** `platform_dependency_request_seconds{operation="receive_message"}` includes up to `WaitTimeSeconds` (20 s) of waiting on an empty queue. Exclude it from latency panels (`operation!="receive_message"`); keep it for error rates.

## No legacy metrics

The standard's Backward Compatibility procedure (emit old and new names in parallel, migrate, deprecate, remove after a sunset) does not apply: no release emitting the pre-standard `events_*` / `outbox_*` / `sqs_*` metrics (or the old build-info gauge) was ever deployed to dev or production, so they were removed outright, together with `events.Init`, `events.InitWithRegisterer` and `events.WithoutLegacyMetrics`. The central Platform Observability Registry (platform-gincommon) keeps each removed name with status `removed` and its successors; the [CHANGELOG](../../CHANGELOG.md) has the old → new mapping. The non-1:1 cases:

- **The consume counter** (with its `status` label) is split into `platform_messages_processed_total` / `platform_messages_failed_total{reason}` / `platform_dlq_messages_total{reason}`.
- **Codec, SNS and SQS client counters and durations** are consolidated into `platform_dependency_request_seconds{dependency,operation,outcome}`; count calls with its `_count` series. It has **no `queue` label**: per-queue SQS error signals are per service now.
- **Outbox errors** are one counter, `platform_outbox_errors_total{operation}`. A failed pending-count query no longer sets the gauge to `-1`: `platform_outbox_pending_events` keeps its last value and `operation="pending_count"` is counted.

## Alerts on Proposed metrics

With the legacy metrics gone, the producer, outbox and SQS-client signals exist only as **Proposed** Tier 1 metrics, which still **await governance ratification**. The reference rules therefore alert on them, but explicitly:

- they live in the `platform_events.proposed.recording` / `platform_events.proposed.alerts` groups of [`platform-events.rules.yml`](../../monitoring/prometheus/platform-events.rules.yml);
- every such alert carries the label `metric_status: proposed`, so Alertmanager can route it according to the service's policy for unratified metrics;
- the reference KEDA trigger, which scales on `platform_outbox_pending_events`, carries the annotation `observability.platform/metric-status: proposed`;
- CI enforces the split: a rule depending on a Proposed metric must be in a `*.proposed` group, and a `*.proposed` group may hold only such rules.

| Alert | Metric (Proposed) |
|---|---|
| `PlatformEventsPublishErrors` | `platform_messages_published_total{outcome}` |
| `PlatformEventsOutboxBacklog` | `platform_outbox_pending_events` |
| `PlatformEventsOutboxPendingUnknown` | `platform_outbox_errors_total{operation="pending_count"}` |
| `PlatformEventsOutboxPollFailing` | `platform_outbox_errors_total{operation="poll"}` |
| `PlatformEventsOutboxDuplicateDeliveryRisk` | `platform_outbox_errors_total{operation="mark_published"}` |
| `PlatformEventsSQSReceiveFailing` | `platform_dependency_request_seconds_count{dependency="sqs",operation="receive_message",outcome="error"}` |
| `PlatformEventsEventTypeLabelOverflow` | `platform_telemetry_label_overflow_total` |

## CI enforcement

`make metrics-lint` runs in the `Validate / Quality` gate (and in `make ci`). It registers the **real** collectors, exercises every recording path with every approved label value, and fails on:

- **namespace classification:** only `platform_*` Tier 1 metrics are emitted, and none of the removed legacy names is registered or emitted;
- **naming:** snake_case, counters end in `_total`, histograms in `_seconds`, gauges borrow neither, and no library, service or domain name appears in a `platform_*` name;
- **required labels:** `domain`, `service`, `environment` present with the identity's values, and no `version` on Tier 1;
- **label vocabulary:** only approved labels, no prohibited label, every value in its approved set, `queue` / `topic` never a URL or ARN, `dependency` / `operation` pairs valid;
- **registry compliance:** every emitted metric is registered and every registered metric is emitted; ratification packets complete;
- **rule files:** only registered metrics, rules on Proposed metrics only in `*.proposed` groups (alerts labelled `metric_status: proposed`), labels within the vocabulary, and every alert has a severity, a summary and an existing runbook anchor;
- **dashboards:** every query and template variable uses registered metrics (or the reference recording rules) and only their labels; a panel querying a Proposed metric must be titled "(Proposed)";
- **autoscaling manifests:** registered metrics and labels only, and a manifest scaling on a Proposed metric must carry `observability.platform/metric-status: proposed`;
- **inventory drift:** `metrics-registry.md` must equal the rendered registry.

`make rules-check` (also in CI) runs `promtool check rules` and the alert unit tests.

## SLO definitions

| SLO | SLI | Objective | Window | Alerting | Source |
|---|---|---|---|---|---|
| Consumer processing success | `1 − platform_messages_failed_total / platform_messages_received_total` per `domain`, `service`, `environment`, `queue` (Canonical) | 99.9% (error budget 0.1%) | 30 days | Fast burn: failure ratio > 14.4 × budget over both 1h and 5m, only on queues with ≥ 1 msg/min → `PlatformEventsConsumerErrorBudgetBurn` (critical) | recording rules `platform_events:messages_failure_ratio:rate{5m,1h}`, `platform_events:messages_received:rate1h` |

- **Latency and propagation SLOs** (`platform_message_processing_duration_seconds`, `platform_event_propagation_seconds`) are deliberately not defined yet. Both metrics are Proposed, and the standard forbids SLOs on unratified metrics. The dashboard shows them as Proposed panels meanwhile.
- **Adjusting the objective:** change `0.001` in both burn-rate expressions and keep the multi-window structure. A service may define a stricter SLO on the same Canonical SLI.

## Known conflicts

IAM services already register `platform_retry_total` and `platform_dependency_request_seconds` with **conflicting label sets** (`{target_service,endpoint}` vs `{dependency,operation,outcome}`; `{event_type,reason}`). The same issue was recorded by platform-pgcommon.

- **What happens in those services.** A Prometheus registry cannot hold one name with two shapes, so registration is refused. `InitMetrics` returns a `RegistrationWarning` and disables only that metric (fail-soft). The service and every other metric keep working.
- **Reference alerts.** None of them depend on `platform_retry_total`, for that reason. `PlatformEventsSQSReceiveFailing` uses `platform_dependency_request_seconds` and cannot fire in a service where it is disabled — rely on the consumer alerts there.
- **Governance.** Each affected packet asks governance to ratify **one** label vocabulary per shared name.

## Registry ratification

Every Proposed entry carries a full packet (semantic definition, required labels, allowed label values, cardinality, aggregation expectations, governance notes) in [metrics-registry.md](metrics-registry.md#ratification-packets-proposed). That document is the submission to observability governance.

When a metric is ratified:
1. Flip its `Status` to `StatusCanonical` in `registry.go`.
2. Run `make metrics-doc`.
3. Move the reference rules that depend on it from the `*.proposed` groups to the Canonical groups, and drop their `metric_status: proposed` label (and the KEDA annotation, for `platform_outbox_pending_events`).
4. Release.

The lint then allows the metric in alerts, recording rules, SLOs and HPA.

## Outbox delivery-stall signal (Proposed)

`platform_outbox_oldest_pending_age` is the age in seconds of the oldest unpublished outbox event (0 when none). Transient publish failures — throttling, an SNS outage, network or credential problems — never dead-letter, so a stuck outbox on a low-volume service may never trip `PlatformEventsOutboxBacklog`; this gauge does. It is Proposed; the alert `PlatformEventsOutboxDeliveryStalled` (`max by (domain, service, environment) (platform_outbox_oldest_pending_age) > 600` for 10m) ships commented out in the `platform_events.proposed.alerts` group of `monitoring/prometheus/platform-events.rules.yml` — enable it there (with `metric_status: proposed`) if the service wants it before ratification. Graph it meanwhile.

**Triage when it climbs:** the `last_error` of the oldest rows (`SELECT id, attempts, last_error FROM outbox_events WHERE published_at IS NULL ORDER BY created_at LIMIT 10`); `platform_outbox_publish_attempts_total{outcome="error"}`; IAM (`sns:Publish`) and the topic's KMS key; whether any runner is up (`platform_outbox_errors_total{operation="poll"}`, pod status). Delivery resumes by itself once the dependency is fixed (transient failures back off to at most `OUTBOX_MAX_RETRY_BACKOFF`, 5m by default).

## Timeouts and per-key ordering (Proposed)

- `platform_message_timeouts_total{queue, event_type, operation}` counts messages whose `WithHandlerTimeout` deadline expired, by stage (`decode`, `dead_letter_handler`, `handler`). They are also in `platform_messages_failed_total` under `decode_error` / `dead_letter_error` / `handler_error`; the timeout share is `sum(rate(platform_message_timeouts_total[5m])) / sum(rate(platform_messages_failed_total[5m]))` per queue.
- `platform_outbox_ordering_blocked_events` is the number of ordered outbox records (`outbox.EnqueueOrdered`) waiting behind an earlier unpublished record with the same key. Steady growth means a head record keeps failing and holds its key: inspect `SELECT ordering_key, id, attempts, last_error, scheduled_at FROM outbox_events WHERE published_at IS NULL AND ordering_key IS NOT NULL ORDER BY ordering_key, ordering_seq`. The key moves again once the head is published or dead-lettered after `OUTBOX_MAX_ATTEMPTS`.
