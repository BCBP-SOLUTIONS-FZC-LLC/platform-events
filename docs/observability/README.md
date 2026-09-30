# Observability — Enterprise Platform Observability Standard

How platform-events implements the standard's three-tier metric taxonomy, how a service wires it, and how the compatibility-period migration works.

| File | What it is |
|---|---|
| [metrics-registry.md](metrics-registry.md) | **Generated** inventory and ratification packets for every metric (`make metrics-doc`). The source of truth is [`internal/adapter/outbound/metrics/registry.go`](../../internal/adapter/outbound/metrics/registry.go). |
| [runbook.md](runbook.md) | One section per reference alert. |
| [`monitoring/prometheus/platform-events.rules.yml`](../../monitoring/prometheus/platform-events.rules.yml) | Reference recording rules, SLIs, SLO burn-rate and operational alerts. |
| [`monitoring/prometheus/platform-events.rules.test.yml`](../../monitoring/prometheus/platform-events.rules.test.yml) | promtool alert unit tests (`make rules-check`). |

---

## Tier classification

platform-events is a **platform library**. Every service in every domain that publishes, consumes, dead-letters or deduplicates events through it emits these metrics with identical semantics, because the library defines them. By the standard's decision tree they are all **Tier 1 (`platform_*`)**.

- **Tier 2 (`<domain>_*`) and Tier 3 (`<domain>_<service>_*`) don't apply to the library.** It has no domain or service of its own. Services add those metrics for their own business signals, e.g. `iam_auth_events_total` or `iam_event_consumer_hmac_failures_total`.
- **Same model as platform-pgcommon.** Its `platform_db_*` metrics work the same way: one identity, one registerer, the same fail-soft and compatibility behaviour.

| Status | Metrics | May be used in alerts / SLOs / HPA |
|---|---|---|
| **Canonical** (named by the standard) | `platform_messages_received_total`, `platform_messages_processed_total`, `platform_messages_failed_total`, `platform_retry_total`, `platform_dlq_messages_total` | Yes. `platform_retry_total` is the exception; see [Known conflicts](#known-conflicts) |
| **Proposed** (registry-proposed examples) | `platform_duplicate_messages_total`, `platform_dependency_request_seconds`, `platform_event_propagation_seconds` | No, until ratified (rules 11/12) |
| **Proposed** (new names, ratification packets submitted) | `platform_messages_published_total`, `platform_message_processing_duration_seconds`, `platform_outbox_pending_events`, `platform_outbox_leased_events`, `platform_outbox_publish_attempts_total`, `platform_outbox_errors_total`, `platform_outbox_dead_letter_operations_total`, `platform_telemetry_label_overflow_total`, `platform_library_info` | No, until ratified |
| **Deprecated** (pre-standard) | `events_*`, `outbox_*`, `sqs_*`, `platform_events_build_info` | Yes during the compatibility period; they are the authoritative source where the successor is still Proposed |

`platform_queue_depth` and `platform_dlq_depth` are not emitted. SQS queue depth comes from CloudWatch (`ApproximateNumberOfMessagesVisible`), not from the consumer. The outbox backlog is a different quantity and has its own gauge, `platform_outbox_pending_events`.

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
id := events.MetricsIdentity{Domain: "iam", Service: "event-consumer", Version: buildVersion}
// Environment empty → APP_ENV, then ENVIRONMENT, else "dev".
// Or: events.MetricsIdentityFromEnv("iam", "", buildVersion) — service from APP_NAME.

warnings, err := events.InitMetrics(id, registry) // nil registry = prometheus.DefaultRegisterer
if err != nil {
    log.Fatal(err) // invalid identity, or a legacy metric could not register
}
for _, w := range warnings {
    logger.Warn(w.Error(), nil) // a platform_* metric the registry refused — disabled, not fatal
}

pgWarnings, err := pgmetrics.InitWithIdentity(pgmetrics.Identity(id), registry)
```

- **Registerer wrappers are fine.** A registerer that already injects some identity labels (e.g. `prometheus.WrapRegistererWith`, platform-gincommon's `WrapRegistererWith`) works: each label is applied once and the wrapper's value wins.
- **gincommon interop.** `events.MetricsIdentityFromLabels(gincommon.MetricsConstLabels())` reuses the service's own label values.
- **Deprecated entry points.** `events.Init` / `events.InitWithRegisterer` still register the legacy metrics only, and staticcheck reports `SA1019` on them. Replacing them with `InitMetrics` is a one-line change. A leftover `events.Init` after `InitMetrics` is a no-op, so it can't switch the Tier 1 metrics off. `InitWithRegisterer` is the test-isolation reset and does clear them.

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

## Compatibility period (Backward Compatibility, steps 1–8)

| Step | Status in platform-events | Service action |
|---|---|---|
| 1. Emit old and new in parallel | **Done.** `InitMetrics` registers both by default | Call `InitMetrics` |
| 2. Migrate dashboards | — | Switch panels to the Tier 1 names (Canonical now; Proposed ones as shadow panels) |
| 3. Migrate alerts | Reference rules already alert on the Canonical consumer metrics | Load the reference rules or port your own |
| 4. Migrate recording rules | Same | — |
| 5. Migrate SLOs | Consumer SLO defined on Canonical metrics (`platform_events:messages_failure_ratio:*`) | Adopt, or keep your own SLO on the Canonical metrics |
| 6. Migrate HPA references | — | HPA/KEDA on the outbox backlog must keep using `outbox_pending_total` until `platform_outbox_pending_events` is ratified; queue-depth scaling uses CloudWatch |
| 7. Deprecate legacy | **Done.** Legacy metrics are marked Deprecated (help text, registry) | — |
| 8. Remove after sunset | Not before 2027-04-01, set by governance | `events.WithoutLegacyMetrics()` once steps 2–6 are complete |

The mapping from each legacy metric to its successor is in [metrics-registry.md](metrics-registry.md#deprecated-compatibility-period). Most are 1:1. The exceptions:

- **`events_consumed_total{status}`** is split into `processed` / `failed{reason}` / `dlq_messages{reason}`.
- **Codec, SNS and SQS client metrics** are consolidated into `platform_dependency_request_seconds{dependency,operation,outcome}`. Count calls with its `_count` series.

## CI enforcement

`make metrics-lint` runs in the `Validate / Quality` gate (and in `make ci`). It registers the **real** collectors, exercises every recording path with every approved label value, and fails on:

- **namespace classification:** `platform_*` must be Tier 1; any other name may only be a Deprecated legacy metric with a Tier 1 successor;
- **naming:** snake_case, counters end in `_total`, histograms in `_seconds`, gauges borrow neither, and no library, service or domain name appears in a `platform_*` name;
- **required labels:** `domain`, `service`, `environment` present with the identity's values, and no `version` on Tier 1;
- **label vocabulary:** only approved labels, no prohibited label, every value in its approved set, `queue` / `topic` never a URL or ARN, `dependency` / `operation` pairs valid;
- **registry compliance:** every emitted metric is registered and every registered metric is emitted; ratification packets complete; `Supersedes` ↔ `SupersededBy` consistent;
- **rule files:** only registered metrics, no Proposed metric as a query target (comments only), labels within the vocabulary, and every alert has a severity, a summary and an existing runbook anchor;
- **inventory drift:** `metrics-registry.md` must equal the rendered registry.

`make rules-check` (also in CI) runs `promtool check rules` and the alert unit tests.

## Known conflicts

IAM services already register `platform_retry_total` and `platform_dependency_request_seconds` with **conflicting label sets** (`{target_service,endpoint}` vs `{dependency,operation,outcome}`; `{event_type,reason}`). The same issue was recorded by platform-pgcommon.

- **What happens in those services.** A Prometheus registry cannot hold one name with two shapes, so registration is refused. `InitMetrics` returns a `RegistrationWarning` and disables only that metric (fail-soft). The service and every other metric keep working.
- **Reference alerts.** None of them depend on `platform_retry_total`, for that reason.
- **Governance.** Each affected packet asks governance to ratify **one** label vocabulary per shared name.

## Registry ratification

Every Proposed entry carries a full packet (semantic definition, required labels, allowed label values, cardinality, aggregation expectations, governance notes) in [metrics-registry.md](metrics-registry.md#ratification-packets-proposed). That document is the submission to observability governance.

When a metric is ratified:
1. Flip its `Status` to `StatusCanonical` in `registry.go`.
2. Run `make metrics-doc`.
3. Switch the reference rules that name it in an `# after ratification:` comment.
4. Release.

The lint then allows the metric in alerts, recording rules, SLOs and HPA.
