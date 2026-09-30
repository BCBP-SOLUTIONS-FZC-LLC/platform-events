# platform-events — metrics registry

<!-- GENERATED from the metrics registry by test/unit/metrics/inventory_test.go — do not edit by hand.
     Regenerate: make metrics-doc -->

platform-events' entry in the Platform Observability Registry (Enterprise Platform Observability Standard). CI checks every registered collector against these records (`make metrics-lint`), so this inventory is exactly what a service using the library exposes. See [README](README.md) for the tier model, wiring and migration plan.

## Summary

| Metric | Type | Tier | Status | Labels | Replaces / replaced by |
|---|---|---|---|---|---|
| `platform_messages_received_total` | counter | platform | canonical | `domain`, `service`, `environment`, `queue` | — |
| `platform_messages_processed_total` | counter | platform | canonical | `domain`, `service`, `environment`, `queue`, `event_type` | `events_consumed_total` |
| `platform_messages_failed_total` | counter | platform | canonical | `domain`, `service`, `environment`, `queue`, `event_type`, `reason` | `events_consumed_total` |
| `platform_retry_total` | counter | platform | canonical | `domain`, `service`, `environment`, `operation`, `event_type` | `outbox_attempts_total` |
| `platform_dlq_messages_total` | counter | platform | canonical | `domain`, `service`, `environment`, `operation`, `event_type`, `reason` | `events_consumed_total`, `outbox_dead_letters_total`, `events_dlq_forwarded_total` |
| `platform_duplicate_messages_total` | counter | platform | proposed | `domain`, `service`, `environment`, `queue`, `event_type` | `events_inbox_duplicates_total` |
| `platform_dependency_request_seconds` | histogram | platform | proposed | `domain`, `service`, `environment`, `dependency`, `operation`, `outcome` | `events_publish_duration_seconds`, `events_codec_encode_total`, `events_codec_encode_duration_seconds`, `events_codec_decode_total`, `events_codec_decode_duration_seconds`, `sqs_receive_errors_total`, `sqs_delete_errors_total`, `sqs_visibility_extension_errors_total` |
| `platform_event_propagation_seconds` | histogram | platform | proposed | `domain`, `service`, `environment`, `queue`, `event_type` | — |
| `platform_queue_depth` | gauge | platform | proposed | `domain`, `service`, `environment`, `queue` | — |
| `platform_dlq_depth` | gauge | platform | proposed | `domain`, `service`, `environment`, `queue` | — |
| `platform_messages_published_total` | counter | platform | proposed | `domain`, `service`, `environment`, `topic`, `event_type`, `outcome` | `events_published_total` |
| `platform_message_processing_duration_seconds` | histogram | platform | proposed | `domain`, `service`, `environment`, `queue`, `event_type` | `events_consume_duration_seconds` |
| `platform_outbox_pending_events` | gauge | platform | proposed | `domain`, `service`, `environment` | `outbox_pending_total` |
| `platform_outbox_leased_events` | gauge | platform | proposed | `domain`, `service`, `environment` | `outbox_leased_total` |
| `platform_outbox_publish_attempts_total` | counter | platform | proposed | `domain`, `service`, `environment`, `event_type`, `outcome` | `outbox_published_total`, `outbox_attempts_total` |
| `platform_outbox_errors_total` | counter | platform | proposed | `domain`, `service`, `environment`, `operation` | `outbox_poll_errors_total`, `outbox_unmarshal_errors_total`, `outbox_mark_published_errors_total` |
| `platform_outbox_dead_letter_operations_total` | counter | platform | proposed | `domain`, `service`, `environment`, `operation` | `outbox_dead_letters_reprocessed_total`, `outbox_dead_letters_discarded_total` |
| `platform_telemetry_label_overflow_total` | counter | platform | proposed | `domain`, `service`, `environment`, `label` | `events_oversized_event_type_label_total` |
| `platform_library_info` | gauge | platform | proposed | `domain`, `service`, `environment`, `library`, `library_version` | `platform_events_build_info` |
| `platform_events_build_info` | gauge | legacy | deprecated | `service`, `version` | `platform_library_info` |
| `events_published_total` | counter | legacy | deprecated | `service`, `topic`, `event_type`, `status` | `platform_messages_published_total` |
| `events_publish_duration_seconds` | histogram | legacy | deprecated | `service`, `topic`, `event_type` | `platform_dependency_request_seconds` |
| `events_consumed_total` | counter | legacy | deprecated | `service`, `queue`, `event_type`, `status` | `platform_messages_processed_total`, `platform_messages_failed_total`, `platform_dlq_messages_total` |
| `events_consume_duration_seconds` | histogram | legacy | deprecated | `service`, `queue`, `event_type` | `platform_message_processing_duration_seconds` |
| `events_codec_encode_total` | counter | legacy | deprecated | `service`, `topic`, `event_type`, `status` | `platform_dependency_request_seconds` |
| `events_codec_encode_duration_seconds` | histogram | legacy | deprecated | `service`, `topic`, `event_type` | `platform_dependency_request_seconds` |
| `events_codec_decode_total` | counter | legacy | deprecated | `service`, `queue`, `event_type`, `status` | `platform_dependency_request_seconds` |
| `events_codec_decode_duration_seconds` | histogram | legacy | deprecated | `service`, `queue`, `event_type` | `platform_dependency_request_seconds` |
| `outbox_pending_total` | gauge | legacy | deprecated | `service` | `platform_outbox_pending_events` |
| `outbox_leased_total` | gauge | legacy | deprecated | `service` | `platform_outbox_leased_events` |
| `outbox_published_total` | counter | legacy | deprecated | `service`, `event_type`, `status` | `platform_outbox_publish_attempts_total` |
| `outbox_attempts_total` | counter | legacy | deprecated | `service`, `event_type` | `platform_outbox_publish_attempts_total`, `platform_retry_total` |
| `outbox_dead_letters_total` | counter | legacy | deprecated | `service`, `event_type` | `platform_dlq_messages_total` |
| `outbox_dead_letters_reprocessed_total` | counter | legacy | deprecated | `service` | `platform_outbox_dead_letter_operations_total` |
| `outbox_dead_letters_discarded_total` | counter | legacy | deprecated | `service` | `platform_outbox_dead_letter_operations_total` |
| `sqs_receive_errors_total` | counter | legacy | deprecated | `service`, `queue` | `platform_dependency_request_seconds` |
| `sqs_delete_errors_total` | counter | legacy | deprecated | `service`, `queue` | `platform_dependency_request_seconds` |
| `sqs_visibility_extension_errors_total` | counter | legacy | deprecated | `service`, `queue` | `platform_dependency_request_seconds` |
| `outbox_poll_errors_total` | counter | legacy | deprecated | `service` | `platform_outbox_errors_total` |
| `outbox_unmarshal_errors_total` | counter | legacy | deprecated | `service` | `platform_outbox_errors_total` |
| `outbox_mark_published_errors_total` | counter | legacy | deprecated | `service` | `platform_outbox_errors_total` |
| `events_inbox_duplicates_total` | counter | legacy | deprecated | `service`, `consumer` | `platform_duplicate_messages_total` |
| `events_dlq_forwarded_total` | counter | legacy | deprecated | `service`, `queue`, `event_type`, `status` | `platform_dlq_messages_total` |
| `events_oversized_event_type_label_total` | counter | legacy | deprecated | `service` | `platform_telemetry_label_overflow_total` |

## Label vocabulary

A label name means the same thing on every metric that uses it; each entry below lists the values that metric can take.

| Label | Kind | Allowed values |
|---|---|---|
| `domain` | required (Tier 1) | Owning business domain, lowercase (e.g. `iam`, `workflow`, `billing`, `tender`, `notifications`, `documents`) — from `events.MetricsIdentity` |
| `service` | required | Service name, lowercase `[a-z][a-z0-9_-]{0,62}` — from `events.MetricsIdentity` (or `APP_NAME` via `events.MetricsIdentityFromEnv`) |
| `environment` | required (Tier 1) | Deployment environment, lowercase — `APP_ENV`, then `ENVIRONMENT`, else `dev` when `MetricsIdentity.Environment` is empty (same precedence as platform-gincommon and platform-pgcommon). Canonical spellings (`prod` vs `production`) are pending governance |
| `queue` | approved | SQS queue name — the last path segment of the queue URL (e.g. `orders`, `orders.fifo`), never the full URL (it carries the account ID). Bounded by the queues a service consumes (typically 1–5). `unknown` when the queue cannot be determined (e.g. the inbox wrapper used outside the SQS consumer). |
| `topic` | requested | SNS topic name — the last segment of the topic ARN (e.g. `iam-events`, `orders.fifo`), never the full ARN. Bounded by the topics a service publishes to (typically 1–3). |
| `event_type` | approved | Envelope `type` (`<domain>.<entity>.<past-tense-verb>[.v<N>]`), expected to come from the event-type registry in EVENT_SCHEMA_GOVERNANCE.md — and enforced in-process: at most 200 distinct values per process (`events.WithEventTypeLimit`), further ones recorded as `__other__`; values over 128 bytes as `__oversized__`; an empty or unparseable type as `unknown`. Replacements are counted in platform_telemetry_label_overflow_total. |
| `reason` | approved | failures: `malformed`, `decode_error`, `handler_error`, `handler_panic`, `dead_letter_error`; dead-letters: `malformed`, `decode_error`, `max_receive_count`, `explicit`, `max_attempts` |
| `operation` | approved | message flow: `consume`, `outbox_publish`; dependency calls: `publish`, `publish_batch`, `receive_message`, `delete_message`, `change_message_visibility`, `send_message`, `get_queue_attributes`, `get_queue_url`, `encode`, `decode`; outbox errors: `poll`, `unmarshal`, `mark_published`, `pending_count`, `leased_count`; dead-letter actions: `reprocess`, `discard` |
| `dependency` | approved | `codec` → `encode`, `decode`; `sns` → `publish`, `publish_batch`; `sqs` → `receive_message`, `delete_message`, `change_message_visibility`, `send_message`, `get_queue_attributes`, `get_queue_url` |
| `outcome` | approved | `success`, `error` |
| `label` | requested | `event_type` |
| `library` / `library_version` | requested | `platform-events`; The platform-events module version the service was built with (e.g. `v1.6.0`), `devel` for an unreleased build, `unknown` when build info is unavailable. One value per deployment; changes only on a library upgrade. |
| `version` | legacy only | the service's build version (`platform_events_build_info`); never on Tier 1 metrics |
| `status` | legacy only | `success`, `error`, `malformed`, `noop`, `dlq_success`, `dlq_error` (split into `outcome` / `reason` on Tier 1) |
| `consumer` | legacy only | inbox consumer name (replaced by `queue` on Tier 1) |

**Prohibited on every metric** (high cardinality / sensitive): `actor`, `correlation_id`, `email`, `event_id`, `message_id`, `request_id`, `session_id`, `span_id`, `subject`, `tenant_id`, `trace_id`, `user_id`.

## Canonical metrics

### `platform_messages_received_total`

- **Type:** counter · **Tier:** platform · **Status:** canonical
- **Semantic definition:** A message delivered to a consumer by its queue (one per SQS delivery, redeliveries included), counted on receipt before it is parsed.
- **Required labels:** `domain`, `service`, `environment`
- **Approved labels:** `queue`
  - `queue`: SQS queue name — the last path segment of the queue URL (e.g. `orders`, `orders.fifo`), never the full URL (it carries the account ID). Bounded by the queues a service consumes (typically 1–5). `unknown` when the queue cannot be determined (e.g. the inbox wrapper used outside the SQS consumer).
- **Cardinality:** queue (≤5 per service).
- **Aggregation:** Inbound rate per service: sum by (domain, service) (rate(platform_messages_received_total[5m])). Platform-wide: sum by (domain) (…).
- **Supersedes:** —
- **Governance notes:** Canonical name per the standard. queue is an approved dimension; its value is the queue name, not the URL.

### `platform_messages_processed_total`

- **Type:** counter · **Tier:** platform · **Status:** canonical
- **Semantic definition:** A received message whose handler completed successfully, after which the message was acknowledged (deleted). Messages dead-lettered instead are counted in platform_dlq_messages_total, not here.
- **Required labels:** `domain`, `service`, `environment`
- **Approved labels:** `queue`, `event_type`
  - `queue`: SQS queue name — the last path segment of the queue URL (e.g. `orders`, `orders.fifo`), never the full URL (it carries the account ID). Bounded by the queues a service consumes (typically 1–5). `unknown` when the queue cannot be determined (e.g. the inbox wrapper used outside the SQS consumer).
  - `event_type`: Envelope `type` (`<domain>.<entity>.<past-tense-verb>[.v<N>]`), expected to come from the event-type registry in EVENT_SCHEMA_GOVERNANCE.md — and enforced in-process: at most 200 distinct values per process (`events.WithEventTypeLimit`), further ones recorded as `__other__`; values over 128 bytes as `__oversized__`; an empty or unparseable type as `unknown`. Replacements are counted in platform_telemetry_label_overflow_total.
- **Cardinality:** queue (≤5) × event_type (registered types a service consumes, typically ≤30).
- **Aggregation:** Success ratio: sum by (domain, service) (rate(platform_messages_processed_total[5m])) / sum by (domain, service) (rate(platform_messages_received_total[5m])).
- **Supersedes:** `events_consumed_total`
- **Governance notes:** Canonical name per the standard. event_type is an approved dimension.

### `platform_messages_failed_total`

- **Type:** counter · **Tier:** platform · **Status:** canonical
- **Semantic definition:** A received message that could not be processed on this delivery, by reason: malformed (not a valid envelope), decode_error (schema-registry codec failure), handler_error, handler_panic, or dead_letter_error (dead-letter handling or DLQ forwarding failed). A failed message is retried or dead-lettered; see platform_retry_total and platform_dlq_messages_total.
- **Required labels:** `domain`, `service`, `environment`
- **Approved labels:** `queue`, `event_type`, `reason`
  - `queue`: SQS queue name — the last path segment of the queue URL (e.g. `orders`, `orders.fifo`), never the full URL (it carries the account ID). Bounded by the queues a service consumes (typically 1–5). `unknown` when the queue cannot be determined (e.g. the inbox wrapper used outside the SQS consumer).
  - `event_type`: Envelope `type` (`<domain>.<entity>.<past-tense-verb>[.v<N>]`), expected to come from the event-type registry in EVENT_SCHEMA_GOVERNANCE.md — and enforced in-process: at most 200 distinct values per process (`events.WithEventTypeLimit`), further ones recorded as `__other__`; values over 128 bytes as `__oversized__`; an empty or unparseable type as `unknown`. Replacements are counted in platform_telemetry_label_overflow_total.
  - `reason`: `malformed`, `decode_error`, `handler_error`, `handler_panic`, `dead_letter_error`
- **Cardinality:** queue (≤5) × event_type (≤30) × reason (5); malformed always has event_type=unknown.
- **Aggregation:** Failure ratio: sum by (domain, service) (rate(platform_messages_failed_total[5m])) / sum by (domain, service) (rate(platform_messages_received_total[5m])). Poison producers: sum by (domain, service, queue) (rate(platform_messages_failed_total{reason="malformed"}[15m])).
- **Supersedes:** `events_consumed_total`
- **Governance notes:** Canonical name per the standard. The reason vocabulary is requested for approval.

### `platform_retry_total`

- **Type:** counter · **Tier:** platform · **Status:** canonical
- **Semantic definition:** An attempt that failed and was left for automatic retry: a consumed message whose processing failed and stays on its queue for redelivery (operation=consume), or an outbox publish that failed with attempts remaining (operation=outbox_publish).
- **Required labels:** `domain`, `service`, `environment`
- **Approved labels:** `operation`, `event_type`
  - `operation`: `consume`, `outbox_publish`
  - `event_type`: Envelope `type` (`<domain>.<entity>.<past-tense-verb>[.v<N>]`), expected to come from the event-type registry in EVENT_SCHEMA_GOVERNANCE.md — and enforced in-process: at most 200 distinct values per process (`events.WithEventTypeLimit`), further ones recorded as `__other__`; values over 128 bytes as `__oversized__`; an empty or unparseable type as `unknown`. Replacements are counted in platform_telemetry_label_overflow_total.
- **Cardinality:** operation (2) × event_type (≤30).
- **Aggregation:** Retry pressure per service: sum by (domain, service, operation) (rate(platform_retry_total[5m])).
- **Supersedes:** `outbox_attempts_total`
- **Governance notes:** Canonical name per the standard, but IAM services already register platform_retry_total with conflicting label sets ({target_service,endpoint}; {event_type,reason}). In such a service registration is refused and reported as a RegistrationWarning (fail-soft), so the reference alerts do not depend on this metric until governance approves ONE label vocabulary.

### `platform_dlq_messages_total`

- **Type:** counter · **Tier:** platform · **Status:** canonical
- **Semantic definition:** A message moved to dead-letter storage: forwarded to its SQS queue's RedrivePolicy DLQ (operation=consume), or moved to outbox_dead_letters after exhausting its publish attempts (operation=outbox_publish), by reason.
- **Required labels:** `domain`, `service`, `environment`
- **Approved labels:** `operation`, `event_type`, `reason`
  - `operation`: `consume`, `outbox_publish`
  - `event_type`: Envelope `type` (`<domain>.<entity>.<past-tense-verb>[.v<N>]`), expected to come from the event-type registry in EVENT_SCHEMA_GOVERNANCE.md — and enforced in-process: at most 200 distinct values per process (`events.WithEventTypeLimit`), further ones recorded as `__other__`; values over 128 bytes as `__oversized__`; an empty or unparseable type as `unknown`. Replacements are counted in platform_telemetry_label_overflow_total.
  - `reason`: `malformed`, `decode_error`, `max_receive_count`, `explicit`, `max_attempts`
- **Cardinality:** operation (2) × event_type (≤30) × reason (5).
- **Aggregation:** Dead-letter inflow: sum by (domain, service, operation, reason) (increase(platform_dlq_messages_total[15m])). Any sustained non-zero rate needs attention.
- **Supersedes:** `events_consumed_total`, `outbox_dead_letters_total`, `events_dlq_forwarded_total`
- **Governance notes:** Canonical name per the standard. operation and reason vocabularies are requested for approval.


## Ratification packets (Proposed)

### `platform_duplicate_messages_total`

- **Type:** counter · **Tier:** platform · **Status:** proposed
- **Semantic definition:** A redelivered message acknowledged without running its handler because the consumer's dedup ledger (inbox processed_events) had already recorded its event ID.
- **Required labels:** `domain`, `service`, `environment`
- **Approved labels:** `queue`, `event_type`
  - `queue`: SQS queue name — the last path segment of the queue URL (e.g. `orders`, `orders.fifo`), never the full URL (it carries the account ID). Bounded by the queues a service consumes (typically 1–5). `unknown` when the queue cannot be determined (e.g. the inbox wrapper used outside the SQS consumer).
  - `event_type`: Envelope `type` (`<domain>.<entity>.<past-tense-verb>[.v<N>]`), expected to come from the event-type registry in EVENT_SCHEMA_GOVERNANCE.md — and enforced in-process: at most 200 distinct values per process (`events.WithEventTypeLimit`), further ones recorded as `__other__`; values over 128 bytes as `__oversized__`; an empty or unparseable type as `unknown`. Replacements are counted in platform_telemetry_label_overflow_total.
- **Cardinality:** queue (≤5) × event_type (≤30).
- **Aggregation:** Duplicate ratio: sum by (domain, service) (rate(platform_duplicate_messages_total[15m])) / sum by (domain, service) (rate(platform_messages_received_total[15m])).
- **Supersedes:** `events_inbox_duplicates_total`
- **Governance notes:** Registry-proposed example in the standard. The legacy metric's consumer label is replaced by queue.

### `platform_dependency_request_seconds`

- **Type:** histogram · **Tier:** platform · **Status:** proposed
- **Semantic definition:** Client-observed wall time of one call to an external dependency — SNS (publish, publish_batch), SQS (receive_message, delete_message, change_message_visibility, send_message, get_queue_attributes, get_queue_url) or the configured schema-registry codec (encode, decode) — by outcome. The _count series counts calls. receive_message is a long poll: its duration includes up to WaitTimeSeconds (default 20 s) of waiting on an empty queue, so exclude it from latency views (it stays meaningful for error rates).
- **Required labels:** `domain`, `service`, `environment`
- **Approved labels:** `dependency`, `operation`, `outcome`
  - `dependency`: `sns`, `sqs`, `codec`
  - `operation`: `publish`, `publish_batch`, `receive_message`, `delete_message`, `change_message_visibility`, `send_message`, `get_queue_attributes`, `get_queue_url`, `encode`, `decode`
  - `outcome`: `success`, `error`
- **Cardinality:** dependency × operation (10 valid pairs) × outcome (2) × 12 buckets.
- **Aggregation:** Error ratio: sum by (domain, service, dependency) (rate(platform_dependency_request_seconds_count{outcome="error"}[5m])) / sum by (domain, service, dependency) (rate(platform_dependency_request_seconds_count[5m])). p99 (long-poll receives excluded): histogram_quantile(0.99, sum by (le, domain, service, dependency, operation) (rate(platform_dependency_request_seconds_bucket{operation!="receive_message"}[5m]))).
- **Supersedes:** `events_publish_duration_seconds`, `events_codec_encode_total`, `events_codec_encode_duration_seconds`, `events_codec_decode_total`, `events_codec_decode_duration_seconds`, `sqs_receive_errors_total`, `sqs_delete_errors_total`, `sqs_visibility_extension_errors_total`
- **Governance notes:** Registry-proposed example in the standard. IAM services register this name with conflicting label sets ({target_service,endpoint} vs {dependency,operation,outcome}); platform-events uses {dependency,operation,outcome}. Where a service's registry already holds another shape the metric is disabled with a RegistrationWarning (fail-soft).

### `platform_event_propagation_seconds`

- **Type:** histogram · **Tier:** platform · **Status:** proposed
- **Semantic definition:** Time from an event's creation (envelope `time`, set by the producer) to its FIRST receipt by a consumer (ApproximateReceiveCount ≤ 1) — end-to-end propagation delay through the outbox, SNS and SQS. Redeliveries are not observed, so retry delay does not inflate it. Negative clock skew is clamped to 0.
- **Required labels:** `domain`, `service`, `environment`
- **Approved labels:** `queue`, `event_type`
  - `queue`: SQS queue name — the last path segment of the queue URL (e.g. `orders`, `orders.fifo`), never the full URL (it carries the account ID). Bounded by the queues a service consumes (typically 1–5). `unknown` when the queue cannot be determined (e.g. the inbox wrapper used outside the SQS consumer).
  - `event_type`: Envelope `type` (`<domain>.<entity>.<past-tense-verb>[.v<N>]`), expected to come from the event-type registry in EVENT_SCHEMA_GOVERNANCE.md — and enforced in-process: at most 200 distinct values per process (`events.WithEventTypeLimit`), further ones recorded as `__other__`; values over 128 bytes as `__oversized__`; an empty or unparseable type as `unknown`. Replacements are counted in platform_telemetry_label_overflow_total.
- **Cardinality:** queue (≤5) × event_type (≤30) × 14 buckets.
- **Aggregation:** p95 propagation per consumer: histogram_quantile(0.95, sum by (le, domain, service, queue) (rate(platform_event_propagation_seconds_bucket[5m]))). Includes producer clock skew.
- **Supersedes:** —
- **Governance notes:** Registry-proposed example in the standard. New signal (no legacy predecessor).

### `platform_queue_depth`

- **Type:** gauge · **Tier:** platform · **Status:** proposed
- **Semantic definition:** Messages waiting on a consumer's queue to be received (SQS ApproximateNumberOfMessages — visible, not in flight or delayed), sampled by the consumer every WithQueueDepthMetrics interval. Approximate by SQS design.
- **Required labels:** `domain`, `service`, `environment`
- **Approved labels:** `queue`
  - `queue`: SQS queue name — the last path segment of the queue URL (e.g. `orders`, `orders.fifo`), never the full URL (it carries the account ID). Bounded by the queues a service consumes (typically 1–5). `unknown` when the queue cannot be determined (e.g. the inbox wrapper used outside the SQS consumer).
- **Cardinality:** queue (≤5 per service).
- **Aggregation:** Every replica samples the same queue, so aggregate with max, not sum: max by (domain, service, queue) (platform_queue_depth). The natural HPA/KEDA scaling signal once ratified.
- **Supersedes:** —
- **Governance notes:** Registry-proposed example in the standard. Emitted by platform-events because consuming services may not use the SQS SDK themselves (depguard); opt-in (WithQueueDepthMetrics) since each replica polls sqs:GetQueueAttributes.

### `platform_dlq_depth`

- **Type:** gauge · **Tier:** platform · **Status:** proposed
- **Semantic definition:** Messages sitting in the dead-letter queue attached to a consumer's queue by its RedrivePolicy (SQS ApproximateNumberOfMessages of the DLQ), sampled with platform_queue_depth. queue is the SOURCE queue's name, so the gauge joins with the consumer's other queue metrics.
- **Required labels:** `domain`, `service`, `environment`
- **Approved labels:** `queue`
  - `queue`: SQS queue name — the last path segment of the queue URL (e.g. `orders`, `orders.fifo`), never the full URL (it carries the account ID). Bounded by the queues a service consumes (typically 1–5). `unknown` when the queue cannot be determined (e.g. the inbox wrapper used outside the SQS consumer).
- **Cardinality:** queue (≤5 per service).
- **Aggregation:** max by (domain, service, queue) (platform_dlq_depth) > 0 — messages awaiting investigation or replay. Complements platform_dlq_messages_total (inflow) with the backlog.
- **Supersedes:** —
- **Governance notes:** Registry-proposed example in the standard. Not emitted when the queue has no RedrivePolicy.

### `platform_messages_published_total`

- **Type:** counter · **Tier:** platform · **Status:** proposed
- **Semantic definition:** One event a producer attempted to publish to a topic, by outcome (error includes validation, encoding and broker failures; a batch counts each message).
- **Required labels:** `domain`, `service`, `environment`
- **Approved labels:** `topic`, `event_type`, `outcome`
  - `topic`: SNS topic name — the last segment of the topic ARN (e.g. `iam-events`, `orders.fifo`), never the full ARN. Bounded by the topics a service publishes to (typically 1–3).
  - `event_type`: Envelope `type` (`<domain>.<entity>.<past-tense-verb>[.v<N>]`), expected to come from the event-type registry in EVENT_SCHEMA_GOVERNANCE.md — and enforced in-process: at most 200 distinct values per process (`events.WithEventTypeLimit`), further ones recorded as `__other__`; values over 128 bytes as `__oversized__`; an empty or unparseable type as `unknown`. Replacements are counted in platform_telemetry_label_overflow_total.
  - `outcome`: `success`, `error`
- **Cardinality:** topic (≤3) × event_type (≤30) × outcome (2).
- **Aggregation:** Publish error ratio: sum by (domain, service, topic) (rate(platform_messages_published_total{outcome="error"}[5m])) / sum by (domain, service, topic) (rate(platform_messages_published_total[5m])).
- **Supersedes:** `events_published_total`
- **Governance notes:** New platform_* name (not among the standard's canonical or registry-proposed examples); submitted under the Registry Ratification Requirement. Shadow-emitted until ratified. The producer-side counterpart of platform_messages_received_total; requests approval of a topic label.

### `platform_message_processing_duration_seconds`

- **Type:** histogram · **Tier:** platform · **Status:** proposed
- **Semantic definition:** Wall time a consumer's handler (or dead-letter handler) spent on one delivery, whatever the outcome.
- **Required labels:** `domain`, `service`, `environment`
- **Approved labels:** `queue`, `event_type`
  - `queue`: SQS queue name — the last path segment of the queue URL (e.g. `orders`, `orders.fifo`), never the full URL (it carries the account ID). Bounded by the queues a service consumes (typically 1–5). `unknown` when the queue cannot be determined (e.g. the inbox wrapper used outside the SQS consumer).
  - `event_type`: Envelope `type` (`<domain>.<entity>.<past-tense-verb>[.v<N>]`), expected to come from the event-type registry in EVENT_SCHEMA_GOVERNANCE.md — and enforced in-process: at most 200 distinct values per process (`events.WithEventTypeLimit`), further ones recorded as `__other__`; values over 128 bytes as `__oversized__`; an empty or unparseable type as `unknown`. Replacements are counted in platform_telemetry_label_overflow_total.
- **Cardinality:** queue (≤5) × event_type (≤30) × 14 buckets.
- **Aggregation:** p99 handler latency: histogram_quantile(0.99, sum by (le, domain, service, queue) (rate(platform_message_processing_duration_seconds_bucket[5m]))).
- **Supersedes:** `events_consume_duration_seconds`
- **Governance notes:** New platform_* name (not among the standard's canonical or registry-proposed examples); submitted under the Registry Ratification Requirement. Shadow-emitted until ratified.

### `platform_outbox_pending_events`

- **Type:** gauge · **Tier:** platform · **Status:** proposed
- **Semantic definition:** Events in a service's transactional outbox that are waiting to be published (unpublished and not leased by a runner), sampled each outbox poll cycle. Left at its last value when the count query fails (see platform_outbox_errors_total{operation="pending_count"}).
- **Required labels:** `domain`, `service`, `environment`
- **Approved labels:** —
- **Cardinality:** One series per service instance.
- **Aggregation:** Backlog per service: max by (domain, service) (platform_outbox_pending_events) — every runner reads the same table, so use max, not sum.
- **Supersedes:** `outbox_pending_total`
- **Governance notes:** New platform_* name (not among the standard's canonical or registry-proposed examples); submitted under the Registry Ratification Requirement. Shadow-emitted until ratified.

### `platform_outbox_leased_events`

- **Type:** gauge · **Tier:** platform · **Status:** proposed
- **Semantic definition:** Outbox events currently claimed by a runner and being published (leased, not yet published or released), sampled each poll cycle.
- **Required labels:** `domain`, `service`, `environment`
- **Approved labels:** —
- **Cardinality:** One series per service instance.
- **Aggregation:** max by (domain, service) (platform_outbox_leased_events).
- **Supersedes:** `outbox_leased_total`
- **Governance notes:** New platform_* name (not among the standard's canonical or registry-proposed examples); submitted under the Registry Ratification Requirement. Shadow-emitted until ratified.

### `platform_outbox_publish_attempts_total`

- **Type:** counter · **Tier:** platform · **Status:** proposed
- **Semantic definition:** One attempt by the outbox runner to publish a claimed outbox event, by outcome.
- **Required labels:** `domain`, `service`, `environment`
- **Approved labels:** `event_type`, `outcome`
  - `event_type`: Envelope `type` (`<domain>.<entity>.<past-tense-verb>[.v<N>]`), expected to come from the event-type registry in EVENT_SCHEMA_GOVERNANCE.md — and enforced in-process: at most 200 distinct values per process (`events.WithEventTypeLimit`), further ones recorded as `__other__`; values over 128 bytes as `__oversized__`; an empty or unparseable type as `unknown`. Replacements are counted in platform_telemetry_label_overflow_total.
  - `outcome`: `success`, `error`
- **Cardinality:** event_type (≤30) × outcome (2).
- **Aggregation:** Outbox publish error ratio: sum by (domain, service) (rate(platform_outbox_publish_attempts_total{outcome="error"}[5m])) / sum by (domain, service) (rate(platform_outbox_publish_attempts_total[5m])).
- **Supersedes:** `outbox_published_total`, `outbox_attempts_total`
- **Governance notes:** New platform_* name (not among the standard's canonical or registry-proposed examples); submitted under the Registry Ratification Requirement. Shadow-emitted until ratified.

### `platform_outbox_errors_total`

- **Type:** counter · **Tier:** platform · **Status:** proposed
- **Semantic definition:** An outbox runner step that failed outside a publish attempt: poll (claiming a batch), unmarshal (a stored envelope that no longer parses), mark_published (an event published but not marked — it will be delivered again), pending_count / leased_count (a backlog gauge query).
- **Required labels:** `domain`, `service`, `environment`
- **Approved labels:** `operation`
  - `operation`: `poll`, `unmarshal`, `mark_published`, `pending_count`, `leased_count`
- **Cardinality:** operation (5).
- **Aggregation:** sum by (domain, service, operation) (rate(platform_outbox_errors_total[5m])) > 0.
- **Supersedes:** `outbox_poll_errors_total`, `outbox_unmarshal_errors_total`, `outbox_mark_published_errors_total`
- **Governance notes:** New platform_* name (not among the standard's canonical or registry-proposed examples); submitted under the Registry Ratification Requirement. Shadow-emitted until ratified.

### `platform_outbox_dead_letter_operations_total`

- **Type:** counter · **Tier:** platform · **Status:** proposed
- **Semantic definition:** Outbox dead-letter records acted on by an operator call: reprocess (moved back to outbox_events for retry) or discard (permanently deleted). Counts records, not calls.
- **Required labels:** `domain`, `service`, `environment`
- **Approved labels:** `operation`
  - `operation`: `reprocess`, `discard`
- **Cardinality:** operation (2).
- **Aggregation:** Audit trail: sum by (domain, service, operation) (increase(platform_outbox_dead_letter_operations_total[1d])).
- **Supersedes:** `outbox_dead_letters_reprocessed_total`, `outbox_dead_letters_discarded_total`
- **Governance notes:** New platform_* name (not among the standard's canonical or registry-proposed examples); submitted under the Registry Ratification Requirement. Shadow-emitted until ratified.

### `platform_telemetry_label_overflow_total`

- **Type:** counter · **Tier:** platform · **Status:** proposed
- **Semantic definition:** A label value the library replaced because it exceeded its cardinality bound: event_type over 128 bytes → `__oversized__`, or a new distinct event_type beyond the per-process limit (default 200) → `__other__`. Counts replacements. Non-zero means a misconfigured or adversarial producer, or a limit set below the service's real number of event types.
- **Required labels:** `domain`, `service`, `environment`
- **Approved labels:** `label`
  - `label`: `event_type`
- **Cardinality:** label (1).
- **Aggregation:** sum by (domain, service, label) (rate(platform_telemetry_label_overflow_total[15m])) > 0.
- **Supersedes:** `events_oversized_event_type_label_total`
- **Governance notes:** New platform_* name (not among the standard's canonical or registry-proposed examples); submitted under the Registry Ratification Requirement. Shadow-emitted until ratified.

### `platform_library_info`

- **Type:** gauge · **Tier:** platform · **Status:** proposed
- **Semantic definition:** Always 1. Identifies a platform library and the version a service was built with, for fleet-wide upgrade tracking.
- **Required labels:** `domain`, `service`, `environment`
- **Approved labels:** `library`, `library_version`
  - `library`: `platform-events`
  - `library_version`: The platform-events module version the service was built with (e.g. `v1.6.0`), `devel` for an unreleased build, `unknown` when build info is unavailable. One value per deployment; changes only on a library upgrade.
- **Cardinality:** One series per service instance.
- **Aggregation:** Services still on an old version: count by (library_version) (platform_library_info{library="platform-events"}).
- **Supersedes:** `platform_events_build_info`
- **Governance notes:** New platform_* name (not among the standard's canonical or registry-proposed examples); submitted under the Registry Ratification Requirement. Shadow-emitted until ratified. Intended to be shared by every platform library (platform-pgcommon, platform-gincommon); the legacy build_info carried the service's own version, which is not in the Tier 1 vocabulary.

## Deprecated (compatibility period)

| Metric | Successor | Sunset |
|---|---|---|
| `platform_events_build_info` | `platform_library_info` | Not before the first release ≥ 2027-04-01 and only after every consumer has migrated dashboards, alerts, recording rules, SLOs and HPA to the platform_* successor (standard §Backward Compatibility steps 2–8); final date set by observability governance. |
| `events_published_total` | `platform_messages_published_total` | Not before the first release ≥ 2027-04-01 and only after every consumer has migrated dashboards, alerts, recording rules, SLOs and HPA to the platform_* successor (standard §Backward Compatibility steps 2–8); final date set by observability governance. |
| `events_publish_duration_seconds` | `platform_dependency_request_seconds` | Not before the first release ≥ 2027-04-01 and only after every consumer has migrated dashboards, alerts, recording rules, SLOs and HPA to the platform_* successor (standard §Backward Compatibility steps 2–8); final date set by observability governance. |
| `events_consumed_total` | `platform_messages_processed_total`, `platform_messages_failed_total`, `platform_dlq_messages_total` | Not before the first release ≥ 2027-04-01 and only after every consumer has migrated dashboards, alerts, recording rules, SLOs and HPA to the platform_* successor (standard §Backward Compatibility steps 2–8); final date set by observability governance. |
| `events_consume_duration_seconds` | `platform_message_processing_duration_seconds` | Not before the first release ≥ 2027-04-01 and only after every consumer has migrated dashboards, alerts, recording rules, SLOs and HPA to the platform_* successor (standard §Backward Compatibility steps 2–8); final date set by observability governance. |
| `events_codec_encode_total` | `platform_dependency_request_seconds` | Not before the first release ≥ 2027-04-01 and only after every consumer has migrated dashboards, alerts, recording rules, SLOs and HPA to the platform_* successor (standard §Backward Compatibility steps 2–8); final date set by observability governance. |
| `events_codec_encode_duration_seconds` | `platform_dependency_request_seconds` | Not before the first release ≥ 2027-04-01 and only after every consumer has migrated dashboards, alerts, recording rules, SLOs and HPA to the platform_* successor (standard §Backward Compatibility steps 2–8); final date set by observability governance. |
| `events_codec_decode_total` | `platform_dependency_request_seconds` | Not before the first release ≥ 2027-04-01 and only after every consumer has migrated dashboards, alerts, recording rules, SLOs and HPA to the platform_* successor (standard §Backward Compatibility steps 2–8); final date set by observability governance. |
| `events_codec_decode_duration_seconds` | `platform_dependency_request_seconds` | Not before the first release ≥ 2027-04-01 and only after every consumer has migrated dashboards, alerts, recording rules, SLOs and HPA to the platform_* successor (standard §Backward Compatibility steps 2–8); final date set by observability governance. |
| `outbox_pending_total` | `platform_outbox_pending_events` | Not before the first release ≥ 2027-04-01 and only after every consumer has migrated dashboards, alerts, recording rules, SLOs and HPA to the platform_* successor (standard §Backward Compatibility steps 2–8); final date set by observability governance. |
| `outbox_leased_total` | `platform_outbox_leased_events` | Not before the first release ≥ 2027-04-01 and only after every consumer has migrated dashboards, alerts, recording rules, SLOs and HPA to the platform_* successor (standard §Backward Compatibility steps 2–8); final date set by observability governance. |
| `outbox_published_total` | `platform_outbox_publish_attempts_total` | Not before the first release ≥ 2027-04-01 and only after every consumer has migrated dashboards, alerts, recording rules, SLOs and HPA to the platform_* successor (standard §Backward Compatibility steps 2–8); final date set by observability governance. |
| `outbox_attempts_total` | `platform_outbox_publish_attempts_total`, `platform_retry_total` | Not before the first release ≥ 2027-04-01 and only after every consumer has migrated dashboards, alerts, recording rules, SLOs and HPA to the platform_* successor (standard §Backward Compatibility steps 2–8); final date set by observability governance. |
| `outbox_dead_letters_total` | `platform_dlq_messages_total` | Not before the first release ≥ 2027-04-01 and only after every consumer has migrated dashboards, alerts, recording rules, SLOs and HPA to the platform_* successor (standard §Backward Compatibility steps 2–8); final date set by observability governance. |
| `outbox_dead_letters_reprocessed_total` | `platform_outbox_dead_letter_operations_total` | Not before the first release ≥ 2027-04-01 and only after every consumer has migrated dashboards, alerts, recording rules, SLOs and HPA to the platform_* successor (standard §Backward Compatibility steps 2–8); final date set by observability governance. |
| `outbox_dead_letters_discarded_total` | `platform_outbox_dead_letter_operations_total` | Not before the first release ≥ 2027-04-01 and only after every consumer has migrated dashboards, alerts, recording rules, SLOs and HPA to the platform_* successor (standard §Backward Compatibility steps 2–8); final date set by observability governance. |
| `sqs_receive_errors_total` | `platform_dependency_request_seconds` | Not before the first release ≥ 2027-04-01 and only after every consumer has migrated dashboards, alerts, recording rules, SLOs and HPA to the platform_* successor (standard §Backward Compatibility steps 2–8); final date set by observability governance. |
| `sqs_delete_errors_total` | `platform_dependency_request_seconds` | Not before the first release ≥ 2027-04-01 and only after every consumer has migrated dashboards, alerts, recording rules, SLOs and HPA to the platform_* successor (standard §Backward Compatibility steps 2–8); final date set by observability governance. |
| `sqs_visibility_extension_errors_total` | `platform_dependency_request_seconds` | Not before the first release ≥ 2027-04-01 and only after every consumer has migrated dashboards, alerts, recording rules, SLOs and HPA to the platform_* successor (standard §Backward Compatibility steps 2–8); final date set by observability governance. |
| `outbox_poll_errors_total` | `platform_outbox_errors_total` | Not before the first release ≥ 2027-04-01 and only after every consumer has migrated dashboards, alerts, recording rules, SLOs and HPA to the platform_* successor (standard §Backward Compatibility steps 2–8); final date set by observability governance. |
| `outbox_unmarshal_errors_total` | `platform_outbox_errors_total` | Not before the first release ≥ 2027-04-01 and only after every consumer has migrated dashboards, alerts, recording rules, SLOs and HPA to the platform_* successor (standard §Backward Compatibility steps 2–8); final date set by observability governance. |
| `outbox_mark_published_errors_total` | `platform_outbox_errors_total` | Not before the first release ≥ 2027-04-01 and only after every consumer has migrated dashboards, alerts, recording rules, SLOs and HPA to the platform_* successor (standard §Backward Compatibility steps 2–8); final date set by observability governance. |
| `events_inbox_duplicates_total` | `platform_duplicate_messages_total` | Not before the first release ≥ 2027-04-01 and only after every consumer has migrated dashboards, alerts, recording rules, SLOs and HPA to the platform_* successor (standard §Backward Compatibility steps 2–8); final date set by observability governance. |
| `events_dlq_forwarded_total` | `platform_dlq_messages_total` | Not before the first release ≥ 2027-04-01 and only after every consumer has migrated dashboards, alerts, recording rules, SLOs and HPA to the platform_* successor (standard §Backward Compatibility steps 2–8); final date set by observability governance. |
| `events_oversized_event_type_label_total` | `platform_telemetry_label_overflow_total` | Not before the first release ≥ 2027-04-01 and only after every consumer has migrated dashboards, alerts, recording rules, SLOs and HPA to the platform_* successor (standard §Backward Compatibility steps 2–8); final date set by observability governance. |
