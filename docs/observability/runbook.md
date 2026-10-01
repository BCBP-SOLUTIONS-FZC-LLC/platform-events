# platform-events — alert runbook

One section per alert in [`monitoring/prometheus/platform-events.rules.yml`](../../monitoring/prometheus/platform-events.rules.yml). CI checks that every alert's `runbook_url` points at a heading here. Metric meanings: [metrics-registry.md](metrics-registry.md).

Alerts labelled `metric_status: proposed` (the producer, outbox and SQS-client alerts from [PlatformEventsPublishErrors](#platformeventspublisherrors) on) query Tier 1 metrics that are still **Proposed** and await governance ratification. They are the only signal for those paths — platform-events emits no legacy metrics — so treat them as real, but route them according to your on-call policy for unratified metrics until the registry marks them Canonical.

## PlatformEventsConsumerErrorBudgetBurn

**Meaning:** more than 1.44% of messages received on a queue failed over both the last hour and the last 5 minutes, on a queue receiving at least 1 message per minute. At that rate the 99.9% monthly consumption SLO is exhausted in about two days. Quieter queues are excluded because a single failure is already a large ratio there; watch them with [PlatformEventsConsumerStalled](#platformeventsconsumerstalled) and [PlatformEventsMessagesDeadLettered](#platformeventsmessagesdeadlettered).

**Triage:**
1. Break failures down by reason: `sum by (reason) (rate(platform_messages_failed_total{service="…",queue="…"}[5m]))`.
2. `handler_error` / `handler_panic`: check the service's logs for `sqs: handler returned error` / `handler panic recovered`, and recent deploys. A downstream dependency outage usually shows here first.
3. `decode_error`: the schema registry is unreachable or a producer changed schema; check `platform_dependency_request_seconds_count{dependency="codec",outcome="error"}`.
4. `malformed`: see [PlatformEventsMalformedMessages](#platformeventsmalformedmessages).
5. `dead_letter_error`: DLQ forwarding or the dead-letter handler is failing; check `sqs: DLQ forward failed` logs and IAM (`sqs:SendMessage` on the DLQ).

**Mitigation:** roll back a bad deploy; if a dependency is down, messages retry automatically and are moved to the DLQ after `maxReceiveCount` — no message is lost.

## PlatformEventsConsumerStalled

**Meaning:** messages keep arriving on a queue but none has been processed successfully for 15 minutes.

**Triage:** every delivery is failing or hanging. Check `platform_messages_failed_total` by reason, handler latency (`platform_message_processing_duration_seconds`), and whether handlers are blocked (DB pool saturation: `pgcommon_pool_*`). A consumer whose visibility timeout is shorter than its handler time redelivers the same messages forever — compare with `SQS_VISIBILITY_TIMEOUT`. `platform_messages_in_flight` stuck at the consumer's concurrency means every worker is busy, typically a hung handler — set `WithHandlerTimeout` (`SQS_HANDLER_TIMEOUT`) so such messages are released for redelivery.

**Mitigation:** fix the failing dependency or roll back; scale out only if handlers are healthy but slow.

## PlatformEventsMalformedMessages

**Meaning:** a queue received bodies that are not valid event envelopes.

**Triage:** the most common cause is an SNS→SQS subscription without `RawMessageDelivery=true` — the body is then an SNS notification wrapper. Otherwise a producer is publishing something other than a platform-events envelope to the topic. Valid JSON that lacks an envelope's `id` / `type` / `source` (the SNS wrapper) counts as malformed too. With `WithDLQForwarding` the original bodies are in the queue's DLQ (`DLQReason` starts with `malformed message body:`); without it they were deleted after being logged (`sqs: message body is not a valid event envelope`, with `body_bytes` and `body_sha256` — the body itself only with `WithMalformedBodyLogging`, since it may carry tenant data).

**Mitigation:** fix the subscription or producer; replay from the DLQ with the SQS console / `StartMessageMoveTask` once fixed.

## PlatformEventsMessagesDeadLettered

**Meaning:** messages were moved to dead-letter storage in the last 15 minutes for a reason other than `explicit` (a handler dead-lettering on purpose is expected and does not alert; graph `platform_dlq_messages_total{reason="explicit"}` if its volume matters). `operation="consume"`: forwarded to the queue's SQS DLQ. `operation="outbox_publish"`: moved to `outbox_dead_letters` after `OUTBOX_MAX_ATTEMPTS` failed publishes.

**Triage by reason:** `max_receive_count` — the handler kept failing (see the consumer error budget). `decode_error` / `malformed` — see above. `max_attempts` — SNS publishing failed repeatedly; check `platform_outbox_publish_attempts_total{outcome="error"}`, `platform_dependency_request_seconds_count{dependency="sns",outcome="error"}` and IAM (`sns:Publish`).

**Mitigation:** SQS DLQ — replay with `StartMessageMoveTask` after the fix. Outbox — inspect with `Runner.ListDeadLetters`, then `ReprocessDeadLettersWith` (or `DiscardDeadLetters` for records that must never be sent).

## PlatformEventsPublishErrors

**Meaning:** over 5% of publishes to a topic (`platform_messages_published_total{outcome="error"}`) have failed for 10 minutes. `topic` is the topic name, not its ARN.

**Triage:** SNS throttling or outage, a missing `sns:Publish` permission, a KMS key problem on the topic, or payloads over the SNS limit. Check the service's `sns: publish failed` logs. Events written through the outbox are not lost — they retry and eventually dead-letter.

## PlatformEventsOutboxBacklog

**Meaning:** more than 1000 outbox events have been waiting to be published for 15 minutes.

**Triage:** is the runner running and publishing (`sum by (outcome) (rate(platform_outbox_publish_attempts_total{service="…"}[5m]))`)? Failing publishes (see [PlatformEventsPublishErrors](#platformeventspublisherrors))? A write burst larger than `OUTBOX_BATCH_SIZE` × poll rate can sustain? Compare the successful attempt rate against the enqueue rate. `platform_outbox_oldest_pending_age` shows how far behind delivery is.

**Mitigation:** raise `OUTBOX_BATCH_SIZE` / `OUTBOX_PUBLISH_CONCURRENCY` or run more replicas (runners use `SKIP LOCKED` and scale horizontally). The reference KEDA trigger ([`keda-scaledobject.example.yaml`](../../monitoring/kubernetes/keda-scaledobject.example.yaml)) scales on `platform_outbox_pending_events`, which is still Proposed.

## PlatformEventsOutboxPendingUnknown

**Meaning:** the pending-count query has been failing for 10 minutes (`platform_outbox_errors_total{operation="pending_count"}` increasing). `platform_outbox_pending_events` stays at its last good value, so the backlog alert and the KEDA trigger are blind.

**Triage:** database connectivity or permissions; check `outbox: failed to query pending count` logs and the pgcommon pool metrics.

## PlatformEventsOutboxPollFailing

**Meaning:** outbox poll cycles have been failing for 10 minutes (`platform_outbox_errors_total{operation="poll"}`) — nothing is being delivered.

**Triage:** check `outbox:` error logs. `relation "outbox_events" does not exist` means `outbox.ApplySchema` was not run. Otherwise database connectivity, permissions or lock timeouts (`PG_LOCK_TIMEOUT`).

## PlatformEventsOutboxDuplicateDeliveryRisk

**Meaning:** events were published to SNS but the runner could not mark them published (`platform_outbox_errors_total{operation="mark_published"}`), so they will be published again.

**Triage:** database write failures after a successful publish. Consumers must deduplicate on the envelope ID (`pkg/inbox`); check `platform_duplicate_messages_total` on the consuming side.

## PlatformEventsSQSReceiveFailing

**Meaning:** `ReceiveMessage` has been failing for 10 minutes (`platform_dependency_request_seconds_count{dependency="sqs",operation="receive_message",outcome="error"}`). The metric has no `queue` label, so the alert is per service; the `sqs: receive message failed` log entries name the queue. In a service whose registry already holds `platform_dependency_request_seconds` with another label set the metric is disabled (`RegistrationWarning` at startup) and this alert cannot fire — rely on the consumer alerts there.

**Triage:** expired or missing credentials (IRSA), a missing `sqs:ReceiveMessage` permission, a deleted queue, a KMS key the role cannot use, or network egress. Check `sqs: receive message failed` logs.

## PlatformEventsEventTypeLabelOverflow

**Meaning:** platform-events replaced `event_type` label values that exceeded their cardinality bound (`platform_telemetry_label_overflow_total{label="event_type"}`): types longer than 128 bytes are recorded as `__oversized__`, and new distinct types beyond the per-process limit (default 200, `events.WithEventTypeLimit`) as `__other__`.

**Triage:** graph which replacement occurs (`platform_messages_processed_total{event_type=~"__oversized__|__other__"}` and the other per-event-type metrics). `__oversized__`: find the producer from the consumer's logs and fix its event type to follow `<domain>.<entity>.<past-tense-verb>[.v<N>]` (EVENT_SCHEMA_GOVERNANCE.md). `__other__`: either a producer sends unbounded event types, or the service legitimately handles more types than the limit — raise it with `events.WithEventTypeLimit` and pre-register the known types with `events.WithEventTypes`.
