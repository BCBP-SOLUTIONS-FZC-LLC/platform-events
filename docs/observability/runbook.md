# platform-events — alert runbook

One section per alert in [`monitoring/prometheus/platform-events.rules.yml`](../../monitoring/prometheus/platform-events.rules.yml). CI checks that every alert's `runbook_url` points at a heading here. Metric meanings: [metrics-registry.md](metrics-registry.md).

## PlatformEventsConsumerErrorBudgetBurn

**Meaning:** more than 1.44% of messages received on a queue failed over both the last hour and the last 5 minutes. At that rate the 99.9% monthly consumption SLO is exhausted in about two days.

**Triage:**
1. Break failures down by reason: `sum by (reason) (rate(platform_messages_failed_total{service="…",queue="…"}[5m]))`.
2. `handler_error` / `handler_panic`: check the service's logs for `sqs: handler returned error` / `handler panic recovered`, and recent deploys. A downstream dependency outage usually shows here first.
3. `decode_error`: the schema registry is unreachable or a producer changed schema; check `platform_dependency_request_seconds_count{dependency="codec",outcome="error"}`.
4. `malformed`: see [PlatformEventsMalformedMessages](#platformeventsmalformedmessages).
5. `dead_letter_error`: DLQ forwarding or the dead-letter handler is failing; check `sqs: DLQ forward failed` logs and IAM (`sqs:SendMessage` on the DLQ).

**Mitigation:** roll back a bad deploy; if a dependency is down, messages retry automatically and are moved to the DLQ after `maxReceiveCount` — no message is lost.

## PlatformEventsConsumerStalled

**Meaning:** messages keep arriving on a queue but none has been processed successfully for 15 minutes.

**Triage:** every delivery is failing or hanging. Check `platform_messages_failed_total` by reason, handler latency (`platform_message_processing_duration_seconds`), and whether handlers are blocked (DB pool saturation: `pgcommon_pool_*`). A consumer whose visibility timeout is shorter than its handler time redelivers the same messages forever — compare with `SQS_VISIBILITY_TIMEOUT`.

**Mitigation:** fix the failing dependency or roll back; scale out only if handlers are healthy but slow.

## PlatformEventsMalformedMessages

**Meaning:** a queue received bodies that are not valid event envelopes.

**Triage:** the most common cause is an SNS→SQS subscription without `RawMessageDelivery=true` — the body is then an SNS notification wrapper. Otherwise a producer is publishing something other than a platform-events envelope to the topic. With `WithDLQForwarding` the original bodies are in the queue's DLQ (`DLQReason` starts with `malformed message body:`); without it they were deleted after being logged (`sqs: failed to unmarshal message body`, first 512 bytes).

**Mitigation:** fix the subscription or producer; replay from the DLQ with the SQS console / `StartMessageMoveTask` once fixed.

## PlatformEventsMessagesDeadLettered

**Meaning:** messages were moved to dead-letter storage in the last 15 minutes. `operation="consume"`: forwarded to the queue's SQS DLQ. `operation="outbox_publish"`: moved to `outbox_dead_letters` after `OUTBOX_MAX_ATTEMPTS` failed publishes.

**Triage by reason:** `max_receive_count` — the handler kept failing (see the consumer error budget). `decode_error` / `malformed` — see above. `explicit` — the service dead-lettered on purpose. `max_attempts` — SNS publishing failed repeatedly; check `events_published_total{status="error"}` and IAM (`sns:Publish`).

**Mitigation:** SQS DLQ — replay with `StartMessageMoveTask` after the fix. Outbox — inspect with `Runner.ListDeadLetters`, then `ReprocessDeadLettersWith` (or `DiscardDeadLetters` for records that must never be sent).

## PlatformEventsPublishErrors

**Meaning:** over 5% of publishes to a topic have failed for 10 minutes.

**Triage:** SNS throttling or outage, a missing `sns:Publish` permission, a KMS key problem on the topic, or payloads over the SNS limit. Check the service's `sns: publish failed` logs. Events written through the outbox are not lost — they retry and eventually dead-letter.

## PlatformEventsOutboxBacklog

**Meaning:** more than 1000 outbox events have been waiting to be published for 15 minutes.

**Triage:** is the runner running and publishing (`outbox_published_total` rate)? Failing publishes (see [PlatformEventsPublishErrors](#platformeventspublisherrors))? A write burst larger than `OUTBOX_BATCH_SIZE` × poll rate can sustain? Run `outbox_published_total` rate against the enqueue rate.

**Mitigation:** raise `OUTBOX_BATCH_SIZE` / `OUTBOX_PUBLISH_CONCURRENCY` or run more replicas (runners use `SKIP LOCKED` and scale horizontally). An HPA on this backlog must use the legacy `outbox_pending_total` until `platform_outbox_pending_events` is ratified.

## PlatformEventsOutboxPendingUnknown

**Meaning:** the pending-count query has failed for 10 minutes (the legacy gauge reads -1), so the backlog alert cannot fire.

**Triage:** database connectivity or permissions; check `outbox: failed to query pending count` logs and the pgcommon pool metrics.

## PlatformEventsOutboxPollFailing

**Meaning:** every outbox poll cycle has failed for 10 minutes — nothing is being delivered.

**Triage:** check `outbox:` error logs. `relation "outbox_events" does not exist` means `outbox.ApplySchema` was not run. Otherwise database connectivity, permissions or lock timeouts (`PG_LOCK_TIMEOUT`).

## PlatformEventsOutboxDuplicateDeliveryRisk

**Meaning:** events were published to SNS but the runner could not mark them published, so they will be published again.

**Triage:** database write failures after a successful publish. Consumers must deduplicate on the envelope ID (`pkg/inbox`); check `platform_duplicate_messages_total` on the consuming side.

## PlatformEventsSQSReceiveFailing

**Meaning:** `ReceiveMessage` has been failing for 10 minutes.

**Triage:** expired or missing credentials (IRSA), a missing `sqs:ReceiveMessage` permission, a deleted queue, a KMS key the role cannot use, or network egress. Check `sqs: receive message failed` logs.

## PlatformEventsOversizedEventType

**Meaning:** a producer sent event types longer than 128 bytes; they are recorded as `__oversized__` to protect metric cardinality.

**Triage:** find the producer from the consumer's logs and fix its event type to follow `<domain>.<entity>.<past-tense-verb>[.v<N>]` (EVENT_SCHEMA_GOVERNANCE.md).
