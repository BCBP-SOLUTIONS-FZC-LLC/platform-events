# Changelog

All notable changes to `platform-events` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Upgrade notes

- **New outbox migration `011`** replaces the dead-letter index `idx_outbox_dead_letters_failed_at (failed_at DESC)` with `idx_outbox_dead_letters_failed_at_id (failed_at, id)` — the order list, replay and discard use. Rolling back past it needs `migrate down` first (see 1.6.0's `ErrVersionNotInSource` note).
- **FIFO source queues are now processed per message group, in order.** A `.fifo` queue's batch is split by `MessageGroupId`; each group runs on one worker in receive order, and a message that is not settled (handler error, failed decode or DLQ forward, panic) hands the group's later messages of that batch back unprocessed. FIFO consumers lose cross-message parallelism within a group (at most one message per group in flight per replica; groups still run in parallel up to `WithConcurrency`) — set `WithVisibilityTimeout` so queued group messages are extended while they wait. Without a visibility timeout a FIFO consumer receives one message per call. A failed `DeleteMessage` also stops the group.
- **`outbox.Enqueue` rejects a NUL character** (`\u0000`) anywhere in the envelope, and in `TenantID` / `TraceID`: Postgres `jsonb` cannot store it, so the INSERT failed — and rolled back the business transaction — on every retry. Strip NULs from user input before enqueueing.
- **`AWS_REGION` falls back to `AWS_DEFAULT_REGION`** (then `us-east-1`) in `config.LoadSNS` / `LoadSQS`.
- **`mock.Consumer.Inject` behaves like the SQS consumer:** it returns `mock.ErrMalformedEnvelope` without calling the handler for an envelope missing id / type / source, round-trips the envelope through JSON, and decodes codec-encoded payloads with the new `Codec` field (an error when none is set). Service tests that injected such envelopes need updating.
- **A `dataschema` on a plain-JSON `data` is passed through undecoded.** Only a JSON-string `data` (the codec wire format) is decoded. Consumers on 1.6.0 or older still dead-letter such messages, so do not use `WithSchemaID` as an informational tag.

### Fixed

- **SQS consumer:**
  - FIFO queues lost per-group order: messages of one group ran in parallel, and after a failure the group's later messages were still processed and deleted.
  - A message dispatched right at its waiting deadline (`WithHandlerTimeout`) could stop being extended before processing and be delivered twice.
  - `Stop` could return before the queue-depth sampler goroutine had exited.
  - A visibility-extension call could race `DeleteMessage`, logging a spurious "possible duplicate delivery" warning and counting a dependency error.
  - A `dataschema` on a JSON-object `data` failed decode on every delivery and dead-lettered the message.
  - (Found in review of the FIFO change, never released) with `WithHandlerTimeout`, a FIFO group's later messages hit their waiting deadline while the earlier ones ran and were handed back again and again until they dead-lettered unprocessed; they now get the running message's budget and are re-armed as each one is dispatched. Without a visibility timeout a group's later messages waited unextended and could be taken by another consumer out of order; a failed delete let the group continue and reorder.
- **SNS publisher:** OTel `baggage` / `tracestate` counted toward SNS's 10-attribute limit, so whether a publish succeeded depended on the caller's request context (outbox records dead-lettered); they are now dropped first (logged once) — `traceparent` and the routing attributes are kept.
- **DLQ publisher:** a FIFO DLQ deduplicated a second forward of the same event within 5 minutes (two queues sharing one DLQ, a redriven message failing again) while reporting success, so the source message was deleted and the dead-letter lost. `MessageDeduplicationId` is now unique per forward.
- **Outbox:**
  - Rolling migration `010` back stranded ordered records waiting at `scheduled_at = 'infinity'` (never claimed by the older code); `010` down now makes them due first.
  - Migration `003` dropped and rebuilt `idx_outbox_events_pending` under an `ACCESS EXCLUSIVE` lock on every run — including the one-time re-run 1.6.0 causes for services that passed their own `x-migrations-table`. It now rebuilds only when the index has a different shape.
  - A shutdown during `PublishBatch` released the unsent records with the shared transient backoff (up to `MaxRetryBackoff`), hiding them from other replicas and advancing the backoff; they are now released at once.
  - A dead letter whose ID was re-enqueued into `outbox_events` failed every `ReprocessDeadLetters` call on the primary key (it is always selected first); it is now left in place and logged.
  - `MarkFailed` on a record another runner had already published or dead-lettered counted `platform_retry_total`.
  - Dead-letter replay inserts first and deletes only what was inserted (`ON CONFLICT DO NOTHING`), so an application enqueueing the same ID concurrently no longer fails the replay; the "left in place" count runs after the commit, best-effort.
  - Migration `003` also rebuilds an INVALID `idx_outbox_events_pending` (a failed `CREATE INDEX CONCURRENTLY`).
- **Metrics:** `InitMetrics` left the legacy collectors it had registered on the registerer when a later legacy registration failed, so they were exported at zero ("healthy") rather than absent.
- **`GlueDecodeCodec`** passed a non-JSON (e.g. Avro-format or empty) payload to the handler; it now returns a decode error.
- **Config:** `OutboxConfigEnv.String()` did not mask `sslpassword`, and `%#v` printed the raw DSN (new `GoString`).

### Added

- `SQS_DRAIN_TIMEOUT` (`config.SQSConfigEnv.DrainTimeout`, wired by `SQSConsumerOptions`).

### Changed

- `BatchError.Error()` names the first failure (`… (first: <id> <code>: <message>)`).
- The reference CLI prints the effective `MaxMessages` / `WaitSeconds` and the drain timeout.
- `make toolchain-check` recognises any `FROM … golang:<version>` form.

- Each visibility-extension `ChangeMessageVisibility` call is bounded by `min(max(VisibilityTimeout/2, 1s), 10s)`.
- FIFO receives carry a fresh `ReceiveRequestAttemptId`, so an SDK-retried receive does not block its message groups until the visibility timeout.
- Monitoring: legacy recording rules and alerts aggregate by `namespace` too, and the KEDA example selects it, so one Prometheus scraping several environments does not mix them. Alert descriptions of `PlatformEventsOutboxPollFailing` and `PlatformEventsConsumerStalled` match their windows; `identityOwner` removed from the KEDA SQS trigger.
- CI: pull requests no longer write the registry build cache the signed `main` / release images are built from; the private-module token is deleted right after `go mod download`; the Trivy DB cache key rolls daily; new `make toolchain-check` (in `make ci` and `Validate / Quality`) keeps the Go toolchain identical across the three `go.mod` files and the Dockerfile.

### Docs

- `SystemTenantID` / `WithSystemTenant`: a `"system"` tenant does **not** disable RLS — the consumer sets `app.tenant_id = 'system'`; global-event handlers need an RLS-bypassing pool or role. `ParseEnvelope` documents that it is stricter than the consumer (it requires `time`).
- HMAC: canonicalisation fixes the envelope field order but does not re-sort payload keys (CLAUDE.md said it did); `WithoutLegacyMetrics` warns that it silences the producer alerts until the successors are ratified; close the pool only after `runner.Stop()` returns nil; LLD rev 2.5 (FIFO consumer, migrations 003 / 010 / 011, replay collision, known limitations L-6 / L-7).

### Tests

- FIFO busy group under `WithHandlerTimeout` not released, failed delete stops the group, one message per receive without a visibility timeout; SNS trace-attribute shedding; `Enqueue` NUL rejection; INVALID-index rebuild; region fallback; `SQS_DRAIN_TIMEOUT`; production-faithful `mock.Consumer`; `BatchError` message; envelope unmarshal reuse.
- FIFO group order and failure hold, FIFO receive input, no extension after delete, `dataschema` pass-through; shutdown mid-batch release; migration `010` down, `003` no-rebuild, `011` index, replay collision, `MarkFailed` on a published record; metrics rollback; Glue non-JSON; `sslpassword` / `%#v` masking; promtool tests for `OutboxPollFailing`, `SQSReceiveFailing`, `OversizedEventType`.

## [1.6.0] - 2026-10-01

### Upgrade notes (action required)

- **platform-pgcommon v1.4.3 is inherited** (v1.4.0's notes follow below; v1.4.2 and v1.4.3 are documentation-only). The v1.4.1 upgrade notes that matter here:
  - `outbox.ApplySchema` / `inbox.ApplySchema` fail with `migrate.ErrVersionNotInSource` when the database was migrated by a newer platform-events, and with `migrate.ErrMigrationDirty` after an interrupted migration. Services that apply the schema at startup will refuse to start an older image after a newer one migrated — and this release adds outbox migrations `009` and `010`. Roll back by migrating the outbox schema down first (tracking table `outbox_migrations`), or run `ApplySchema` as a separate migration job.
  - The migration lock wait is the full `lock_timeout` (30s by default) instead of 15s.
  - `NewPool` rejects negative pool durations, and DSN `pool_*` parameters are now honoured.
  - pgcommon query metrics count the caller's statements only (internal `set_config` / BEGIN / COMMIT are excluded unless they fail), so query rates drop and error ratios rise to their true values.
  - Span errors carry only the SQLSTATE unless full statements are allowed.
- **Outbox retries back off, and transient failures no longer count toward `MaxAttempts`.**
  - A permanent publish failure retries after `RetryBackoff·2^(attempt-1)` (default 1s, capped at `MaxRetryBackoff` = 5m, jittered) instead of on the next poll. With the defaults the first retries still land on the next poll, so a poison record reaches `outbox_dead_letters` in about the same ~25s; larger `MaxAttempts` values spread out up to 5m apart.
  - Throttling, SNS-side errors, HTTP 5xx / 429, timeouts and failures without an AWS answer (network, DNS, TLS, credentials) release the lease without counting an attempt, so an SNS outage builds a backlog instead of dead-lettering it. Shutdown no longer counts an attempt either.
  - Tune with `outbox.Config.RetryBackoff` / `MaxRetryBackoff` or `OUTBOX_RETRY_BACKOFF` / `OUTBOX_MAX_RETRY_BACKOFF`.
- **Custom `events.Publisher` implementations:** a `BatchFailure` is transient only when `Retryable` is set (or its Code is `TransportError`). Set it only for failures that say nothing about the message.
- **`outbox.Enqueue` requires the canonical lowercase UUID form for the envelope ID** (what `events.NewEnvelope` produces). Postgres stores the ID canonicalised, and a mismatch with the payload's ID made a failed batch publish look delivered.
- **`outbox.ApplySchema` always tracks its migrations in `outbox_migrations`**, also when the DSN sets `x-migrations-table` (inbox already did this). A service that passed its own tracking table re-runs outbox migrations `001`–`010` into `outbox_migrations` once; every one is idempotent.
- **The outbox does not preserve publish order by default** — not even per aggregate on a FIFO topic (a failed or backed-off record is published after later ones; replicas publish concurrently). The docs previously said a FIFO `MessageGroupID` was enough. Use the new per-key ordering (`outbox.EnqueueOrdered`) or a per-aggregate sequence number.
- **`ReprocessDeadLetters` / `ReprocessDeadLettersWith` reset `created_at` to the replay time**, so the oldest-pending-age gauge does not jump to a dead letter's age. A record that dead-letters again shows the replay time as its `created_at`; the envelope keeps its original `time`.
- **Outbox backlog gauges** (`outbox_pending_total`, `outbox_leased_total` and their Proposed successors) refresh every `GaugeInterval` (15s), not every poll, and are capped at 100 000. `outbox_leased_total` also counts records waiting out a retry backoff (not ordered records waiting for their key — those are `platform_outbox_ordering_blocked_events`).
- **SQS consumer:** a JSON body without `id` / `type` / `source` (e.g. an SNS notification wrapper) is malformed — forwarded to the DLQ or deleted — instead of reaching the handler with an empty envelope. Malformed bodies are no longer logged by default (size and SHA-256 only; opt back in with `WithMalformedBodyLogging`).
- **`events.NewSQSConsumer` / `NewSQSConsumerWithClient` return an error for a nil handler** (previously every message panicked); `WithDeadLetterHandler(nil)` is ignored.
- **The `mock` package behaves like production** (service tests may need updating):
  - `mock.Consumer.Inject` passes the real consumer's handler context (tenant GUC for RLS, trace ID, source message, dead-letter attribution), and returns the error instead of calling the handler when the envelope cannot be serialised.
  - `mock.DLQPublisher.SendToDLQ` counts the forward and marks the dead-letter attribution, so `inbox.Handler` / `Store.Process` skip a dead-lettered message.
  - `mock.Publisher` rejects envelopes without ID, Type or Source, and gains `SetBatchError`.
Upgrading from 1.5.x. Everything else in this release is additive or a fix. This stays a **minor** release on purpose: no exported API is removed or changed incompatibly (`outbox.Enqueue` takes `pgcommon.Tx`, an alias of `pgx.Tx`), and the items below are small, local changes.

- **Switch to `events.InitMetrics`.** `events.Init` / `InitWithRegisterer` are deprecated (staticcheck `SA1019` fails lint until you switch). Call `events.InitMetrics(events.MetricsIdentity{Domain: "<domain>", Service: "<service>", Version: buildVersion}, registerer)` once at startup, with the same registerer and identity you pass to platform-pgcommon's `pgmetrics.InitWithIdentity`. Log the returned `RegistrationWarning`s. In IAM services that already register `platform_retry_total` / `platform_dependency_request_seconds` with another label set, those two are disabled rather than failing startup.
- **Dashboards and alerts.** The new `platform_*` metrics are emitted alongside the legacy `events_*` / `outbox_*` / `sqs_*` ones, which keep working. Migrate panels and alerts using `docs/observability` and `monitoring/` (reference rules, dashboard and KEDA example). Proposed metrics must not back alerts, SLOs or HPA until ratified.
- **`event_type` label cap (also on the legacy metrics).** At most 200 distinct values per process; further ones are recorded as `__other__`. A service legitimately handling more event types must raise the cap with `events.WithEventTypeLimit(n)`.
- **`DLQPublisher.SendToDLQ` behaviour:**
  - Messages with more than 10 attributes have their lowest-priority caller attributes dropped instead of being rejected; set `DLQConfig.StrictAttributes` to keep rejecting.
  - Rejections of the message itself by `SendMessage` (`InvalidParameterValue`, `InvalidMessageContents`, `InvalidAttributeName`, `InvalidAttributeValue`) now return `ErrDLQInvalidMessage` instead of `ErrDLQSendFailed`. Revisit code that branches on `errors.Is(err, events.ErrDLQSendFailed)`.
- **`WithDLQForwarding` (new) fails `Start()` on a queue without a usable `RedrivePolicy`.** This is intended: otherwise poison messages are never deleted. Provision the DLQ before enabling it.
- **platform-pgcommon v1.4.0 and pgx v5.11 are inherited.** Follow pgcommon's 1.4.0 upgrade notes. The ones most likely to matter: custom `pgx.Rows` mocks need `TypeMap()`; connection URIs are parsed like libpq; text-format `timestamptz` values come back in `time.Local`; pgcommon metrics gain a `pool` label and its span attributes are renamed. A service that already pinned the original v1.4.0 tag must clear its module cache, including `$(go env GOMODCACHE)/cache/vcs`, and refresh `go.sum`: the tag was re-released.
- **`go` directive `1.26.0` + `toolchain go1.26.8`** (was `go 1.26.6`). Consumers are no longer pinned to a patch release.
- **`config.LoadOTel` / `OTelConfigEnv` are deprecated.** Tracing is configured by the service through platform-gincommon's `InitTracingFromEnv`. The `OTEL_*` variables were never read by the library.

### Added

- **Per-key outbox ordering (opt-in per record).** `outbox.EnqueueOrdered(ctx, tx, env, orderingKey)`: records with the same key are published one at a time, in enqueue (INSERT) order, across replicas — callers take the aggregate's row lock before enqueueing so insert order is commit order. A record behind an unpublished one of its key waits (`scheduled_at = 'infinity'`) and is promoted once the key's head is published (best-effort, with a sweep as fallback) or dead-lettered, so claims never scan a key's backlog. Migration `010` adds `ordering_key` (outbox and dead-letter tables, kept through dead-lettering and replay), `ordering_seq` (a sequence) and their partial index — metadata-only on existing rows.
- **Outbox runner re-polls while batches publish** (bounded by `PollInterval`), so a backlog drains at publish speed instead of one batch per tick; it stops on a batch that published nothing or hit a transient failure, and on `Stop`.
- **`inbox.Store.Process(ctx, env, fn)`** claims the event ID inside the handler's transaction, so Postgres writes happen exactly once, including for concurrent copies.
- `events.WithHandlerTimeout` (`SQS_HANDLER_TIMEOUT`): one deadline per message, from when a worker picks it up, for codec decode, dead-letter handler, handler and the visibility extension — a hung handler no longer keeps its message invisible and out of redrive forever.
- `events.WithMalformedBodyLogging`, `events.WithEventTypes`, `outbox.Config.GaugeInterval` (`OUTBOX_GAUGE_INTERVAL`), `BatchFailure.Retryable`.
- Proposed metrics:
  - `platform_outbox_oldest_pending_age` — age in seconds of the oldest unpublished outbox event; catches a stalled outbox whose backlog is too small for `PlatformEventsOutboxBacklog`. Its alert ships commented out until ratification. Migration `009` adds the partial index it reads (`CREATE INDEX` without `CONCURRENTLY` — on a large outbox, create it concurrently by hand first).
  - `platform_outbox_ordering_blocked_events` — ordered records waiting behind their key's head.
  - `platform_message_timeouts_total{queue,event_type,operation}` — `WithHandlerTimeout` expiries by stage (`decode`, `dead_letter_handler`, `handler`). They are still counted in `platform_messages_failed_total` under their usual reason; this separates them without changing that Canonical metric's vocabulary.
  - `platform_messages_in_flight{queue}` — messages a consumer replica is processing.
- **Enterprise Platform Observability Standard metrics.** All platform-events metrics are now Tier 1 `platform_*`, the same model as platform-pgcommon's `platform_db_*`. See [docs/observability](docs/observability/README.md).
  - `events.InitMetrics(MetricsIdentity{Domain, Service, Environment, Version}, registerer, ...MetricsOption)` injects the required `domain` / `service` / `environment` labels centrally as const labels. An empty Environment falls back to `APP_ENV` → `ENVIRONMENT` → `dev`.
  - The legacy metrics are registered in parallel for the compatibility period unless `WithoutLegacyMetrics()` is given.
  - Fail-soft: a `platform_*` metric the registry already holds with another shape is disabled and returned as a `RegistrationWarning`. IAM services register `platform_retry_total` / `platform_dependency_request_seconds` with conflicting label sets. Registerer wrappers that already inject identity labels are handled, with each label applied once and the wrapper's value winning.
  - Helpers: `MetricsIdentityFromEnv`, `MetricsIdentityFromLabels` (platform-gincommon interop), `MetricsEnvironmentFromEnv`, `MetricsRegistry()`.
  - **Canonical:** `platform_messages_received_total{queue}`, `platform_messages_processed_total{queue,event_type}`, `platform_messages_failed_total{queue,event_type,reason}`, `platform_retry_total{operation,event_type}`, `platform_dlq_messages_total{operation,event_type,reason}`. Each delivery is received once and ends processed, failed (and retried) or dead-lettered. A dead-lettered message is counted once, whether it was forwarded by `WithDLQForwarding`, the dead-letter handler, or a handler calling `SendToDLQ`.
  - **Proposed** (shadow-emitted; ratification packets in `docs/observability/metrics-registry.md`): `platform_duplicate_messages_total`, `platform_dependency_request_seconds{dependency,operation,outcome}` (SNS, SQS, codec), `platform_event_propagation_seconds`, `platform_messages_published_total`, `platform_message_processing_duration_seconds`, `platform_outbox_pending_events`, `platform_outbox_leased_events`, `platform_outbox_publish_attempts_total`, `platform_outbox_errors_total`, `platform_outbox_dead_letter_operations_total`, `platform_telemetry_label_overflow_total`, `platform_library_info`.
  - `event_type` values are bounded in-process: at most 200 distinct values per process (`WithEventTypeLimit`), with further ones recorded as `__other__` and over-128-byte ones as `__oversized__`. Every replacement is counted in `platform_telemetry_label_overflow_total`. This also bounds the legacy metrics, which previously capped only length.
  - `platform_event_propagation_seconds` is measured to the first receipt only, so redeliveries don't inflate it. The `platform_dependency_request_seconds` definition notes that SQS `receive_message` includes long-poll wait and should be excluded from latency views.
  - A leftover `events.Init` after `InitMetrics` is a no-op instead of silently disabling the Tier 1 metrics.
  - Label rules: `queue` / `topic` values are names, never URLs or ARNs. High-cardinality labels (`tenant_id`, `event_id`, …) are prohibited. Label-static counters are pre-created at 0 so the first error after a restart is visible to `increase()`.
- **Queue depth metrics** (Proposed): `platform_queue_depth{queue}` / `platform_dlq_depth{queue}`, sampled by consumers started with `events.WithQueueDepthMetrics(interval)` (minimum 10s), or `SQS_QUEUE_DEPTH_INTERVAL` via `config.SQSConsumerOptions`. Services may not use the SQS SDK, so this is the only way they can get SQS depth into Prometheus.
  - Each replica calls `sqs:GetQueueAttributes` on the queue and on its RedrivePolicy DLQ. The DLQ URL is derived from the policy ARN, so no `GetQueueUrl` call is needed. Calls are timed in `platform_dependency_request_seconds`.
  - The client capability is detected by type assertion, so `SQSClientAPI` / `SQSClientLike` are unchanged and existing test doubles keep compiling.
- **Reference dashboard and autoscaling**: `monitoring/grafana/platform-events.json` has 16 panels: Canonical consumer panels, the SLO, Proposed shadow panels and legacy panels where still authoritative. `monitoring/kubernetes/keda-scaledobject.example.yaml` scales on the outbox backlog (legacy metric) and the queue backlog (KEDA's SQS scaler). `make metrics-lint` also enforces both: registered metrics and labels only, Proposed and legacy panels labelled as such, and no autoscaling on Proposed metrics. SLO definitions are documented in `docs/observability/README.md`.
- **Metrics registry**: `internal/adapter/outbound/metrics/registry.go` is platform-events' entry in the Platform Observability Registry. It records tier, status, semantic definition, approved labels and values, cardinality, aggregation expectations, successor and sunset. `docs/observability/metrics-registry.md` is generated from it (`make metrics-doc`).
- **CI enforcement**: `make metrics-lint` runs in `Validate / Quality` and `make ci`.
  - It registers the real collectors and checks namespace classification, naming (`_total` / `_seconds`), required labels, label vocabulary, prohibited labels and registry ↔ instrumentation parity.
  - It also checks the rule files (registered metrics only, no Proposed metric outside comments, labels in vocabulary, runbook anchors exist) and inventory drift.
  - `make rules-check` runs `promtool check rules` and the alert unit tests, with the promtool image digest-pinned (refreshed by `make pin-base-images`, recorded in `.docker-digests`).
- **Reference monitoring bundle**: `monitoring/prometheus/platform-events.rules.yml` contains recording rules, a consumer SLO (99.9%, multi-window burn rate, with a minimum-traffic guard of 1 msg/min so quiet queues don't page on a single failure) and 11 operational alerts. The dead-letter alert ignores `reason="explicit"`. Each alert has a `docs/observability/runbook.md` section, and all of them are covered by promtool tests.

- **`events.WithDLQForwarding(dlq)`** consumer option — forwards poison messages to the source queue's `RedrivePolicy` DLQ with their **original raw body and attributes**, deleting the source message only after the forward succeeds (on failure it stays visible; SQS redrive remains the backstop). Forwarded: bodies that are not a valid envelope (`malformed message body: …` — previously deleted and only logged, i.e. lost), messages past the `WithMaxReceiveCount` threshold (after `WithDeadLetterHandler`, when set, succeeds), and messages past it whose codec decode fails. `WithMaxReceiveCount` defaults to 5 when only `WithDLQForwarding` is set. With forwarding enabled, `Start` resolves the DLQ first and returns an error wrapping `ErrDLQNotConfigured` / `ErrDLQInvalidRedrivePolicy` when the queue has no usable `RedrivePolicy` (transient failures are logged and the consumer starts), so poison messages can never be left redelivered until retention expires. Each forward is bounded by 30 s, capped at half of `WithVisibilityTimeout` (min 1 s), so a slow forward cannot let the message reappear and be forwarded twice.
- **`events.SourceMessageFromContext(ctx)`** / `events.SourceMessage` — built lazily on first call, so handlers that never use it pay no copy — the raw SQS message (body before codec decoding, String/Number attributes, queue URL, message ID, receive count) on every `Handler` and `WithDeadLetterHandler` context. Forward this rather than `env.JSON()`, which drops attributes and, with `WithConsumerCodec`, re-serialises a decoded payload under a still-set `SchemaID` that fails to decode on redrive.
- `DLQPublisher`: `sqs.dlq_forward` OTel span (`SpanKindProducer`); its SQS calls are timed in `platform_dependency_request_seconds`.
- `DLQConfig.CacheTTL` (default 15 min; negative disables expiry) — a retargeted `RedrivePolicy` is picked up without a restart. The cache entry is also evicted when `SendMessage` reports the DLQ no longer exists (`ErrDLQUnresolved`).
- `DLQConfig.StrictAttributes` — opt back into rejecting messages with more than 10 attributes.

### Changed

- `github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon` v1.4.0 → **v1.4.2** (root and `test/` modules); pgx stays v5.11.0. No code change was needed.
- AWS SDK for Go v2 upgraded: core v1.47.1, `service/sns` v1.47.2, `service/sqs` v1.52.1, `smithy-go` v1.28.2.
- Release workflow parity with platform-pgcommon v1.4.1:
  - `verify-release-tag.sh` refuses a `workflow_dispatch` from a ref other than `main` or the tag, and a tag not reachable from `origin/main`.
  - `release-image-tags.sh`: the floating image tags `X.Y` / `X` / `latest` only move forward, so a hotfix of an older line no longer re-points them, and pre-releases never move them.
  - `changelog-section.sh`: a release candidate may use its base version's CHANGELOG section.
- Docs parity with platform-pgcommon v1.4.2: `make docs-check` (`.github/scripts/docs-mermaid-sync.sh`) fails when a `docs/architecture/mermaid/*.mmd` is not embedded byte-identically in `ARCHITECTURE.md`. It runs in `make ci`, the pre-commit hook (re-run `make install-hooks`), `Validate / Quality`, and a new `docs.yml` workflow for the docs-only changes `ci.yml` skips. It caught one drifted diagram (layer model), now refreshed.
- **CI parity with platform-pgcommon:** `actions/setup-go` v7.0.0, `actions/checkout` v7.0.1, govulncheck v1.8.0, and a coverage gate of **97%** (was 95%; the merged total was 97.7% at release, 99.1% after the Unreleased test additions). The disabled Dependabot config now lists all three Go modules, so it works as soon as it is re-enabled.
- **pgcommon-only now covers tests too.** The last two pgx imports, both in unit-test `pgx.Tx` fakes, are gone: `noopTx` embeds `pgcommon.Tx`, and `stubTx` infers `Exec`'s result type from `pgcommon.Tx` itself (`newStubTx(pgcommon.Tx.Exec)`). The depguard `pgcommon-only` rule applies to every file and also rejects `golang-migrate`. No file in the repository imports pgx, `database/sql` or golang-migrate; pgx is only an indirect dependency through platform-pgcommon.
- **Shared Postgres container per test package**, with a fresh database per `fixtures.NewTestDB` (both schemas applied, dropped `WITH (FORCE)` on cleanup) instead of a container per test. The integration suite drops from about 116s to about 15s.
- **Shared floci resources are cleaned up per test:** `CreateTopic` / `CreateQueue` delete what they created when the test ends. Before this, a repeat run (`-count=2`) reused the previous run's queues and read their leftover messages. Found by running the suites twice in one process, which now pass, integration under `-race`.
- `make ci` runs the same gates as CI (`mod-verify`, `rules-check`, `dashboards-check` added). `.dockerignore` excludes `tools/` and `monitoring/`, so neither invalidates the image build cache. `InitMetrics` documents that it is called once per process. Two README / `test/README.md` commands updated for the `test/` module.
- **Module layout: three modules, as in platform-pgcommon.** The test suites and fixtures moved into a `test/` module (`replace … => ../`) and golangci-lint into a `tools/` module. The library's `go.mod` drops from 289 to 55 lines, so consuming services no longer inherit testcontainers, the linter's dependency tree and other test-only modules. The `make` targets and CI (`tidy`, `vet`, `lint`, `mod-verify`, `test-*`, the drift check, cache paths) cover all three. Run single tests with `cd test && go test ./unit/...`.
- **`go` directive `1.26.0` with `toolchain go1.26.8`** (was `go 1.26.6`), matching platform-pgcommon: consumers are no longer forced onto one exact patch release. The Dockerfile builder moves to `golang:1.26.8-alpine`, the same digest pgcommon uses.
- **Queue-depth sampler:** it no longer polls SQS when the platform metrics aren't initialised (one warning instead). A persistent `GetQueueAttributes` failure, such as a missing IAM permission, is logged once and its recovery once, instead of on every poll; every failure is still counted in `platform_dependency_request_seconds`.
- **All images digest-pinned:** docker-compose (floci, floci-ui, postgres) and the testcontainers fixtures (floci, postgres), in addition to the Dockerfile and promtool. `make pin-base-images` refreshes all of them, and CI rejects an unpinned one.
- **One shared floci container per test package** (`fixtures.StartFloci`, torn down by each package's `TestMain`), as in iam-org-membership, instead of one per test. Reusing a resource name within a package now fails the test.
- **CI:** new `make dashboards-check`, the PromQL syntax gate for the reference dashboard (platform-pgcommon's script), in `Validate / Quality`.
- **floci replaces LocalStack** as the AWS emulator, matching iam-org-membership. [floci](https://floci.io) is open-source (MIT) and always free: SNS, SQS and Glue Schema Registry with no Pro tier or auth token.
  - The integration and e2e suites start `floci/floci:2.1.0` through the new `test/fixtures/floci.go`: `StartFloci`, `FlociRegion`, `FlociAccount`, and the `CreateTopic` / `CreateQueue` / `SubscribeQueueToTopic` helpers, with static `test` credentials. The fixture replaces `localstack.go`. Every suite passes unchanged.
  - `make docker-up` starts `floci/floci:2.1.0-compat`, the `floci/floci-ui:0.5.0` web console and Postgres. `scripts/init-floci.sh` provisions a demo topology (`platform-events-demo` topic, `platform-events-demo-q` queue with raw delivery, and `-dlq` via `RedrivePolicy`), and `.env-example` now points at it.
  - Host ports are unique across the org's local stacks (floci `4574`, floci-ui `4505`, Postgres `5538`; overridable with `FLOCI_PORT` / `FLOCI_UI_PORT` / `POSTGRES_PORT`), so the stack runs alongside pgcommon's and every IAM service's.
  - Docs updated: README (local walkthrough with floci-ui, Docker table), `test/README.md`, CONTRIBUTING, ARCHITECTURE and diagrams, guides, CLAUDE.md, and code comments.
- `.env-example`: `DATABASE_URL` now matches the compose Postgres (database `platform_events_dev`, `sslmode=disable` locally). It previously named `platform_dev` with `sslmode=require` and could not connect.
- **Tracing and logging configuration belong to the consuming service.** platform-events reads no `OTEL_*` or log variables, like platform-pgcommon. It uses the global tracer provider installed by platform-gincommon's `InitTracingFromEnv`, and logs through the injected `port.Logger`.
  - `config.LoadOTel` / `config.OTelConfigEnv` are **deprecated**. Nothing in the library used them, and their parsing had diverged from gincommon's: no `APP_NAME` fallback for `OTEL_SERVICE_NAME`, `yes`/`no` rejected, `APP_ENV` case-folded, sampler and baggage variables ignored. They will be removed in the next major version.
  - The reference CLI no longer prints an OTel config.
- **Metrics: legacy metrics are Deprecated.** `events_*`, `outbox_*`, `sqs_*` and `platform_events_build_info` are unchanged and still emitted, but are marked Deprecated in their help text and the registry, each with a Tier 1 successor. Sunset is not before 2027-04-01. `events.Init` / `events.InitWithRegisterer` are deprecated (legacy metrics only) in favour of `events.InitMetrics`.
- The reference CLI initialises metrics with `InitMetrics` (`domain="platform"`).
- CI no longer skips the pipeline for changes under `docs/observability/`: those files are checked by `make metrics-lint`.

- **All database access goes through platform-pgcommon (now v1.4.0).** Library code no longer imports `github.com/jackc/pgx` or `database/sql`; a depguard rule (`pgcommon-only`) keeps it that way.
  - `outbox.Enqueue` and `port.OutboxStore.Enqueue` take a `pgcommon.Tx`. It is a type alias of `pgx.Tx`, so existing callers passing a `pgx.Tx` compile unchanged.
  - The outbox store's `ClaimBatch` and `MarkFailed` transactions now run through `pgcommon.RunInTx` instead of `WithConn` + `conn.Begin`, so pgcommon's PgBouncer-mode GUC injection and its new per-transaction `StatementTimeout` / `LockTimeout` apply to them. `ClaimBatch` no longer returns records alongside an error when the claim transaction fails to commit.
- **`config.LoadOutbox` loads database configuration via `pgcommon.ConfigFromEnv`.** New fields: `DB pgcommon.Config` (pass to `pgcommon.NewPool`) and `MigrationDatabaseURL` (`pgcommon.MigrationDSNFromEnv`: `MIGRATION_DATABASE_URL`, else the app DSN). `DatabaseURL` is now `DB.DSN`, so the `PG_HOST`/`PG_PORT`/`PG_USER`/`PG_PASSWORD`/`PG_DBNAME`/`PG_SSLMODE` form works as well as `DATABASE_URL`. pgcommon's config warnings (e.g. insecure `sslmode`, invalid `PG_*` values, no DSN) are appended to `Warnings` with a `platform-pgcommon:` prefix. `String()` also masks `MigrationDatabaseURL`.
- `DLQPublisher.SendToDLQ` no longer rejects a message whose attributes exceed the SQS limit of 10: the lowest-priority caller attributes are dropped (kept first: `TenantID`, `EventID`, `Source`, `Subject`, `traceparent`, `tracestate`, `baggage`, then lexical) and logged at WARN, so a message is never kept out of the DLQ by its attribute count. Set `StrictAttributes` for the v1.5.0 behaviour.
- `EventType` (and the FIFO identity) is taken from the body only when it is a full envelope (`id`, `type`, `source`, `time`), so arbitrary JSON with a `type` key — or an SNS notification wrapper — cannot mint `event_type` metric label values.
- `SendToDLQ` trims excess attributes *before* validating, then validates everything SQS would reject before any AWS call — characters outside the SQS-allowed set in the body or attribute values, attribute names (`AWS.`/`Amazon.` prefixes, disallowed characters, periods, > 256 chars), and body + attributes over 1 MiB — returning `ErrDLQInvalidMessage`. `SendMessage` rejections of the message itself (`InvalidParameterValue`, `InvalidMessageContents`, `InvalidAttributeName`, `InvalidAttributeValue`) are now `ErrDLQInvalidMessage` instead of `ErrDLQSendFailed`. `DLQReason` characters SQS rejects are replaced with U+FFFD.
- `mock.DLQPublisher.SendToDLQ` applies the same input validation as the SQS publisher, so a call that fails in production fails in tests.
- `events.DLQPublisher` is now a type alias of the internal port interface (same method set — source compatible).

### Removed

- The unexported `internal/adapter/outbound/logger` zap adapter. No consuming module could import it, but the documented wiring (`logger.NewLogger(os.Getenv("APP_ENV"))`) told services to. Inject platform-gincommon's `ZapLogger` (or any `port.Logger`) instead. `go.uber.org/zap` is no longer a dependency of the library packages; it is only in `tools/go.mod` (golangci-lint). platform-pgcommon removed its equivalent adapter in v1.4.0.

### Fixed

- **Outbox:**
  - A short SNS outage (≈30s with the defaults) moved the whole pending backlog to `outbox_dead_letters`: failed records were retried every poll with no backoff, and transient failures used up attempts.
  - Rows written before `Enqueue` required canonical IDs, or replayed from `outbox_dead_letters`, could have a payload ID that differed in case from the stored ID; a failed batch publish of such a row was marked published. Failure IDs are now matched in canonical form.
  - `ListDeadLetters`, `ReprocessDeadLetters(With)` and `DiscardDeadLetters` ordered only by `failed_at`, so dead letters failed in the same instant could be selected in a different order by each call, and a list-then-replay or list-then-discard could act on rows the list did not show. They now order by `failed_at, id`.
  - `Runner.Ready()` called before `Start` (the documented `go runner.Start(ctx); <-runner.Ready()` pattern) could return a channel that was never closed.
  - `ReprocessDeadLetters` / `ReprocessDeadLettersWith` replay in `failed_at` order, like `ListDeadLetters` / `DiscardDeadLetters`, so list-then-replay replays exactly the inspected records.
  - The backlog gauges ran two uncapped `COUNT(*)` queries every poll on every replica with a 2s timeout. Under a large backlog they timed out, the gauge read -1 and `PlatformEventsOutboxBacklog` went blind (and the KEDA example scaled in at the peak).
- **SNS publisher:**
  - SNS's own throttle and internal codes (`Throttled`, `InternalError`, `KMSThrottling`) and body-less 5xx / 429 responses (`UnknownError`) were treated as permanent, so throttling used up attempts. Per-entry failures with `SenderFault=false` are transient too; a body-less 4xx (a proxy's 403 / 413) stays permanent. The SQS DLQ publisher classifies 5xx / 429 the same way.
  - `PublishBatch` splits each 10-message chunk by SNS's 256 KiB request limit as well, so one large event no longer fails its neighbours with `BatchRequestTooLong`.
  - A codec encode failure in the batch path (the default `PublishConcurrency=1`) always counted toward `MaxAttempts`, so a schema-registry outage dead-lettered the backlog, while the single-`Publish` path already treated a codec error wrapping `ErrRetryable` as transient. Both paths now do; a `Codec` should wrap `events.ErrRetryable` for transient registry errors.
- **SQS consumer:**
  - A message only started extending its visibility once a worker picked it up. With slow handlers, messages queued behind busy workers reappeared and were processed twice — and with `WithMaxReceiveCount` / `WithDLQForwarding`, healthy messages were dead-lettered. Every received message is now extended from receipt; batches are still received whole. Without `WithVisibilityTimeout` each receive asks only for as many messages as there are free workers. With `WithHandlerTimeout`, a message still waiting when the timeout passes is handed back to the queue; `Stop` hands back undispatched messages at once.
  - An SNS notification wrapper decoded into an envelope with `Type = "Notification"` and no ID, so a handler ignoring unknown types deleted it as processed — silently losing the event.
  - Malformed bodies were logged (first 512 bytes) at ERROR, putting tenant payloads in logs.
  - A dead-letter handler that forwarded the message itself with `WithDLQForwarding` also enabled put it in the DLQ twice and counted it twice.
  - A `Stop()` during `Start`'s DLQ check (up to 10s) was lost and `Start` kept running.
  - A panic in the dead-letter handler or `Codec.Decode` left the delivery with no outcome metric; it is now counted as failed and retried.
  - Codec decode ran on the loop context, so `Stop()` turned in-flight decodes into spurious `decode_error`s.
  - The visibility timeout is now also extended during codec decode, the dead-letter handler and the DLQ forward.
- **Inbox:**
  - A message the handler dead-lettered (`SendToDLQ`, then nil) was recorded as processed, so a DLQ redrive after the fix was acked as a duplicate and never processed.
  - `Handler`'s best-effort semantics are now documented, and the `Prune` retention guidance is corrected.
- **Metrics:**
  - An `event_type` that is not valid UTF-8 made the Prometheus client panic inside `Publish`; it is now repaired (U+FFFD).
  - Counters without variable labels are exported at 0 on registration, so `increase()` alerts see their first increment after a restart.
  - A failed `InitMetrics` (invalid identity) no longer resets the `event_type` cap.
  - `event_type` slots are first come, first served, so unknown types from a misbehaving producer could take all 200 and turn real types into `__other__`; see `events.WithEventTypes`.
- **Alert rules:**
  - `PlatformEventsMessagesDeadLettered` missed the first dead-letter of each `event_type` / reason per pod (those series are born at 1); the recording rule now counts series with no sample in the hour before the window, without re-counting after a scrape gap.
  - `PlatformEventsConsumerStalled` no longer fires on a queue whose messages are all dead-lettered on purpose, and dead-lettering on one queue cannot hide a stall on another.
  - The promtool tests now start those series absent instead of at 0, which had hidden the gap.
- **Config:** `OutboxConfigEnv.String()` masks quoted (`password='a b'`) and backslash-escaped (`password=a\ b`) keyword/value passwords whole; previously part of them was printed.
- **CI:**
  - The release image signature check only accepted tag refs, so a manual `workflow_dispatch` release failed after pushing the image.
  - `PR summary` now runs (`always()`) when a gate fails, and reports the 97% coverage gate.
  - Every third-party action and the interop reusable workflow (which receives a private token) are pinned by commit SHA.
  - The lint exclusion for `test/smoke` used v1 syntax and was ignored.
  - Release: `docker/metadata-action`'s default `flavor: latest=auto` tagged `latest` on every stable release, whatever the floating-tag selection said; it is now `latest=false`.
- `platform_events_build_info` failed to register on a registerer that injects a `service` label (e.g. a `WrapRegistererWith` wrapper): `service` was a variable label colliding with the injected one. It is now a const label like on every other metric; the exposed series is unchanged.

- FIFO DLQs: an envelope ID containing characters invalid for `MessageGroupId`/`MessageDeduplicationId` now falls back to the body's SHA-256 instead of failing `SendMessage`.

### Security

- Inherited from platform-pgcommon v1.4.0: session-level RLS GUCs (`app.tenant_id`, …) no longer leak to the next caller of a pooled connection in direct-Postgres mode, and DSN passwords are removed from every `NewPool` / `migrate.Runner` error.

### Dependencies

- `github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon` v1.1.0 → **v1.4.3** (v1.4.0 brings `github.com/jackc/pgx/v5` v5.10.0 → v5.11.0). Consuming services inherit both; see pgcommon's [1.4.0 upgrade notes](https://github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/blob/main/CHANGELOG.md). The ones most likely to matter:
  - custom `pgx.Rows` mocks need a `TypeMap()` method;
  - pgx now parses connection URIs like libpq (`+` is literal; IPv6 hosts need brackets) and returns text-format `timestamptz` in `time.Local`;
  - pgcommon metrics gain a `pool` label and its OTel span attributes follow the stable semantic conventions (`db.statement` → `db.operation.name` / `db.query.text`, `net.peer.*` → `server.*`) — update dashboards and trace queries that filter on the old names.

---

### Docs

- `VERSIONING.md`, CLAUDE.md, the envelope, consuming and outbox guides, `EVENT_SCHEMA_GOVERNANCE.md` and `CONTRIBUTING.md` use the envelope's real wire keys (`time`, `specversion`, `dataschema`, `data`).
- The `SQSConsumerOptions` godoc, CLAUDE.md, ARCHITECTURE and the consuming guide describe `WithMaxReceiveCount` routing and trace-link propagation correctly; the consumer `Stop()` godoc describes its actual drain-timeout behaviour.
- ARCHITECTURE covers `Store.Process` and per-key ordering; the outbox guide covers ordering, the canonical-ID rule, `RetryBackoff` and the full transient classification; the observability README covers the new metrics.
- Removed the stale `APP_ENV` README row and the nonexistent `events.ErrBatchTooLarge`; ARCHITECTURE's public-API tables list `InitMetrics`, the new consumer options, `EnqueueOrdered`, `Store.Process` and `GaugeInterval`; CLAUDE.md's wiring example declares `ctx` first and enqueues before shutdown, and describes `port.Logger` and `outbox.Config` correctly. The reference CLI prints `HandlerTimeout` and the queue-depth interval.
- The LLD (rev 2.3) was checked against the code in both directions; README, ARCHITECTURE, CLAUDE.md and the outbox guide now match it: the layer rule is a convention (depguard enforces only `pgcommon-only`), Start/Stop lifecycle, connection budget, extender bound, gauge-refresh cadence, settle budget on shutdown and the `failed_at, id` dead-letter order.
- `docs/lld/platform-events-lld.md` (rev 2.0) follows the platform LLD structure (the `iam-org-membership` LLD's 21 sections): API IDs, caching design, event / failure / consistency / operational invariant registers, failure-scenario table, deployment and scaling, data lifecycle, sign-off register, error taxonomy, integration, migration, operations and performance. ARCHITECTURE's threat model describes malformed-body hashing and the `platform_telemetry_label_overflow_total` overflow counter.

### Tests

- Merged coverage is 99.0% (from 97.7%). New tests cover outbox and inbox store error paths, SNS failure classification and batch splitting, the consumer's visibility, timeout, malformed-body and shutdown paths, per-key ordering against Postgres (including commit order vs transaction start and an end-to-end runner run with a failing head), the runner's re-poll and gauges, and mock fidelity.
- Fixed `TestDispatch_VisibilityExtension_NilReceiptHandle`: it slept 150ms, shorter than the extender's 1s minimum tick, so it never exercised the extension path.

## [1.5.0] - 2026-09-30

### Added

- **`.githooks/pre-commit`** (from `iam-org-membership`) — runs `make tidy`, `make fmt-check` and `make lint` before every commit, the first checks of CI's `Validate / Quality` job. Unlike the reference, it also fails when `go mod tidy` changed `go.mod`/`go.sum`, since that fix is not part of the commit until staged and CI rejects the drift. Installed by `make setup` or `make install-hooks`; bypass once with `git commit --no-verify`.
- **`pkg/inbox`** — consumer-side deduplication, the counterpart of `pkg/outbox`: an embedded `processed_events` schema (`ApplySchema`, own `inbox_migrations` tracking table; `CREATE TABLE IF NOT EXISTS`, so it adopts an identically-shaped existing table), a `Store` on a `pgcommon.Pool` (`IsProcessed`, `MarkProcessed`, batched `Prune`), and `Handler(ledger, next)` which skips already-recorded envelope IDs and records an ID only after the handler succeeds. New metric `events_inbox_duplicates_total{consumer}`.
- **`events.DLQPublisher`** — forwards a failed/poison message to the dead-letter queue already configured on its source queue's `RedrivePolicy`, so consumer services can dead-letter explicitly without importing `github.com/aws/aws-sdk-go-v2/service/sqs` (keeps them depguard-compliant). No replay, redrive or DLQ provisioning — forwarding only.
  - `NewSQSDLQPublisher(DLQConfig)` / `NewSQSDLQPublisherWithClient(DLQConfig, DLQClientLike)`; `DLQConfig{Region, EndpointURL, ConsumerName, Logger}`.
  - `SendToDLQ(ctx, sourceQueueURL, body, attrs, reason)` resolves the DLQ (`GetQueueAttributes` → `deadLetterTargetArn` → `GetQueueUrl`, cached per source queue — failed lookups are not cached), forwards the body verbatim with caller attributes, and adds `EventType`, `DLQReason`, `OriginalQueue`, `FailedAt` and (when configured) `ConsumerName` (names exported as `events.DLQAttr*`). Standard attributes override caller values; `DLQReason` is capped at 1 KiB. FIFO DLQs get `MessageGroupId`/`MessageDeduplicationId` from the envelope ID (SHA-256 of the body otherwise).
  - `ResolveDLQ(ctx, sourceQueueURL)` — call at startup to fail fast on a missing or malformed `RedrivePolicy`.
  - Errors are `*events.DLQError{Kind, SourceQueue, Cause}` where `Kind` is one of `ErrDLQNotConfigured`, `ErrDLQInvalidRedrivePolicy`, `ErrDLQUnresolved`, `ErrDLQSendFailed`, `ErrDLQInvalidMessage`. Transient AWS failures (throttling, service unavailable, network timeout) additionally match the newly exported **`events.ErrRetryable`**. Invalid input (empty body/reason, invalid UTF-8, > 10 attributes) is rejected before any AWS call.
  - New `mock.DLQPublisher` test double and `events_dlq_forwarded_total{queue,event_type,status}` metric.
  - Required IAM: `sqs:GetQueueAttributes` (source queue), `sqs:GetQueueUrl` + `sqs:SendMessage` (DLQ).
- **`events.GlueDecodeCodec`** — decode-only `Codec` that strips the AWS Glue Schema Registry wire header (no registry client needed), for consumers of Glue-encoded events; rejects compressed payloads explicitly.

### Security

- **Go toolchain 1.26.5 → 1.26.6** (`go` directive in `go.mod`, which CI's `setup-go` reads) — fixes five standard-library vulnerabilities `govulncheck` reported as reachable: `GO-2026-6218` (`net/url` quadratic `resolvePath`), `GO-2026-6090` (`crypto/tls` post-handshake message limit), `GO-2026-6088` (`encoding/xml` recursion depth), `GO-2026-5972` (`encoding/asn1` recursion depth), `GO-2026-5026` (`net/http` / `idna` Punycode labels). `make vuln-check` is clean. Consuming services should build with Go ≥ 1.26.6 as well — the fixes live in their binaries' standard library.

### Changed

- **CI pipeline aligned with `iam-org-membership`** (the org's reference pipeline, whose job names are the required status checks on `main`): `validate-test.yml` / `validate-quality.yml` reusable gates, `Build image (cache)` → `Trivy CVE scan` / `Smoke tests`, `PR summary`, Cosign-signed `Push image → GHCR` on `main`, `changelog-check.yml`, and a `release.yml` that runs **the same job graph as `ci.yml`** at the tag (identical `Validate / Test`, `Validate / Quality`, `Build image (cache)`, `Trivy CVE scan`, `Smoke tests`, `Cross-language compatibility` jobs) behind a fail-fast tag + CHANGELOG `verify` job, adds 5-platform CLI binaries, and publishes the image and GitHub Release only after every gate passes — the pushed image is a cache hit of the one that was scanned and smoke-tested (shared `BUILD_VERSION` / `SOURCE_DATE_EPOCH`). `ci.yml`'s Trivy/smoke jobs now also reuse the cached image's `SOURCE_DATE_EPOCH`, and its GHCR push additionally waits for interop. Replaces `quality.yml` / `test.yml` / `validate.yml`. Keeps the `Cross-language compatibility` interop job. Omits the reference's service-only jobs (schema registry, production deploy gate). Also fixes, relative to the reference, missing private-module access in the release `build` job, the missing `go_private_token` build secret in the release `docker` job, missing `secrets: inherit` on the release gates, a provenance export that could never succeed (`export-image-provenance.sh` queried a Cosign attestation, but `build-push-action` stores provenance as a BuildKit attestation — now read with `buildx imagetools inspect`), a GitHub Release that attached only the linux/amd64 binary while `checksums.txt` listed all five, and a SIGPIPE in `extract-release-notes.sh` (`tee … | true` under `pipefail` exited 141 once the release notes outgrew the pipe buffer, which would have failed the `publish` job).
- **`Dockerfile` for the reference CLI** — digest-pinned `golang:1.26.6-alpine` builder and `gcr.io/distroless/static-debian13:nonroot` runtime (~6 MB, non-root); `.dockerignore`, `.docker-digests`, `make docker-build`, `make pin-base-images`. Debian 13 rather than the reference's Debian 12: the current Debian 12 distroless image fails the Trivy gate on a fixable `tzdata` advisory (`DLA-4792-1`).
- **Makefile aligned with `iam-org-membership`** — `vet` and `lint` now run a second pass with every test build tag (`integration,e2e`), so tagged test files are checked by CI's quality gate for the first time (fixed the one finding this surfaced in `test/e2e/outbox_test.go`); `test-ci` runs unit / integration / e2e in parallel (`make -j3`), writes per-suite profiles to `.coverage/`, prints a `--- FAIL:` summary per failing suite, and merges them into `coverage.out` via `scripts/merge_coverage.py`; `cover` / `cover-func` / `race` reuse `test-ci`; `ci` now includes `fmt-check`; `fmt` / `fmt-check` cover the whole module; exports `GOPRIVATE` / `GONOSUMDB` and a detected `DOCKER_HOST` for testcontainers; new `test-integration` target (`test-int` kept as an alias).

### Documentation

- README: new [Forwarding to the SQS DLQ](docs/guides/consuming.md#forwarding-to-the-sqs-dlq) section, `mock.DLQPublisher` testing guide, DLQ error-reference rows, `events_dlq_forwarded_total` metric/alert rows, and adoption-checklist items. The consumer checklist now states that `WithMaxReceiveCount(n)` must be **strictly lower** than the queue's `RedrivePolicy` `maxReceiveCount` — with `n ≥ maxReceiveCount` SQS moves the message before `WithDeadLetterHandler` runs.
- ARCHITECTURE: new *Consumer-side DLQ forwarding* section and `dlq-forward-flow.mmd` sequence diagram; failure-lifecycle, naming-collision, runbook, public-API, observable-signal and invariant tables updated; `layer-model`, `sqs-consume-flow` (now shows the dead-letter-handler branch), `consuming-service-wiring` and `documentation-assets` diagrams updated; embedded diagrams re-synced with their `.mmd` sources (fixes pre-existing drift in `outbox-poll-cycle` and `sqs-consume-flow`).
- `docs/README.md`: diagram index now lists `dlq-management-flow.mmd` and `dlq-forward-flow.mmd`.
- **README restructured** to the `iam-org-membership` layout (Mental model → Why → API overview → Validation and errors → Architecture → Integrating → Local development → Testing events locally → Testing → Environment variables → Security → Observability → Releasing → CI → Docker → Compatibility → Out of scope → Contributing). The long how-to sections moved verbatim into `docs/guides/` (quick start, envelope, publishing, consuming, outbox, codec, HMAC, observability, operations, testing); every cross-document link and anchor was repointed. Also documents `pkg/inbox` and `GlueDecodeCodec`, which the README previously omitted.
- `.env-example`: `SQS_MAX_RECEIVE_COUNT` example changed from `5` to `4` with a note that it must be lower than the queue's `RedrivePolicy` `maxReceiveCount`.
- **ARCHITECTURE restructured** to the `iam-org-membership` layout: Layer model → Package dependency graph → Public API → SNS publish flow → Write flow and transactional outbox → SQS consume flow → Data model → Failure lifecycle → Observability stack → Tenant propagation and RLS → Concurrency → Failure domains → Key invariants → Distribution and service wiring → Testing strategy → Consumer conformance checklist → Envelope compatibility → Threat model (STRIDE) → Developer tools → Design decisions → Performance → Documentation assets. All existing content kept; new sections and three new diagrams (`write-flow.mmd`, `data-model.mmd`, `observability-stack.mmd`); `pkg/inbox` added to the layer/package diagrams and API tables. Documents two previously unstated behaviours: `inbox.Handler` is check-then-act (not exactly-once), and a `Codec.Encode` outage counts toward `MaxAttempts`.
- Fixed a pre-existing broken anchor in `EVENT_SCHEMA_GOVERNANCE.md` (`#breaking-vs-non-breaking-changes-decision-tree`).

---

## [1.4.0] - 2026-08-05

### Added

- **`events.Codec`** (aliased from `port.Codec`) — pluggable hook for encoding/decoding an envelope's JSON `Payload` into a schema-registry-specific wire format (e.g. AWS Glue Schema Registry). `platform-events` ships no concrete implementation and adds no schema-registry SDK dependency — consuming services implement `Codec` against their own registry client, mirroring the existing `port.Logger` pattern.
- **`events.NoopCodec`** (aliased from `port.NoopCodec`) — identity/reference `Codec` implementation.
- `events.WithCodec(codec Codec) PublisherOption` — encodes the payload immediately before SNS publish. Unset (the default), behaviour is unchanged: `Payload` stays plain JSON and `SchemaID` stays empty.
- `events.WithConsumerCodec(codec Codec) ConsumerOption` — decodes incoming payloads whose `SchemaID` is non-empty, before the message reaches the handler or `WithDeadLetterHandler`. `SchemaID` empty means the message was never codec-encoded and is left as plain JSON.
- `events_codec_encode_total` / `events_codec_encode_duration_seconds` / `events_codec_decode_total` / `events_codec_decode_duration_seconds` Prometheus metrics.

---

## [1.3.1] - 2026-07-28

### Fixed

- **Outbox writes under `PGBouncerMode: true`** — `OutboxRecord.Payload` was `[]byte`; `platform-pgcommon`'s `PGBouncerMode` sets pgx's `DefaultQueryExecMode` to `SimpleProtocol`, which encodes parameters client-side using pgx's default codec for the Go type with no server round-trip to describe the target column. A plain `[]byte` defaults to the `bytea` codec, and binding that to the `payload` `JSONB` column failed with `invalid input syntax for type json` (`SQLSTATE 22P02`) — even though the underlying bytes were valid JSON. `OutboxRecord.Payload` is now `json.RawMessage`, which pgx encodes with its JSON codec instead. Affects `outbox.Enqueue`, `OutboxService.Enqueue`, `ClaimBatch`, and the dead-letter insert path in `MarkFailed`. No public API changes — `outbox.Enqueue` and `Runner` callers are unaffected.
- **`govulncheck` findings** — bumped the Go toolchain requirement to `1.26.5` (fixes `GO-2026-5856`, an Encrypted Client Hello privacy leak in `crypto/tls`) and `golang.org/x/text` to `v0.40.0` (fixes `GO-2026-5970`, an infinite loop on invalid input in `golang.org/x/text/unicode/norm`, reached transitively through `platform-pgcommon`'s migration runner). No source changes required.

---

## [1.3.0] - 2026-06-23

### Added

- **`Envelope.Subject`** (`json:"subject,omitempty"`) — resource URI or identifier the event is about (e.g. `"users/01926e4f-..."`). Set via `events.WithSubject(subject string)`. Forwarded as an SNS message attribute (`Subject`) when non-empty, enabling SQS subscription filter policies without body parsing.
- **`Envelope.Actor`** (`json:"actor,omitempty"`) — identity that caused the event (user UUID, service-account name, etc.). Set via `events.WithActor(actor string)`. Audit trail field — not forwarded as an SNS attribute.
- `events.WithSubject(subject string) EnvelopeOpt` — sets `Subject` on the envelope at construction time.
- `events.WithActor(actor string) EnvelopeOpt` — sets `Actor` on the envelope at construction time.
- **`Envelope.SchemaID`** (`json:"dataschema,omitempty"`) — schema registry version identifier (e.g. AWS Glue Schema Registry UUID). Set via `events.WithSchemaID(id string)`. Technical registry pointer used by the codec for Avro/JSON deserialization; distinct from `specversion`. Not forwarded as an SNS attribute.
- `events.WithSchemaID(id string) EnvelopeOpt` — sets `SchemaID` on the envelope at construction time.
- **`Envelope.IPAddress`** (`json:"ip_address,omitempty"`) — client IP at the time the event was triggered. Set via `events.WithIPAddress(ip string)`. Audit trail field — not forwarded as an SNS attribute.
- **`Envelope.UserAgent`** (`json:"user_agent,omitempty"`) — HTTP `User-Agent` header from the triggering request. Set via `events.WithUserAgent(ua string)`. Audit trail field — not forwarded as an SNS attribute.
- `events.WithIPAddress(ip string) EnvelopeOpt` — sets `IPAddress` on the envelope at construction time.
- `events.WithUserAgent(ua string) EnvelopeOpt` — sets `UserAgent` on the envelope at construction time.
- **`runner.ListDeadLetters(ctx, DLQFilter, limit) ([]DeadLetterRecord, error)`** — returns up to `limit` records from `outbox_dead_letters` matching the filter, ordered by `failed_at` ascending. Returns an empty slice when no records match.
- **`runner.ReprocessDeadLettersWith(ctx, DLQFilter, limit) (int, error)`** — selective replay: moves up to `limit` filtered records from `outbox_dead_letters` back to `outbox_events`, resetting attempts to 0. Supports filtering by `EventType`, `TenantID`, and `FailedBefore`.
- **`runner.DiscardDeadLetters(ctx, DLQFilter, limit) (int64, error)`** — permanently deletes up to `limit` filtered records from `outbox_dead_letters`. Always call `ListDeadLetters` first to confirm the selection.
- **`outbox.DLQFilter`** — public type with fields `EventType string`, `TenantID string`, `FailedBefore time.Time`. All fields optional; zero value matches all records.
- **`outbox.DeadLetterRecord`** — public type returned by `ListDeadLetters` with fields `ID`, `EventType`, `TenantID`, `TraceID`, `Attempts`, `LastError`, `CreatedAt`, `FailedAt`.
- **`outbox_dead_letters_discarded_total`** Prometheus counter — incremented by `DiscardDeadLetters`.
- **Migration 008** — composite index `idx_outbox_dead_letters_event_type_tenant_id` on `(event_type, tenant_id)` for efficient DLQ filter queries.
- **`pkg/outbox/dlq.go`** — documents the public DLQ API surface with usage examples.

### Changed

- **`Envelope` wire format — full CloudEvents alignment (JSON key renames; Go field names unchanged):**
  - `payload` → `data` (CloudEvents `data`)
  - `timestamp` → `time` (CloudEvents `time`)
  - `schema_version` → `specversion` (CloudEvents `specversion`)
  - `schema_id` → `dataschema` (CloudEvents `dataschema`)
  - `hmacEnvelope` canonical bytes updated for all renamed fields — `SignEnvelope`/`VerifyEnvelope` callers must redeploy signer and verifier together.
- `buildMessageAttributes` (SNS adapter) emits `Subject` as an SNS message attribute when non-empty.
- `hmacEnvelope` (internal) updated to include `Subject`, `Actor`, `IPAddress`, and `UserAgent` in the canonical HMAC payload.
- `publicToDomain` / `domainToPublic` carry all envelope fields including `Subject`, `Actor`, `IPAddress`, `UserAgent`, `SchemaID` without loss.
- `outbox-poll-cycle.mmd` updated to reference DLQ management operations.
- `ARCHITECTURE.md` and `README.md` updated to reflect all new fields and the CloudEvents wire format.

---

## [1.2.0] - 2026-06-12

### Added

- **`pkg/config`** — public env loading (`LoadSNS`, `LoadSQS`, `LoadOutbox`, `LoadOTel`) plus wiring helpers (`RunnerConfigFromEnv`, `SQSConfigFromEnv`, `SQSConsumerOptions`, `LogWarnings`, `LogWarningsTo`)
- `config.LogWarningsTo(logger port.Logger, warnings []string)` — emits configuration warnings via a structured logger (Warn level); falls back to stderr when logger is nil. Prefer over `LogWarnings` when a structured logger is available so warnings reach log aggregators (Loki, CloudWatch)
- `outbox.Runner.PrunePublished(ctx, olderThan time.Duration, limit int) (int64, error)` — removes published `outbox_events` rows older than `olderThan`; call periodically from a scheduled job to prevent unbounded table growth. Delegates to `port.OutboxStore.PrunePublished`
- `port.OutboxStore.PrunePublished(ctx, olderThan time.Duration, limit int) (int64, error)` — **breaking interface change**: all `OutboxStore` implementations must add this method. The Postgres implementation in `internal/adapter/outbound/outboxstore` applies a 30 s internal timeout and uses the `idx_outbox_events_published_at` partial index (migration 007) for efficient batch deletes
- Migration `007_add_prune_index` — partial index `idx_outbox_events_published_at ON outbox_events(published_at) WHERE published_at IS NOT NULL`; makes `PrunePublished` an index scan instead of a sequential scan as the table grows
- `metrics.OversizedEventTypeLabelTotal` counter (`events_oversized_event_type_label_total`) — incremented by `SanitizeEventType` whenever an `event_type` value exceeds 128 bytes and is replaced with `"__oversized__"`; alert when non-zero to detect misconfigured or adversarial producers
- `OUTBOX_PUBLISH_CONCURRENCY`, `OUTBOX_PUBLISH_TIMEOUT`, `OUTBOX_DRAIN_TIMEOUT` env vars for outbox runner tuning
- `SQS_MAX_RECEIVE_COUNT` env var — map to `events.WithMaxReceiveCount` when wiring `WithDeadLetterHandler`
- Sequential outbox publish path (`PublishConcurrency=1`) uses SNS `PublishBatch` (up to 10 per API call) for higher throughput
- `outbox.MigrationsTable` — exported constant (`"outbox_migrations"`); `ApplySchema` injects `x-migrations-table=outbox_migrations` into the DSN so the outbox migration history is tracked in its own table, isolated from the consuming service's `schema_migrations` and `pgcommon_migrations` tables

### Breaking Changes

- **`port.OutboxStore` interface** has a new required method: `PrunePublished(ctx context.Context, olderThan time.Duration, limit int) (int64, error)`. Any code that implements `OutboxStore` (e.g. test mocks) must add this method. The no-op implementation is `func (s *myStore) PrunePublished(_ context.Context, _ time.Duration, _ int) (int64, error) { return 0, nil }`.

### Changed

- **Breaking:** `outbox.NewRunner` now returns `(*Runner, error)` instead of `*Runner` — returns an error when `ClaimLeaseDuration` is too short for the configured `BatchSize × PublishTimeout`; panics are reserved for nil `Publisher` / nil `Store+Pool` (programming errors). Update all call sites: `runner, err := outbox.NewRunner(cfg)`.
- `ClaimLeaseDuration` validation: when `PublishTimeout` is disabled (≤0), a minimum floor of 30 s is enforced so leases never expire instantly, preventing duplicate delivery across concurrent runners
- Panicking SQS handlers now record the panic as an OTel span error (via `span.RecordError` + `span.SetStatus(codes.Error)`) before re-panicking, giving end-to-end trace visibility even when handlers crash
- `maskDSN` (internal) now also masks `password=` / `passwd=` values that appear in URL query parameters (e.g. `postgres://host/db?password=secret`), preventing credential leaks in `OutboxConfigEnv.String()` log output
- `publishClaimedSequential` (sequential outbox batch path): per-failure `Code == "TransportError"` in a `domain.BatchError` now uses `threshold = MaxAttempts+1`, matching the concurrent single-record path — SNS transport failures (ThrottlingException, ServiceUnavailable) no longer consume a retry slot and will not prematurely dead-letter healthy records
- `outboxstore.MarkPublished` and `outboxstore.MarkFailed` now apply a 5 s per-call DB timeout (`defaultStoreQueryTimeout`), consistent with `ClaimBatch`, `PendingCount`, and `LeasedCount` — a saturated DB can no longer stall the entire batch bookkeeping loop
- `outboxstore.ReprocessDeadLetters` now applies a 30 s internal DB timeout (`defaultPruneTimeout`) so a saturated DB does not block the caller indefinitely
- `WithDrainTimeout(0)` is now documented: a zero drain timeout disables the drain window — in-flight handler contexts are cancelled immediately when `Stop` is called. Negative values are rejected (default 30 s preserved). Use zero only in tests
- `bridge.PublishBatch`: `RecordOutboxAttempt` and `RecordOutboxPublished` are now recorded post-call so a panicking publisher leaves both counters at 0 rather than creating a permanent `attempts > published{success+error}` mismatch in dashboards
- `events.SystemTenantID` re-exported from `internal/core/domain` — single canonical definition
- Reference CLI (`cmd/platform-events`) calls `events.Init`, logs config `Warnings`, and prints production wiring reminders; `-strict` exits non-zero on missing required env vars
- `internal/config` is a deprecated alias of `pkg/config` — import `pkg/config` in new code
- `logger.NewLogger` default encoder corrected: `"dev"`, `"development"`, and `"local"` use a colored console encoder; all other values (including `"staging"`, unknown, empty) use a JSON production encoder. Previously any env value other than `"production"`/`"prod"` triggered dev mode, causing staging deployments to emit colored console output.
- SQS consumer receive-error backoff now includes ±25% random jitter so concurrent consumer replicas do not retry in lock-step after a shared SQS error.
- Handler `ctx` uses `context.WithoutCancel` — `ctx.Deadline()` always returns a zero time. Handlers must not rely on the parent deadline for timeouts; use `context.WithTimeout` explicitly instead. Cancellation is only delivered once the consumer's drain timeout expires.

### Fixed

- `CHANGELOG` claim-lease default corrected: `OUTBOX_CLAIM_LEASE_DURATION` unset → **10 minutes** (store default), not 30s
- `.env-example` documents `OUTBOX_CLAIM_LEASE_DURATION`, `OUTBOX_STARTUP_JITTER`, runner tuning vars, and `RawMessageDelivery` requirement
- `outbox_service.truncateError`: truncation marker (`"…[truncated]"`, 12 runes) is now counted within the `maxLastErrorLen` cap rather than appended after it, preventing error strings of up to `maxLastErrorLen+12` runes from reaching the database column.
- `metrics.Init` now acquires `metricsMu` inside the `sync.Once` callback, closing a race window with concurrent `InitWithRegisterer` calls.
- `outboxstore.MarkPublished` doc comment corrected: returns `nil` (not an error) when 0 rows are affected and logs a `WARN` instead — the concurrent-update case (record published/removed by another runner between claim and mark) is an expected non-error condition.
- `publishChunk` (SNS adapter sequential batch path): transport-level errors are now wrapped with `wrapIfRetryable` before returning so the outbox service's `failureThreshold` correctly identifies retryable SNS errors (ThrottlingException, ServiceUnavailable) and uses `MaxAttempts+1`

---

## [1.1.0] - 2026-06-05

### Added

**Public API (`pkg/`)**

- `events.SystemTenantID` constant and `events.WithSystemTenant()` envelope option — use for background jobs that publish cross-tenant events without an HTTP request context
- `events.TraceIDFromContext(ctx)` — retrieves the trace ID injected by `NewSQSConsumer` into the handler context; use in downstream log lines and spans to preserve the publishing service's trace
- `events.SQSClientLike` interface in `pkg/events` — allows consuming services to mock the SQS client without importing internal packages; satisfies `aws-sdk-go-v2/service/sqs` automatically
- `outbox.Runner.ReprocessDeadLetters(ctx, limit int) (int, error)` — moves up to `limit` dead-letter records back to `outbox_events` for re-delivery; safe to call in a scheduled job or admin endpoint
- `events.WithSchemaVersion(v string)` — new `EnvelopeOpt` that sets `Envelope.SchemaVersion`; use `"1"` at inception and increment on additive payload changes. Absent field is `omitempty` — fully backward-compatible with v1.0 publishers and consumers.
- `Envelope.SchemaVersion string` — new optional wire field (`json:"schema_version,omitempty"`); carries the payload contract version across the SNS/SQS boundary so consumers can assert and reject unrecognised versions without silent misparse

**Core domain / ports**

- `domain.RetryableError` struct and `domain.ErrRetryable` sentinel — wraps transient AWS errors; callers use `errors.Is(err, domain.ErrRetryable)` to distinguish throttling from permanent failures without importing AWS SDK types
- `port.WithEnvelopeTraceID(ctx, traceID)` / `port.EnvelopeTraceIDFromContext(ctx)` — internal context key used by `NewSQSConsumer` to thread the publishing service's OTel trace ID through the handler call chain
- `port.OutboxStore.LeasedCount(ctx) (int, error)` — returns the count of records currently held under a lease; surfaced as the `outbox_leased_total` gauge
- `port.OutboxStore.ReprocessDeadLetters(ctx, limit int) (int, error)` — moves dead-letter records back into `outbox_events` for retry

**SQS Consumer hardening**

- Per-call `ReceiveMessage` timeout: each poll wraps a `context.WithTimeout(WaitSeconds + 5s)` to prevent indefinitely hung receive calls from stalling the loop
- `VisibilityTimeout > 12h` validation — `NewSQSConsumer` returns an error at construction time if the configured timeout exceeds the SQS maximum; previously resulted in a silent AWS error at runtime
- Malformed message handling — if a message body cannot be unmarshalled into an `Envelope`, the message is immediately deleted and counted as `events_consumed_total{status=malformed}` rather than retried forever
- Panic recovery in handler goroutines — a panicking handler logs the stack trace and leaves the message visible for retry instead of crashing the consumer loop
- `WithEnvelopeTraceID(ctx, env.TraceID)` injection — the handler context now carries the publishing trace ID, accessible via `events.TraceIDFromContext(ctx)`, so consumer-side spans link back to the publisher's trace without relying on message attributes

**SNS Publisher hardening**

- `wrapIfRetryable` — SNS adapter inspects `smithy.APIError.ErrorCode()` for `ThrottlingException`, `ServiceUnavailable`, `InternalFailure`, and `RequestTimeout`; wraps matching errors in `domain.RetryableError` so the outbox runner does not dead-letter on transient failures
- Envelope field validation at publish time — `Publish` returns an error immediately if `env.ID`, `env.Type`, or `env.Source` is empty, preventing invalid Prometheus label cardinality

**Outbox Runner hardening**

- Lease-based concurrency (`ClaimLeaseDuration`) — the claim step updates `scheduled_at = NOW() + ClaimLeaseDuration` so a record cannot be claimed by a second runner while in flight; replaces the prior optimistic locking approach
- `StartupJitter` — on `Runner.Start`, sleep a random duration between `0` and `StartupJitter` before the first poll to desync horizontally scaled replicas and reduce thundering-herd contention on startup
- Retryable-error threshold — `OutboxService` uses `threshold = MaxAttempts + 1` when a retryable error is detected, so transient throttling never advances the attempt counter toward dead-letter; the record is rescheduled without penalty

**Metrics**

- `outbox_leased_total` (`GaugeVec`, label `service`) — tracks records currently claimed by a runner; combine with `outbox_pending_total` for a complete in-flight picture
- `outbox_dead_letters_total` (`Counter`, label `service`) — incremented each time a record is moved to `outbox_dead_letters`; alert on `rate() > 0`
- `events_consumed_total` now includes `status=malformed` for envelope-parse failures

**Config**

- `OutboxConfigEnv.ClaimLeaseDuration` — loaded from `OUTBOX_CLAIM_LEASE_DURATION` (unset → **10 minutes** store default); controls how long a leased record is held before it becomes reclaimable
- `OutboxConfigEnv.StartupJitter` — loaded from `OUTBOX_STARTUP_JITTER` (default `0s`); set to `5s`–`10s` in scaled deployments
- `OutboxConfigEnv.String()` — safe, credential-redacted log representation; masks passwords in both URL (`postgres://user:***@host/db`) and key-value (`password=***`) DSN formats

**Event schema governance**

- `EVENT_SCHEMA_GOVERNANCE.md` — new document covering: event type naming rules (including `.v<N>` versioning for breaking changes), payload evolution rules (additive-only, `omitempty` discipline, forbidden mutations), `schema_version` publisher and consumer contracts, migration window pattern, consumer compatibility contract, event type registry, and deprecation/sunset process
- Updated `event_type` convention to `<domain>.<entity>.<past-tense-verb>[.v<N>]` — `.v<N>` suffix only appears for breaking payload changes; v1 remains implicit (no suffix)
- `CONTRIBUTING.md` PR checklist: added event type registry, payload struct, and breaking-change rules
- `README.md` and `ARCHITECTURE.md` updated with `schema_version` wire field, `WithSchemaVersion` usage examples, and links to `EVENT_SCHEMA_GOVERNANCE.md`

### Changed

- `OutboxService.PublishPending` skips attempt-count increment for retryable errors (uses threshold `MaxAttempts + 1`); records remain in `outbox_events` and are rescheduled rather than progressing toward dead-letter
- `NewSQSConsumer` injects `TraceID` into handler context via `port.WithEnvelopeTraceID` in addition to the existing `pgcommon` GUC injection, giving handlers a single `events.TraceIDFromContext(ctx)` call site
- `EnvelopeOpt` is now `func(*envelopeConfig)` internally (unexported config struct) — all library-provided option functions (`WithTenantID`, `WithTraceID`, `WithCorrelationID`, `WithSystemTenant`) continue to work without change; consumers that wrote raw `EnvelopeOpt` closures must update to the new signature (expected impact: none — the type was not documented for direct construction)

### Fixed

- SQS consumer no longer hangs indefinitely on a `ReceiveMessage` call when the AWS endpoint becomes unresponsive — bounded by `WaitSeconds + 5s` per call
- Outbox runner no longer dead-letters records on transient AWS throttling; they are rescheduled and retried on the next poll cycle

---

## [1.0.0] - 2026-05-29

### Added

- Initial implementation of `platform-events` shared library
- `pkg/events` — typed `Envelope[T]` with UUID v7 IDs, `NewEnvelope`, `ParseEnvelope`, `JSON()`
- `pkg/events` — `Publisher` interface and `NewSNSPublisher` (AWS SNS, FIFO support, batch)
- `pkg/events` — `Consumer` interface and `NewSQSConsumer` (long-poll, concurrency, drain)
- `pkg/events` — HMAC helpers: `Sign`, `Verify`, `SignEnvelope`, `VerifyEnvelope`
- `pkg/events` — Prometheus metrics: `Init`, `InitWithRegisterer`
- `pkg/events/mock` — `MockPublisher` and `MockConsumer` for unit testing
- `pkg/outbox` — transactional outbox `Runner`, `Enqueue`, `ApplySchema`
- `internal/core` — domain entities, port interfaces, HMAC and outbox services
- `internal/adapter/outbound` — SNS publisher, SQS consumer, Postgres outbox store, Zap logger, Prometheus metrics
- `internal/config` — environment-variable loading with defaults
- OTel tracing on SNS publish and SQS receive spans
- GUC injection into SQS handler context for `platform-pgcommon` RLS
- GitHub Actions CI/CD workflows (validate, CI, release)

### Chore

- Bump `actions/upload-artifact` from v4 to v7 in CI workflows
