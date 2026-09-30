# Contributing

This is an internal shared library for BCBP Solutions platform services. This guide covers development setup, how to add new features, test requirements, and how to cut a release.

## Prerequisites

- Go 1.26+ (matches `go.mod`)
- Docker (required for integration tests via `testcontainers-go`)
- `golangci-lint` is managed as a Go tool — no separate install needed (`go tool golangci-lint run`)

## Development setup

```bash
git clone https://github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events
cd platform-events
make setup   # copies .env-example → .env
make tidy    # go mod tidy
make lint    # verify linter passes
make test    # run all tests (requires Docker)
```

To run only unit tests (no Docker required):

```bash
make test-unit
```

### Local module workspace

`go.mod` pins `platform-pgcommon` at a released version (for CI and consumers). For local development against sibling repos, use a `go.work` file (gitignored):

```bash
go work init . ../platform-pgcommon
# optional: ../platform-gincommon if you are testing cross-library changes
```

CI for **platform-events** only checks out this repository and fetches private modules via `GOPRIVATE` — it does not clone sibling repos.

## Project layout

```
pkg/            ← Public API (consumers import these — SemVer applies)
  events/       ← Envelope, Publisher, Consumer, HMAC helpers, metrics
  outbox/       ← Transactional outbox runner, Enqueue, ApplySchema
internal/
  core/domain/  ← Entities: Envelope, OutboxRecord, errors (no external deps)
  core/port/    ← Interfaces owned by the use-case layer (Publisher, Consumer, Codec, Logger, Clock)
  core/service/ ← Use cases: OutboxService, HMACService
  adapter/outbound/  ← SNS publisher, SQS consumer, Postgres outbox store, Zap logger, metrics
  config/       ← Env-var loading
cmd/platform-events/  ← Reference CLI (version info; not imported by consumers)
pkg/outbox/migrations/  ← Embedded SQL migration files
test/           ← All tests live here, not beside production code
```

**Dependency rule:** `domain` ← `port` ← `service` ← `adapter` ← `pkg`. Inner layers must never import outer layers. `domain/` must have zero external dependencies.

## Adding a new event type

No library changes needed — types are generic. Define a Go struct and use `events.NewEnvelope[YourType](...)` in the consuming service. Always pass `WithTenantID(rc.TenantID)`, `WithTraceID(rc.TraceID)`, and `WithSchemaVersion("1")` from the `gincommon.RequestContext` when publishing from an HTTP handler.

Register the new event type in [EVENT_SCHEMA_GOVERNANCE.md](EVENT_SCHEMA_GOVERNANCE.md#event-type-registry) before publishing. Follow the naming convention `<domain>.<entity>.<past-tense-verb>` (`.v<N>` suffix only for breaking changes).

**Payload struct rules:**
- **Event types are immutable once published** — treat a published `event_type` string as a permanent contract
- All optional fields must be tagged `json:",omitempty"`; consumers must never use `json.Decoder.DisallowUnknownFields()` on event payloads
- No field removals, renames, type changes, or semantic changes without minting a new versioned event type
- Adding a new optional field is non-breaking — increment `schema_version` so consumers can distinguish payload generations

See [EVENT_SCHEMA_GOVERNANCE.md](EVENT_SCHEMA_GOVERNANCE.md) for the full ruleset, migration window pattern, and consumer compatibility contract.

## Adding a new Publisher backend (e.g. EventBridge)

1. Implement `port.Publisher` in `internal/adapter/outbound/eventbridge/`.
2. Expose a constructor in `pkg/events/` following the SNS adapter as a template.
3. Call `otel.Tracer("platform-events")` (not gincommon directly) and record Prometheus metrics in the adapter.
4. Add unit tests using `test/fixtures.MockPublisher` and integration tests under `test/integration/`.

## Adding a new Consumer backend (e.g. Kinesis)

1. Implement `port.Consumer` in `internal/adapter/outbound/kinesis/`.
2. Expose via `pkg/events/`. The `Handler` signature is shared — no changes to calling code.
3. Inject `pgcommon.WithGUCSet` into the handler context so `platform-pgcommon` GUC injection works transparently.

## Adding a new Codec implementation (e.g. AWS Glue Schema Registry)

`platform-events` defines the `port.Codec` interface (aliased as `events.Codec`) but ships no concrete implementation and adds no schema-registry SDK dependency — this keeps every consuming service's dependency tree free of AWS Glue (or any other registry) unless it actually uses one, mirroring how `port.Logger` works with `platform-gincommon`'s `ZapLogger`.

1. Implement `port.Codec` (`Encode`/`Decode`) in the consuming service, or in a separate shared package if multiple services need the same registry client — **not** inside `platform-events`.
2. Inject it via `events.WithCodec(codec)` on the SNS publisher and `events.WithConsumerCodec(codec)` on the SQS consumer.
3. `Encode` must return plain-JSON-compatible bytes only through the library's wrapping (`domain.WrapCodecPayload` handles base64-encoding internally) — never assign raw binary bytes to `Envelope.Payload` yourself.
4. Do not add a new AWS SDK service dependency (e.g. `aws-sdk-go-v2/service/glue`) to this module's `go.mod` for this purpose.

## Adding a new outbox migration

1. Add `NNN_description.up.sql` and `NNN_description.down.sql` to `pkg/outbox/migrations/`.
2. The embedded `fs.FS` is recompiled on next `go build` — no code changes needed.
3. Verify: run `outbox.ApplySchema(ctx, runner)` against a local Postgres instance.

## Adding a new metric

1. Declare the package-level var in `internal/adapter/outbound/metrics/metrics.go`.
2. Initialise it inside `initMetricsWithRegisterer` and register it.
3. Add a unit test in `test/unit/metrics/metrics_test.go` using `InitWithRegisterer` with `prometheus.NewRegistry()`.

## Testing requirements

| Layer | Location | Build tag | Docker | Notes |
|-------|----------|-----------|--------|-------|
| Unit | `test/unit/` | *(none)* | No | Fully isolated; mock deps only |
| Integration | `test/integration/` | `integration` | Yes | LocalStack + Postgres via testcontainers |
| Smoke | `test/smoke/` | `smoke` | — | Targets live AWS resources; optional |

All unit tests must pass without Docker (`make test-unit`). Integration tests spin up LocalStack (SNS + SQS) and Postgres containers automatically.

## PR checklist

- [ ] `make ci` passes locally (tidy + vet + lint + test-ci + build)
- [ ] New public API is documented with exported symbol comments
- [ ] New env vars are added to `.env-example` and the README Configuration Reference table
- [ ] New sentinel errors are added to `internal/core/domain/errors.go` and the README Error Reference table
- [ ] New public package exports have a `doc.go` package overview
- [ ] `CHANGELOG.md` `[Unreleased]` section is updated
- [ ] New event types are registered in `EVENT_SCHEMA_GOVERNANCE.md` event type registry
- [ ] New payload structs use `json:",omitempty"` on all optional fields and set `WithSchemaVersion`
- [ ] Breaking payload changes mint a new versioned event type (`iam.user.created.v2`) — never mutate existing
- [ ] Any new `publisher.Publish` call site is reviewed against the [Publishing rules](docs/guides/publishing.md#publishing-rules) decision table — if it is tied to a DB write, it must be converted to `outbox.Enqueue`

## Release process

See [VERSIONING.md](VERSIONING.md) for the full release process.
