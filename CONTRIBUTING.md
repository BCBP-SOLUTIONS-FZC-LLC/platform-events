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

The library depends on `platform-pgcommon`, which is a sibling private module. For local development, create a `go.work` file (gitignored) to use the local copy:

```bash
go work init . ../platform-pgcommon
```

This avoids needing a published version of `platform-pgcommon` during development.

## Project layout

```
pkg/            ← Public API (consumers import these — SemVer applies)
  events/       ← Envelope, Publisher, Consumer, HMAC helpers, metrics
  outbox/       ← Transactional outbox runner, Enqueue, ApplySchema
internal/
  core/domain/  ← Entities: Envelope, OutboxRecord, errors (no external deps)
  core/port/    ← Interfaces owned by the use-case layer (Publisher, Consumer, Logger, Clock)
  core/service/ ← Use cases: OutboxService, HMACService
  adapter/outbound/  ← SNS publisher, SQS consumer, Postgres outbox store, Zap logger, metrics
  config/       ← Env-var loading
cmd/platform-events/  ← Reference CLI (version info; not imported by consumers)
pkg/outbox/migrations/  ← Embedded SQL migration files
test/           ← All tests live here, not beside production code
```

**Dependency rule:** `domain` ← `port` ← `service` ← `adapter` ← `pkg`. Inner layers must never import outer layers. `domain/` must have zero external dependencies.

## Adding a new event type

No library changes needed — types are generic. Define a Go struct and use `events.NewEnvelope[YourType](...)` in the consuming service. Always pass `WithTenantID(rc.TenantID)` and `WithTraceID(rc.TraceID)` from the `gincommon.RequestContext` when publishing from an HTTP handler.

## Adding a new Publisher backend (e.g. EventBridge)

1. Implement `port.Publisher` in `internal/adapter/outbound/eventbridge/`.
2. Expose a constructor in `pkg/events/` following the SNS adapter as a template.
3. Call `otel.Tracer("platform-events")` (not gincommon directly) and record Prometheus metrics in the adapter.
4. Add unit tests using `test/fixtures.MockPublisher` and integration tests under `test/integration/`.

## Adding a new Consumer backend (e.g. Kinesis)

1. Implement `port.Consumer` in `internal/adapter/outbound/kinesis/`.
2. Expose via `pkg/events/`. The `Handler` signature is shared — no changes to calling code.
3. Inject `pgcommon.WithGUCSet` into the handler context so `platform-pgcommon` GUC injection works transparently.

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

## Release process

See [VERSIONING.md](./VERSIONING.md) for the full release process.
