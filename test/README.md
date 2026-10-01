# Test Guide

This document explains the test layout, how to run each layer, and patterns for adding new tests.

---

## Test layers

| Layer | Location | Build tag | Docker | Run command |
|-------|----------|-----------|--------|-------------|
| Unit | `test/unit/` | *(none)* | No | `make test-unit` |
| Integration | `test/integration/` | `integration` | Yes | `make test-int` |
| Smoke | `test/smoke/` | `smoke` | — | `make test-smoke` |

**Unit tests** have no external dependencies — no Docker, no network. They use mock implementations from `test/fixtures/`.

**Integration tests** spin up [floci](https://floci.io) (`floci/floci:2.1.0` — the open-source, always-free AWS emulator the platform uses instead of LocalStack; SNS + SQS on :4566, no auth token) and Postgres containers via [testcontainers-go](https://testcontainers.com/guides/getting-started-with-testcontainers-for-go/). Docker must be running locally. Pass `-short` to skip them when Docker is unavailable.

**Smoke tests** target live AWS resources at `SMOKE_SNS_TOPIC_ARN` and `SMOKE_SQS_QUEUE_URL`. They are not part of the standard CI gate.

---

## Running tests

```bash
make test           # unit + integration (requires Docker)
make test-unit      # unit tests only (no Docker required)
make test-int       # integration tests (requires Docker)
make test-smoke     # smoke tests (requires live AWS resources)
make race           # unit + integration with the -race detector
make cover-func     # per-function coverage summary in terminal
make cover          # coverage HTML report
```

Run a single test:

```bash
cd test   # test/ is its own Go module (replace → ../)
go test ./unit/envelope/...     -run TestEnvelopeSign -v
go test ./integration/...       -tags=integration -run TestSNSPublishRoundTrip -v
```

Skip integration tests when Docker is unavailable:

```bash
cd test && go test -short -tags=integration ./integration/...
```

---

## Shared fixtures (`test/fixtures/`)

### MockLogger

`fixtures.MockLogger` implements `port.Logger` and records all log calls so unit tests can assert on log output without a real logger.

```go
ml := &fixtures.MockLogger{}
consumer, _ := sqs.New(sqs.Config{QueueURL: "...", Logger: ml}, handler)
// ... trigger error path ...
require.True(t, ml.HasError())
```

### MockPublisher

`fixtures.MockPublisher` implements `port.Publisher` in memory. Use it in outbox service unit tests and wherever a real SNS client would be too heavy.

```go
pub := &fixtures.MockPublisher{}
svc := service.NewOutboxService(store, pub, logger, clock, 5)
// ... call PublishBatch ...
require.Len(t, pub.Published(), 1)
```

### FakeClock

`fixtures.FakeClock` implements `port.Clock` with a fixed, advanceable time. Use it to test time-sensitive behaviour (e.g. outbox `scheduled_at`) deterministically.

```go
clk := &fixtures.FakeClock{T: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
// clk.Advance(5 * time.Second) to move time forward
```

### floci

`fixtures.StartFloci(ctx, t)` returns the package's **shared** floci container, started on first use, as in iam-org-membership. Isolation comes from the helpers: `CreateTopic` / `CreateQueue` delete what they created when the test ends, so repeat runs (`-count=N`) start from empty resources. They also fail a test that reuses a name another live test holds. Each package's `TestMain` calls `fixtures.TerminateSharedFloci()`. The container runs (region `fixtures.FlociRegion` = `us-east-1`, account `000000000000`, static `test` / `test` credentials) and the returned `*fixtures.Floci` provides ready SNS and SQS clients, the mapped `EndpointURL`, and the helpers `CreateTopic`, `CreateQueue` and `SubscribeQueueToTopic` (raw delivery, as production requires). Tests are skipped under `-short`. Images are digest-pinned; refresh them with `make pin-base-images`.

```go
//go:build integration

func TestSNSPublish(t *testing.T) {
    ctx := context.Background()
    emu := fixtures.StartFloci(ctx, t)
    topicARN := emu.CreateTopic(ctx, t, "orders")
    pub, err := events.NewSNSPublisher(events.SNSConfig{TopicARN: topicARN, Region: fixtures.FlociRegion, EndpointURL: emu.EndpointURL})
    // …
}
```

Use `fixtures.FlociRegion` for every client: floci treats region as an isolation boundary, so a resource created in one region is invisible from another. For manual runs against a long-lived emulator, use `make docker-up` (floci on :4574, floci-ui on http://localhost:4505, demo topology provisioned by `scripts/init-floci.sh`) instead.

---

## Adding a unit test

1. Create or add to a file under `test/unit/<package>/`.
2. No build tag needed.
3. Do not start real AWS services or Postgres — use fixtures (`MockLogger`, `MockPublisher`, `MockConsumer`, `FakeClock`) or stubs.
4. Prefer table-driven tests for exhaustive input coverage.

Example skeleton:

```go
package envelope_test

import (
    "testing"

    "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/events"
    "github.com/stretchr/testify/require"
)

func TestNewEnvelope_IDIsSet(t *testing.T) {
    env := events.NewEnvelope("iam.user.created", "platform-iam", struct{}{})
    require.NotEmpty(t, env.ID)
}
```

---

## Adding an integration test

1. Create or add to a file under `test/integration/`.
2. Add `//go:build integration` at the top of the file.
3. Use `fixtures.StartFloci(ctx, t)` for SNS/SQS, or `fixtures.NewTestDB(ctx, t)` for outbox and inbox tests. It creates a fresh database, with both schemas applied, in the package's shared Postgres container and drops it in `cleanup`. Each package's `TestMain` calls `fixtures.TerminateSharedPostgres()` alongside `TerminateSharedFloci()`.
4. Apply outbox migrations with `outbox.ApplySchema(ctx, runner)` when the test requires the schema.
5. Always `defer cleanup()` immediately after starting containers.

Example skeleton:

```go
//go:build integration

package integration_test

import (
    "context"
    "testing"

    "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/events"
    "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/test/fixtures"
    "github.com/stretchr/testify/require"
)

func TestSNSPublishRoundTrip(t *testing.T) {
    ctx := context.Background()
    emu := fixtures.StartFloci(ctx, t)
    topicARN := emu.CreateTopic(ctx, t, "orders")
    queueURL := emu.CreateQueue(ctx, t, "orders-q")
    emu.SubscribeQueueToTopic(ctx, t, topicARN, queueURL)
    // publish with events.NewSNSPublisher(EndpointURL: emu.EndpointURL), receive, assert
}
```

---

## Coverage measurement

Coverage is measured over `./internal/...` and `./pkg/...` only — the `test/` helpers themselves are excluded. Running `go test ./...` without `-coverpkg` reports 0% for source packages because tests live in a separate tree.

Always use `make cover` or `make cover-func`:

```bash
make cover-func   # per-function summary (quick check)
make cover        # HTML report (open in browser)
```

The CI gate requires **≥95%** combined coverage.

---

## testenv (`test/testenv/`)

`testenv` loads `.env-example` into the process environment for tests that read env vars (e.g. `internal/config` tests). Import it for its side effects:

```go
import _ "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/test/testenv"
```
