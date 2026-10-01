# Quick start

End-to-end wiring of the publisher, outbox and consumer in a service. One of the detailed guides linked from the [project README](../../README.md#contributing).

---

## Quick start

```go
import (
    "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/config"
    "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/events"
    "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/outbox"
    "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/migrate"
    "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"
    "github.com/prometheus/client_golang/prometheus"
)

func main() {
    ctx := context.Background()

    // 1. Register the Tier 1 platform_* metrics (Enterprise Platform Observability
    //    Standard) once — same registerer and identity as pgmetrics.InitWithIdentity.
    //    Environment defaults to APP_ENV / ENVIRONMENT / "dev"; service to APP_NAME.
    id := events.MetricsIdentityFromEnv("iam", "")
    metricWarnings, err := events.InitMetrics(id, prometheus.DefaultRegisterer)
    if err != nil {
        log.Fatal(err)
    }
    for _, w := range metricWarnings {
        log.Println(w) // a platform_* metric the registry refused — disabled, not fatal
    }

    // 2. Load config. Database settings come from platform-pgcommon's
    //    ConfigFromEnv (DATABASE_URL or PG_*, PG_MAX_CONNS, PG_STATEMENT_TIMEOUT, …).
    outboxEnv := config.LoadOutbox()
    config.LogWarnings(outboxEnv.Warnings)
    if err := outboxEnv.Validate(); err != nil {
        log.Fatal(err)
    }

    // 3. Open the connection pool for the outbox runner.
    dbCfg := outboxEnv.DB
    dbCfg.GUCProvider = pgcommon.GUCSetFromContext
    pool, err := pgcommon.NewPool(ctx, dbCfg)
    if err != nil {
        log.Fatal(err)
    }
    defer pool.Close()

    // 4. Apply the outbox schema migration (MIGRATION_DATABASE_URL when set).
    migrateRunner := &migrate.Runner{DSN: outboxEnv.MigrationDatabaseURL}
    if err := outbox.ApplySchema(ctx, migrateRunner); err != nil {
        log.Fatal(err)
    }

    // 5. Construct the SNS publisher.
    snsEnv := config.LoadSNS()
    publisher, err := events.NewSNSPublisher(config.SNSConfigFromEnv(snsEnv, logger))
    if err != nil {
        log.Fatal(err)
    }

    // 6. Start the outbox runner (delivers events asynchronously).
    runner, err := outbox.NewRunner(config.RunnerConfigFromEnv(outboxEnv, pool, publisher, logger))
    if err != nil {
        log.Fatal(err)
    }
    go runner.Start(ctx)
    defer runner.Stop()

    // 7. Construct and start the SQS consumer.
    sqsEnv := config.LoadSQS()
    config.LogWarnings(sqsEnv.Warnings)
    consumer, err := events.NewSQSConsumer(
        config.SQSConfigFromEnv(sqsEnv, logger),
        func(ctx context.Context, env events.Envelope[json.RawMessage]) error {
            // handle event...
            return nil
        },
        config.SQSConsumerOptions(sqsEnv)...,
    )
    if err != nil {
        log.Fatal(err)
    }
    go consumer.Start(ctx)
    defer consumer.Stop()
}
```

