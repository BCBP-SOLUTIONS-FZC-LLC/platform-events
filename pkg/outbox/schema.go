// Package outbox provides the transactional outbox pattern for platform services.
package outbox

import (
	"context"
	"embed"
	"fmt"
	"io/fs"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/migrate"
)

//go:embed migrations
var migrationFS embed.FS

// ApplySchema applies the outbox schema migrations using DSN and Logger from the
// provided migrate.Runner. The provided runner is NOT mutated — a new runner is
// constructed internally with the embedded outbox FS so the caller can safely
// reuse their runner for domain migrations before or after this call.
//
//	migrateRunner := &migrate.Runner{DSN: cfg.DatabaseURL, Logger: logger}
//	if err := outbox.ApplySchema(ctx, migrateRunner); err != nil {
//	    log.Fatal(err)
//	}
//	// migrateRunner.FS is unchanged — safe to call migrateRunner.Up for domain schema.
func ApplySchema(ctx context.Context, runner *migrate.Runner) error {
	if runner == nil {
		return fmt.Errorf("outbox: ApplySchema requires a non-nil migrate.Runner")
	}
	if runner.DSN == "" {
		return fmt.Errorf("outbox: ApplySchema requires a non-empty DSN on the migrate.Runner")
	}
	sub, err := fs.Sub(migrationFS, "migrations")
	if err != nil {
		return err
	}
	outboxRunner := &migrate.Runner{
		FS:     sub,
		DSN:    runner.DSN,
		Logger: runner.Logger,
	}
	return outboxRunner.Up(ctx)
}
