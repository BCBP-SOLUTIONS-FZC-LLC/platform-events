// Package outbox provides the transactional outbox pattern for platform services.
package outbox

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"net/url"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/migrate"
)

//go:embed migrations
var migrationFS embed.FS

// MigrationsTable is the golang-migrate tracking table used exclusively by the
// outbox runner. Isolating it from the consuming service's "schema_migrations"
// table (and from platform-pgcommon's "pgcommon_migrations" table) prevents
// version-number collisions and dirty-state cascades when multiple runners share
// the same database.
const MigrationsTable = "outbox_migrations"

// ApplySchema applies the outbox schema migrations using DSN and Logger from the
// provided migrate.Runner. The provided runner is NOT mutated — a new runner is
// constructed internally with the embedded outbox FS and a dedicated tracking
// table ([MigrationsTable]) so the caller can safely reuse their runner for
// domain migrations before or after this call.
//
//	migrateRunner := &migrate.Runner{DSN: cfg.DatabaseURL, Logger: logger}
//	if err := outbox.ApplySchema(ctx, migrateRunner); err != nil {
//	    log.Fatal(err)
//	}
//	// migrateRunner.FS is unchanged — safe to reuse for domain migrations.
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
	dsn, err := dsnWithMigrationsTable(runner.DSN, MigrationsTable)
	if err != nil {
		return fmt.Errorf("outbox: ApplySchema could not set migrations table in DSN: %w", err)
	}
	outboxRunner := &migrate.Runner{
		FS:     sub,
		DSN:    dsn,
		Logger: runner.Logger,
	}
	return outboxRunner.Up(ctx)
}

// dsnWithMigrationsTable sets x-migrations-table=table on the DSN so that
// golang-migrate's pgx/v5 driver tracks the outbox schema in its own table. An
// explicit value already in the DSN (typically the service's own tracking
// table) is replaced: sharing it would make outbox versions 1–9 collide with
// the service's migration versions. Same rule as inbox.ApplySchema.
func dsnWithMigrationsTable(dsn, table string) (string, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return "", err
	}
	q := u.Query()
	q.Set("x-migrations-table", table)
	u.RawQuery = q.Encode()
	return u.String(), nil
}
