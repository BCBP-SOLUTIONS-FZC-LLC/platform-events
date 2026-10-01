package inbox

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"net/url"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/migrate"
)

//go:embed migrations
var migrationFS embed.FS

// MigrationsTable is the golang-migrate tracking table used exclusively for
// the inbox schema, isolated from the service's own and the outbox's
// ("outbox_migrations") tracking tables.
const MigrationsTable = "inbox_migrations"

// ApplySchema applies the inbox schema (processed_events) using the DSN and
// Logger of runner. runner is not mutated, so it can be reused for domain
// migrations before or after this call. The DSN must point at Postgres
// directly (not PgBouncer): the migration lock is session-scoped.
//
// The schema version recorded in the tracking table must exist in this
// library's embedded migrations: since platform-pgcommon v1.4.1, a database
// migrated by a newer platform-events (e.g. after rolling a service back to
// an older image) makes ApplySchema fail with migrate.ErrVersionNotInSource,
// and an interrupted migration with migrate.ErrMigrationDirty
// (errors.Is works on the returned error). Roll back by migrating the inbox
// schema down first, or run ApplySchema as a separate migration job rather
// than at service startup.
func ApplySchema(ctx context.Context, runner *migrate.Runner) error {
	if runner == nil {
		return fmt.Errorf("inbox: ApplySchema requires a non-nil migrate.Runner")
	}
	if runner.DSN == "" {
		return fmt.Errorf("inbox: ApplySchema requires a non-empty DSN on the migrate.Runner")
	}
	sub, err := fs.Sub(migrationFS, "migrations")
	if err != nil {
		return err
	}
	dsn, err := withMigrationsTable(runner.DSN)
	if err != nil {
		return fmt.Errorf("inbox: ApplySchema could not set migrations table in DSN: %w", err)
	}
	return (&migrate.Runner{FS: sub, DSN: dsn, Logger: runner.Logger}).Up(ctx)
}

// withMigrationsTable sets x-migrations-table=MigrationsTable on dsn, so
// golang-migrate tracks the inbox schema separately. An explicit value in the
// DSN is replaced: sharing a tracking table with other migrations would make
// their versions collide.
func withMigrationsTable(dsn string) (string, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return "", err
	}
	q := u.Query()
	q.Set("x-migrations-table", MigrationsTable)
	u.RawQuery = q.Encode()
	return u.String(), nil
}
