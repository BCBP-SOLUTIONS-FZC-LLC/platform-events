//go:build integration || e2e

package fixtures

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/inbox"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/outbox"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/migrate"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"
)

// One Postgres container per test binary (package), with a fresh database per
// NewTestDB call: tests stay fully isolated (own schema, own migration table,
// own pool) without paying a container start each — which used to dominate
// the integration suite's runtime. Call TerminateSharedPostgres from the
// package's TestMain: CI disables the testcontainers reaper (Ryuk).
var sharedPG struct {
	once  sync.Once
	c     testcontainers.Container
	admin *pgcommon.Pool
	base  string // DSN without the database name: postgres://…@host:port
	err   error
	seq   atomic.Int64
}

const pgStartupTimeout = 2 * time.Minute

// testDBName turns a test name into a valid, unique database name.
var nonIdent = regexp.MustCompile(`[^a-z0-9_]+`)

func testDBName(t *testing.T) string {
	name := nonIdent.ReplaceAllString(strings.ToLower(t.Name()), "_")
	prefix := fmt.Sprintf("t%d_", sharedPG.seq.Add(1))
	if len(prefix)+len(name) > 63 { // Postgres identifier limit
		name = name[:63-len(prefix)]
	}
	return prefix + name
}

func startSharedPostgres() {
	// Not a test's ctx: the container outlives every test in the package.
	ctx, cancel := context.WithTimeout(context.Background(), pgStartupTimeout)
	defer cancel()
	req := testcontainers.ContainerRequest{
		Image:        "postgres:16-alpine@sha256:721873c34ceb9f8d8fc265984940dc982404c105f19ad51be9fdc5970a6080ea", // digest-pinned; make pin-base-images
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "postgres",
			"POSTGRES_PASSWORD": "postgres",
			"POSTGRES_DB":       "postgres",
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).
			WithStartupTimeout(pgStartupTimeout),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	if err != nil {
		sharedPG.err = fmt.Errorf("failed to start container: %w", err)
		return
	}
	sharedPG.c = c
	host, err := c.Host(ctx)
	if err != nil {
		sharedPG.err = fmt.Errorf("failed to get container host: %w", err)
		return
	}
	port, err := c.MappedPort(ctx, "5432")
	if err != nil {
		sharedPG.err = fmt.Errorf("failed to get mapped port: %w", err)
		return
	}
	sharedPG.base = fmt.Sprintf("postgres://postgres:postgres@%s:%s", host, port.Port())
	sharedPG.admin, err = pgcommon.NewPool(ctx, pgcommon.Config{DSN: sharedPG.base + "/postgres?sslmode=disable", MaxConns: 4})
	if err != nil {
		sharedPG.err = fmt.Errorf("failed to connect as admin: %w", err)
	}
}

// adminExec runs a statement that cannot run in a transaction (CREATE /
// DROP DATABASE) on the shared container's maintenance database.
func adminExec(ctx context.Context, sql string) error {
	return sharedPG.admin.WithConn(ctx, func(ctx context.Context, conn *pgcommon.Conn) error {
		_, err := conn.Exec(ctx, sql)
		return err
	})
}

// TerminateSharedPostgres stops the package's shared Postgres container, if
// one was started. Call it from TestMain after m.Run.
func TerminateSharedPostgres() {
	if sharedPG.admin != nil {
		sharedPG.admin.Close()
	}
	if sharedPG.c != nil {
		_ = sharedPG.c.Terminate(context.Background())
	}
}

// NewTestDB creates a fresh database in the package's shared Postgres 16
// container, applies the outbox and inbox schemas, and returns a
// *pgcommon.Pool on it along with a cleanup function that closes the pool and
// drops the database.
//
// The test is skipped automatically when testing.Short() is true.
func NewTestDB(ctx context.Context, t *testing.T) (*pgcommon.Pool, func()) {
	t.Helper()
	return NewTestDBWithConfig(ctx, t, func(*pgcommon.Config) {})
}

// NewEmptyTestDB creates a fresh database with no schema on the shared
// Postgres container and returns its DSN and a function that drops it.
func NewEmptyTestDB(ctx context.Context, t *testing.T) (dsn string, drop func()) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping integration test: -short flag is set (Docker not required)")
	}
	sharedPG.once.Do(startSharedPostgres)
	if sharedPG.err != nil {
		t.Fatalf("fixtures.NewEmptyTestDB: %v", sharedPG.err)
	}
	db := testDBName(t)
	if err := adminExec(ctx, "CREATE DATABASE "+db); err != nil {
		t.Fatalf("fixtures.NewEmptyTestDB: create database %s: %v", db, err)
	}
	drop = func() {
		// FORCE (Postgres 13+) terminates connections a test left open, e.g.
		// after deliberately closing its pool mid-test.
		_ = adminExec(context.Background(), "DROP DATABASE IF EXISTS "+db+" WITH (FORCE)")
	}
	return sharedPG.base + "/" + db + "?sslmode=disable", drop
}

// NewTestDBWithConfig is NewTestDB with a Config that configure may mutate
// (e.g. setting PGBouncerMode: true to reproduce SimpleProtocol exec-mode
// behaviour) before connecting.
func NewTestDBWithConfig(ctx context.Context, t *testing.T, configure func(cfg *pgcommon.Config)) (*pgcommon.Pool, func()) {
	t.Helper()

	if testing.Short() {
		t.Skip("skipping integration test: -short flag is set (Docker not required)")
	}
	sharedPG.once.Do(startSharedPostgres)
	if sharedPG.err != nil {
		t.Fatalf("fixtures.NewTestDBWithConfig: %v", sharedPG.err)
	}

	dsn, drop := NewEmptyTestDB(ctx, t)

	runner := &migrate.Runner{DSN: dsn}
	if err := outbox.ApplySchema(ctx, runner); err != nil {
		drop()
		t.Fatalf("fixtures.NewTestDBWithConfig: failed to apply outbox schema: %v", err)
	}
	if err := inbox.ApplySchema(ctx, runner); err != nil {
		drop()
		t.Fatalf("fixtures.NewTestDBWithConfig: failed to apply inbox schema: %v", err)
	}

	cfg := pgcommon.Config{DSN: dsn}
	configure(&cfg)

	pool, err := pgcommon.NewPool(ctx, cfg)
	if err != nil {
		drop()
		t.Fatalf("fixtures.NewTestDBWithConfig: failed to create pool: %v", err)
	}

	cleanup := func() {
		pool.Close()
		drop()
	}
	return pool, cleanup
}
