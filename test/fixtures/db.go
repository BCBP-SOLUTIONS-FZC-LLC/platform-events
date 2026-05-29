//go:build integration

package fixtures

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/outbox"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/migrate"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/pgcommon"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// NewTestDB starts a throwaway Postgres 16 container, applies the outbox schema,
// and returns a *pgcommon.Pool along with a cleanup function.
//
// The test is skipped automatically when testing.Short() is true.
func NewTestDB(ctx context.Context, t *testing.T) (*pgcommon.Pool, func()) {
	t.Helper()

	if testing.Short() {
		t.Skip("skipping integration test: -short flag is set (Docker not required)")
	}

	req := testcontainers.ContainerRequest{
		Image:        "postgres:16-alpine",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "postgres",
			"POSTGRES_PASSWORD": "postgres",
			"POSTGRES_DB":       "testdb",
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).
			WithStartupTimeout(120 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("fixtures.NewTestDB: failed to start container: %v", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		_ = container.Terminate(ctx)
		t.Fatalf("fixtures.NewTestDB: failed to get container host: %v", err)
	}

	mappedPort, err := container.MappedPort(ctx, "5432")
	if err != nil {
		_ = container.Terminate(ctx)
		t.Fatalf("fixtures.NewTestDB: failed to get mapped port: %v", err)
	}

	dsn := fmt.Sprintf("postgres://postgres:postgres@%s:%s/testdb?sslmode=disable",
		host, mappedPort.Port())

	// Apply outbox schema via migrate runner.
	runner := &migrate.Runner{DSN: dsn}
	if err := outbox.ApplySchema(ctx, runner); err != nil {
		_ = container.Terminate(ctx)
		t.Fatalf("fixtures.NewTestDB: failed to apply outbox schema: %v", err)
	}

	pool, err := pgcommon.NewPool(ctx, pgcommon.Config{DSN: dsn})
	if err != nil {
		_ = container.Terminate(ctx)
		t.Fatalf("fixtures.NewTestDB: failed to create pool: %v", err)
	}

	cleanup := func() {
		pool.Close()
		_ = container.Terminate(context.Background())
	}

	return pool, cleanup
}
