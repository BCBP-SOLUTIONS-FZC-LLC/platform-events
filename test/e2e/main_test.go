//go:build e2e

package e2e_test

import (
	"os"
	"testing"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/test/fixtures"
)

// TestMain terminates the package's shared containers — floci
// (fixtures.StartFloci) and Postgres (fixtures.NewTestDB) — after the suite:
// CI runs with the testcontainers reaper disabled.
func TestMain(m *testing.M) {
	code := m.Run()
	fixtures.TerminateSharedFloci()
	fixtures.TerminateSharedPostgres()
	os.Exit(code)
}
