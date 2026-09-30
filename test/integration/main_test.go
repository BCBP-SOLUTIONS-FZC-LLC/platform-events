//go:build integration

package integration_test

import (
	"os"
	"testing"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/test/fixtures"
)

// TestMain terminates the package's shared floci container (fixtures.StartFloci)
// after the suite: CI runs with the testcontainers reaper disabled.
func TestMain(m *testing.M) {
	code := m.Run()
	fixtures.TerminateSharedFloci()
	os.Exit(code)
}
