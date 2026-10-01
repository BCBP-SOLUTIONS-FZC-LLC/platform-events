package sqs_test

import (
	"os"
	"testing"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/test/fixtures"
)

// TestMain registers the Tier 1 metrics once before all tests in this
// package, so every recording path runs against registered collectors.
func TestMain(m *testing.M) {
	fixtures.MustInitPlatformMetrics()
	os.Exit(m.Run())
}
