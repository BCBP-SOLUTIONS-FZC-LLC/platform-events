package sqs_test

import (
	"os"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/adapter/outbound/metrics"
)

// TestMain initialises metrics once before all tests in this package so that
// dispatch goroutines always see non-nil metric globals, avoiding a data race
// between goroutines reading the globals and test functions writing them via
// metrics.InitWithRegisterer.
func TestMain(m *testing.M) {
	metrics.InitWithRegisterer("sqs-unit-test", "v0.0.0-test", prometheus.NewRegistry())
	os.Exit(m.Run())
}
