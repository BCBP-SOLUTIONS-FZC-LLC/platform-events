package fixtures

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/internal/adapter/outbound/metrics"
)

// MetricsIdentity is the identity the test suites register the Tier 1
// metrics with.
var MetricsIdentity = metrics.Identity{Domain: "iam", Service: "event-consumer", Environment: "test"}

// InitPlatformMetrics registers the Tier 1 platform_* metrics on a fresh
// registry for one test, makes them the active set and restores the previous
// set when t ends. It returns the registry to gather from.
func InitPlatformMetrics(t testing.TB) *prometheus.Registry {
	t.Helper()
	prev := metrics.CurrentPlatform()
	t.Cleanup(func() { metrics.ReplacePlatform(prev) })
	reg := prometheus.NewRegistry()
	warnings, err := metrics.InitWithIdentity(MetricsIdentity, reg)
	if err != nil {
		t.Fatalf("metrics.InitWithIdentity: %v", err)
	}
	if len(warnings) > 0 {
		t.Fatalf("metrics.InitWithIdentity warnings: %v", warnings)
	}
	return reg
}

// MustInitPlatformMetrics registers the Tier 1 metrics on a fresh registry
// for a whole test binary (TestMain), so recording paths always run against
// registered collectors. It panics on failure.
func MustInitPlatformMetrics() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	if _, err := metrics.InitWithIdentity(MetricsIdentity, reg); err != nil {
		panic(err)
	}
	return reg
}
