package events

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/adapter/outbound/metrics"
)

// Init registers Prometheus metrics using the default registerer.
// Panics if serviceName is empty.
// Safe to call multiple times — only the first call registers metrics.
func Init(serviceName, buildVersion string) {
	metrics.Init(serviceName, buildVersion)
}

// InitWithRegisterer registers metrics using the provided registerer.
// Bypasses the sync.Once guard — intended for test isolation with separate registries.
func InitWithRegisterer(serviceName, buildVersion string, reg prometheus.Registerer) {
	metrics.InitWithRegisterer(serviceName, buildVersion, reg)
}
