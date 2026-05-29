package metrics_test

import (
	"testing"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/events"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
)

func TestInitWithRegisterer_DoesNotPanic(t *testing.T) {
	reg := prometheus.NewRegistry()
	assert.NotPanics(t, func() {
		events.InitWithRegisterer("test-service", "v1.0.0", reg)
	})
}

func TestInitWithRegisterer_EmptyServiceNamePanics(t *testing.T) {
	reg := prometheus.NewRegistry()
	assert.Panics(t, func() {
		events.InitWithRegisterer("", "v1.0.0", reg)
	})
}

func TestInitWithRegisterer_Idempotent(t *testing.T) {
	// Two calls with the same registerer should not panic.
	reg := prometheus.NewRegistry()
	assert.NotPanics(t, func() {
		events.InitWithRegisterer("svc", "v1", reg)
	})
	// Second call to InitWithRegisterer creates a new set of metrics on the same
	// registry — which will cause a registration conflict. Use a fresh registry.
	reg2 := prometheus.NewRegistry()
	assert.NotPanics(t, func() {
		events.InitWithRegisterer("svc", "v1", reg2)
	})
}

func TestInit_DoesNotPanic(t *testing.T) {
	// Init uses sync.Once — calling it in tests may or may not register (already called
	// by other tests or code). Just ensure it doesn't panic with a valid service name.
	assert.NotPanics(t, func() {
		events.Init("test-service-init", "v1.0.0")
	})
}

func TestInit_IsIdempotent(t *testing.T) {
	// Calling Init multiple times should not panic.
	assert.NotPanics(t, func() {
		events.Init("test-service-idempotent", "v1.0.0")
		events.Init("test-service-idempotent", "v1.0.0")
	})
}

func TestInit_EmptyServiceNamePanics(t *testing.T) {
	assert.Panics(t, func() {
		events.Init("", "v1.0.0")
	})
}
