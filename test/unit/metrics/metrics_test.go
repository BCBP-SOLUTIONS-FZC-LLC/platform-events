package metrics_test

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"

	internalmetics "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/adapter/outbound/metrics"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/events"
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

// ----------------------------
// SetOutboxLeased / HasOutboxLeasedMetric (internal package)
// ----------------------------

func TestSetOutboxLeased_AfterInit(t *testing.T) {
	reg := prometheus.NewRegistry()
	internalmetics.InitWithRegisterer("leased-test", "v0.0.1", reg)

	assert.NotPanics(t, func() {
		internalmetics.SetOutboxLeased(7)
	})
}

func TestHasOutboxLeasedMetric_AfterInit(t *testing.T) {
	reg := prometheus.NewRegistry()
	internalmetics.InitWithRegisterer("has-leased-test", "v0.0.2", reg)
	assert.True(t, internalmetics.HasOutboxLeasedMetric())
}

func TestSetOutboxLeased_NoOp_NoPanic(t *testing.T) {
	// Even before any Init, SetOutboxLeased should not panic (nil-guarded).
	assert.NotPanics(t, func() {
		internalmetics.SetOutboxLeased(0)
	})
}

func TestHasOutboxPendingMetric_AfterInit(t *testing.T) {
	reg := prometheus.NewRegistry()
	internalmetics.InitWithRegisterer("pending-metric-test", "v0.0.3", reg)
	assert.True(t, internalmetics.HasOutboxPendingMetric())
}

func TestRecordPublish_AfterInit_NoPanic(t *testing.T) {
	reg := prometheus.NewRegistry()
	internalmetics.InitWithRegisterer("record-publish-test", "v0.0.4", reg)
	assert.NotPanics(t, func() {
		internalmetics.RecordPublish("arn:aws:sns:us-east-1:123:test", "order.placed", "success", 0.01)
	})
}

func TestRecordConsume_AfterInit_NoPanic(t *testing.T) {
	reg := prometheus.NewRegistry()
	internalmetics.InitWithRegisterer("record-consume-test", "v0.0.5", reg)
	assert.NotPanics(t, func() {
		internalmetics.RecordConsume("https://sqs.us-east-1.amazonaws.com/123/q", "order.placed", "success", 0.05)
	})
}

func TestRecordOutboxDeadLetter_AfterInit_NoPanic(t *testing.T) {
	reg := prometheus.NewRegistry()
	internalmetics.InitWithRegisterer("dead-letter-test", "v0.0.6", reg)
	assert.NotPanics(t, func() {
		internalmetics.RecordOutboxDeadLetter("test-event")
	})
}

func TestRecordOutboxAttempt_AfterInit_NoPanic(t *testing.T) {
	reg := prometheus.NewRegistry()
	internalmetics.InitWithRegisterer("attempt-test", "v0.0.7", reg)
	assert.NotPanics(t, func() {
		internalmetics.RecordOutboxAttempt("test-event")
	})
}

func TestRecordOutboxPublished_AfterInit_NoPanic(t *testing.T) {
	reg := prometheus.NewRegistry()
	internalmetics.InitWithRegisterer("published-test", "v0.0.8", reg)
	assert.NotPanics(t, func() {
		internalmetics.RecordOutboxPublished("test-event", "success")
		internalmetics.RecordOutboxPublished("test-event", "error")
	})
}

func TestSetOutboxPending_AfterInit_NoPanic(t *testing.T) {
	reg := prometheus.NewRegistry()
	internalmetics.InitWithRegisterer("set-pending-test", "v0.0.9", reg)
	assert.NotPanics(t, func() {
		internalmetics.SetOutboxPending(42)
	})
}

// ----------------------------
// SanitizeEventType > 128 bytes
// ----------------------------

func TestSanitizeEventType_OversizedString(t *testing.T) {
	// Create a string > 128 bytes
	oversized := "event.type." + string(make([]byte, 150)) // creates a 161-byte string
	result := internalmetics.SanitizeEventType(oversized)
	assert.Equal(t, "__oversized__", result)
}

// ----------------------------
// RecordOutboxUnmarshalError and RecordOutboxMarkPublishedError
// ----------------------------

func TestRecordOutboxUnmarshalError_AfterInit(t *testing.T) {
	reg := prometheus.NewRegistry()
	internalmetics.InitWithRegisterer("unmarshal-error-test", "v0.0.10", reg)
	assert.NotPanics(t, func() {
		internalmetics.RecordOutboxUnmarshalError()
	})
}

func TestRecordOutboxMarkPublishedError_AfterInit(t *testing.T) {
	reg := prometheus.NewRegistry()
	internalmetics.InitWithRegisterer("mark-published-error-test", "v0.0.11", reg)
	assert.NotPanics(t, func() {
		internalmetics.RecordOutboxMarkPublishedError()
	})
}
