package metrics_test

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	internalmetics "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/internal/adapter/outbound/metrics"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/events"
)

// initPlatform registers the Tier 1 metrics on a fresh registry for t and
// returns the active set; the previous set is restored when t ends.
func initPlatform(t *testing.T) *internalmetics.Platform {
	t.Helper()
	isolatePlatform(t)
	warnings, err := events.InitMetrics(testIdentity, prometheus.NewRegistry())
	require.NoError(t, err)
	require.Empty(t, warnings)
	p := internalmetics.CurrentPlatform()
	require.NotNil(t, p)
	return p
}

func TestInitMetrics_RequiresIdentity(t *testing.T) {
	isolatePlatform(t)
	_, err := events.InitMetrics(events.MetricsIdentity{}, prometheus.NewRegistry())
	require.Error(t, err, "the identity is mandatory")
}

// ----------------------------
// Outbox gauges
// ----------------------------

func TestSetOutboxLeased_AfterInit(t *testing.T) {
	p := initPlatform(t)
	internalmetics.SetOutboxLeased(7)
	assert.InDelta(t, 7, testutil.ToFloat64(p.OutboxLeased.WithLabelValues()), 0)
	assert.True(t, internalmetics.HasOutboxLeasedMetric())
}

func TestSetOutboxLeased_NoOp_NoPanic(t *testing.T) {
	isolatePlatform(t)
	internalmetics.ReplacePlatform(nil)
	assert.NotPanics(t, func() {
		internalmetics.SetOutboxLeased(0)
	})
}

func TestSetOutboxPending_AfterInit(t *testing.T) {
	p := initPlatform(t)
	assert.True(t, internalmetics.HasOutboxPendingMetric())
	internalmetics.SetOutboxPending(42)
	assert.InDelta(t, 42, testutil.ToFloat64(p.OutboxPending.WithLabelValues()), 0)

	// A failed count (-1) keeps the last value and counts the error.
	internalmetics.SetOutboxPending(-1)
	assert.InDelta(t, 42, testutil.ToFloat64(p.OutboxPending.WithLabelValues()), 0)
	assert.InDelta(t, 1, testutil.ToFloat64(p.OutboxErrors.WithLabelValues("pending_count")), 0)
}

// ----------------------------
// Producer / outbox counters
// ----------------------------

func TestIncPublished_AfterInit(t *testing.T) {
	p := initPlatform(t)
	internalmetics.IncPublished("arn:aws:sns:us-east-1:123:test", "order.placed", "success")
	internalmetics.IncPublished("arn:aws:sns:us-east-1:123:test", "order.placed", "error")
	assert.InDelta(t, 1, testutil.ToFloat64(p.MessagesPublished.WithLabelValues("test", "order.placed", "success")), 0, "topic is the topic name")
	assert.InDelta(t, 1, testutil.ToFloat64(p.MessagesPublished.WithLabelValues("test", "order.placed", "error")), 0)
}

func TestRecordOutboxDeadLetter_AfterInit(t *testing.T) {
	p := initPlatform(t)
	internalmetics.RecordOutboxDeadLetter("test-event")
	assert.InDelta(t, 1, testutil.ToFloat64(p.DLQMessages.WithLabelValues("outbox_publish", "test-event", "max_attempts")), 0)
}

func TestIncOutboxPublishAttempt_AfterInit(t *testing.T) {
	p := initPlatform(t)
	internalmetics.IncOutboxPublishAttempt("test-event", "success")
	internalmetics.IncOutboxPublishAttempt("test-event", "error")
	assert.InDelta(t, 1, testutil.ToFloat64(p.OutboxAttempts.WithLabelValues("test-event", "success")), 0)
	assert.InDelta(t, 1, testutil.ToFloat64(p.OutboxAttempts.WithLabelValues("test-event", "error")), 0)
}

func TestRecordOutboxErrors_AfterInit(t *testing.T) {
	p := initPlatform(t)
	internalmetics.RecordOutboxUnmarshalError()
	internalmetics.RecordOutboxMarkPublishedError()
	internalmetics.RecordOutboxPollError()
	internalmetics.RecordOutboxLeasedCountError()
	for _, op := range []string{"unmarshal", "mark_published", "poll", "leased_count"} {
		assert.InDelta(t, 1, testutil.ToFloat64(p.OutboxErrors.WithLabelValues(op)), 0, op)
	}
}

// ----------------------------
// SanitizeEventType > 128 bytes
// ----------------------------

func TestSanitizeEventType_OversizedString(t *testing.T) {
	oversized := "event.type." + string(make([]byte, 150)) // a 161-byte string
	assert.Equal(t, "__oversized__", internalmetics.SanitizeEventType(oversized))
}

func TestSanitizeEventType_OversizedCounter_Increments(t *testing.T) {
	p := initPlatform(t)
	oversized := "iam.user.created." + string(make([]byte, 200))
	assert.Equal(t, "__oversized__", internalmetics.SanitizeEventType(oversized))
	assert.InDelta(t, 1, testutil.ToFloat64(p.LabelOverflow.WithLabelValues("event_type")), 0)
}

// ----------------------------
// Dead-letter operations guard paths
// ----------------------------

func TestRecordOutboxDeadLettersReprocessed(t *testing.T) {
	p := initPlatform(t)
	internalmetics.RecordOutboxDeadLettersReprocessed(0)  // zero is not a reprocess event
	internalmetics.RecordOutboxDeadLettersReprocessed(-1) // nor is a negative count
	assert.Zero(t, testutil.ToFloat64(p.OutboxDLOperations.WithLabelValues("reprocess")))
	internalmetics.RecordOutboxDeadLettersReprocessed(3)
	assert.InDelta(t, 3, testutil.ToFloat64(p.OutboxDLOperations.WithLabelValues("reprocess")), 0)
}

func TestRecordOutboxDeadLettersDiscarded(t *testing.T) {
	p := initPlatform(t)
	internalmetics.RecordOutboxDeadLettersDiscarded(0)
	internalmetics.RecordOutboxDeadLettersDiscarded(-1)
	assert.Zero(t, testutil.ToFloat64(p.OutboxDLOperations.WithLabelValues("discard")))
	internalmetics.RecordOutboxDeadLettersDiscarded(5)
	assert.InDelta(t, 5, testutil.ToFloat64(p.OutboxDLOperations.WithLabelValues("discard")), 0)
}
