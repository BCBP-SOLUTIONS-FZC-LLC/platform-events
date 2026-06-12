// Package metrics registers Prometheus metrics for the platform-events library.
package metrics

import (
	"sync"
	"unicode/utf8"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Global metric variables, initialised once by Init or InitWithRegisterer.
var (
	EventsPublishedTotal              *prometheus.CounterVec
	EventsPublishDuration             *prometheus.HistogramVec
	EventsConsumedTotal               *prometheus.CounterVec
	EventsConsumeDuration             *prometheus.HistogramVec
	OutboxPendingTotal                *prometheus.GaugeVec
	OutboxPublishedTotal              *prometheus.CounterVec
	OutboxAttemptsTotal               *prometheus.CounterVec
	OutboxDeadLettersTotal            *prometheus.CounterVec
	OutboxDeadLettersReprocessedTotal *prometheus.CounterVec
	OutboxLeasedTotal                 *prometheus.GaugeVec

	// SQS-level infrastructure error counters.
	SQSReceiveErrorsTotal    *prometheus.CounterVec
	SQSDeleteErrorsTotal     *prometheus.CounterVec
	SQSVisibilityErrorsTotal *prometheus.CounterVec

	// Outbox infrastructure error counters.
	OutboxPollErrorsTotal          *prometheus.CounterVec
	OutboxUnmarshalErrorsTotal     *prometheus.CounterVec
	OutboxMarkPublishedErrorsTotal *prometheus.CounterVec

	// OversizedEventTypeLabelTotal counts calls where an event_type value exceeded
	// maxEventTypeLabelLen and was replaced with "__oversized__". Alert when non-zero
	// to detect misconfigured or adversarial producers generating unbounded label values.
	OversizedEventTypeLabelTotal *prometheus.CounterVec

	initOnce  sync.Once
	metricsMu sync.RWMutex // guards all global metric vars for InitWithRegisterer concurrency
)

// maxEventTypeLabelLen is the maximum byte length of an event_type value used as
// a Prometheus label. Values exceeding this are replaced with "__oversized__" to
// prevent cardinality explosion from misbehaving or adversarial producers.
const maxEventTypeLabelLen = 128

// SanitizeEventType caps event_type to maxEventTypeLabelLen bytes and returns
// "__oversized__" for inputs that exceed the limit. Exported so callers outside
// this package (consumer.go, publisher.go) apply the same cap before metric calls.
// Increments OversizedEventTypeLabelTotal when truncation occurs so operators can
// detect misconfigured or adversarial producers via a non-zero counter rate.
func SanitizeEventType(s string) string {
	if utf8.RuneCountInString(s) == 0 {
		return "unknown"
	}
	if len(s) <= maxEventTypeLabelLen {
		return s
	}
	recordOversizedLabel()
	return "__oversized__"
}

func recordOversizedLabel() {
	metricsMu.RLock()
	c := OversizedEventTypeLabelTotal
	metricsMu.RUnlock()
	if c != nil {
		c.WithLabelValues().Inc()
	}
}

// Init registers metrics using the default Prometheus registerer.
// Panics if serviceName is empty (prevents invalid label cardinality).
// Safe to call multiple times — only the first call registers metrics.
func Init(serviceName, buildVersion string) {
	if serviceName == "" {
		panic("platform-events: metrics.Init requires a non-empty serviceName")
	}
	initOnce.Do(func() {
		// Acquire metricsMu so a concurrent InitWithRegisterer call (which also
		// acquires it) cannot write globals simultaneously.
		metricsMu.Lock()
		defer metricsMu.Unlock()
		initMetricsWithRegisterer(serviceName, buildVersion, prometheus.DefaultRegisterer)
	})
}

// InitWithRegisterer registers metrics using the provided Prometheus registerer.
// Bypasses the sync.Once guard — intended for test isolation with separate registries.
// Must not be called concurrently with any Publish, Consume, or outbox operation.
func InitWithRegisterer(serviceName, buildVersion string, reg prometheus.Registerer) {
	if serviceName == "" {
		panic("platform-events: metrics.InitWithRegisterer requires a non-empty serviceName")
	}
	metricsMu.Lock()
	defer metricsMu.Unlock()
	initMetricsWithRegisterer(serviceName, buildVersion, reg)
}

func initMetricsWithRegisterer(serviceName, buildVersion string, reg prometheus.Registerer) {
	factory := promauto.With(reg)

	// Build-info gauge: exposes service name and library version as a labelled
	// gauge fixed at 1. Standard pattern for querying version in Prometheus/Grafana.
	factory.NewGaugeVec(prometheus.GaugeOpts{
		Name: "platform_events_build_info",
		Help: "Build information for the platform-events library.",
	}, []string{"service", "version"}).
		WithLabelValues(serviceName, buildVersion).Set(1)

	EventsPublishedTotal = factory.NewCounterVec(prometheus.CounterOpts{
		Name:        "events_published_total",
		Help:        "Total number of events published.",
		ConstLabels: prometheus.Labels{"service": serviceName},
	}, []string{"topic", "event_type", "status"})

	EventsPublishDuration = factory.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "events_publish_duration_seconds",
		Help:        "Duration of event publish operations in seconds.",
		ConstLabels: prometheus.Labels{"service": serviceName},
		Buckets:     []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	}, []string{"topic", "event_type"})

	EventsConsumedTotal = factory.NewCounterVec(prometheus.CounterOpts{
		Name:        "events_consumed_total",
		Help:        "Total number of events consumed.",
		ConstLabels: prometheus.Labels{"service": serviceName},
	}, []string{"queue", "event_type", "status"})

	EventsConsumeDuration = factory.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "events_consume_duration_seconds",
		Help:        "Duration of event consume handler operations in seconds.",
		ConstLabels: prometheus.Labels{"service": serviceName},
		Buckets:     []float64{.025, .05, .1, .25, .5, 1, 2.5, 5, 10, 20, 30},
	}, []string{"queue", "event_type"})

	OutboxPendingTotal = factory.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "outbox_pending_total",
		Help:        "Number of pending (unpublished) outbox records. -1 indicates a stale/error reading.",
		ConstLabels: prometheus.Labels{"service": serviceName},
	}, []string{})

	// event_type label allows alerting per-type dead-letter accumulation.
	OutboxPublishedTotal = factory.NewCounterVec(prometheus.CounterOpts{
		Name:        "outbox_published_total",
		Help:        "Total number of outbox records published.",
		ConstLabels: prometheus.Labels{"service": serviceName},
	}, []string{"event_type", "status"})

	OutboxAttemptsTotal = factory.NewCounterVec(prometheus.CounterOpts{
		Name:        "outbox_attempts_total",
		Help:        "Total number of outbox publish attempts.",
		ConstLabels: prometheus.Labels{"service": serviceName},
	}, []string{"event_type"})

	// OutboxDeadLettersTotal counts records moved to outbox_dead_letters after
	// exhausting MaxAttempts. This is the publish-side failure sink (distinct from
	// the consumption-side SQS DLQ); alert on rate() > 0 to catch undelivered events.
	OutboxDeadLettersTotal = factory.NewCounterVec(prometheus.CounterOpts{
		Name:        "outbox_dead_letters_total",
		Help:        "Total number of outbox records moved to the dead-letter table.",
		ConstLabels: prometheus.Labels{"service": serviceName},
	}, []string{"event_type"})

	// OutboxDeadLettersReprocessedTotal counts records moved back from
	// outbox_dead_letters to outbox_events via ReprocessDeadLetters. Alert on
	// rate() > 0 as a signal of previously-failed events being retried.
	OutboxDeadLettersReprocessedTotal = factory.NewCounterVec(prometheus.CounterOpts{
		Name:        "outbox_dead_letters_reprocessed_total",
		Help:        "Total number of outbox dead-letter records re-queued for redelivery.",
		ConstLabels: prometheus.Labels{"service": serviceName},
	}, []string{})

	// OutboxLeasedTotal is a gauge of records currently claimed by a runner
	// (scheduled_at > NOW()). Combined with OutboxPendingTotal, this gives a
	// complete picture: pending = awaiting pickup, leased = being published.
	OutboxLeasedTotal = factory.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "outbox_leased_total",
		Help:        "Number of outbox records currently leased (claimed) by a runner.",
		ConstLabels: prometheus.Labels{"service": serviceName},
	}, []string{})

	// SQS infrastructure error counters — alert when these are non-zero.
	SQSReceiveErrorsTotal = factory.NewCounterVec(prometheus.CounterOpts{
		Name:        "sqs_receive_errors_total",
		Help:        "Total SQS ReceiveMessage errors (excludes context cancellation).",
		ConstLabels: prometheus.Labels{"service": serviceName},
	}, []string{"queue"})

	SQSDeleteErrorsTotal = factory.NewCounterVec(prometheus.CounterOpts{
		Name:        "sqs_delete_errors_total",
		Help:        "Total SQS DeleteMessage errors. A non-zero rate causes duplicate message delivery.",
		ConstLabels: prometheus.Labels{"service": serviceName},
	}, []string{"queue"})

	SQSVisibilityErrorsTotal = factory.NewCounterVec(prometheus.CounterOpts{
		Name:        "sqs_visibility_extension_errors_total",
		Help:        "Total SQS ChangeMessageVisibility errors. A non-zero rate causes duplicate delivery for long-running handlers.",
		ConstLabels: prometheus.Labels{"service": serviceName},
	}, []string{"queue"})

	// Outbox infrastructure error counters.
	OutboxPollErrorsTotal = factory.NewCounterVec(prometheus.CounterOpts{
		Name:        "outbox_poll_errors_total",
		Help:        "Total outbox poll cycle errors (ClaimBatch / DB errors).",
		ConstLabels: prometheus.Labels{"service": serviceName},
	}, []string{})

	OutboxUnmarshalErrorsTotal = factory.NewCounterVec(prometheus.CounterOpts{
		Name:        "outbox_unmarshal_errors_total",
		Help:        "Total outbox records that failed JSON unmarshal during publish.",
		ConstLabels: prometheus.Labels{"service": serviceName},
	}, []string{})

	OutboxMarkPublishedErrorsTotal = factory.NewCounterVec(prometheus.CounterOpts{
		Name:        "outbox_mark_published_errors_total",
		Help:        "Total failures to mark an outbox record as published after successful SNS delivery. A non-zero rate causes duplicate delivery.",
		ConstLabels: prometheus.Labels{"service": serviceName},
	}, []string{})

	OversizedEventTypeLabelTotal = factory.NewCounterVec(prometheus.CounterOpts{
		Name:        "events_oversized_event_type_label_total",
		Help:        "Total event_type values that exceeded the label length cap and were replaced with __oversized__. A non-zero rate indicates misconfigured or adversarial producers.",
		ConstLabels: prometheus.Labels{"service": serviceName},
	}, []string{})
}

// RecordPublish increments publish counters and records duration.
// Safe to call before Init — no-ops when metrics are not initialised.
func RecordPublish(topic, eventType, status string, durSeconds float64) {
	metricsMu.RLock()
	pt, pd := EventsPublishedTotal, EventsPublishDuration
	metricsMu.RUnlock()
	et := SanitizeEventType(eventType)
	if pt != nil {
		pt.WithLabelValues(topic, et, status).Inc()
	}
	if pd != nil {
		pd.WithLabelValues(topic, et).Observe(durSeconds)
	}
}

// RecordConsume increments consume counters and records duration.
// Safe to call before Init — no-ops when metrics are not initialised.
func RecordConsume(queue, eventType, status string, durSeconds float64) {
	metricsMu.RLock()
	ct, cd := EventsConsumedTotal, EventsConsumeDuration
	metricsMu.RUnlock()
	et := SanitizeEventType(eventType)
	if ct != nil {
		ct.WithLabelValues(queue, et, status).Inc()
	}
	if cd != nil {
		cd.WithLabelValues(queue, et).Observe(durSeconds)
	}
}

// SetOutboxPending updates the pending outbox gauge.
// Pass -1 to signal that the reading is stale due to a query error.
// Safe to call before Init — no-ops when metrics are not initialised.
func SetOutboxPending(n float64) {
	metricsMu.RLock()
	g := OutboxPendingTotal
	metricsMu.RUnlock()
	if g != nil {
		g.WithLabelValues().Set(n)
	}
}

// RecordOutboxDeadLettersReprocessed increments the reprocessed dead-letter
// counter by n (the number of records moved back to outbox_events).
// Safe to call before Init — no-ops when metrics are not initialised.
func RecordOutboxDeadLettersReprocessed(n int) {
	if n <= 0 {
		return
	}
	metricsMu.RLock()
	c := OutboxDeadLettersReprocessedTotal
	metricsMu.RUnlock()
	if c != nil {
		c.WithLabelValues().Add(float64(n))
	}
}

// RecordOutboxDeadLetter increments the dead-letter counter.
// Safe to call before Init — no-ops when metrics are not initialised.
func RecordOutboxDeadLetter(eventType string) {
	metricsMu.RLock()
	c := OutboxDeadLettersTotal
	metricsMu.RUnlock()
	if c != nil {
		c.WithLabelValues(SanitizeEventType(eventType)).Inc()
	}
}

// RecordOutboxAttempt increments the per-record publish attempt counter.
// Safe to call before Init — no-ops when metrics are not initialised.
func RecordOutboxAttempt(eventType string) {
	metricsMu.RLock()
	c := OutboxAttemptsTotal
	metricsMu.RUnlock()
	if c != nil {
		c.WithLabelValues(SanitizeEventType(eventType)).Inc()
	}
}

// RecordOutboxPublished increments the outbox published counter with the
// given event type and status label ("success" or "error").
// Safe to call before Init — no-ops when metrics are not initialised.
func RecordOutboxPublished(eventType, status string) {
	metricsMu.RLock()
	c := OutboxPublishedTotal
	metricsMu.RUnlock()
	if c != nil {
		c.WithLabelValues(SanitizeEventType(eventType), status).Inc()
	}
}

// HasOutboxPendingMetric reports whether the outbox pending gauge is registered.
// Callers use this to skip the PendingCount DB query when metrics are disabled.
func HasOutboxPendingMetric() bool {
	metricsMu.RLock()
	g := OutboxPendingTotal
	metricsMu.RUnlock()
	return g != nil
}

// SetOutboxLeased updates the leased outbox gauge.
// Safe to call before Init — no-ops when metrics are not initialised.
func SetOutboxLeased(n float64) {
	metricsMu.RLock()
	g := OutboxLeasedTotal
	metricsMu.RUnlock()
	if g != nil {
		g.WithLabelValues().Set(n)
	}
}

// HasOutboxLeasedMetric reports whether the outbox leased gauge is registered.
func HasOutboxLeasedMetric() bool {
	metricsMu.RLock()
	g := OutboxLeasedTotal
	metricsMu.RUnlock()
	return g != nil
}

// RecordSQSReceiveError increments the SQS ReceiveMessage error counter.
func RecordSQSReceiveError(queue string) {
	metricsMu.RLock()
	c := SQSReceiveErrorsTotal
	metricsMu.RUnlock()
	if c != nil {
		c.WithLabelValues(queue).Inc()
	}
}

// RecordSQSDeleteError increments the SQS DeleteMessage error counter.
func RecordSQSDeleteError(queue string) {
	metricsMu.RLock()
	c := SQSDeleteErrorsTotal
	metricsMu.RUnlock()
	if c != nil {
		c.WithLabelValues(queue).Inc()
	}
}

// RecordSQSVisibilityError increments the SQS ChangeMessageVisibility error counter.
func RecordSQSVisibilityError(queue string) {
	metricsMu.RLock()
	c := SQSVisibilityErrorsTotal
	metricsMu.RUnlock()
	if c != nil {
		c.WithLabelValues(queue).Inc()
	}
}

// RecordOutboxPollError increments the outbox poll cycle error counter.
func RecordOutboxPollError() {
	metricsMu.RLock()
	c := OutboxPollErrorsTotal
	metricsMu.RUnlock()
	if c != nil {
		c.WithLabelValues().Inc()
	}
}

// RecordOutboxUnmarshalError increments the outbox unmarshal error counter.
func RecordOutboxUnmarshalError() {
	metricsMu.RLock()
	c := OutboxUnmarshalErrorsTotal
	metricsMu.RUnlock()
	if c != nil {
		c.WithLabelValues().Inc()
	}
}

// RecordOutboxMarkPublishedError increments the counter for MarkPublished failures
// that occur after a successful SNS publish. Each increment signals a potential
// duplicate delivery on the next poll cycle.
func RecordOutboxMarkPublishedError() {
	metricsMu.RLock()
	c := OutboxMarkPublishedErrorsTotal
	metricsMu.RUnlock()
	if c != nil {
		c.WithLabelValues().Inc()
	}
}
