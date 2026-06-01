// Package metrics registers Prometheus metrics for the platform-events library.
package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Global metric variables, initialised once by Init or InitWithRegisterer.
var (
	EventsPublishedTotal  *prometheus.CounterVec
	EventsPublishDuration *prometheus.HistogramVec
	EventsConsumedTotal   *prometheus.CounterVec
	EventsConsumeDuration *prometheus.HistogramVec
	OutboxPendingTotal    *prometheus.GaugeVec
	OutboxPublishedTotal  *prometheus.CounterVec
	OutboxAttemptsTotal   *prometheus.CounterVec
	OutboxDeadLettersTotal *prometheus.CounterVec

	initOnce sync.Once
)

// Init registers metrics using the default Prometheus registerer.
// Panics if serviceName is empty (prevents invalid label cardinality).
// Safe to call multiple times — only the first call registers metrics.
func Init(serviceName, buildVersion string) {
	if serviceName == "" {
		panic("platform-events: metrics.Init requires a non-empty serviceName")
	}
	initOnce.Do(func() {
		initMetricsWithRegisterer(serviceName, buildVersion, prometheus.DefaultRegisterer)
	})
}

// InitWithRegisterer registers metrics using the provided Prometheus registerer.
// Bypasses the sync.Once guard — intended for test isolation with separate registries.
func InitWithRegisterer(serviceName, buildVersion string, reg prometheus.Registerer) {
	if serviceName == "" {
		panic("platform-events: metrics.InitWithRegisterer requires a non-empty serviceName")
	}
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
		Buckets:     prometheus.DefBuckets,
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
		Buckets:     prometheus.DefBuckets,
	}, []string{"queue", "event_type"})

	OutboxPendingTotal = factory.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "outbox_pending_total",
		Help:        "Number of pending (unpublished) outbox records.",
		ConstLabels: prometheus.Labels{"service": serviceName},
	}, []string{})

	OutboxPublishedTotal = factory.NewCounterVec(prometheus.CounterOpts{
		Name:        "outbox_published_total",
		Help:        "Total number of outbox records published.",
		ConstLabels: prometheus.Labels{"service": serviceName},
	}, []string{"status"})

	OutboxAttemptsTotal = factory.NewCounterVec(prometheus.CounterOpts{
		Name:        "outbox_attempts_total",
		Help:        "Total number of outbox publish attempts.",
		ConstLabels: prometheus.Labels{"service": serviceName},
	}, []string{})

	// OutboxDeadLettersTotal counts records moved to outbox_dead_letters after
	// exhausting MaxAttempts. This is the publish-side failure sink (distinct from
	// the consumption-side SQS DLQ); alert on rate() > 0 to catch undelivered events.
	OutboxDeadLettersTotal = factory.NewCounterVec(prometheus.CounterOpts{
		Name:        "outbox_dead_letters_total",
		Help:        "Total number of outbox records moved to the dead-letter table.",
		ConstLabels: prometheus.Labels{"service": serviceName},
	}, []string{})
}
