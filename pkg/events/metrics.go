package events

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/adapter/outbound/metrics"
)

// MetricsIdentity is the Enterprise Platform Observability Standard's
// required label set for Tier 1 metrics: Domain, Service and Environment are
// applied as const labels to every platform_* metric, centrally, so no call
// site can omit or misspell them. The identity is mandatory: there is no way
// to register platform-events' metrics without one. Values must be lowercase
// [a-z][a-z0-9_-]{0,62}. It is the same shape and rule as platform-pgcommon's
// pgmetrics.Identity.
type MetricsIdentity = metrics.Identity

// RegistrationWarning reports a platform_* metric that could not be
// registered (e.g. the registry already holds that name with other labels).
// That one metric is disabled; everything else keeps working.
type RegistrationWarning = metrics.RegistrationWarning

// MetricsOption configures InitMetrics.
type MetricsOption func(*metricsOptions)

type metricsOptions struct {
	eventTypeLimit int
	eventTypes     []string
}

// WithEventTypeLimit sets how many distinct event_type label values the
// process records (default 200). Further distinct values are recorded as
// "__other__" and counted in platform_telemetry_label_overflow_total, so a
// producer sending unbounded event types cannot explode metric cardinality.
// Size it above the number of event types the service legitimately handles.
func WithEventTypeLimit(n int) MetricsOption {
	return func(o *metricsOptions) { o.eventTypeLimit = n }
}

// WithEventTypes pre-registers the event types the service publishes and
// consumes. They always keep their own event_type label: the limit admits
// values first come, first served, and consumed event types come from message
// bodies — without this, unknown values from a misbehaving producer on a
// shared topic could take every slot and turn the real types into
// "__other__" until restart. The limit counts these types and is raised to fit
// them; WithEventTypes(known...) with WithEventTypeLimit(len(known)) records
// only the known types.
func WithEventTypes(types ...string) MetricsOption {
	return func(o *metricsOptions) { o.eventTypes = append(o.eventTypes, types...) }
}

// InitMetrics registers the Tier 1 platform_* metrics — the only metrics
// platform-events emits — with id's {domain, service, environment} const
// labels, and makes them the active set. Call it once at startup, before
// publishing or consuming; until then every recording call is a no-op.
//
// Call it once per process. Calling it again with the same identity and
// registerer reuses the registered collectors; calling it with a different
// identity leaves the first identity's series registered with frozen values —
// there is no unregister — so don't re-initialise with another identity.
//
// It returns an error, changing nothing, for an invalid identity. A
// platform_* metric that cannot be registered (the registry already holds
// that name with another shape) is disabled and returned as a warning — log
// the warnings:
//
//	warnings, err := events.InitMetrics(events.MetricsIdentity{
//	    Domain: "iam", Service: "event-consumer", Environment: "prod",
//	}, registry)
//
// A nil reg means prometheus.DefaultRegisterer. A registerer that already
// injects some of the identity labels (e.g. a prometheus.WrapRegistererWith
// wrapper) is fine: each label is applied once and the wrapper's value wins.
// Use the same registerer — and identity — as platform-pgcommon's
// pgmetrics.InitWithIdentity so both libraries report the same values.
//
// An empty Environment is filled from the service's environment variables
// (MetricsEnvironmentFromEnv: APP_ENV, then ENVIRONMENT, else "dev").
func InitMetrics(id MetricsIdentity, reg prometheus.Registerer, opts ...MetricsOption) ([]RegistrationWarning, error) {
	var o metricsOptions
	for _, opt := range opts {
		opt(&o)
	}
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	source := ""
	if id.Environment == "" {
		id.Environment, source = metrics.Environment()
	}
	warnings, err := metrics.InitWithIdentity(id, reg)
	if err != nil {
		if source != "" && source != "default" {
			err = fmt.Errorf("%w (environment %q was read from %s)", err, id.Environment, source)
		}
		return warnings, err // nothing changed, including the event_type cap
	}
	metrics.SetEventTypes(o.eventTypeLimit, o.eventTypes)
	return warnings, nil
}

// MetricsEnvironmentFromEnv returns the deployment environment from the
// service's environment variables with platform-gincommon's precedence:
// APP_ENV, then ENVIRONMENT, else "dev" — trimmed and lowercased.
func MetricsEnvironmentFromEnv() string {
	env, _ := metrics.Environment()
	return env
}

// MetricsIdentityFromEnv builds a MetricsIdentity from the service's
// environment: Environment from MetricsEnvironmentFromEnv, Service from
// APP_NAME when service is empty. Domain has no environment variable — it is
// the service's fixed business domain (e.g. "iam").
func MetricsIdentityFromEnv(domain, service string) MetricsIdentity {
	if service == "" {
		service = metrics.ServiceName()
	}
	return MetricsIdentity{Domain: domain, Service: service, Environment: MetricsEnvironmentFromEnv()}
}

// MetricsIdentityFromLabels builds a MetricsIdentity from a const-label map
// using the standard's label names — domain, service, environment — e.g.
// platform-gincommon's MetricsConstLabels(), so platform-events' metrics carry
// the values the service's own metrics do. Other keys are ignored.
func MetricsIdentityFromLabels(labels map[string]string) MetricsIdentity {
	return MetricsIdentity{
		Domain:      labels[metrics.LabelDomain],
		Service:     labels[metrics.LabelService],
		Environment: labels[metrics.LabelEnvironment],
	}
}

// MetricsRegistryEntry is one metric's record in platform-events' entry of
// the Platform Observability Registry: tier, status, semantic definition,
// labels and allowed values.
type MetricsRegistryEntry = metrics.RegistryEntry

// Metric registry statuses.
const (
	MetricStatusCanonical = metrics.StatusCanonical
	MetricStatusProposed  = metrics.StatusProposed
)

// MetricsRegistry returns every metric platform-events registers, for
// tooling (inventories, dashboard/alert linting). CI enforces that the
// instrumentation matches it exactly.
func MetricsRegistry() []MetricsRegistryEntry { return metrics.Registry() }
