package events

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/adapter/outbound/metrics"
)

// Init registers the legacy (pre-standard) metrics using the default
// registerer. Panics if serviceName is empty. Idempotent.
//
// Deprecated: use InitMetrics, which also registers the Tier 1 platform_*
// metrics required by the Enterprise Platform Observability Standard (with
// the mandatory domain/service/environment labels).
func Init(serviceName, buildVersion string) {
	metrics.Init(serviceName, buildVersion)
}

// InitWithRegisterer registers the legacy metrics on reg, bypassing the
// sync.Once guard (test isolation). Panics if serviceName is empty.
//
// Deprecated: use InitMetrics.
func InitWithRegisterer(serviceName, buildVersion string, reg prometheus.Registerer) {
	metrics.InitWithRegisterer(serviceName, buildVersion, reg)
}

// MetricsIdentity is the Enterprise Platform Observability Standard's
// required label set for Tier 1 metrics: Domain, Service and Environment are
// applied as const labels to every platform_* metric, centrally, so no call
// site can omit or misspell them. Version is applied to the legacy build-info
// gauge only. Values must be lowercase [a-z][a-z0-9_-]{0,62}. It is the same
// shape and rule as platform-pgcommon's pgmetrics.Identity.
type MetricsIdentity = metrics.Identity

// RegistrationWarning reports a platform_* metric that could not be
// registered (e.g. the registry already holds that name with other labels).
// That one metric is disabled; everything else keeps working.
type RegistrationWarning = metrics.RegistrationWarning

// MetricsOption configures InitMetrics.
type MetricsOption func(*metricsOptions)

type metricsOptions struct{ legacy bool }

// WithoutLegacyMetrics stops registering the Deprecated events_* / outbox_* /
// sqs_* metrics. Use it once a service has migrated its dashboards, alerts,
// recording rules, SLOs and HPA references to the platform_* metrics
// (Backward Compatibility steps 7–8).
func WithoutLegacyMetrics() MetricsOption {
	return func(o *metricsOptions) { o.legacy = false }
}

// InitMetrics registers the Tier 1 platform_* metrics with id's
// {domain, service, environment} const labels and — during the compatibility
// period, unless WithoutLegacyMetrics is given — the legacy metrics in
// parallel, and makes them the active set. Call it once at startup, before
// publishing or consuming.
//
// It returns an error, changing nothing, for an invalid identity or a legacy
// registration failure. A platform_* metric that cannot be registered (the
// registry already holds that name with another shape) is disabled and
// returned as a warning — log the warnings:
//
//	warnings, err := events.InitMetrics(events.MetricsIdentity{
//	    Domain: "iam", Service: "event-consumer", Version: buildVersion,
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
	o := metricsOptions{legacy: true}
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
	warnings, err := metrics.InitWithIdentity(id, reg, o.legacy)
	if err != nil && source != "" && source != "default" {
		err = fmt.Errorf("%w (environment %q was read from %s)", err, id.Environment, source)
	}
	return warnings, err
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
func MetricsIdentityFromEnv(domain, service, version string) MetricsIdentity {
	if service == "" {
		service = metrics.ServiceName()
	}
	return MetricsIdentity{Domain: domain, Service: service, Environment: MetricsEnvironmentFromEnv(), Version: version}
}

// MetricsIdentityFromLabels builds a MetricsIdentity from a const-label map
// using the standard's label names — domain, service, environment, version —
// e.g. platform-gincommon's MetricsConstLabels(), so platform-events' metrics
// carry the values the service's own metrics do.
func MetricsIdentityFromLabels(labels map[string]string) MetricsIdentity {
	return MetricsIdentity{
		Domain:      labels[metrics.LabelDomain],
		Service:     labels[metrics.LabelService],
		Environment: labels[metrics.LabelEnvironment],
		Version:     labels[metrics.LabelVersion],
	}
}

// MetricsRegistryEntry is one metric's record in platform-events' entry of
// the Platform Observability Registry: tier, status, semantic definition,
// labels and allowed values.
type MetricsRegistryEntry = metrics.RegistryEntry

// Metric registry statuses.
const (
	MetricStatusCanonical  = metrics.StatusCanonical
	MetricStatusProposed   = metrics.StatusProposed
	MetricStatusDeprecated = metrics.StatusDeprecated
)

// MetricsRegistry returns every metric platform-events registers, for
// tooling (inventories, dashboard/alert linting). CI enforces that the
// instrumentation matches it exactly.
func MetricsRegistry() []MetricsRegistryEntry { return metrics.Registry() }
