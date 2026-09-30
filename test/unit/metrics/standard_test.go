package metrics_test

// Conformance suite for the Enterprise Platform Observability Standard
// (run by `make metrics-lint` in CI).
//
// It registers the REAL collectors into a fresh registry, exercises every
// recording path with every approved label value, gathers, and checks each
// emitted metric against the registry (internal/adapter/outbound/metrics/
// registry.go):
//
//   - registry compliance: every emitted metric has an entry and every entry
//     is emitted (no undocumented or stale metrics)
//   - namespace classification: platform_* ⇔ Tier 1; other names only as a
//     Deprecated legacy metric with a registered Tier 1 successor
//   - naming: snake_case; Tier 1 counters end _total, histograms _seconds,
//     gauges borrow neither; no library, service or domain name encoded in a
//     Tier 1 name
//   - required labels: Tier 1 carries {domain, service, environment} with
//     the identity's values, injected centrally
//   - label vocabulary: only required + approved labels, no prohibited label,
//     every value in its approved set or bound
//   - ratification packets complete; Supersedes ↔ SupersededBy consistent
//
// It inspects registered collectors, not source text, so it cannot be fooled
// by formatting and covers every collector however it is built.

import (
	"errors"
	"regexp"
	"runtime/debug"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	internalmetrics "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/adapter/outbound/metrics"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/events"
)

var testIdentity = events.MetricsIdentity{Domain: "iam", Service: "event-consumer", Environment: "prod", Version: "v1.2.3"}

const (
	testQueueURL = "https://sqs.us-east-1.amazonaws.com/123456789012/orders"
	testTopicARN = "arn:aws:sns:us-east-1:123456789012:iam-events"
)

var snakeCase = regexp.MustCompile(`^[a-z][a-z0-9]*(_[a-z0-9]+)*$`)

// isolatePlatform restores the process-wide Tier 1 set when t ends.
func isolatePlatform(t *testing.T) {
	t.Helper()
	prev := internalmetrics.CurrentPlatform()
	t.Cleanup(func() { internalmetrics.ReplacePlatform(prev) })
}

// exerciseAll drives every recording function with every approved value.
func exerciseAll() {
	boom := errors.New("boom")
	et := "iam.user.created"
	internalmetrics.InitQueue(testQueueURL)
	internalmetrics.ObserveReceived(testQueueURL)
	internalmetrics.ObservePropagation(testQueueURL, et, time.Now().Add(-time.Second), time.Now())
	internalmetrics.ObserveProcessingDuration(testQueueURL, et, time.Millisecond)
	internalmetrics.IncProcessed(testQueueURL, et)
	for _, r := range internalmetrics.FailureReasonValues {
		internalmetrics.IncFailed(testQueueURL, et, r)
	}
	for _, op := range internalmetrics.FlowOperationValues {
		internalmetrics.IncRetry(op, et)
		for _, r := range internalmetrics.DLQReasonValues {
			internalmetrics.IncDLQ(op, et, r)
		}
	}
	internalmetrics.IncDuplicate(testQueueURL, et)
	internalmetrics.IncDuplicate("", et) // inbox outside the SQS consumer → queue="unknown"
	internalmetrics.SetQueueDepth(testQueueURL, 42)
	internalmetrics.SetDLQDepth(testQueueURL, 3)
	for dep, ops := range internalmetrics.DependencyOperations {
		for _, op := range ops {
			internalmetrics.ObserveDependency(dep, op, nil, time.Millisecond)
			internalmetrics.ObserveDependency(dep, op, boom, time.Millisecond)
		}
	}
	internalmetrics.RecordPublish(testTopicARN, et, "success", 0.01)
	internalmetrics.RecordPublish(testTopicARN, et, "error", 0)
	internalmetrics.SetOutboxPending(3)
	internalmetrics.SetOutboxPending(-1)
	internalmetrics.SetOutboxLeased(1)
	internalmetrics.RecordOutboxLeasedCountError()
	internalmetrics.RecordOutboxAttempt(et)
	internalmetrics.RecordOutboxPublished(et, "success")
	internalmetrics.RecordOutboxPublished(et, "error")
	internalmetrics.RecordOutboxDeadLetter(et)
	internalmetrics.RecordOutboxDeadLettersReprocessed(1)
	internalmetrics.RecordOutboxDeadLettersDiscarded(1)
	internalmetrics.RecordOutboxPollError()
	internalmetrics.RecordOutboxUnmarshalError()
	internalmetrics.RecordOutboxMarkPublishedError()
	internalmetrics.SanitizeEventType(strings.Repeat("x", 200))
	// Legacy-only recording paths.
	internalmetrics.RecordConsume(testQueueURL, et, "success", 0.01)
	internalmetrics.RecordCodecEncode(testTopicARN, et, "success", 0.01)
	internalmetrics.RecordCodecDecode(testQueueURL, et, "success", 0.01)
	internalmetrics.RecordSQSReceiveError(testQueueURL)
	internalmetrics.RecordSQSDeleteError(testQueueURL)
	internalmetrics.RecordSQSVisibilityError(testQueueURL)
	internalmetrics.RecordInboxDuplicate("orders-consumer")
	internalmetrics.RecordDLQForward(testQueueURL, et, "success")
}

// gatherAll registers everything platform-events can emit, exercises it and
// returns the gathered families by name. Not parallel-safe.
func gatherAll(t *testing.T, opts ...events.MetricsOption) map[string]*dto.MetricFamily {
	t.Helper()
	isolatePlatform(t)
	reg := prometheus.NewRegistry()
	warnings, err := events.InitMetrics(testIdentity, reg, opts...)
	require.NoError(t, err)
	require.Empty(t, warnings)
	exerciseAll()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	out := map[string]*dto.MetricFamily{}
	for _, mf := range mfs {
		out[mf.GetName()] = mf
	}
	return out
}

func typeOf(mf *dto.MetricFamily) internalmetrics.MetricType {
	switch mf.GetType() {
	case dto.MetricType_COUNTER:
		return internalmetrics.TypeCounter
	case dto.MetricType_HISTOGRAM:
		return internalmetrics.TypeHistogram
	case dto.MetricType_GAUGE:
		return internalmetrics.TypeGauge
	default:
		return internalmetrics.MetricType(mf.GetType().String())
	}
}

func TestStandard_EveryEmittedMetricConforms(t *testing.T) {
	families := gatherAll(t)
	require.NotEmpty(t, families)

	for name, mf := range families {
		t.Run(name, func(t *testing.T) {
			entry, ok := internalmetrics.Lookup(name)
			require.True(t, ok, "shared metric registry compliance: %s is emitted but has no registry entry", name)

			assert.Regexp(t, snakeCase, name, "names must be snake_case")
			assert.Equal(t, entry.Type, typeOf(mf), "type must match the registry")

			switch entry.Tier {
			case internalmetrics.TierPlatform:
				require.True(t, strings.HasPrefix(name, "platform_"), "a Tier 1 metric must be named platform_*")
				assert.NotEqual(t, internalmetrics.StatusDeprecated, entry.Status)
				switch entry.Type {
				case internalmetrics.TypeCounter:
					assert.True(t, strings.HasSuffix(name, "_total"), "counters MUST end in _total")
				case internalmetrics.TypeHistogram:
					assert.True(t, strings.HasSuffix(name, "_seconds"), "histograms MUST end in _seconds")
				case internalmetrics.TypeGauge:
					assert.False(t, strings.HasSuffix(name, "_total") || strings.HasSuffix(name, "_seconds"),
						"gauges must not borrow counter/histogram suffixes")
				}
				rest := strings.TrimPrefix(name, "platform_")
				for _, forbidden := range []string{"events_", testIdentity.Service, strings.ReplaceAll(testIdentity.Service, "-", "_"), testIdentity.Domain + "_"} {
					assert.False(t, strings.HasPrefix(rest, forbidden) || strings.Contains(rest, "_"+forbidden),
						"never encode a library, service or domain name (%q) into a platform_* metric", forbidden)
				}
			case internalmetrics.TierLegacy:
				assert.Equal(t, internalmetrics.StatusDeprecated, entry.Status, "a non-tier legacy name may only exist as a Deprecated metric")
				require.NotEmpty(t, entry.SupersededBy)
				for _, succ := range entry.SupersededBy {
					se, ok := internalmetrics.Lookup(succ)
					require.True(t, ok, "successor %s must be registered", succ)
					assert.Equal(t, internalmetrics.TierPlatform, se.Tier)
				}
			default:
				t.Fatalf("namespace classification: %s has tier %q", name, entry.Tier)
			}

			allowed := append(slices.Clone(entry.RequiredLabels), entry.ApprovedLabels...)
			for _, m := range mf.GetMetric() {
				got := map[string]string{}
				for _, lp := range m.GetLabel() {
					got[lp.GetName()] = lp.GetValue()
				}
				for _, req := range entry.RequiredLabels {
					assert.Contains(t, got, req, "required label %q missing", req)
				}
				for label, value := range got {
					assert.Contains(t, allowed, label, "label %q is not in %s's approved vocabulary", label, name)
					assert.NotContains(t, internalmetrics.ProhibitedLabels, label, "high-cardinality label %q is prohibited", label)
					if vals, ok := entry.LabelValues[label]; ok {
						assert.Contains(t, vals, value, "%s=%q is not an approved value", label, value)
					}
					if entry.Tier == internalmetrics.TierPlatform {
						switch label {
						case "queue", "topic":
							assert.NotContains(t, value, "/", "%s must be a name, not a URL", label)
							assert.NotContains(t, value, "arn:", "%s must be a name, not an ARN", label)
						}
					}
				}
				if dep, ok := got["dependency"]; ok {
					assert.Contains(t, internalmetrics.DependencyOperations[dep], got["operation"],
						"operation %q is not valid for dependency %q", got["operation"], dep)
				}
				if entry.Tier == internalmetrics.TierPlatform {
					assert.Equal(t, testIdentity.Domain, got["domain"])
					assert.Equal(t, testIdentity.Service, got["service"])
					assert.Equal(t, testIdentity.Environment, got["environment"])
					assert.NotContains(t, got, "version", "the service build version is not part of the Tier 1 vocabulary")
				}
			}
		})
	}
}

// TestStandard_RegistryMatchesInstrumentation: the registry and the code
// cannot drift apart — a stale entry fails here, an undocumented metric above.
func TestStandard_RegistryMatchesInstrumentation(t *testing.T) {
	families := gatherAll(t)
	var emitted, registered []string
	for name := range families {
		emitted = append(emitted, name)
	}
	for _, e := range events.MetricsRegistry() {
		registered = append(registered, e.Name)
	}
	sort.Strings(emitted)
	sort.Strings(registered)
	assert.Equal(t, registered, emitted)
}

func TestStandard_RatificationPacketsComplete(t *testing.T) {
	seen := map[string]bool{}
	for _, e := range events.MetricsRegistry() {
		assert.False(t, seen[e.Name], "duplicate registry entry %s", e.Name)
		seen[e.Name] = true
		assert.NotEmpty(t, e.SemanticDefinition, e.Name)
		assert.NotEmpty(t, e.RequiredLabels, e.Name)
		for _, l := range append(slices.Clone(e.RequiredLabels), e.ApprovedLabels...) {
			assert.NotContains(t, internalmetrics.ProhibitedLabels, l, "%s: prohibited label %s", e.Name, l)
		}
		for _, prev := range e.Supersedes {
			pe, ok := internalmetrics.Lookup(prev)
			require.True(t, ok, "%s supersedes unknown metric %s", e.Name, prev)
			assert.Contains(t, pe.SupersededBy, e.Name, "%s ↔ %s must reference each other", prev, e.Name)
		}
		for _, next := range e.SupersededBy {
			ne, ok := internalmetrics.Lookup(next)
			require.True(t, ok, "%s is superseded by unknown metric %s", e.Name, next)
			assert.Contains(t, ne.Supersedes, e.Name, "%s ↔ %s must reference each other", e.Name, next)
		}
		if e.Tier != internalmetrics.TierPlatform {
			assert.NotEmpty(t, e.Sunset, "%s: Deprecated metrics need a sunset", e.Name)
			continue
		}
		assert.Equal(t, internalmetrics.PlatformRequiredLabels, e.RequiredLabels, "%s: Tier 1 required labels", e.Name)
		assert.NotEmpty(t, e.Cardinality, e.Name)
		assert.NotEmpty(t, e.AggregationNotes, e.Name)
		assert.NotEmpty(t, e.GovernanceNotes, e.Name)
		for _, l := range e.ApprovedLabels {
			_, enumerated := e.LabelValues[l]
			_, ruled := e.LabelValueRules[l]
			assert.True(t, enumerated || ruled, "%s: approved label %q needs allowed values or a documented bound", e.Name, l)
		}
	}
}

func TestStandard_WithoutLegacyMetricsEmitsOnlyTier1(t *testing.T) {
	families := gatherAll(t, events.WithoutLegacyMetrics())
	for name := range families {
		e, _ := internalmetrics.Lookup(name)
		assert.Equal(t, internalmetrics.TierPlatform, e.Tier, "legacy metric %s emitted after WithoutLegacyMetrics", name)
	}
	assert.Nil(t, internalmetrics.EventsPublishedTotal, "legacy vars are cleared")
	// Restore the package-wide legacy set other tests rely on.
	internalmetrics.InitWithRegisterer("metrics-unit", "test", prometheus.NewRegistry())
}

func TestStandard_IdentityValidation(t *testing.T) {
	isolatePlatform(t)
	for _, id := range []events.MetricsIdentity{
		{Service: "svc", Environment: "prod"},
		{Domain: "IAM", Service: "svc", Environment: "prod"},
		{Domain: "iam", Service: "svc name", Environment: "prod"},
		{Domain: "iam", Service: "1svc", Environment: "prod"},
		{Domain: "iam", Environment: "prod"},
	} {
		_, err := events.InitMetrics(id, prometheus.NewRegistry())
		assert.Error(t, err, "%+v", id)
	}

	// An empty Environment is read from the service's env.
	t.Setenv("APP_ENV", "")
	t.Setenv("ENVIRONMENT", "Staging ")
	_, err := events.InitMetrics(events.MetricsIdentity{Domain: "iam", Service: "svc"}, prometheus.NewRegistry())
	require.NoError(t, err)
	got, _ := internalmetrics.CurrentPlatform().Identity()
	assert.Equal(t, "staging", got.Environment)

	// A malformed env value names the variable it came from.
	t.Setenv("APP_ENV", "prod env")
	_, err = events.InitMetrics(events.MetricsIdentity{Domain: "iam", Service: "svc"}, prometheus.NewRegistry())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read from APP_ENV")
}

func TestStandard_EnvironmentAndIdentityHelpers(t *testing.T) {
	t.Setenv("APP_ENV", "")
	t.Setenv("ENVIRONMENT", "")
	t.Setenv("APP_NAME", " Order-Service ")
	assert.Equal(t, "dev", events.MetricsEnvironmentFromEnv())
	assert.Equal(t, events.MetricsIdentity{Domain: "billing", Service: "order-service", Environment: "dev", Version: "v1"},
		events.MetricsIdentityFromEnv("billing", "", "v1"))
	assert.Equal(t, "explicit", events.MetricsIdentityFromEnv("billing", "explicit", "v1").Service)
	t.Setenv("APP_ENV", "PROD")
	assert.Equal(t, "prod", events.MetricsEnvironmentFromEnv())

	id := events.MetricsIdentityFromLabels(map[string]string{"domain": "iam", "service": "svc", "environment": "dev", "version": "v1", "extra": "x"})
	assert.Equal(t, events.MetricsIdentity{Domain: "iam", Service: "svc", Environment: "dev", Version: "v1"}, id)
}

// TestStandard_ConflictingSharedNameIsFailSoft: IAM services already register
// platform_retry_total / platform_dependency_request_seconds with other label
// sets. That must not crash the service: the metric is disabled and reported,
// everything else keeps working.
func TestStandard_ConflictingSharedNameIsFailSoft(t *testing.T) {
	isolatePlatform(t)
	reg := prometheus.NewRegistry()
	reg.MustRegister(prometheus.NewCounterVec(prometheus.CounterOpts{Name: "platform_retry_total", Help: "someone else's"}, []string{"target_service", "endpoint"}))
	reg.MustRegister(prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "platform_dependency_request_seconds", Help: "someone else's"}, []string{"target_service", "endpoint"}))

	warnings, err := events.InitMetrics(testIdentity, reg)
	require.NoError(t, err)
	require.Len(t, warnings, 2)
	var names []string
	for _, w := range warnings {
		names = append(names, w.Metric)
		assert.Contains(t, w.Error(), "metric disabled")
	}
	assert.ElementsMatch(t, []string{"platform_retry_total", "platform_dependency_request_seconds"}, names)

	p := internalmetrics.CurrentPlatform()
	assert.Nil(t, p.Retries)
	assert.Nil(t, p.DependencyRequests)
	assert.NotNil(t, p.MessagesProcessed, "the other platform metrics still register")
	assert.NotPanics(t, exerciseAll)
	internalmetrics.InitWithRegisterer("metrics-unit", "test", prometheus.NewRegistry())
}

// TestStandard_RegistererInjectedLabelsAppliedOnce: a registerer that already
// injects identity labels must not make platform-events apply them twice —
// every metric carries each label exactly once, the wrapper's value wins, and
// nothing is refused.
func TestStandard_RegistererInjectedLabelsAppliedOnce(t *testing.T) {
	for _, tc := range []struct {
		name    string
		wrapped prometheus.Labels
	}{
		{"domain+environment", prometheus.Labels{"domain": "iam", "environment": "production"}},
		{"service too", prometheus.Labels{"domain": "iam", "environment": "production", "service": "wrapped-svc"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			isolatePlatform(t)
			reg := prometheus.NewRegistry()
			warnings, err := events.InitMetrics(testIdentity, prometheus.WrapRegistererWith(tc.wrapped, reg))
			require.NoError(t, err)
			require.Empty(t, warnings, "registerer-injected labels must not cause refusals")
			exerciseAll()

			mfs, err := reg.Gather()
			require.NoError(t, err)
			platformSeen := 0
			for _, mf := range mfs {
				for _, m := range mf.GetMetric() {
					count := map[string]int{}
					values := map[string]string{}
					for _, lp := range m.GetLabel() {
						count[lp.GetName()]++
						values[lp.GetName()] = lp.GetValue()
					}
					for l, n := range count {
						assert.Equal(t, 1, n, "%s: label %s appears %d times", mf.GetName(), l, n)
					}
					for l, v := range tc.wrapped {
						assert.Equal(t, v, values[l], "%s: the wrapper's %s wins", mf.GetName(), l)
					}
					if e, _ := internalmetrics.Lookup(mf.GetName()); e.Tier == internalmetrics.TierPlatform {
						platformSeen++
						for _, req := range internalmetrics.PlatformRequiredLabels {
							assert.Contains(t, values, req, "%s still carries %s", mf.GetName(), req)
						}
					}
				}
			}
			assert.Positive(t, platformSeen)
			internalmetrics.InitWithRegisterer("metrics-unit", "test", prometheus.NewRegistry())
		})
	}
}

// TestStandard_LabelStaticCountersStartAtZero: a counter series that first
// appears at 1 is invisible to increase()/rate(), so the first error after a
// deploy would not alert.
func TestStandard_LabelStaticCountersStartAtZero(t *testing.T) {
	isolatePlatform(t)
	reg := prometheus.NewRegistry()
	_, err := events.InitMetrics(testIdentity, reg, events.WithoutLegacyMetrics())
	require.NoError(t, err)
	internalmetrics.InitQueue(testQueueURL)

	mfs, err := reg.Gather()
	require.NoError(t, err)
	want := map[string]int{
		"platform_outbox_errors_total":                 len(internalmetrics.OutboxErrorOperationValues),
		"platform_outbox_dead_letter_operations_total": len(internalmetrics.DeadLetterOperationValues),
		"platform_telemetry_label_overflow_total":      1,
		"platform_messages_received_total":             1,
		"platform_messages_failed_total":               1,
	}
	got := map[string]int{}
	for _, mf := range mfs {
		if _, ok := want[mf.GetName()]; !ok {
			continue
		}
		for _, m := range mf.GetMetric() {
			got[mf.GetName()]++
			assert.Zero(t, m.GetCounter().GetValue(), "%s must start at 0", mf.GetName())
		}
	}
	assert.Equal(t, want, got)
	internalmetrics.InitWithRegisterer("metrics-unit", "test", prometheus.NewRegistry())
}

// TestStandard_LegacyRegistrationFailureIsAnError: unlike a Tier 1 metric, a
// legacy metric that can't register is an error and nothing changes.
func TestStandard_LegacyRegistrationFailureIsAnError(t *testing.T) {
	isolatePlatform(t)
	before := internalmetrics.CurrentPlatform()
	legacyBefore := internalmetrics.EventsPublishedTotal
	reg := prometheus.NewRegistry()
	reg.MustRegister(prometheus.NewCounterVec(prometheus.CounterOpts{Name: "events_published_total", Help: "clash"}, []string{"other"}))
	_, err := events.InitMetrics(testIdentity, reg)
	require.Error(t, err)
	assert.Same(t, before, internalmetrics.CurrentPlatform(), "a failed init must not replace the active set")
	assert.Same(t, legacyBefore, internalmetrics.EventsPublishedTotal)
}

func TestStandard_NilRegistererUsesDefault(t *testing.T) {
	isolatePlatform(t)
	id := testIdentity
	id.Service = "nil-registerer-probe"
	_, err := events.InitMetrics(id, nil, events.WithoutLegacyMetrics())
	require.NoError(t, err)
	mfs, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	found := false
	for _, mf := range mfs {
		if mf.GetName() == "platform_library_info" {
			found = true
		}
	}
	assert.True(t, found, "registered on prometheus.DefaultRegisterer")
	internalmetrics.InitWithRegisterer("metrics-unit", "test", prometheus.NewRegistry())
}

// TestWrapCollisionMessagePinned pins the client_golang error text the
// label-collision fallback relies on.
func TestWrapCollisionMessagePinned(t *testing.T) {
	reg := prometheus.WrapRegistererWith(prometheus.Labels{"domain": "x"}, prometheus.NewRegistry())
	err := reg.Register(prometheus.NewCounter(prometheus.CounterOpts{Name: "probe_total", Help: "h", ConstLabels: prometheus.Labels{"domain": "y"}}))
	require.Error(t, err)
	assert.Regexp(t, `already existing label name "domain"`, err.Error())
}

func TestStandard_LabelValueHelpers(t *testing.T) {
	assert.Equal(t, "orders", internalmetrics.QueueName(testQueueURL))
	assert.Equal(t, "orders.fifo", internalmetrics.QueueName(testQueueURL+".fifo"))
	assert.Equal(t, "unknown", internalmetrics.QueueName(""))
	assert.Equal(t, "iam-events", internalmetrics.TopicName(testTopicARN))
	assert.Equal(t, "unknown", internalmetrics.TopicName(""))
	assert.Equal(t, "devel", internalmetrics.LibraryVersion(), "this module's own test binary")

	var nilPlatform *internalmetrics.Platform
	_, ok := nilPlatform.Identity()
	assert.False(t, ok)
}

func TestStandard_PropagationClampsSkewAndIgnoresZero(t *testing.T) {
	isolatePlatform(t)
	reg := prometheus.NewRegistry()
	_, err := events.InitMetrics(testIdentity, reg, events.WithoutLegacyMetrics())
	require.NoError(t, err)
	now := time.Now()
	internalmetrics.ObservePropagation(testQueueURL, "a.b.c", now.Add(time.Minute), now) // producer clock ahead
	internalmetrics.ObservePropagation(testQueueURL, "a.b.c", time.Time{}, now)          // no timestamp

	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() == "platform_event_propagation_seconds" {
			h := mf.GetMetric()[0].GetHistogram()
			assert.Equal(t, uint64(1), h.GetSampleCount(), "zero timestamp ignored")
			assert.Zero(t, h.GetSampleSum(), "negative skew clamped to 0")
		}
	}
	internalmetrics.InitWithRegisterer("metrics-unit", "test", prometheus.NewRegistry())
}

func TestStandard_LibraryVersionFrom(t *testing.T) {
	const mod = "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events"
	dep := func(m debug.Module) *debug.BuildInfo {
		return &debug.BuildInfo{Main: debug.Module{Path: "example.com/svc", Version: "v9.9.9"}, Deps: []*debug.Module{{Path: "other/mod", Version: "v1.0.0"}, &m}}
	}
	for name, tc := range map[string]struct {
		bi   *debug.BuildInfo
		want string
	}{
		"dependency release":    {dep(debug.Module{Path: mod, Version: "v1.6.0"}), "v1.6.0"},
		"dependency replaced":   {dep(debug.Module{Path: mod, Version: "v1.6.0", Replace: &debug.Module{Path: "../fork", Version: "v1.6.1-fork"}}), "v1.6.1-fork"},
		"dependency local path": {dep(debug.Module{Path: mod, Version: "(devel)"}), "devel"},
		"main module release":   {&debug.BuildInfo{Main: debug.Module{Path: mod, Version: "v1.6.0"}}, "v1.6.0"},
		"main module devel":     {&debug.BuildInfo{Main: debug.Module{Path: mod, Version: "(devel)"}}, "devel"},
		"not linked":            {&debug.BuildInfo{Main: debug.Module{Path: "example.com/svc"}}, "unknown"},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, internalmetrics.LibraryVersionFrom(tc.bi))
		})
	}
}

// TestStandard_ReinitOnSameRegistryReusesCollectors: calling InitMetrics twice
// on one registry (e.g. a test harness or a restart path) reuses the
// registered collectors instead of failing.
func TestStandard_ReinitOnSameRegistryReusesCollectors(t *testing.T) {
	isolatePlatform(t)
	reg := prometheus.NewRegistry()
	_, err := events.InitMetrics(testIdentity, reg)
	require.NoError(t, err)
	first := internalmetrics.CurrentPlatform().MessagesReceived
	warnings, err := events.InitMetrics(testIdentity, reg)
	require.NoError(t, err)
	assert.Empty(t, warnings)
	assert.Same(t, first, internalmetrics.CurrentPlatform().MessagesReceived)
	internalmetrics.InitWithRegisterer("metrics-unit", "test", prometheus.NewRegistry())
}

func TestStandard_LegacyInitPanicsOnConflict(t *testing.T) {
	reg := prometheus.NewRegistry()
	reg.MustRegister(prometheus.NewCounterVec(prometheus.CounterOpts{Name: "events_published_total", Help: "clash"}, []string{"other"}))
	assert.Panics(t, func() { internalmetrics.InitWithRegisterer("svc", "v1", reg) })
}

// TestStandard_LegacyInitDoesNotDisableTier1: a leftover events.Init (old
// bootstrap, shared helper) after InitMetrics must not switch the Tier 1
// metrics off.
func TestStandard_LegacyInitDoesNotDisableTier1(t *testing.T) {
	isolatePlatform(t)
	reg := prometheus.NewRegistry()
	_, err := events.InitMetrics(testIdentity, reg)
	require.NoError(t, err)
	p := internalmetrics.CurrentPlatform()
	require.NotNil(t, p)

	events.Init("legacy-helper", "v0") //nolint:staticcheck // exercises the deprecated API
	assert.Same(t, p, internalmetrics.CurrentPlatform(), "Init must be a no-op once InitMetrics ran")
	internalmetrics.IncProcessed(testQueueURL, "iam.user.created")
	assert.InDelta(t, 1, testutil.ToFloat64(p.MessagesProcessed.WithLabelValues("orders", "iam.user.created")), 0)
	internalmetrics.InitWithRegisterer("metrics-unit", "test", prometheus.NewRegistry())
}

// TestStandard_EventTypeCardinalityCap: the byte cap alone does not bound
// cardinality — distinct values beyond the limit collapse into __other__.
func TestStandard_EventTypeCardinalityCap(t *testing.T) {
	isolatePlatform(t)
	t.Cleanup(func() { internalmetrics.SetEventTypeLimit(0) })
	reg := prometheus.NewRegistry()
	_, err := events.InitMetrics(testIdentity, reg, events.WithEventTypeLimit(3), events.WithoutLegacyMetrics())
	require.NoError(t, err)
	p := internalmetrics.CurrentPlatform()

	for _, et := range []string{"a.b.one", "a.b.two", "a.b.three", "a.b.four", "a.b.five", "a.b.one"} {
		internalmetrics.IncProcessed(testQueueURL, et)
	}
	assert.InDelta(t, 2, testutil.ToFloat64(p.MessagesProcessed.WithLabelValues("orders", "a.b.one")), 0, "admitted values keep their label")
	assert.InDelta(t, 2, testutil.ToFloat64(p.MessagesProcessed.WithLabelValues("orders", internalmetrics.EventTypeOther)), 0)
	assert.Equal(t, 4, testutil.CollectAndCount(p.MessagesProcessed), "3 admitted values + __other__")
	assert.InDelta(t, 2, testutil.ToFloat64(p.LabelOverflow.WithLabelValues("event_type")), 0)

	assert.Equal(t, internalmetrics.EventTypeUnknown, internalmetrics.SanitizeEventType(""))
	assert.Equal(t, internalmetrics.EventTypeOversized, internalmetrics.SanitizeEventType(strings.Repeat("x", 129)))
	internalmetrics.InitWithRegisterer("metrics-unit", "test", prometheus.NewRegistry())
}
