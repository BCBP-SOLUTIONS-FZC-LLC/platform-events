package metrics_test

// The observability inventory (docs/observability/metrics-registry.md) is
// rendered from the registry — the same records CI enforces the
// instrumentation against — so it cannot drift from the code. Regenerate:
//
//	make metrics-doc

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	internalmetrics "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/adapter/outbound/metrics"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/events"
)

var update = flag.Bool("update", false, "regenerate docs/observability/metrics-registry.md")

const inventoryPath = "../../../docs/observability/metrics-registry.md"

func code(s string) string { return "`" + s + "`" }

func codeList(ss []string) string {
	if len(ss) == 0 {
		return "—"
	}
	out := make([]string, len(ss))
	for i, s := range ss {
		out[i] = code(s)
	}
	return strings.Join(out, ", ")
}

func renderInventory() string {
	var b strings.Builder
	w := func(format string, a ...any) { fmt.Fprintf(&b, format, a...) }
	entries := events.MetricsRegistry()

	w("# platform-events — metrics registry\n\n")
	w("<!-- GENERATED from the metrics registry by test/unit/metrics/inventory_test.go — do not edit by hand.\n")
	w("     Regenerate: make metrics-doc -->\n\n")
	w("platform-events' entry in the Platform Observability Registry (Enterprise Platform Observability Standard). ")
	w("CI checks every registered collector against these records (`make metrics-lint`), so this inventory is exactly what a service using the library exposes. ")
	w("See [README](README.md) for the tier model, wiring and migration plan.\n\n")

	w("## Summary\n\n")
	w("| Metric | Type | Tier | Status | Labels | Replaces / replaced by |\n|---|---|---|---|---|---|\n")
	for _, e := range entries {
		rel := e.Supersedes
		if e.Status == events.MetricStatusDeprecated {
			rel = e.SupersededBy
		}
		w("| %s | %s | %s | %s | %s | %s |\n", code(e.Name), e.Type, e.Tier, e.Status,
			codeList(append(append([]string{}, e.RequiredLabels...), e.ApprovedLabels...)), codeList(rel))
	}

	w("\n## Label vocabulary\n\n")
	w("A label name means the same thing on every metric that uses it; each entry below lists the values that metric can take.\n\n")
	w("| Label | Kind | Allowed values |\n|---|---|---|\n")
	w("| `domain` | required (Tier 1) | Owning business domain, lowercase (e.g. `iam`, `workflow`, `billing`, `tender`, `notifications`, `documents`) — from `events.MetricsIdentity` |\n")
	w("| `service` | required | Service name, lowercase `[a-z][a-z0-9_-]{0,62}` — from `events.MetricsIdentity` (or `APP_NAME` via `events.MetricsIdentityFromEnv`) |\n")
	w("| `environment` | required (Tier 1) | Deployment environment, lowercase — `APP_ENV`, then `ENVIRONMENT`, else `dev` when `MetricsIdentity.Environment` is empty (same precedence as platform-gincommon and platform-pgcommon). Canonical spellings (`prod` vs `production`) are pending governance |\n")
	w("| `queue` | approved | %s |\n", internalmetrics.QueueLabelRule)
	w("| `topic` | requested | %s |\n", internalmetrics.TopicLabelRule)
	w("| `event_type` | approved | %s |\n", internalmetrics.EventTypeLabelRule)
	w("| `reason` | approved | failures: %s; dead-letters: %s |\n", codeList(internalmetrics.FailureReasonValues), codeList(internalmetrics.DLQReasonValues))
	w("| `operation` | approved | message flow: %s; dependency calls: %s; outbox errors: %s; dead-letter actions: %s |\n",
		codeList(internalmetrics.FlowOperationValues), codeList(internalmetrics.DependencyOperationValues),
		codeList(internalmetrics.OutboxErrorOperationValues), codeList(internalmetrics.DeadLetterOperationValues))
	deps := make([]string, 0, len(internalmetrics.DependencyOperations))
	for d := range internalmetrics.DependencyOperations {
		deps = append(deps, d)
	}
	sort.Strings(deps)
	pairs := make([]string, 0, len(deps))
	for _, d := range deps {
		pairs = append(pairs, fmt.Sprintf("%s → %s", code(d), codeList(internalmetrics.DependencyOperations[d])))
	}
	w("| `dependency` | approved | %s |\n", strings.Join(pairs, "; "))
	w("| `outcome` | approved | %s |\n", codeList(internalmetrics.OutcomeValues))
	w("| `label` | requested | %s |\n", codeList(internalmetrics.OverflowLabelValues))
	w("| `library` / `library_version` | requested | %s; %s |\n", codeList(internalmetrics.LibraryValues), internalmetrics.LibraryVersionLabelRule)
	w("| `version` | legacy only | the service's build version (`platform_events_build_info`); never on Tier 1 metrics |\n")
	w("| `status` | legacy only | `success`, `error`, `malformed`, `noop`, `dlq_success`, `dlq_error` (split into `outcome` / `reason` on Tier 1) |\n")
	w("| `consumer` | legacy only | inbox consumer name (replaced by `queue` on Tier 1) |\n")
	prohibited := append([]string{}, internalmetrics.ProhibitedLabels...)
	sort.Strings(prohibited)
	w("\n**Prohibited on every metric** (high cardinality / sensitive): %s.\n", codeList(prohibited))

	for _, status := range []events.MetricsRegistryEntry{{Status: events.MetricStatusCanonical}, {Status: events.MetricStatusProposed}} {
		title := "Canonical metrics"
		if status.Status == events.MetricStatusProposed {
			title = "Ratification packets (Proposed)"
		}
		w("\n## %s\n\n", title)
		for _, e := range entries {
			if e.Status != status.Status {
				continue
			}
			w("### %s\n\n", code(e.Name))
			w("- **Type:** %s · **Tier:** %s · **Status:** %s\n", e.Type, e.Tier, e.Status)
			w("- **Semantic definition:** %s\n", e.SemanticDefinition)
			w("- **Required labels:** %s\n", codeList(e.RequiredLabels))
			w("- **Approved labels:** %s\n", codeList(e.ApprovedLabels))
			for _, l := range e.ApprovedLabels {
				if vals, ok := e.LabelValues[l]; ok {
					w("  - %s: %s\n", code(l), codeList(vals))
				} else if rule, ok := e.LabelValueRules[l]; ok {
					w("  - %s: %s\n", code(l), rule)
				}
			}
			w("- **Cardinality:** %s\n", e.Cardinality)
			w("- **Aggregation:** %s\n", e.AggregationNotes)
			w("- **Supersedes:** %s\n", codeList(e.Supersedes))
			w("- **Governance notes:** %s\n\n", e.GovernanceNotes)
		}
	}

	w("## Deprecated (compatibility period)\n\n")
	w("| Metric | Successor | Sunset |\n|---|---|---|\n")
	for _, e := range entries {
		if e.Status == events.MetricStatusDeprecated {
			w("| %s | %s | %s |\n", code(e.Name), codeList(e.SupersededBy), e.Sunset)
		}
	}
	return b.String()
}

func TestInventory_UpToDate(t *testing.T) {
	want := renderInventory()
	if *update {
		require.NoError(t, os.WriteFile(inventoryPath, []byte(want), 0o644)) //nolint:gosec // documentation file
		return
	}
	got, err := os.ReadFile(inventoryPath)
	require.NoError(t, err, "inventory missing — run: make metrics-doc")
	require.Equal(t, want, string(got), "docs/observability/metrics-registry.md is out of date with the metrics registry — run: make metrics-doc")
}
