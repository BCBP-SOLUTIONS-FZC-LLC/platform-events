package metrics_test

// Governance for the rest of the reference monitoring bundle (the rule files
// are covered by rules_test.go):
//
//   - monitoring/grafana/*.json dashboards: every query references registered
//     metrics (or recording rules from the rule files) and only labels in
//     their vocabulary; a panel querying a Proposed metric must say
//     "(Proposed)" in its title, one querying a Deprecated metric "(legacy)"
//     — so no dashboard presents a shadow or legacy series as the contract.
//   - monitoring/kubernetes/*.yaml autoscaling: HPA/KEDA must not scale on a
//     Proposed metric (Backward Compatibility step 6; rules 11/12), and every
//     scaling query uses only registered metrics and labels.

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	internalmetrics "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/adapter/outbound/metrics"
)

const monitoringDir = "../../../monitoring"

type grafanaDashboard struct {
	Panels     []grafanaPanel `json:"panels"`
	Templating struct {
		List []struct {
			Name  string `json:"name"`
			Type  string `json:"type"`
			Query any    `json:"query"`
		} `json:"list"`
	} `json:"templating"`
}

type grafanaPanel struct {
	Type    string         `json:"type"`
	Title   string         `json:"title"`
	Panels  []grafanaPanel `json:"panels"`
	Targets []struct {
		Expr string `json:"expr"`
	} `json:"targets"`
}

// recordingRules returns every recording rule in the rule files and the
// labels it keeps.
func recordingRules(t *testing.T) map[string][]string {
	t.Helper()
	out := map[string][]string{}
	for _, rf := range loadRuleFiles(t) {
		for _, g := range rf.Groups {
			for _, r := range g.Rules {
				if r.Record != "" {
					out[r.Record] = groupingLabels(r.Expr)
				}
			}
		}
	}
	return out
}

// checkExpr validates one PromQL expression and returns the statuses of the
// registry metrics it references.
func checkExpr(t *testing.T, where, expr string, records map[string][]string) []internalmetrics.RegistryStatus {
	t.Helper()
	allowed := map[string]bool{"le": true, "namespace": true} // namespace: scrape-target label (see rules_test.go)
	var statuses []internalmetrics.RegistryStatus
	refs := 0
	for _, ref := range metricRefs(expr) {
		refs++
		m := baseMetric(ref)
		e, ok := internalmetrics.Lookup(m)
		if !assert.True(t, ok, "%s references %s, which is not in the metrics registry", where, ref) {
			continue
		}
		statuses = append(statuses, e.Status)
		for _, l := range append(slices.Clone(e.RequiredLabels), e.ApprovedLabels...) {
			allowed[l] = true
		}
	}
	for _, rec := range recordRef.FindAllString(expr, -1) {
		refs++
		kept, ok := records[rec]
		assert.True(t, ok, "%s references unknown recording rule %s", where, rec)
		for _, l := range kept {
			allowed[l] = true
		}
	}
	assert.Positive(t, refs, "%s references no platform-events metric", where)
	for _, l := range append(groupingLabels(expr), selectorLabels(expr)...) {
		assert.True(t, allowed[l], "%s uses label %q outside the vocabulary of the metrics it queries", where, l)
	}
	return statuses
}

func TestMonitoring_DashboardsRegistryCompliance(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join(monitoringDir, "grafana", "*.json"))
	require.NoError(t, err)
	require.NotEmpty(t, paths, "no reference dashboards found")
	records := recordingRules(t)
	for _, p := range paths {
		raw, err := os.ReadFile(p) //nolint:gosec // paths come from a fixed glob in the repo
		require.NoError(t, err)
		var d grafanaDashboard
		require.NoError(t, json.Unmarshal(raw, &d), p)

		for _, v := range d.Templating.List {
			if v.Type != "query" {
				continue
			}
			q, _ := json.Marshal(v.Query)
			checkExpr(t, filepath.Base(p)+" variable $"+v.Name, strings.Trim(string(q), `"`), records)
		}

		var walk func([]grafanaPanel)
		walk = func(ps []grafanaPanel) {
			for _, panel := range ps {
				walk(panel.Panels)
				if panel.Type == "row" {
					continue
				}
				where := filepath.Base(p) + " panel " + panel.Title
				require.NotEmpty(t, panel.Targets, "%s has no queries", where)
				var statuses []internalmetrics.RegistryStatus
				for _, tg := range panel.Targets {
					statuses = append(statuses, checkExpr(t, where, tg.Expr, records)...)
				}
				if slices.Contains(statuses, internalmetrics.StatusProposed) {
					assert.Contains(t, panel.Title, "(Proposed)", "%s queries a Proposed metric; label it as a shadow panel", where)
				}
				if slices.Contains(statuses, internalmetrics.StatusDeprecated) {
					assert.Contains(t, panel.Title, "(legacy)", "%s queries a Deprecated metric; label it as legacy", where)
				}
			}
		}
		walk(d.Panels)
	}
}

func TestMonitoring_AutoscalingNeverUsesProposedMetrics(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join(monitoringDir, "kubernetes", "*.yaml"))
	require.NoError(t, err)
	require.NotEmpty(t, paths, "no reference autoscaling manifests found")
	mentions := 0
	records := recordingRules(t)
	for _, p := range paths {
		raw, err := os.ReadFile(p) //nolint:gosec // paths come from a fixed glob in the repo
		require.NoError(t, err)
		for i, line := range strings.Split(string(raw), "\n") {
			comment := strings.HasPrefix(strings.TrimSpace(line), "#")
			if q, ok := strings.CutPrefix(strings.TrimSpace(line), "query:"); ok {
				// A live scaling query: registered metrics and labels only.
				checkExpr(t, filepath.Base(p)+" query", strings.TrimSpace(q), records)
			}
			for _, ref := range metricRefs(line) {
				e, ok := internalmetrics.Lookup(baseMetric(ref))
				require.True(t, ok, "%s:%d: %s is not in the metrics registry", filepath.Base(p), i+1, ref)
				if e.Status == internalmetrics.StatusProposed {
					mentions++
					assert.True(t, comment, "%s:%d: autoscaling on Proposed metric %s — forbidden until ratified", filepath.Base(p), i+1, ref)
				}
			}
		}
	}
	assert.Positive(t, mentions, "manifests should name each trigger's post-ratification successor in a comment")
}
