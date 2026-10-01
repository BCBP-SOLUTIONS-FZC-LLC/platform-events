package metrics_test

// Rule-file governance for monitoring/prometheus/*.rules.yml (Enterprise
// Platform Observability Standard, rules 9–12 and Backward Compatibility):
//
//   - every metric an expression references is in the registry
//   - no expression references a Proposed (unratified) metric — they may be
//     named in comments only
//   - every label an expression filters or groups by belongs to the
//     vocabulary of the metrics it references (or is `le`)
//   - every alert has a severity, a summary and a runbook_url whose anchor
//     exists in docs/observability/runbook.md
//
// promtool (make rules-check) validates PromQL syntax and runs the alert unit
// tests; this test validates the governance rules promtool knows nothing about.

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"

	internalmetrics "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/adapter/outbound/metrics"
)

const (
	rulesDir    = "../../../monitoring/prometheus"
	runbookPath = "../../../docs/observability/runbook.md"
)

type ruleFile struct {
	Groups []struct {
		Name  string `yaml:"name"`
		Rules []struct {
			Record      string            `yaml:"record"`
			Alert       string            `yaml:"alert"`
			Expr        string            `yaml:"expr"`
			Labels      map[string]string `yaml:"labels"`
			Annotations map[string]string `yaml:"annotations"`
		} `yaml:"rules"`
	} `yaml:"groups"`
}

var (
	recordRef   = regexp.MustCompile(`\bplatform_events:[a-z0-9_:]+`)
	metricRef   = regexp.MustCompile(`\b(?:platform|events|outbox|sqs)_[a-z0-9_]+`)
	groupingRef = regexp.MustCompile(`\b(?:by|without)\s*\(([^)]*)\)`)
	selectorRef = regexp.MustCompile(`\{([^}]*)\}`)
	matcherName = regexp.MustCompile(`([a-zA-Z_][a-zA-Z0-9_]*)\s*(?:=~|!~|!=|=)`)
	anchorRef   = regexp.MustCompile(`#([a-z0-9-]+)$`)
	headingChar = regexp.MustCompile(`[^a-z0-9 -]`)
)

// baseMetric maps a histogram series (_bucket/_count/_sum) to its metric.
func baseMetric(ref string) string {
	if _, ok := internalmetrics.Lookup(ref); ok {
		return ref
	}
	for _, suffix := range []string{"_bucket", "_count", "_sum"} {
		if base, ok := strings.CutSuffix(ref, suffix); ok {
			if e, ok := internalmetrics.Lookup(base); ok && e.Type == internalmetrics.TypeHistogram {
				return base
			}
		}
	}
	return ref
}

// metricRefs returns the metric names in expr, ignoring recording-rule names.
func metricRefs(expr string) []string {
	return metricRef.FindAllString(recordRef.ReplaceAllString(expr, " "), -1)
}

func loadRuleFiles(t *testing.T) map[string]ruleFile {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(rulesDir, "*.rules.yml"))
	require.NoError(t, err)
	require.NotEmpty(t, paths, "no rule files found under %s", rulesDir)
	out := map[string]ruleFile{}
	for _, p := range paths {
		raw, err := os.ReadFile(p) //nolint:gosec // paths come from a fixed glob in the repo
		require.NoError(t, err)
		var rf ruleFile
		require.NoError(t, yaml.Unmarshal(raw, &rf), p)
		out[filepath.Base(p)] = rf
	}
	return out
}

func runbookAnchors(t *testing.T) map[string]bool {
	t.Helper()
	raw, err := os.ReadFile(runbookPath)
	require.NoError(t, err)
	anchors := map[string]bool{}
	for _, line := range strings.Split(string(raw), "\n") {
		if h, ok := strings.CutPrefix(line, "## "); ok {
			a := headingChar.ReplaceAllString(strings.ToLower(strings.TrimSpace(h)), "")
			anchors[strings.ReplaceAll(a, " ", "-")] = true
		}
	}
	return anchors
}

func TestRules_RegistryCompliance(t *testing.T) {
	anchors := runbookAnchors(t)
	for file, rf := range loadRuleFiles(t) {
		records := map[string][]string{} // recording rule → labels it keeps
		for _, g := range rf.Groups {
			for _, r := range g.Rules {
				if r.Record != "" {
					records[r.Record] = groupingLabels(r.Expr)
				}
			}
		}
		for _, g := range rf.Groups {
			for _, r := range g.Rules {
				name := r.Record + r.Alert
				t.Run(file+"/"+name, func(t *testing.T) {
					// namespace is a scrape-target label (Kubernetes service
					// discovery), not a metric label: legacy rules keep it so
					// one Prometheus scraping several environments does not
					// mix them (legacy metrics have no environment label).
					allowed := map[string]bool{"le": true, "namespace": true}
					var refs []string
					for _, ref := range metricRefs(r.Expr) {
						m := baseMetric(ref)
						refs = append(refs, m)
						e, ok := internalmetrics.Lookup(m)
						if !assert.True(t, ok, "%s references %s, which is not in the metrics registry", name, ref) {
							continue
						}
						assert.NotEqual(t, internalmetrics.StatusProposed, e.Status,
							"%s uses Proposed metric %s as a live query target — forbidden until it is ratified (rules 11/12); use its legacy predecessor %v",
							name, m, e.Supersedes)
						for _, l := range append(slices.Clone(e.RequiredLabels), e.ApprovedLabels...) {
							allowed[l] = true
						}
					}
					for _, rec := range recordRef.FindAllString(r.Expr, -1) {
						kept, ok := records[rec]
						assert.True(t, ok, "%s references unknown recording rule %s", name, rec)
						refs = append(refs, rec)
						for _, l := range kept {
							allowed[l] = true
						}
					}
					require.NotEmpty(t, refs, "%s references no platform-events metric", name)

					for _, l := range append(groupingLabels(r.Expr), selectorLabels(r.Expr)...) {
						assert.True(t, allowed[l], "%s uses label %q, which is not in the vocabulary of %v", name, l, refs)
					}

					if r.Alert != "" {
						assert.Contains(t, []string{"critical", "warning", "info"}, r.Labels["severity"], "%s: severity", name)
						assert.NotEmpty(t, r.Annotations["summary"], "%s: summary", name)
						m := anchorRef.FindStringSubmatch(r.Annotations["runbook_url"])
						if assert.NotNil(t, m, "%s: runbook_url must link to a runbook section", name) {
							assert.True(t, anchors[m[1]], "%s: runbook anchor #%s does not exist in %s", name, m[1], runbookPath)
						}
					}
				})
			}
		}
	}
}

// TestRules_ProposedMetricsOnlyInComments: the files name Proposed
// successors, but only in comments.
func TestRules_ProposedMetricsOnlyInComments(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join(rulesDir, "*.rules.yml"))
	require.NoError(t, err)
	mentions := 0
	for _, p := range paths {
		raw, err := os.ReadFile(p) //nolint:gosec // paths come from a fixed glob in the repo
		require.NoError(t, err)
		for i, line := range strings.Split(string(raw), "\n") {
			for _, ref := range metricRefs(line) {
				e, ok := internalmetrics.Lookup(baseMetric(ref))
				if !ok || e.Status != internalmetrics.StatusProposed {
					continue
				}
				mentions++
				assert.True(t, strings.HasPrefix(strings.TrimSpace(line), "#"),
					"%s:%d: Proposed metric %s outside a comment", filepath.Base(p), i+1, ref)
			}
		}
	}
	assert.Positive(t, mentions, "the rule files should name each legacy rule's post-ratification successor in a comment")
}

// TestRules_EveryAlertHasARunbookSection: no orphan runbook sections either.
func TestRules_EveryRunbookSectionHasAnAlert(t *testing.T) {
	alerts := map[string]bool{}
	for _, rf := range loadRuleFiles(t) {
		for _, g := range rf.Groups {
			for _, r := range g.Rules {
				if r.Alert != "" {
					alerts[strings.ToLower(r.Alert)] = true
				}
			}
		}
	}
	for anchor := range runbookAnchors(t) {
		assert.True(t, alerts[anchor], "runbook section #%s has no matching alert", anchor)
	}
}

func groupingLabels(expr string) []string {
	var out []string
	for _, m := range groupingRef.FindAllStringSubmatch(expr, -1) {
		for _, l := range strings.Split(m[1], ",") {
			if l = strings.TrimSpace(l); l != "" {
				out = append(out, l)
			}
		}
	}
	return out
}

func selectorLabels(expr string) []string {
	var out []string
	for _, m := range selectorRef.FindAllStringSubmatch(expr, -1) {
		for _, mm := range matcherName.FindAllStringSubmatch(m[1], -1) {
			out = append(out, mm[1])
		}
	}
	return out
}
