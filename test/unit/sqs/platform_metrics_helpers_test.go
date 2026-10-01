package sqs_test

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/test/fixtures"
)

// initPlatformMetrics registers the Tier 1 metrics on a fresh registry for
// one test and restores the previous Tier 1 set afterwards.
func initPlatformMetrics(t *testing.T) *prometheus.Registry {
	t.Helper()
	return fixtures.InitPlatformMetrics(t)
}

// findMetric returns the series of family name whose labels include want.
func findMetric(t *testing.T, reg *prometheus.Registry, name string, want map[string]string) *dto.Metric {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
	series:
		for _, m := range mf.GetMetric() {
			got := map[string]string{}
			for _, lp := range m.GetLabel() {
				got[lp.GetName()] = lp.GetValue()
			}
			for k, v := range want {
				if got[k] != v {
					continue series
				}
			}
			return m
		}
	}
	return nil
}

func counterValue(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) float64 {
	t.Helper()
	if m := findMetric(t, reg, name, labels); m != nil {
		return m.GetCounter().GetValue()
	}
	return 0
}

func histogramCount(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) uint64 {
	t.Helper()
	if m := findMetric(t, reg, name, labels); m != nil {
		return m.GetHistogram().GetSampleCount()
	}
	return 0
}
