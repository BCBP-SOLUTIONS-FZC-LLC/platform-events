package metrics

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"runtime/debug"
	"slices"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

// Identity is the Tier 1 required label set, injected as const labels on
// every platform_* metric so no call site can omit or misspell them (rule 8).
// Version is applied to the legacy build-info gauge only: a per-build label
// on Tier 1 metrics would multiply series on every deploy.
type Identity struct {
	Domain      string // e.g. "iam", "workflow", "billing"
	Service     string // e.g. "event-consumer"
	Environment string // e.g. "dev", "staging", "prod"
	Version     string // service build version, legacy build-info only
}

// identityValueRe bounds identity label values: lowercase, starts with a
// letter, [a-z0-9_-], at most 63 characters (same rule as platform-pgcommon).
var identityValueRe = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,62}$`)

// Validate reports whether every required label is present and well formed.
func (i Identity) Validate() error {
	var errs []error
	for _, f := range []struct{ label, value string }{
		{LabelDomain, i.Domain}, {LabelService, i.Service}, {LabelEnvironment, i.Environment},
	} {
		switch {
		case f.value == "":
			errs = append(errs, fmt.Errorf("%s is required", f.label))
		case !identityValueRe.MatchString(f.value):
			errs = append(errs, fmt.Errorf("%s %q must match %s", f.label, f.value, identityValueRe))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("platform-events: invalid metrics identity: %w", errors.Join(errs...))
	}
	return nil
}

func (i Identity) platformLabels() prometheus.Labels {
	return prometheus.Labels{LabelDomain: i.Domain, LabelService: i.Service, LabelEnvironment: i.Environment}
}

// DefaultEnvironment is used when neither APP_ENV nor ENVIRONMENT is set.
const DefaultEnvironment = "dev"

// Environment returns the deployment environment with platform-gincommon's
// (and platform-pgcommon's) precedence — APP_ENV, then ENVIRONMENT, else
// "dev" — trimmed and lowercased, plus the variable it came from.
func Environment() (env, source string) {
	for _, key := range []string{"APP_ENV", "ENVIRONMENT"} {
		if v := strings.ToLower(strings.TrimSpace(os.Getenv(key))); v != "" {
			return v, key
		}
	}
	return DefaultEnvironment, "default"
}

// ServiceName returns APP_NAME, trimmed and lowercased ("" when unset).
func ServiceName() string {
	return strings.ToLower(strings.TrimSpace(os.Getenv("APP_NAME")))
}

// modulePath is this library's Go module path.
const modulePath = "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events"

// LibraryVersion returns the platform-events module version the running
// binary was built with: the dependency version in a consuming service,
// "devel" in this module's own builds and tests, "unknown" without build info.
func LibraryVersion() string {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	return LibraryVersionFrom(bi)
}

// LibraryVersionFrom resolves the platform-events version from bi (see
// LibraryVersion).
func LibraryVersionFrom(bi *debug.BuildInfo) string {
	for _, dep := range bi.Deps {
		if dep.Path != modulePath {
			continue
		}
		if dep.Replace != nil {
			// A module replacement keeps its version; a filesystem
			// replacement (replace … => ../path) has none: a local build.
			if dep.Replace.Version != "" {
				return dep.Replace.Version
			}
			return "devel"
		}
		if dep.Version != "" && dep.Version != "(devel)" {
			return dep.Version
		}
		return "devel"
	}
	if bi.Main.Path == modulePath {
		if v := bi.Main.Version; v != "" && v != "(devel)" {
			return v
		}
		return "devel"
	}
	// This repository's own sub-modules (test/, tools/) build the library
	// from source. Test binaries carry no dependency list in their build
	// info, so this is how the suites see a local build.
	if strings.HasPrefix(bi.Main.Path, modulePath+"/") {
		return "devel"
	}
	return "unknown"
}

// RegistrationWarning reports a platform_* metric that could not be
// registered — typically because the registry already holds that name with a
// different label set or help text. That one shadow metric is disabled; the
// service and every other metric keep working.
type RegistrationWarning struct {
	Metric string
	Err    error
}

func (w RegistrationWarning) Error() string {
	return fmt.Sprintf("platform-events: %s not registered (metric disabled): %v", w.Metric, w.Err)
}

// tryRegister registers c with reg. An identical collector already registered
// is reused (so a re-init shares series); any other failure is returned.
func tryRegister[T prometheus.Collector](reg prometheus.Registerer, c T) (T, error) {
	if err := reg.Register(c); err != nil {
		if are, ok := errors.AsType[prometheus.AlreadyRegisteredError](err); ok {
			if existing, ok := are.ExistingCollector.(T); ok {
				return existing, nil
			}
		}
		var zero T
		return zero, err
	}
	return c, nil
}

// wrapCollision matches the error prometheus.WrapRegistererWith returns when
// a collector's const label is one the wrapper already applies. Pinned by
// test/unit/metrics/standard_test.go against the client_golang version.
var wrapCollision = regexp.MustCompile(`already existing label name "([a-zA-Z_][a-zA-Z0-9_]*)"`)

// registrar registers collectors on one registerer, remembering const labels
// the registerer itself injects (e.g. a gincommon.WrapRegistererWith wrapper
// adding service/environment) so they are applied exactly once — the
// wrapper's value wins.
type registrar struct {
	reg      prometheus.Registerer
	injected map[string]bool
}

func newRegistrar(reg prometheus.Registerer) *registrar {
	return &registrar{reg: reg, injected: map[string]bool{}}
}

func (r *registrar) withoutInjected(labels prometheus.Labels) prometheus.Labels {
	out := prometheus.Labels{}
	for k, v := range labels {
		if !r.injected[k] {
			out[k] = v
		}
	}
	return out
}

// register registers build(constLabels), retrying without a droppable label
// the registerer reports it already applies.
func register[T prometheus.Collector](r *registrar, labels prometheus.Labels, droppable []string, build func(prometheus.Labels) T) (T, error) {
	for range len(droppable) + 1 {
		c, err := tryRegister(r.reg, build(r.withoutInjected(labels)))
		if err == nil {
			return c, nil
		}
		m := wrapCollision.FindStringSubmatch(err.Error())
		if m == nil || r.injected[m[1]] || !slices.Contains(droppable, m[1]) {
			var zero T
			return zero, err
		}
		r.injected[m[1]] = true
	}
	var zero T
	return zero, errors.New("platform-events: could not resolve registerer label collisions")
}
