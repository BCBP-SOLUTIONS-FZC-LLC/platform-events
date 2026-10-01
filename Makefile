# -----------------------------
# CONFIG
# -----------------------------
-include .env

APP_NAME      ?= platform-events
APP_ENV       ?= dev
GO            ?= go
BUILD_VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

# Private modules — resolved via SSH (git@github.com:) using the global URL
# rewrite. No tokens needed for local dev; an SSH key registered with the
# BCBP-SOLUTIONS-FZC-LLC org is required.
export GOPRIVATE  ?= github.com/BCBP-SOLUTIONS-FZC-LLC/*
export GONOSUMDB  ?= github.com/BCBP-SOLUTIONS-FZC-LLC/*

# Docker socket for testcontainers-go (Mac Docker Desktop uses a user socket).
# Only exported when a socket actually exists, so a Docker context set up by
# another runtime (e.g. colima) is not overridden with a dead path.
DOCKER_SOCKET := $(shell if [ -S /Users/$(USER)/.docker/run/docker.sock ]; then echo unix:///Users/$(USER)/.docker/run/docker.sock; elif [ -S /var/run/docker.sock ]; then echo unix:///var/run/docker.sock; fi)
ifneq ($(DOCKER_SOCKET),)
export DOCKER_HOST ?= $(DOCKER_SOCKET)
endif

export APP_NAME APP_ENV BUILD_VERSION

# Module layout — three modules so consumers of the library inherit only what
# the library itself imports (same as platform-pgcommon):
#   .       the library (what services `require`)
#   test/   unit / integration / e2e / smoke suites + testcontainers fixtures
#   tools/  golangci-lint (run via `go tool -modfile=tools/go.mod`)
# White-box tests beside the sources (internal/core/service/*_test.go) stay in
# the root module and run as ROOT_TEST_PKGS.
ROOT_TEST_PKGS := ./internal/core/service/...
TEST_UNIT_PKGS := ./unit/...
TEST_INT_PKGS  := ./integration/...
TEST_E2E_PKGS  := ./e2e/...
TEST_MOD       := cd test &&
LINT           := $(GO) tool -modfile=$(CURDIR)/tools/go.mod golangci-lint

# Every build tag the test/ tree declares (integration: test/integration,
# e2e: test/e2e). vet and lint run a second pass with all of them — CI's
# quality gate calls `make vet`/`make lint`, so without it the tagged test
# files were never vetted or linted. smoke is excluded: test/smoke needs live
# AWS resources and is excluded from golangci-lint in .golangci.yml.
ALL_TEST_TAGS := integration,e2e

# Source packages measured for coverage (excludes test helpers and cmd).
# Uses tr+sed instead of paste -sd, because macOS BSD paste rejects combined flags.
COVER_PKG_LIST := $(shell $(GO) list ./internal/... ./pkg/... 2>/dev/null | tr '\n' ',' | sed 's/,$$//')

# Pinned to prevent unintended breakage from new advisories landing mid-CI.
# To upgrade: go run golang.org/x/vuln/cmd/govulncheck@latest --version, then update below.
GOVULNCHECK_VERSION ?= v1.8.0

# -----------------------------
# SETUP
# -----------------------------

.PHONY: setup
setup:
	@test -f .env || cp .env-example .env
	@mkdir -p .git/hooks
	@test -f .githooks/pre-commit && cp .githooks/pre-commit .git/hooks/pre-commit && chmod +x .git/hooks/pre-commit || true
	@echo "Environment ready (.env)"

.PHONY: install-hooks
install-hooks:
	@mkdir -p .git/hooks
	@cp .githooks/pre-commit .git/hooks/pre-commit
	@chmod +x .git/hooks/pre-commit
	@echo "Installed git hooks"

# godoc: serve package documentation locally using pkgsite.
# Opens http://localhost:8080 — browse to the module path in the UI.
.PHONY: godoc
godoc:
	@echo "Starting pkgsite at http://localhost:8080 — press Ctrl-C to stop"
	$(GO) run golang.org/x/pkgsite/cmd/pkgsite@latest -open .

.PHONY: help
help:
	@echo "Available commands:"
	@echo "  make setup            - copy .env-example to .env if missing; install git hooks"
	@echo "  make install-hooks    - install .githooks/pre-commit into .git/hooks"
	@echo "  make tidy             - go mod tidy"
	@echo "  make fmt              - gofmt -w the whole module"
	@echo "  make fmt-check        - verify gofmt formatting (mirrors CI)"
	@echo "  make vet              - go vet (default build + every test build tag)"
	@echo "  make lint             - run golangci-lint (default build + every test build tag)"
	@echo "  make metrics-lint     - observability standard gate (metric conformance, rule files, inventory)"
	@echo "  make metrics-doc      - regenerate docs/observability/metrics-registry.md from the registry"
	@echo "  make rules-check      - promtool check + unit tests for monitoring/prometheus (requires Docker)"
	@echo "  make test             - unit + integration tests in parallel (requires Docker)"
	@echo "  make test-ci          - unit + integration + e2e with race detector + merged coverage (used in CI)"
	@echo "  make test-unit        - unit tests only (no Docker required)"
	@echo "  make test-integration - integration tests (Docker; floci + Postgres via testcontainers); alias: test-int"
	@echo "  make test-e2e         - e2e tests (Docker; floci + Postgres via testcontainers)"
	@echo "  make test-smoke       - smoke tests (requires live AWS resources)"
	@echo "  make race             - unit + integration + e2e with -race flag"
	@echo "  make build            - compile reference CLI to bin/"
	@echo "  make cover            - coverage HTML report (runs test-ci)"
	@echo "  make cover-func       - coverage summary by function (runs test-ci)"
	@echo "  make ci               - tidy + mod-verify + fmt-check + vet + lint + docs-check + metrics-lint + rules-check + dashboards-check + test-ci + build (the same gates as CI)"
	@echo "  make docs-check       - every docs/architecture/mermaid/*.mmd embedded verbatim in ARCHITECTURE.md"
	@echo "  make docker-up        - start floci (SNS/SQS, :4574) + floci-ui (http://localhost:4505) + Postgres (:5538)"
	@echo "  make docker-build     - build the reference-CLI image as CI does (needs GO_PRIVATE_TOKEN)"
	@echo "  make pin-base-images  - fetch + pin SHA digests for Dockerfile base images"
	@echo "  make docker-down      - stop the local containers"
	@echo "  make mod-verify       - go mod verify (check module download integrity)"
	@echo "  make vuln-check       - govulncheck on internal + pkg"
	@echo "  make godoc            - serve local godoc/pkgsite at http://localhost:8080"
	@echo "  make clean            - remove build artefacts"

# pin-base-images: fetch and pin the current SHA digests for Dockerfile base
# images. Writes the digests both to the Dockerfile FROM lines and to
# .docker-digests (a checked-in provenance record). CI's quality gate rejects
# any FROM line without a digest.
.PHONY: pin-base-images
pin-base-images:
	@echo "Fetching SHA digests for Dockerfile base images..."
	@GOLANG_DIGEST=$$(docker buildx imagetools inspect golang:1.26.8-alpine --format '{{.Manifest.Digest}}') && \
	 DISTROLESS_DIGEST=$$(docker buildx imagetools inspect gcr.io/distroless/static-debian13:nonroot --format '{{.Manifest.Digest}}') && \
	 sed -i.bak -E \
	   -e "s|FROM golang:1.26.8-alpine(@sha256:[a-f0-9]+)?|FROM golang:1.26.8-alpine@$$GOLANG_DIGEST|" \
	   -e "s|FROM gcr.io/distroless/static-debian13:nonroot(@sha256:[a-f0-9]+)?|FROM gcr.io/distroless/static-debian13:nonroot@$$DISTROLESS_DIGEST|" \
	   Dockerfile && rm -f Dockerfile.bak && \
	 echo "golang:1.26.8-alpine $$GOLANG_DIGEST" > .docker-digests && \
	 echo "gcr.io/distroless/static-debian13:nonroot $$DISTROLESS_DIGEST" >> .docker-digests && \
	 PROMETHEUS_DIGEST=$$(docker buildx imagetools inspect prom/prometheus:v3.5.0 --format '{{.Manifest.Digest}}') && \
	 sed -i.bak -E "s|^PROMETHEUS_IMAGE \?= prom/prometheus:v3.5.0(@sha256:[a-f0-9]+)?|PROMETHEUS_IMAGE ?= prom/prometheus:v3.5.0@$$PROMETHEUS_DIGEST|" Makefile && rm -f Makefile.bak && \
	 echo "prom/prometheus:v3.5.0 $$PROMETHEUS_DIGEST" >> .docker-digests && \
	 for img in floci/floci:2.1.0 floci/floci:2.1.0-compat floci/floci-ui:0.5.0 postgres:16-alpine; do \
	   d=$$(docker buildx imagetools inspect $$img --format '{{.Manifest.Digest}}') && \
	   IMG=$$img D=$$d perl -pi -e 's/\Q$$ENV{IMG}\E(?:\@sha256:[a-f0-9]+)?(?=["\s]|$$)/$$ENV{IMG}\@$$ENV{D}/g' \
	     docker-compose.yml test/fixtures/floci.go test/fixtures/db.go && \
	   echo "$$img $$d" >> .docker-digests; \
	 done && \
	 echo "Digests written to .docker-digests — commit Dockerfile, Makefile, docker-compose.yml, test/fixtures and .docker-digests"

# docker-build: build the reference-CLI image the way CI does. Needs a GitHub
# token with read access to the private BCBP modules in GO_PRIVATE_TOKEN
# (e.g. GO_PRIVATE_TOKEN=$$(gh auth token) make docker-build).
.PHONY: docker-build
docker-build:
	@test -n "$$GO_PRIVATE_TOKEN" || { echo "GO_PRIVATE_TOKEN is not set"; exit 1; }
	docker build --platform linux/amd64 \
	  --secret id=go_private_token,env=GO_PRIVATE_TOKEN \
	  --build-arg BUILD_VERSION=$(BUILD_VERSION) \
	  -t $(APP_NAME)-ci-test .

# -----------------------------
# GO BASICS
# -----------------------------

.PHONY: tidy
tidy:
	$(GO) mod tidy
	cd test && $(GO) mod tidy
	cd tools && $(GO) mod tidy

.PHONY: fmt
fmt:
	@gofmt -l -w .

.PHONY: vet
vet:
	$(GO) vet ./...
	$(TEST_MOD) $(GO) vet ./...
	$(TEST_MOD) $(GO) vet -tags=$(ALL_TEST_TAGS) ./...

.PHONY: fmt-check
fmt-check:
	@unformatted=$$(gofmt -l . 2>/dev/null); \
	if [ -n "$$unformatted" ]; then \
		echo "FAIL: unformatted files:"; \
		echo "$$unformatted"; \
		exit 1; \
	fi
	@echo "gofmt: all files formatted"

.PHONY: mod-verify
mod-verify:
	$(GO) mod verify
	cd test && $(GO) mod verify
	cd tools && $(GO) mod verify

.PHONY: vuln-check
vuln-check:
	$(GO) run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) ./internal/... ./pkg/...

# -----------------------------
# LINT
# -----------------------------

.PHONY: lint
lint:
	@echo "Running linter..."
	$(LINT) run ./...
	$(TEST_MOD) $(LINT) run --build-tags=$(ALL_TEST_TAGS) ./...

# -----------------------------
# TESTS
# -----------------------------

# run_suite runs one coverage-instrumented suite into .coverage/<name>.out.
# Suites run in parallel (make -j3), so their logs interleave — on failure the
# `--- FAIL:` lines are re-printed in a summary block at the end.
#   $(1) = suite name   $(2) = go test packages + flags   $(3) = module prefix
#   ($(TEST_MOD) for the test/ module, empty for the root module)
define run_suite
	rm -f $(CURDIR)/.coverage/$(1).exitcode $(CURDIR)/.coverage/$(1).out; \
	{ $(3) $(GO) test $(2) \
	  -coverpkg=$(COVER_PKG_LIST) \
	  -coverprofile=$(CURDIR)/.coverage/$(1).out \
	  2>&1; echo $$? >$(CURDIR)/.coverage/$(1).exitcode; } | tee $(CURDIR)/.coverage/$(1).raw; \
	_exit=$$(cat $(CURDIR)/.coverage/$(1).exitcode 2>/dev/null || echo 1); \
	[ "$$_exit" = "0" ] || { \
	  printf '\n\n=== FAILING $(1) TESTS (see full log above for details) ===\n'; \
	  grep '^--- FAIL:' $(CURDIR)/.coverage/$(1).raw || printf '(no --- FAIL lines — check for DATA RACE or panic above)\n'; \
	  printf '=============================================================\n\n'; \
	}; \
	exit "$$_exit"
endef

.PHONY: _test-root
_test-root: | .coverage
	$(call run_suite,root,$(ROOT_TEST_PKGS) -race -count=1 -timeout 120s,)

.PHONY: _test-unit
_test-unit: | .coverage
	$(call run_suite,unit,$(TEST_UNIT_PKGS) -race -count=1 -timeout 120s,$(TEST_MOD))

.PHONY: _test-integration
_test-integration: | .coverage
	$(call run_suite,integration,$(TEST_INT_PKGS) -tags=integration -race -count=1 -timeout 300s,$(TEST_MOD))

.PHONY: _test-e2e
_test-e2e: | .coverage
	$(call run_suite,e2e,$(TEST_E2E_PKGS) -tags=e2e -race -count=1 -timeout 300s,$(TEST_MOD))

.PHONY: test
test:
	$(MAKE) -j2 _test-unit-plain _test-integration-plain

.PHONY: _test-unit-plain _test-integration-plain
_test-unit-plain:
	$(GO) test $(ROOT_TEST_PKGS) -count=1 -timeout 120s -v
	$(TEST_MOD) $(GO) test $(TEST_UNIT_PKGS) -count=1 -timeout 120s -v
_test-integration-plain:
	$(TEST_MOD) $(GO) test $(TEST_INT_PKGS) -tags=integration -count=1 -timeout 300s -v

# Merge the per-suite profiles into a single coverage.out (max-count
# strategy — any suite covering a block wins). Mirrors iam-org-membership.
.PHONY: _merge-coverage
_merge-coverage:
	@python3 scripts/merge_coverage.py \
	  .coverage/root.out .coverage/unit.out .coverage/integration.out .coverage/e2e.out \
	  > coverage.out
	@echo "==> coverage.out merged from all suites (max-count strategy)"

# test-ci: used by CI and release workflows; enables the race detector and
# writes the merged coverage.out that ci.yml / release.yml read.
# Includes e2e tests (requires Docker on the runner).
.PHONY: test-ci
test-ci: | .coverage
	$(MAKE) -j4 _test-root _test-unit _test-integration _test-e2e
	$(MAKE) _merge-coverage

.PHONY: test-unit
test-unit:
	$(GO) test $(ROOT_TEST_PKGS) -count=1 -timeout 60s -v
	$(TEST_MOD) $(GO) test $(TEST_UNIT_PKGS) -count=1 -timeout 60s -v

.PHONY: test-integration
test-integration:
	$(TEST_MOD) $(GO) test $(TEST_INT_PKGS) -tags=integration -count=1 -timeout 300s -v

# test-int: kept as an alias — referenced by README / CLAUDE.md / CONTRIBUTING.
.PHONY: test-int
test-int: test-integration

.PHONY: test-e2e
test-e2e:
	$(TEST_MOD) $(GO) test $(TEST_E2E_PKGS) -tags=e2e -count=1 -timeout 300s -v

.PHONY: test-smoke
test-smoke:
	$(TEST_MOD) $(GO) test ./smoke/... -tags=smoke -count=1 -timeout 60s -v

.PHONY: race
race:
	$(MAKE) -j4 _test-root _test-unit _test-integration _test-e2e

# -----------------------------
# BUILD
# -----------------------------

.PHONY: build
build:
	@echo "Building binary..."
	@mkdir -p bin
	$(GO) build -ldflags "-X main.version=$(BUILD_VERSION)" -o bin/$(APP_NAME) ./cmd/platform-events
	@echo "Verifying library packages compile..."
	$(GO) build ./internal/... ./pkg/...

# -----------------------------
# DOCKER (LOCAL FLOCI)
# -----------------------------

.PHONY: docker-up
docker-up:
	@echo "Starting floci (SNS/SQS, always free) + floci-ui (http://localhost:$${FLOCI_UI_PORT:-4505}) + Postgres..."
	docker compose up -d --wait floci floci-ui postgres
	@echo "Demo topology ready (scripts/init-floci.sh): topic platform-events-demo, queue platform-events-demo-q (+ -dlq)"

.PHONY: docker-down
docker-down:
	@echo "Stopping local containers..."
	docker compose down

# -----------------------------
# CI
# -----------------------------

.PHONY: ci
ci: tidy mod-verify fmt-check vet lint docs-check metrics-lint rules-check dashboards-check test-ci build

# -----------------------------
# OBSERVABILITY STANDARD
# -----------------------------

# metrics-lint: Enterprise Platform Observability Standard gate — runtime
# conformance of every registered collector against the metrics registry
# (namespace tier, naming, _total/_seconds suffixes, required labels, label
# vocabulary, registry compliance), rule-file governance (registry metrics
# only, no Proposed metric as a query target, labels in vocabulary, runbook
# anchors) and the generated inventory being up to date.
# See docs/observability/README.md.
.PHONY: metrics-lint
metrics-lint:
	$(TEST_MOD) $(GO) test -count=1 -run 'TestStandard_|TestRules_|TestMonitoring_|TestInventory_|TestWrapCollision' ./unit/metrics/

# metrics-doc: regenerate docs/observability/metrics-registry.md from the registry.
.PHONY: metrics-doc
metrics-doc:
	$(TEST_MOD) $(GO) test -count=1 -run TestInventory ./unit/metrics/ -update

# rules-check: promtool syntax check + alert unit tests (requires Docker).
# Digest-pinned like the Dockerfile base images; refreshed by pin-base-images.
PROMETHEUS_IMAGE ?= prom/prometheus:v3.5.0@sha256:63805ebb8d2b3920190daf1cb14a60871b16fd38bed42b857a3182bc621f4996

# dashboards-check: PromQL syntax gate for monitoring/grafana/*.json (same
# script as platform-pgcommon; requires Docker and jq). Governance — registry
# metrics, labels, "(Proposed)"/"(legacy)" titles — is metrics-lint's job.
# docs-check: diagram drift gate — every docs/architecture/mermaid/*.mmd must
# be embedded byte-identically in ARCHITECTURE.md (same script as
# platform-pgcommon v1.4.2).
.PHONY: docs-check
docs-check:
	bash .github/scripts/docs-mermaid-sync.sh

.PHONY: dashboards-check
dashboards-check:
	PROMETHEUS_IMAGE=$(PROMETHEUS_IMAGE) bash .github/scripts/dashboard-promql.sh

.PHONY: rules-check
rules-check:
	docker run --rm -v "$(CURDIR)/monitoring/prometheus":/rules -w /rules --entrypoint promtool $(PROMETHEUS_IMAGE) check rules platform-events.rules.yml
	docker run --rm -v "$(CURDIR)/monitoring/prometheus":/rules -w /rules --entrypoint promtool $(PROMETHEUS_IMAGE) test rules platform-events.rules.test.yml

# -----------------------------
# COVERAGE
# -----------------------------

.PHONY: cover
cover: test-ci
	$(GO) tool cover -html=coverage.out

.PHONY: cover-func
cover-func: test-ci
	$(GO) tool cover -func=coverage.out

# -----------------------------
# CLEAN
# -----------------------------

.coverage:
	@mkdir -p .coverage

.PHONY: clean
clean:
	rm -rf bin .coverage
	rm -f coverage.out coverage.html coverage_*.out *.coverprofile profile.cov
