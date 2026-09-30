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

# Test package groups (explicit to handle per-group build tags cleanly).
# White-box tests that live alongside source (e.g. internal/core/service/)
# are included in TEST_UNIT_PKGS so they run with coverage instrumentation.
TEST_UNIT_PKGS := ./test/unit/... ./internal/core/service/...
TEST_INT_PKGS  := ./test/integration/...
TEST_E2E_PKGS  := ./test/e2e/...

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
GOVULNCHECK_VERSION ?= v1.1.4

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
	@echo "  make test-integration - integration tests (requires Docker / LocalStack); alias: test-int"
	@echo "  make test-e2e         - e2e tests (requires Docker / LocalStack)"
	@echo "  make test-smoke       - smoke tests (requires live AWS resources)"
	@echo "  make race             - unit + integration + e2e with -race flag"
	@echo "  make build            - compile reference CLI to bin/"
	@echo "  make cover            - coverage HTML report (runs test-ci)"
	@echo "  make cover-func       - coverage summary by function (runs test-ci)"
	@echo "  make ci               - tidy + fmt-check + vet + lint + metrics-lint + test-ci + build"
	@echo "  make docker-up        - start LocalStack (SNS + SQS + Postgres)"
	@echo "  make docker-build     - build the reference-CLI image as CI does (needs GO_PRIVATE_TOKEN)"
	@echo "  make pin-base-images  - fetch + pin SHA digests for Dockerfile base images"
	@echo "  make docker-down      - stop LocalStack"
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
	@GOLANG_DIGEST=$$(docker buildx imagetools inspect golang:1.26.6-alpine --format '{{.Manifest.Digest}}') && \
	 DISTROLESS_DIGEST=$$(docker buildx imagetools inspect gcr.io/distroless/static-debian13:nonroot --format '{{.Manifest.Digest}}') && \
	 sed -i.bak -E \
	   -e "s|FROM golang:1.26.6-alpine(@sha256:[a-f0-9]+)?|FROM golang:1.26.6-alpine@$$GOLANG_DIGEST|" \
	   -e "s|FROM gcr.io/distroless/static-debian13:nonroot(@sha256:[a-f0-9]+)?|FROM gcr.io/distroless/static-debian13:nonroot@$$DISTROLESS_DIGEST|" \
	   Dockerfile && rm -f Dockerfile.bak && \
	 echo "golang:1.26.6-alpine $$GOLANG_DIGEST" > .docker-digests && \
	 echo "gcr.io/distroless/static-debian13:nonroot $$DISTROLESS_DIGEST" >> .docker-digests && \
	 echo "Digests written to .docker-digests — commit both Dockerfile and .docker-digests"

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

.PHONY: fmt
fmt:
	@gofmt -l -w .

.PHONY: vet
vet:
	$(GO) vet ./...
	$(GO) vet -tags=$(ALL_TEST_TAGS) ./...

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

.PHONY: vuln-check
vuln-check:
	$(GO) run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) ./internal/... ./pkg/...

# -----------------------------
# LINT
# -----------------------------

.PHONY: lint
lint:
	@echo "Running linter..."
	$(GO) tool golangci-lint run ./...
	$(GO) tool golangci-lint run --build-tags=$(ALL_TEST_TAGS) ./...

# -----------------------------
# TESTS
# -----------------------------

# run_suite runs one coverage-instrumented suite into .coverage/<name>.out.
# Suites run in parallel (make -j3), so their logs interleave — on failure the
# `--- FAIL:` lines are re-printed in a summary block at the end.
#   $(1) = suite name   $(2) = go test packages + flags
define run_suite
	{ $(GO) test $(2) \
	  -coverpkg=$(COVER_PKG_LIST) \
	  -coverprofile=.coverage/$(1).out \
	  2>&1; echo $$? >.coverage/$(1).exitcode; } | tee .coverage/$(1).raw; \
	_exit=$$(cat .coverage/$(1).exitcode 2>/dev/null || echo 1); \
	[ "$$_exit" = "0" ] || { \
	  printf '\n\n=== FAILING $(1) TESTS (see full log above for details) ===\n'; \
	  grep '^--- FAIL:' .coverage/$(1).raw || printf '(no --- FAIL lines — check for DATA RACE or panic above)\n'; \
	  printf '=============================================================\n\n'; \
	}; \
	exit "$$_exit"
endef

.PHONY: _test-unit
_test-unit: | .coverage
	$(call run_suite,unit,$(TEST_UNIT_PKGS) -race -count=1 -timeout 120s)

.PHONY: _test-integration
_test-integration: | .coverage
	$(call run_suite,integration,$(TEST_INT_PKGS) -tags=integration -race -count=1 -timeout 300s)

.PHONY: _test-e2e
_test-e2e: | .coverage
	$(call run_suite,e2e,$(TEST_E2E_PKGS) -tags=e2e -race -count=1 -timeout 300s)

.PHONY: test
test:
	$(MAKE) -j2 _test-unit-plain _test-integration-plain

.PHONY: _test-unit-plain _test-integration-plain
_test-unit-plain:
	$(GO) test $(TEST_UNIT_PKGS) -count=1 -timeout 120s -v
_test-integration-plain:
	$(GO) test $(TEST_INT_PKGS) -tags=integration -count=1 -timeout 300s -v

# Merge the per-suite profiles into a single coverage.out (max-count
# strategy — any suite covering a block wins). Mirrors iam-org-membership.
.PHONY: _merge-coverage
_merge-coverage:
	@python3 scripts/merge_coverage.py \
	  .coverage/unit.out .coverage/integration.out .coverage/e2e.out \
	  > coverage.out
	@echo "==> coverage.out merged from all suites (max-count strategy)"

# test-ci: used by CI and release workflows; enables the race detector and
# writes the merged coverage.out that ci.yml / release.yml read.
# Includes e2e tests (requires Docker on the runner).
.PHONY: test-ci
test-ci: | .coverage
	$(MAKE) -j3 _test-unit _test-integration _test-e2e
	$(MAKE) _merge-coverage

.PHONY: test-unit
test-unit:
	$(GO) test $(TEST_UNIT_PKGS) -count=1 -timeout 60s -v

.PHONY: test-integration
test-integration:
	$(GO) test $(TEST_INT_PKGS) -tags=integration -count=1 -timeout 300s -v

# test-int: kept as an alias — referenced by README / CLAUDE.md / CONTRIBUTING.
.PHONY: test-int
test-int: test-integration

.PHONY: test-e2e
test-e2e:
	$(GO) test $(TEST_E2E_PKGS) -tags=e2e -count=1 -timeout 300s -v

.PHONY: test-smoke
test-smoke:
	$(GO) test ./test/smoke/... -tags=smoke -count=1 -timeout 60s -v

.PHONY: race
race:
	$(MAKE) -j3 _test-unit _test-integration _test-e2e

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
# DOCKER (LOCAL LOCALSTACK)
# -----------------------------

.PHONY: docker-up
docker-up:
	@echo "Starting LocalStack (SNS + SQS + Postgres)..."
	docker compose up -d

.PHONY: docker-down
docker-down:
	@echo "Stopping LocalStack..."
	docker compose down

# -----------------------------
# CI
# -----------------------------

.PHONY: ci
ci: tidy fmt-check vet lint metrics-lint test-ci build

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
	$(GO) test -count=1 -run 'TestStandard_|TestRules_|TestInventory_|TestWrapCollision' ./test/unit/metrics/

# metrics-doc: regenerate docs/observability/metrics-registry.md from the registry.
.PHONY: metrics-doc
metrics-doc:
	$(GO) test -count=1 -run TestInventory ./test/unit/metrics/ -update

# rules-check: promtool syntax check + alert unit tests (requires Docker).
PROMETHEUS_IMAGE ?= prom/prometheus:v3.5.0

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
