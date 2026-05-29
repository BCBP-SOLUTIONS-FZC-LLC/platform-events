# -----------------------------
# CONFIG
# -----------------------------
-include .env

APP_NAME      ?= platform-events
APP_ENV       ?= dev
GO            ?= go
BUILD_VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

export APP_NAME APP_ENV BUILD_VERSION

# Test package groups (explicit to handle per-group build tags cleanly)
TEST_UNIT_PKGS := ./test/unit/...
TEST_INT_PKGS  := ./test/integration/...
TEST_E2E_PKGS  := ./test/e2e/...

# Source packages measured for coverage (excludes test helpers and cmd).
# Uses tr+sed instead of paste -sd, because macOS BSD paste rejects combined flags.
COVER_PKG_LIST := $(shell $(GO) list ./internal/... ./pkg/... | tr '\n' ',' | sed 's/,$$//')

# -----------------------------
# SETUP
# -----------------------------

.PHONY: setup
setup:
	@test -f .env || cp .env-example .env
	@echo "Environment ready (.env)"

.PHONY: help
help:
	@echo "Available commands:"
	@echo "  make setup           - copy .env-example to .env if missing"
	@echo "  make tidy            - go mod tidy"
	@echo "  make fmt             - go fmt ./..."
	@echo "  make vet             - go vet all packages"
	@echo "  make lint            - run golangci-lint"
	@echo "  make test            - unit + integration tests (requires Docker)"
	@echo "  make test-ci         - test with race detector (used in CI)"
	@echo "  make test-unit       - unit tests only"
	@echo "  make test-int        - integration tests (requires Docker / LocalStack)"
	@echo "  make test-smoke      - smoke tests (requires live AWS resources)"
	@echo "  make test-e2e        - e2e tests (requires Docker / LocalStack)"
	@echo "  make build           - compile reference CLI to bin/"
	@echo "  make docker-up       - start LocalStack (SNS + SQS + Postgres)"
	@echo "  make docker-down     - stop LocalStack"
	@echo "  make cover           - coverage profile + open HTML report"
	@echo "  make cover-func      - coverage summary by function"
	@echo "  make ci              - tidy + vet + lint + test-ci + build"
	@echo "  make godoc           - serve docs locally via pkgsite (http://localhost:8080)"
	@echo "  make fmt-check       - verify gofmt formatting (no changes applied)"
	@echo "  make mod-verify      - go mod verify (check module download integrity)"
	@echo "  make vuln-check      - govulncheck on library packages"
	@echo "  make race            - unit + integration tests with -race flag"
	@echo "  make clean           - remove build artefacts"

# -----------------------------
# GO BASICS
# -----------------------------

.PHONY: tidy
tidy:
	$(GO) mod tidy

.PHONY: fmt
fmt:
	$(GO) fmt ./...

.PHONY: vet
vet:
	$(GO) vet ./...

# -----------------------------
# LINT
# -----------------------------

.PHONY: lint
lint:
	@echo "Running linter..."
	$(GO) tool golangci-lint run

# -----------------------------
# TESTS
# -----------------------------

.PHONY: test
test:
	$(GO) test $(TEST_UNIT_PKGS) -count=1 -timeout 120s -v
	$(GO) test $(TEST_INT_PKGS)  -tags=integration -count=1 -timeout 300s -v

# test-ci: used by CI and release workflows; enables the race detector.
.PHONY: test-ci
test-ci:
	$(GO) test $(TEST_UNIT_PKGS) -race -count=1 -timeout 120s
	$(GO) test $(TEST_INT_PKGS)  -tags=integration -race -count=1 -timeout 300s

.PHONY: test-unit
test-unit:
	$(GO) test $(TEST_UNIT_PKGS) -count=1 -timeout 60s -v

.PHONY: test-int
test-int:
	$(GO) test $(TEST_INT_PKGS) -tags=integration -count=1 -timeout 120s -v

.PHONY: test-smoke
test-smoke:
	$(GO) test ./test/smoke/... -tags=smoke -count=1 -timeout 60s -v

.PHONY: test-e2e
test-e2e:
	$(GO) test $(TEST_E2E_PKGS) -tags=e2e -count=1 -timeout 300s -v

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
ci: tidy vet lint test-ci build

# -----------------------------
# COVERAGE
# -----------------------------

.PHONY: cover
cover:
	$(GO) test \
	  $(TEST_UNIT_PKGS) $(TEST_INT_PKGS) $(TEST_E2E_PKGS) \
	  -tags=integration,e2e \
	  -race -count=1 -timeout 300s \
	  -coverpkg=$(COVER_PKG_LIST) \
	  -coverprofile=coverage.out
	$(GO) tool cover -html=coverage.out

.PHONY: cover-func
cover-func:
	$(GO) test \
	  $(TEST_UNIT_PKGS) $(TEST_INT_PKGS) $(TEST_E2E_PKGS) \
	  -tags=integration,e2e \
	  -race -count=1 -timeout 300s \
	  -coverpkg=$(COVER_PKG_LIST) \
	  -coverprofile=coverage.out
	$(GO) tool cover -func=coverage.out

# -----------------------------
# DEBUG HELPERS
# -----------------------------

.PHONY: race
race:
	$(GO) test \
	  $(TEST_UNIT_PKGS) $(TEST_INT_PKGS) $(TEST_E2E_PKGS) \
	  -tags=integration,e2e \
	  -race -count=1 -timeout 300s

# -----------------------------
# GODOC
# -----------------------------

# godoc: serve package documentation locally using pkgsite.
# Opens http://localhost:8080 — browse to the module path in the UI.
.PHONY: godoc
godoc:
	@echo "Starting pkgsite at http://localhost:8080 — press Ctrl-C to stop"
	$(GO) run golang.org/x/pkgsite/cmd/pkgsite@latest -open .

# -----------------------------
# CHECKS (mirror what CI runs; safe to call locally before pushing)
# -----------------------------

# fmt-check: verify formatting without modifying files (mirrors CI gofmt step).
.PHONY: fmt-check
fmt-check:
	@unformatted=$$(gofmt -l cmd/ internal/ pkg/ test/); \
	if [ -n "$$unformatted" ]; then \
		echo "FAIL: unformatted files:"; \
		echo "$$unformatted"; \
		exit 1; \
	fi
	@echo "gofmt: all files formatted"

# mod-verify: check that downloaded module zips match go.sum hashes.
.PHONY: mod-verify
mod-verify:
	$(GO) mod verify

# vuln-check: scan library packages for known vulnerabilities (excludes cmd/).
.PHONY: vuln-check
vuln-check:
	$(GO) run golang.org/x/vuln/cmd/govulncheck@latest ./internal/... ./pkg/...

# -----------------------------
# CLEAN
# -----------------------------

.PHONY: clean
clean:
	rm -rf bin
	rm -f coverage.out coverage.html coverage_*.out *.coverprofile profile.cov
