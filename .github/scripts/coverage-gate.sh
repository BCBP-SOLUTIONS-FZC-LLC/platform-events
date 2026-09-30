#!/usr/bin/env bash
# Enforces minimum total test coverage (default 95%).
set -euo pipefail

THRESHOLD="${COVERAGE_THRESHOLD:-95}"

test -f coverage.out || {
  echo "::error file=coverage.out,title=Coverage gate::coverage.out missing — run 'make test-ci' before the coverage gate"
  exit 1
}

echo "::group::Coverage report summary"
pct=$(go tool cover -func=coverage.out | tail -1 | awk '{print $3}' | tr -d '%')
echo "Total coverage: ${pct}%"
echo "::endgroup::"

# Emit before the gate check so the value is available even when coverage fails.
echo "pct=${pct}" >> "$GITHUB_OUTPUT"

gate=$(awk -v p="$pct" -v t="$THRESHOLD" 'BEGIN { print (p+0 < t) ? "FAIL" : "OK" }')
if [ "${gate}" = "FAIL" ]; then
  echo "::error file=coverage.out,title=Coverage gate::Coverage is ${pct}% — below the ${THRESHOLD}% threshold. Run 'make cover-func' locally to identify gaps."
  exit 1
fi
