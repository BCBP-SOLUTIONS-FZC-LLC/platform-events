#!/usr/bin/env bash
# PromQL syntax gate for monitoring/grafana/*.json. promtool has no offline
# check for bare expressions, so every panel and template-variable query is
# written into a temporary rule file (Grafana variables replaced by valid
# placeholders) and checked with `promtool check rules`. Governance (registry,
# labels) is TestDashboards_RegistryCompliance's job; this is syntax only.
set -euo pipefail

PROMETHEUS_IMAGE="${PROMETHEUS_IMAGE:?set by make dashboards-check (digest-pinned)}"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

jq -rn '
  [ inputs | (.. | objects | select(has("targets")) | .targets[]? | .expr | select(. != null)),
    (.templating.list[]? | select(.type == "query") | (.query | if type == "object" then .query else . end)
       | capture("^label_values\\((?<sel>.+),\\s*[A-Za-z_][A-Za-z0-9_]*\\)$").sel) ]
  | to_entries
  | "groups:\n  - name: dashboard\n    rules:\n"
    + (map("      - record: dashboard:q\(.key)\n        expr: \(.value | gsub("\\$\\{?__rate_interval\\}?"; "5m") | gsub("\\$\\{?[A-Za-z_][A-Za-z0-9_]*(:[a-z]+)?\\}?"; ".*") | tojson)") | join("\n"))
' monitoring/grafana/*.json > "$tmp/dashboards.rules.yml"

# mktemp -d is 0700 and the prometheus image runs promtool as nobody, so on a
# Linux host (CI) the container could not read the mount; Docker Desktop hides this.
chmod 755 "$tmp"
chmod 644 "$tmp/dashboards.rules.yml"

docker run --rm -v "$tmp":/rules -w /rules --entrypoint promtool "$PROMETHEUS_IMAGE" check rules dashboards.rules.yml
