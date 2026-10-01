#!/usr/bin/env bash
# Writes today's UTC date (YYYY-MM-DD) as the "day" step output — a cache-key
# suffix that rolls a cache over once a day (e.g. the Trivy vulnerability DB,
# which actions/cache would otherwise save once and never refresh).
set -euo pipefail
echo "day=$(date -u +%Y-%m-%d)" >> "$GITHUB_OUTPUT"
