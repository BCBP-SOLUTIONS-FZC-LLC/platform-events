#!/usr/bin/env bash
# Fails the release if CHANGELOG.md has no section for RELEASE_TAG
# (a prerelease tag may use its base version's section — see
# changelog-section.sh).
set -euo pipefail

: "${RELEASE_TAG:?RELEASE_TAG is required}"

# shellcheck source=changelog-section.sh
source "$(dirname "$0")/changelog-section.sh"

if ! section=$(changelog_section_version "$RELEASE_TAG"); then
  echo "::error file=CHANGELOG.md::Missing entry for ${RELEASE_TAG#v} — add a '## [${RELEASE_TAG#v}]' section (a prerelease may use its base version's section)"
  exit 1
fi
echo "  ✔  changelog entry for ${RELEASE_TAG#v} found in section [${section}]"
