#!/usr/bin/env bash
# Shared helper, sourced by verify-changelog-entry.sh and
# extract-release-notes.sh: resolves which CHANGELOG.md section documents
# RELEASE_TAG.
#
#   v1.5.0        → "## [1.5.0]"
#   v1.5.0-rc.1   → "## [1.5.0-rc.1]" if present, else the base "## [1.5.0]"
#                   (so a release-candidate dry run needs no section of its own)
#
# Matching is by exact string (awk index), never regex: semver dots must not
# act as wildcards ("1.5.0" must not match "1x5y0").

# changelog_heading_exists VERSION — exit 0 when "## [VERSION]" starts a line.
changelog_heading_exists() {
  awk -v h="## [$1]" 'index($0, h) == 1 { found = 1; exit } END { exit !found }' CHANGELOG.md 2>/dev/null
}

# changelog_section_version TAG — prints the version whose section documents
# TAG, or returns 1 when there is none.
changelog_section_version() {
  local version="${1#v}"
  if changelog_heading_exists "$version"; then
    printf '%s\n' "$version"
    return 0
  fi
  local base="${version%%-*}"
  if [ "$base" != "$version" ] && changelog_heading_exists "$base"; then
    printf '%s\n' "$base"
    return 0
  fi
  return 1
}

# changelog_section_body VERSION — prints the section's body (between its
# heading and the next "## [" heading), blank-only lines emptied.
changelog_section_body() {
  awk -v h="## [$1]" '
    index($0, h) == 1 { found = 1; next }
    found && index($0, "## [") == 1 { exit }
    found { print }
  ' CHANGELOG.md | sed -e 's/^[[:space:]]*$//'
}
