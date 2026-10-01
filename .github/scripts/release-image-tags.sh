#!/usr/bin/env bash
# Decides which floating image tags a release may move, so a hotfix of an
# older line never re-points a newer one (releasing v1.3.5 after v1.4.0 must
# not move `v1` or `latest` back to 1.3.5).
#
#   latest      ← RELEASE_TAG is the highest stable tag in the repo
#   vMAJOR      ← RELEASE_TAG is the highest stable tag with that major
#   vMAJOR.MINOR ← RELEASE_TAG is the highest stable tag with that major.minor
#
# A pre-release (any `-suffix`) never moves a floating tag. Only exact
# `vX.Y.Z` tags count as stable; RELEASE_TAG is added to the list, so the
# script also answers "what if" questions for a tag that does not exist yet:
#
#   RELEASE_TAG=v1.3.9 bash .github/scripts/release-image-tags.sh
#
# Tags are read from `git tag` (the verify job checks out with fetch-depth 0,
# which fetches every tag). Set TAGS (newline-separated) to override.
# Writes latest= / major= / minor= (true|false) to $GITHUB_OUTPUT when set.
set -euo pipefail

: "${RELEASE_TAG:?RELEASE_TAG is required}"

stable_re='^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$'
semver_re='^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[0-9A-Za-z.-]+)?$'

if ! [[ "$RELEASE_TAG" =~ $semver_re ]]; then
  echo "::error title=Release tag::${RELEASE_TAG} is not a vMAJOR.MINOR.PATCH[-pre] tag"
  exit 1
fi
major="${BASH_REMATCH[1]}"
minor="${BASH_REMATCH[2]}"

# ${TAGS-…}, not ${TAGS:-…}: an explicitly empty TAGS means "no other tags".
# Every v* git tag counts, including one whose release failed and published
# no image — delete such a tag (git push --delete origin vX.Y.Z) so it does
# not hold the floating tags back.
all_tags="${TAGS-$(git tag -l 'v*')}"

# Highest stable version among the lines that match the given prefix regex
# (RELEASE_TAG included when it is stable). Prints e.g. "1.4.0", or nothing.
highest() {
  local prefix_re=$1
  { printf '%s\n' "$all_tags"; printf '%s\n' "$RELEASE_TAG"; } \
    | grep -E "$stable_re" \
    | sed 's/^v//' \
    | grep -E "$prefix_re" \
    | sort -t. -k1,1n -k2,2n -k3,3n -u \
    | tail -n 1 || true
}

latest=false
major_tag=false
minor_tag=false
reason="pre-release: only ${RELEASE_TAG} is tagged"

if [[ "$RELEASE_TAG" =~ $stable_re ]]; then
  version="${RELEASE_TAG#v}"
  top_all=$(highest '.')
  top_major=$(highest "^${major}\.")
  top_minor=$(highest "^${major}\.${minor}\.")
  [ "$version" = "$top_all" ] && latest=true
  [ "$version" = "$top_major" ] && major_tag=true
  [ "$version" = "$top_minor" ] && minor_tag=true
  reason="highest stable: overall v${top_all}, v${major}.x v${top_major}, v${major}.${minor}.x v${top_minor}"
fi

echo "Release ${RELEASE_TAG} (${reason})"
printf '  %-24s %s\n' "${RELEASE_TAG}" true \
  "v${major}.${minor} (minor line)" "$minor_tag" \
  "v${major} (major line)" "$major_tag" \
  latest "$latest"

if [ -n "${GITHUB_OUTPUT:-}" ]; then
  {
    echo "latest=${latest}"
    echo "major=${major_tag}"
    echo "minor=${minor_tag}"
  } >> "$GITHUB_OUTPUT"
fi
