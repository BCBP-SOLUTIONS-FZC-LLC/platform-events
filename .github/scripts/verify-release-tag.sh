#!/usr/bin/env bash
# Release front gate — run in the verify job, after a fetch-depth 0 checkout
# of RELEASE_TAG. Fails the release unless:
#
#   1. a workflow_dispatch was started from refs/heads/main or from
#      refs/tags/RELEASE_TAG. GitHub runs release.yml and the reusable
#      validate-*.yml from the *dispatching* ref, not from the tag, so a
#      dispatch from a feature branch would release the tag with that
#      branch's (unreviewed) pipeline;
#   2. the checked-out HEAD is exactly RELEASE_TAG (not a branch tip);
#   3. the tagged commit is reachable from origin/main, so only reviewed,
#      merged code is released (a tag on an unmerged branch is refused).
#
# GITHUB_EVENT_NAME / GITHUB_REF are set by the runner.
set -euo pipefail

: "${RELEASE_TAG:?RELEASE_TAG is required}"
EVENT_NAME="${GITHUB_EVENT_NAME:-push}"
REF="${GITHUB_REF:-}"

# 1. Dispatch ref
if [ "$EVENT_NAME" = "workflow_dispatch" ]; then
  case "$REF" in
    refs/heads/main | "refs/tags/${RELEASE_TAG}")
      echo "  ✔  dispatched from ${REF}"
      ;;
    *)
      echo "::error title=Dispatch ref::workflow_dispatch from '${REF}' is refused — the release workflow and its reusable validate workflows would run from that ref, not from ${RELEASE_TAG}. Re-run with 'Use workflow from' set to main or to the tag ${RELEASE_TAG}."
      exit 1
      ;;
  esac
fi

# 2. HEAD carries the tag. `git describe --exact-match` returns only ONE of
#    several tags on a commit (e.g. v1.6.0-rc.1 and v1.6.0 on the same
#    commit), so ask for every tag pointing at HEAD instead.
if ! git tag --points-at HEAD | grep -qxF -- "$RELEASE_TAG"; then
  echo "::error title=Tag mismatch::RELEASE_TAG=${RELEASE_TAG} does not point at HEAD (tags at HEAD: '$(git tag --points-at HEAD | paste -sd ' ' -)')"
  exit 1
fi
echo "  ✔  tag verified: ${RELEASE_TAG} ($(git rev-parse --short HEAD))"

# 3. Tagged commit is on main. A fetch-depth 0 checkout already has
#    origin/main; fetch it only if missing (needs credentials on a private
#    repo, which persist-credentials: false withholds — hence the check).
if ! git rev-parse --verify --quiet refs/remotes/origin/main >/dev/null; then
  git fetch --no-tags --quiet origin +refs/heads/main:refs/remotes/origin/main || {
    echo "::error title=main not available::could not fetch origin/main to check that ${RELEASE_TAG} is merged — check out with fetch-depth: 0"
    exit 1
  }
fi
if ! git merge-base --is-ancestor HEAD refs/remotes/origin/main; then
  echo "::error title=Tag not on main::${RELEASE_TAG} ($(git rev-parse --short HEAD)) is not reachable from origin/main ($(git rev-parse --short refs/remotes/origin/main)). Merge the change to main and tag a commit on main."
  exit 1
fi
echo "  ✔  ${RELEASE_TAG} is reachable from origin/main"
