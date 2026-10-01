#!/usr/bin/env bash
# Decides whether ci.yml's build/test pipeline must run (code=true) or the
# change touches only documentation (code=false), and records the
# SOURCE_DATE_EPOCH every image build in ci.yml shares.
#
# Replaces `paths-ignore` on the workflow trigger: a workflow that does not run
# never reports its required checks, so a docs-only PR waited forever on them.
# Now the workflow always runs, and the jobs behind the required checks are
# skipped by `if:` (a skipped job reports success to branch protection).
#
# Documentation = any *.md file, or a *.mmd diagram under docs/architecture/
# (docs.yml checks those). NOT documentation: anything under
# docs/observability/ (the generated metrics registry and the runbook are
# checked by `make metrics-lint`, which only the required validate-quality job
# runs), and any other file under docs/architecture/ (a .go file there is a
# package of the root module).
#
# Every path of the change is judged: the diff runs with --no-renames, so a
# rename or move is listed as a deletion of the old path plus an addition of
# the new one. Renaming pkg/x.go to pkg/x.md, moving code under
# docs/architecture/, or deleting a .go file therefore counts as code (with
# rename detection on, only the new, docs-looking path was listed).
#
# Paths are read NUL-separated (-z: no quoting, spaces and newlines intact)
# and printed sanitised, so a file named `::warning::x` cannot inject a
# workflow command into the log.
#
# Inputs (env): EVENT_NAME, BEFORE (push: previous tip). Needs the checkout's
# history: fetch-depth 0 (push) or ≥ 2 (pull_request: the merge commit and its
# first parent, the base tip). Tested by detect-changes_test.sh
# (`make ci-scripts-test`).
set -euo pipefail

: "${EVENT_NAME:?EVENT_NAME is required}"
: "${GITHUB_OUTPUT:?GITHUB_OUTPUT is required}"

is_docs() {
  case "$1" in
    docs/observability/*) return 1 ;;
    docs/architecture/*.mmd) return 0 ;;
    docs/architecture/*) [[ "$1" == *.md ]] ;;
    *.md) return 0 ;;
    *) return 1 ;;
  esac
}

# One log line per path: control characters (newline, CR, ...) become '?',
# and the fixed prefix means no line ever starts with '::'.
print_path() {
  local p=${1//[[:cntrl:]]/?}
  printf -- '- %s\n' "$p"
}

code=true
base=''
case "$EVENT_NAME" in
  pull_request)
    # actions/checkout checks out the PR's merge commit; its first parent is
    # the base branch tip, so this is exactly what the PR would change.
    base='HEAD^1'
    ;;
  push)
    if [ -n "${BEFORE:-}" ] && ! [[ "$BEFORE" =~ ^0+$ ]] \
       && git cat-file -e "${BEFORE}^{commit}" 2>/dev/null; then
      base="$BEFORE"
    fi
    ;;
esac

if [ -n "$base" ]; then
  # Into a file, not a process substitution: a failed diff must stop the
  # script (set -e), never look like an empty one.
  list=$(mktemp)
  trap 'rm -f "$list"' EXIT
  git diff --name-only --no-renames -z "$base" HEAD -- > "$list"
  files=()
  while IFS= read -r -d '' f; do
    files+=("$f")
  done < "$list"

  code=false
  if [ "${#files[@]}" -eq 0 ]; then
    code=true # empty diff (e.g. a revert pair): run, to be safe
  fi
  for f in ${files[@]+"${files[@]}"}; do
    if ! is_docs "$f"; then
      code=true
      break
    fi
  done

  # Belt and braces: suspend workflow-command processing while the paths are
  # printed (the token is random, so a path cannot end the suspension).
  token="detect-changes-$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')"
  echo "::group::Changed files (${base}..HEAD)"
  echo "::stop-commands::${token}"
  for f in ${files[@]+"${files[@]}"}; do
    print_path "$f"
  done
  echo "::${token}::"
  echo "::endgroup::"
else
  echo "No comparable base for ${EVENT_NAME} — running the full pipeline."
fi

echo "code=${code}" >> "$GITHUB_OUTPUT"
if [ "$code" = true ]; then
  echo "Code changed — full pipeline."
else
  echo "Documentation-only change — build/test jobs are skipped (docs.yml checks it)."
fi

bash .github/scripts/set-source-date-epoch.sh
