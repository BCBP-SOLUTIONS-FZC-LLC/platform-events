#!/usr/bin/env bash
# Regression tests for detect-changes.sh (no bats: plain bash + scratch git
# repositories). Run by `make ci-scripts-test` and in validate-quality.yml.
#
# Each case builds a base commit, applies a change on top, runs the script in
# push mode (BEFORE = base) and checks the `code` output it writes; one case
# runs it in pull_request mode on a --no-ff merge commit. Every case also
# checks that no printed path can start a workflow command.
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
script="$here/detect-changes.sh"
sde="$here/set-source-date-epoch.sh"

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

fails=0
n=0

new_repo() {
  local dir="$work/repo$n"
  mkdir -p "$dir/.github/scripts" "$dir/pkg" "$dir/docs/architecture/mermaid" "$dir/docs/observability"
  cp "$script" "$sde" "$dir/.github/scripts/"
  (
    cd "$dir"
    git init -q -b main
    git config user.email ci@example.invalid
    git config user.name ci
    git config commit.gpgsign false
    echo 'package x' > pkg/x.go
    echo '# readme' > README.md
    echo 'graph TD' > docs/architecture/mermaid/a.mmd
    echo '# runbook' > docs/observability/runbook.md
    git add -A
    git commit -q -m base
  )
  echo "$dir"
}

# run_case NAME WANT CHANGE_CMD — CHANGE_CMD runs in the repo, then is committed.
run_case() {
  local name=$1 want=$2 change=$3
  n=$((n + 1))
  local dir
  dir=$(new_repo)
  local out="$work/out$n" log="$work/log$n"
  : > "$out"
  # Setup errors abort the whole test run (set -e); only the script under
  # test runs in an `||` context.
  local base
  base=$(cd "$dir" && git rev-parse HEAD)
  (
    cd "$dir"
    eval "$change"
    git add -A
    git commit -q --allow-empty -m change
  )
  # A case may move the base (it commits a setup step first).
  if [ -f "$dir/.git/case-base" ]; then base=$(cat "$dir/.git/case-base"); fi
  run_script "$name" "$want" "$dir" "$out" "$log" push "$base"
}

# run_script NAME WANT DIR OUT LOG EVENT [BEFORE]
run_script() {
  local name=$1 want=$2 dir=$3 out=$4 log=$5 event=$6 before=${7:-}
  if ! (cd "$dir" && EVENT_NAME="$event" BEFORE="$before" GITHUB_OUTPUT="$out" GITHUB_ENV=/dev/null \
          bash .github/scripts/detect-changes.sh > "$log" 2>&1); then
    echo "FAIL $name: script exited non-zero"
    sed 's/^/    /' "$log"
    fails=$((fails + 1))
    return
  fi
  check "$name" "$want" "$out" "$log"
}

check() {
  local name=$1 want=$2 out=$3 log=$4
  local got
  got=$(sed -n 's/^code=//p' "$out")
  if [ "$got" != "$want" ]; then
    echo "FAIL $name: code=$got, want $want"
    sed 's/^/    /' "$log"
    fails=$((fails + 1))
    return
  fi
  # Only the script's own markers may start with '::'.
  local bad
  bad=$(grep -E '^[[:space:]]*::' "$log" \
        | grep -vE '^::(group::Changed files .*|endgroup::|stop-commands::detect-changes-[0-9a-f]{32}|detect-changes-[0-9a-f]{32}::)$' || true)
  if [ -n "$bad" ]; then
    echo "FAIL $name: a printed path could inject a workflow command:"
    printf '    %s\n' "$bad"
    fails=$((fails + 1))
    return
  fi
  echo "ok   $name (code=$got)"
}

run_case 'rename .go to .md'                    true  'git mv pkg/x.go pkg/x.md'
run_case 'move .go under docs/architecture'     true  'git mv pkg/x.go docs/architecture/x.go'
run_case 'move .go to docs/architecture as .md' true  'git mv pkg/x.go docs/architecture/x.md'
run_case 'delete .go'                           true  'git rm -q pkg/x.go'
run_case 'rename rls_test.go to .md'            true  'mkdir -p test/integration && echo "package t" > test/integration/rls_test.go && git add -A && git commit -q -m t && git rev-parse HEAD > .git/case-base && git mv test/integration/rls_test.go test/integration/rls_test.md'
run_case 'pure .md edit'                        false 'echo more >> README.md'
run_case 'architecture diagram edit'            false 'echo "A-->B" >> docs/architecture/mermaid/a.mmd'
run_case 'rename .md to .md'                    false 'git mv README.md GUIDE.md'
run_case 'docs/observability is code'           true  'echo more >> docs/observability/runbook.md'
run_case '.md with spaces'                      false 'echo x > "my notes.md"'
run_case '.go with spaces'                      true  'echo "package x" > "pkg/my file.go"'
run_case "path starting with '::' (docs)"       false 'echo x > "::warning::injected.md"'
run_case "path starting with '::' (code)"       true  'echo x > "::error::injected.go"'
run_case 'path with a newline then ::'          false 'echo x > "a"$'"'"'\n'"'"'"::warning::b.md"'
run_case 'empty diff runs the pipeline'         true  ':'

# pull_request: the merge commit vs its first parent.
pr_case() {
  local name=$1 want=$2 change=$3
  n=$((n + 1))
  local dir
  dir=$(new_repo)
  local out="$work/out$n" log="$work/log$n"
  : > "$out"
  (
    cd "$dir"
    git checkout -q -b feature
    eval "$change"
    git add -A
    git commit -q -m change
    git checkout -q main
    echo other >> README.md
    git commit -q -am 'main moves on'
    git merge -q --no-ff -m merge feature
  )
  run_script "$name" "$want" "$dir" "$out" "$log" pull_request
}
pr_case 'pull_request: rename .go to .md' true  'git mv pkg/x.go pkg/x.md'
pr_case 'pull_request: docs only'         false 'echo x > docs/architecture/README.md'

# push without a usable BEFORE: full run.
nobase_case() {
  local name=$1 before=$2
  n=$((n + 1))
  local dir
  dir=$(new_repo)
  local out="$work/out$n" log="$work/log$n"
  : > "$out"
  run_script "$name" true "$dir" "$out" "$log" push "$before"
}
nobase_case 'push: all-zero BEFORE' 0000000000000000000000000000000000000000
nobase_case 'push: unknown BEFORE'  1234567890123456789012345678901234567890

if [ "$fails" -ne 0 ]; then
  echo "$fails of $n detect-changes.sh cases failed."
  exit 1
fi
echo "All $n detect-changes.sh cases passed."
