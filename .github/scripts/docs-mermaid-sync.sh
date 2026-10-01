#!/usr/bin/env bash
# Diagram drift gate: every docs/architecture/mermaid/*.mmd must be embedded
# byte-identically in ARCHITECTURE.md, as the first ```mermaid block after its
# "> Source: [`docs/architecture/mermaid/<file>.mmd`](...)" link.
#
# Fails when
#   - a .mmd file has no Source link, or its block differs from the file;
#   - a Source link names a file that does not exist, is not followed by a
#     ```mermaid block, or appears twice;
#   - ARCHITECTURE.md has a ```mermaid block with no Source link before it.
#
# Portable: bash, awk, diff, mktemp only (macOS and Linux). Run from the repo
# root (make docs-check) or pass the root as $1.
set -euo pipefail

ROOT="${1:-.}"
DOC="$ROOT/ARCHITECTURE.md"
SRC_DIR="docs/architecture/mermaid"

if [ ! -f "$DOC" ]; then
  echo "::error title=Diagram sync::$DOC not found"
  exit 1
fi

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

# Extract each Source-linked block to $tmp/blocks/<file>.mmd and report
# structural problems on stdout, one per line.
mkdir "$tmp/blocks"
problems="$(awk -v out="$tmp/blocks" -v dir="$SRC_DIR" '
  function problem(msg) { printf "ARCHITECTURE.md:%d: %s\n", NR, msg }
  BEGIN { pending = ""; inblock = 0 }
  inblock {
    if ($0 == "```") { close(path); inblock = 0; next }
    print > path
    next
  }
  index($0, "> Source: [`" dir "/") == 1 {
    if (pending != "") problem("Source link for " pending " is not followed by a mermaid block")
    name = substr($0, length("> Source: [`" dir "/") + 1)
    name = substr(name, 1, index(name, "`") - 1)
    if (name !~ /^[A-Za-z0-9._-]+\.mmd$/) { problem("malformed Source link: " $0); pending = ""; next }
    if (name in seen) problem("second Source link for " name)
    seen[name] = 1
    pending = name
    next
  }
  $0 == "```mermaid" {
    if (pending == "") { problem("mermaid block with no Source link before it"); path = "/dev/null" }
    else { path = out "/" pending; printf "" > path }
    inblock = 1; pending = ""
    next
  }
  END {
    if (inblock) problem("unterminated mermaid block")
    if (pending != "") problem("Source link for " pending " is not followed by a mermaid block")
  }
' "$DOC")"

status=0
if [ -n "$problems" ]; then
  printf '%s\n' "$problems" | while IFS= read -r line; do
    echo "::error title=Diagram sync::$line"
  done
  status=1
fi

shopt -s nullglob
count=0
for src in "$ROOT/$SRC_DIR"/*.mmd; do
  name="${src##*/}"
  count=$((count + 1))
  block="$tmp/blocks/$name"
  if [ ! -f "$block" ]; then
    echo "::error file=$SRC_DIR/$name,title=Diagram sync::$name is not embedded in ARCHITECTURE.md (add a '> Source:' link and a mermaid block)"
    status=1
    continue
  fi
  if ! diff -u --label "$SRC_DIR/$name" --label "ARCHITECTURE.md (embedded)" "$src" "$block"; then
    echo "::error file=$SRC_DIR/$name,title=Diagram sync::the mermaid block in ARCHITECTURE.md differs from $name — copy the .mmd content into the block verbatim"
    status=1
  fi
  rm -f "$block"
done

for orphan in "$tmp/blocks"/*; do
  echo "::error file=ARCHITECTURE.md,title=Diagram sync::Source link names ${orphan##*/}, which does not exist in $SRC_DIR"
  status=1
done

if [ "$status" -eq 0 ]; then
  echo "  ✔  $count diagrams in $SRC_DIR are embedded verbatim in ARCHITECTURE.md"
fi
exit "$status"
