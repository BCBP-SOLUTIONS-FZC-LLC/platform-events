#!/usr/bin/env bash
# Fails when the Go toolchain differs between the three go.mod files and the
# Dockerfile builder image. A standard-library govulncheck fix bumps them
# together; missing one ships the vulnerable toolchain in that place.
set -euo pipefail
cd "$(dirname "$0")/../.."

want=$(sed -n 's/^toolchain go\(.*\)$/\1/p' go.mod)
[ -n "$want" ] || { echo "::error file=go.mod::no toolchain line"; exit 1; }

status=0
for f in test/go.mod tools/go.mod; do
  got=$(sed -n 's/^toolchain go\(.*\)$/\1/p' "$f")
  if [ "$got" != "$want" ]; then
    echo "::error file=$f::toolchain go${got:-<none>} — go.mod has go${want}"
    status=1
  fi
done
# Any FROM form: --platform=…, a registry prefix (docker.io/library/golang),
# with or without a variant suffix (-alpine) or digest.
got=$(grep -E '^FROM ' Dockerfile | grep -oE 'golang:[0-9][0-9.]*[0-9]' | head -n1 | cut -d: -f2)
if [ "$got" != "$want" ]; then
  echo "::error file=Dockerfile::builder image golang:${got:-<none>} — go.mod has toolchain go${want}"
  status=1
fi
[ "$status" -eq 0 ] && echo "  ✔  toolchain go${want} in go.mod, test/go.mod, tools/go.mod and the Dockerfile"
exit "$status"
