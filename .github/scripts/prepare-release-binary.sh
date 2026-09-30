#!/usr/bin/env bash
# Cross-compiles release binaries for all target platforms and writes per-binary
# checksums. The artifact directory is the working directory when this runs.
#
# Produces for each platform:
#   platform-events_{version}_{os}_{arch}[.exe]
#   platform-events_{version}_{os}_{arch}[.exe].sha256
#
# The caller (create-github-release.sh) aggregates the .sha256 files into
# a single checksums.txt with `sha256sum platform-events_* | grep -v '\.sha256$'`.
set -euo pipefail

: "${RELEASE_TAG:?RELEASE_TAG is required}"

TARGETS=(
  "linux   amd64"
  "linux   arm64"
  "darwin  amd64"
  "darwin  arm64"
  "windows amd64"
)

echo "Building release binaries for tag ${RELEASE_TAG}"

for target in "${TARGETS[@]}"; do
  read -r GOOS GOARCH <<< "$target"
  EXT=""
  [ "${GOOS}" = "windows" ] && EXT=".exe"
  ASSET_NAME="platform-events_${RELEASE_TAG}_${GOOS}_${GOARCH}${EXT}"

  echo "  → ${GOOS}/${GOARCH}"
  CGO_ENABLED=0 GOOS="${GOOS}" GOARCH="${GOARCH}" \
    go build -trimpath \
      -ldflags "-s -w -X main.version=${RELEASE_TAG}" \
      -o "${ASSET_NAME}" \
      ./cmd/platform-events

  chmod +x "${ASSET_NAME}"
  sha256sum "${ASSET_NAME}" > "${ASSET_NAME}.sha256"
  echo "    ✔  ${ASSET_NAME}"
done

# Write the primary asset name (linux/amd64) for downstream steps that
# need a single canonical file reference.
echo "platform-events_${RELEASE_TAG}_linux_amd64" > release-asset.name
echo "All binaries prepared."
