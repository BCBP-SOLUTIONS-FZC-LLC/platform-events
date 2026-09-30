#!/usr/bin/env bash
# Creates the GitHub release with versioned assets attached.
set -euo pipefail

: "${RELEASE_TAG:?RELEASE_TAG is required}"
: "${GH_TOKEN:?GH_TOKEN is required}"
: "${IMAGE_DIGEST:?IMAGE_DIGEST is required}"

test -f release-asset.name || {
  echo "::error::release-asset.name missing — run prepare-release-binary.sh first"
  exit 1
}

ASSET_NAME=$(cat release-asset.name)
CHECKSUM_FILE="${ASSET_NAME}.sha256"

for f in release-notes.md "$ASSET_NAME" "$CHECKSUM_FILE" provenance.slsa.json sbom.cyclonedx.json; do
  test -f "$f" || {
    echo "::error::Release asset missing: ${f}"
    exit 1
  }
done

# Aggregate per-binary checksums into a single verifiable file.
# Format matches `sha256sum --check checksums.txt`.
sha256sum platform-events_* | grep -v '\.sha256$' > checksums.txt

PRERELEASE_FLAG=""
if echo "$RELEASE_TAG" | grep -q -- '-'; then
  PRERELEASE_FLAG="--prerelease"
fi

DIGEST_SHORT="${IMAGE_DIGEST#sha256:}"
DIGEST_SHORT="${DIGEST_SHORT:0:12}"
RELEASE_TITLE="${RELEASE_TAG} · sha256:${DIGEST_SHORT}"

gh release create "$RELEASE_TAG" \
  --title "$RELEASE_TITLE" \
  --notes-file release-notes.md \
  "$ASSET_NAME" \
  checksums.txt \
  provenance.slsa.json \
  sbom.cyclonedx.json \
  $PRERELEASE_FLAG
