#!/usr/bin/env bash
# Exports the image's SLSA provenance to provenance.slsa.json.
#
# docker/build-push-action (`provenance: mode=max`) stores provenance as a
# BuildKit attestation manifest inside the pushed image index — not as a
# Cosign attestation — so it is read with `buildx imagetools inspect`, not
# `cosign download attestation` (which finds nothing and failed the release).
# Requires a prior registry login (the push job's docker/login-action).
set -euo pipefail

: "${IMAGE_NAME:?IMAGE_NAME is required}"
: "${IMAGE_DIGEST:?IMAGE_DIGEST is required}"

IMAGE_REF="${IMAGE_NAME}@${IMAGE_DIGEST}"

# .Provenance.SLSA is the predicate for a single-platform index, or a map
# keyed by platform ("linux/amd64") for a multi-platform one — take the
# linux/amd64 entry in that case.
if ! docker buildx imagetools inspect "${IMAGE_REF}" --format '{{ json .Provenance.SLSA }}' \
  | jq -e 'if has("linux/amd64") then .["linux/amd64"] else . end
           | select(type == "object" and (has("buildDefinition") or has("buildType")))' \
  > provenance.slsa.json; then
  echo "::error title=Provenance export::SLSA provenance attestation not found on ${IMAGE_REF} — was the image built with provenance enabled?"
  exit 1
fi

echo "  ✔  exported SLSA provenance to provenance.slsa.json ($(jq -r '.buildDefinition.buildType // .buildType' provenance.slsa.json))"
