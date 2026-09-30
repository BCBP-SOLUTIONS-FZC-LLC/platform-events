# Image for the platform-events reference CLI (cmd/platform-events).
# platform-events is a library — this image exists so CI can build, CVE-scan
# (Trivy) and smoke-test the compiled binary, including every dependency and
# the Go standard library linked into it. Mirrors the sibling libraries
# (platform-pgcommon, platform-gincommon), which ship their CLI the same way.
#
# Pin base images to SHA digests for reproducible, supply-chain-safe builds.
# Update with: make pin-base-images
FROM golang:1.26.8-alpine@sha256:8ac98ca534ac3f51e1f420a1dd2c15e74c75cfa0f23f3ad27eb5d7236c349a0c AS builder

WORKDIR /build

ENV GOPRIVATE="github.com/BCBP-SOLUTIONS-FZC-LLC/*"
ENV GONOSUMDB="github.com/BCBP-SOLUTIONS-FZC-LLC/*"

# hadolint ignore=DL3018
RUN apk add --no-cache git

ARG BUILD_VERSION=dev
ARG SOURCE_DATE_EPOCH=0

COPY go.mod go.sum ./

RUN --mount=type=secret,id=go_private_token \
    TOKEN=$(cat /run/secrets/go_private_token 2>/dev/null || true) && \
    if [ -z "$TOKEN" ]; then echo "ERROR: go_private_token secret is missing or empty — pass --secret id=go_private_token,src=<token-file>"; exit 1; fi && \
    git config --global url."https://x-access-token:${TOKEN}@github.com/".insteadOf "https://github.com/" && \
    go mod download && \
    git config --global --unset url."https://x-access-token:${TOKEN}@github.com/".insteadOf

COPY --link . .

RUN CGO_ENABLED=0 GOOS=linux \
    SOURCE_DATE_EPOCH=${SOURCE_DATE_EPOCH} \
    go build -trimpath \
    -ldflags="-s -w -X main.version=${BUILD_VERSION}" \
    -o bin/platform-events ./cmd/platform-events

FROM gcr.io/distroless/static-debian13:nonroot@sha256:e2e927ec666bae08560abb3c55d0659eceabb657f56b6782ab500a9fc7f555e3

ARG BUILD_VERSION=dev
ENV BUILD_VERSION=${BUILD_VERSION}

LABEL org.opencontainers.image.title="platform-events" \
      org.opencontainers.image.description="Reference CLI for the platform-events messaging library" \
      org.opencontainers.image.source="https://github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events" \
      org.opencontainers.image.vendor="BCBP Solutions FZC LLC" \
      org.opencontainers.image.revision="${BUILD_VERSION}"

COPY --from=builder /build/bin/platform-events /platform-events

# -strict: exit non-zero when required configuration is missing (the smoke
# test's startup gate relies on this with an empty environment).
ENTRYPOINT ["/platform-events", "-strict"]
