#!/usr/bin/env bash
# Image size + startup gate smoke tests for the CI-built Docker image of the
# platform-events reference CLI. Invoked by ci.yml (keeps shell operators out
# of inline YAML run blocks). Live-AWS smoke tests (test/smoke, make
# test-smoke) are separate and never run in CI.
set -euo pipefail

IMAGE=platform-events-ci-test

echo "::group::Image size check (linux/amd64)"
# The image is one static CLI binary on distroless (~6 MB). The limit is
# deliberately generous so it catches regressions (e.g. accidentally COPYing
# test assets or the module cache), not normal dependency growth.
MAX_MB=50
size=$(docker image inspect "${IMAGE}" --format='{{.Size}}')
mb=$((size / 1024 / 1024))
echo "Image size: ${mb} MB (limit: ${MAX_MB} MB)"
size_ok=true
[ "${mb}" -le "${MAX_MB}" ] || size_ok=false
echo "::endgroup::"

echo "::group::Startup gate"
# The image's ENTRYPOINT runs the CLI with -strict, which must exit non-zero
# when required configuration (SNS_TOPIC_ARN, SQS_QUEUE_URL, DATABASE_URL, ...)
# is missing — proving the binary starts, loads config and validates it.
# timeout 10s kills the container if it hangs instead of exiting.
exit_code=0
timeout 10s docker run --rm "${IMAGE}" >/dev/null 2>&1 || exit_code=$?
echo "Container exit code: ${exit_code} (expected non-zero)"
gate_ok=true
{ [ "${exit_code}" -ne 0 ] && [ "${exit_code}" -ne 124 ]; } || gate_ok=false
echo "::endgroup::"

echo "::group::Version stamp"
# Without -strict the CLI prints "platform-events <version>" first; BUILD_VERSION
# is injected by the build (ci.yml passes the commit SHA).
first_line=$(timeout 10s docker run --rm --entrypoint /platform-events "${IMAGE}" 2>/dev/null | head -1 || true)
echo "First line: ${first_line}"
version_ok=true
echo "${first_line}" | grep -Eq '^platform-events [^[:space:]]+$' || version_ok=false
echo "::endgroup::"

"${size_ok}" || {
  echo "::error file=Dockerfile,title=Image size::Image is ${mb} MB, exceeds ${MAX_MB} MB limit — check COPY/ADD instructions and .dockerignore for accidental inclusions"
  exit 1
}
"${gate_ok}" || {
  echo "::error file=cmd/platform-events/main.go,title=Startup gate::CLI exited ${exit_code} with no configuration — -strict must exit non-zero (124 = timed out)"
  exit 1
}
"${version_ok}" || {
  echo "::error file=cmd/platform-events/main.go,title=Version stamp::CLI did not print 'platform-events <version>' as its first line"
  exit 1
}

{
  echo "### Smoke test results"
  echo "- ✅ Startup gate: CLI exits ${exit_code} with no configuration (-strict validation fires)"
  echo "- ✅ Version stamp: \`${first_line}\`"
  echo "- 📦 Image size (linux/amd64): **${mb} MB** (limit: ${MAX_MB} MB)"
  echo "- 🔍 [Security (Trivy SARIF results)](https://github.com/${GITHUB_REPOSITORY}/security/code-scanning)"
} >> "$GITHUB_STEP_SUMMARY"
