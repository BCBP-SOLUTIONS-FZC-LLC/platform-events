# docs/

This directory contains supplementary documentation assets for `platform-events`.

---

## Guides (`docs/guides/`)

Detailed how-to guides split out of the top-level README, which keeps a short, table-driven overview and links here. Each guide is self-contained; headings and anchors are unchanged from when they lived in the README.

| Guide | Covers |
|-------|--------|
| [`quick-start.md`](guides/quick-start.md) | End-to-end service wiring |
| [`envelope.md`](guides/envelope.md) | Envelope construction, serialisation, payload typing, compatibility |
| [`publishing.md`](guides/publishing.md) | Publishing rules, anti-patterns, SNS publisher, FIFO, attributes |
| [`consuming.md`](guides/consuming.md) | SQS consumer, handler contract, error classification, poison messages, DLQ forwarding, idempotency |
| [`outbox.md`](guides/outbox.md) | Outbox wiring, poll cycle, retries, per-key ordering, dead letters, pruning, replay |
| [`codec.md`](guides/codec.md) | Schema-registry `Codec` hook |
| [`hmac.md`](guides/hmac.md) | HMAC helpers and when to use them |
| [`observability.md`](guides/observability.md) | Prometheus metrics, OpenTelemetry, logging correlation |
| [`operations.md`](guides/operations.md) | Backpressure, production defaults, service adoption checklist |
| [`testing-in-services.md`](guides/testing-in-services.md) | `mock.Publisher` / `mock.Consumer` / `mock.DLQPublisher` |

---

## Low-level design (`docs/lld/`)

| Document | Covers |
|----------|--------|
| [`platform-events-lld.md`](lld/platform-events-lld.md) | Responsibilities and boundaries, module/package layout, data model (migrations 001–010), public API contract, envelope wire format, publish / consume / outbox / ordering / inbox flows, retry classification, configuration, observability, concurrency, security, testing, CI/CD, open questions |

---

## Observability (`docs/observability/`)

| Document | Covers |
|----------|--------|
| [`README.md`](observability/README.md) | The Enterprise Platform Observability Standard as applied here: tiers, wiring, migration plan, Proposed metrics |
| [`metrics-registry.md`](observability/metrics-registry.md) | Generated from the registry (`make metrics-doc`) — every metric, label and status |
| [`runbook.md`](observability/runbook.md) | One section per alert in `monitoring/prometheus/platform-events.rules.yml` |

---

## Mermaid diagrams (`docs/architecture/mermaid/`)

Architecture diagrams in [Mermaid](https://mermaid.js.org/) format. Each `.mmd` file is the authoritative source for one diagram embedded in [`ARCHITECTURE.md`](../ARCHITECTURE.md).

| File | Section in ARCHITECTURE.md | Diagram type |
|------|---------------------------|--------------|
| [`layer-model.mmd`](architecture/mermaid/layer-model.mmd) | Layer model | `graph TD` |
| [`package-dependencies.mmd`](architecture/mermaid/package-dependencies.mmd) | Package dependency graph | `graph LR` |
| [`sns-publish-flow.mmd`](architecture/mermaid/sns-publish-flow.mmd) | SNS publish flow | `sequenceDiagram` |
| [`sqs-consume-flow.mmd`](architecture/mermaid/sqs-consume-flow.mmd) | SQS consume flow | `sequenceDiagram` |
| [`outbox-poll-cycle.mmd`](architecture/mermaid/outbox-poll-cycle.mmd) | Write flow and transactional outbox → Outbox poll cycle | `flowchart TD` |
| [`dlq-management-flow.mmd`](architecture/mermaid/dlq-management-flow.mmd) | Failure lifecycle → DLQ management flow (outbox dead letters) | `flowchart TD` |
| [`dlq-forward-flow.mmd`](architecture/mermaid/dlq-forward-flow.mmd) | Failure lifecycle → Consumer-side DLQ forwarding (`DLQPublisher`) | `sequenceDiagram` |
| [`write-flow.mmd`](architecture/mermaid/write-flow.mmd) | Write flow and transactional outbox | `sequenceDiagram` |
| [`data-model.mmd`](architecture/mermaid/data-model.mmd) | Data model | `erDiagram` |
| [`observability-stack.mmd`](architecture/mermaid/observability-stack.mmd) | Observability stack | `flowchart LR` |
| [`hmac-flow.mmd`](architecture/mermaid/hmac-flow.mmd) | HMAC signing flow | `flowchart TD` |
| [`consuming-service-wiring.mmd`](architecture/mermaid/consuming-service-wiring.mmd) | Consuming service wiring | `graph LR` |
| [`documentation-assets.mmd`](architecture/mermaid/documentation-assets.mmd) | Documentation assets | `graph LR` |

---

## Viewing diagrams

**GitHub** renders `.mmd` files and fenced `mermaid` blocks in Markdown natively — no extra steps needed.

**VS Code** — install the [Markdown Preview Mermaid Support](https://marketplace.visualstudio.com/items?itemName=bierner.markdown-mermaid) extension to preview diagrams inside the editor.

**CLI** — install [`@mermaid-js/mermaid-cli`](https://github.com/mermaid-js/mermaid-cli) and render to SVG/PNG:

```bash
npx -p @mermaid-js/mermaid-cli mmdc \
  -i docs/architecture/mermaid/layer-model.mmd \
  -o layer-model.svg
```

---

## Keeping diagrams in sync

When you change the public API or an internal data flow:

1. Edit the relevant `.mmd` file in `docs/architecture/mermaid/`.
2. Copy the updated content into the fenced code block in `ARCHITECTURE.md` (the two are kept in sync manually — there is no automatic generation step).

> **Mermaid gotcha:** avoid `;` inside sequence-diagram message and note text — Mermaid treats it as a statement separator and the diagram fails to render (see the fix in `4322a0c`).

The `> Source: [...]` blockquote above each diagram in `ARCHITECTURE.md` links reviewers directly to the `.mmd` source so it is easy to find during code review.
