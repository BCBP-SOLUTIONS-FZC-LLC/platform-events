# docs/

This directory contains supplementary documentation assets for `platform-events`.

---

## Mermaid diagrams (`docs/architecture/mermaid/`)

Architecture diagrams in [Mermaid](https://mermaid.js.org/) format. Each `.mmd` file is the authoritative source for one diagram embedded in [`ARCHITECTURE.md`](../ARCHITECTURE.md).

| File | Section in ARCHITECTURE.md | Diagram type |
|------|---------------------------|--------------|
| [`layer-model.mmd`](architecture/mermaid/layer-model.mmd) | Layer model | `graph TD` |
| [`package-dependencies.mmd`](architecture/mermaid/package-dependencies.mmd) | Package dependency graph | `graph LR` |
| [`sns-publish-flow.mmd`](architecture/mermaid/sns-publish-flow.mmd) | SNS publish flow | `sequenceDiagram` |
| [`sqs-consume-flow.mmd`](architecture/mermaid/sqs-consume-flow.mmd) | SQS consume flow | `sequenceDiagram` |
| [`outbox-poll-cycle.mmd`](architecture/mermaid/outbox-poll-cycle.mmd) | Outbox poll cycle | `flowchart TD` |
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

The `> Source: [...]` blockquote above each diagram in `ARCHITECTURE.md` links reviewers directly to the `.mmd` source so it is easy to find during code review.
