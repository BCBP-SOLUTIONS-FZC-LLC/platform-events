# Versioning and releases

This repository is a **Go module** consumed by platform services. Versions are published with **Git tags** and described in [CHANGELOG.md](CHANGELOG.md).

## Semantic versioning (SemVer)

We use [SemVer 2.0.0](https://semver.org/): `MAJOR.MINOR.PATCH` (e.g. `v1.2.3`).

| Bump | When you change | Examples |
|------|-----------------|----------|
| **MAJOR** | Breaking change in the **public** API (`pkg/events`, `pkg/outbox`) | Removed export, changed `Envelope` or `Runner` signature, incompatible default behaviour |
| **MINOR** | New backward-compatible capability | New `Config` field, new helper function, new `ConsumerOption` |
| **PATCH** | Backward-compatible fix | Bug fix, performance improvement, documentation-only correction |

### What counts as public API

| In scope (SemVer applies) | Out of scope (may change without MAJOR) |
|---------------------------|----------------------------------------|
| `pkg/events/*` | `internal/*` (except `internal/config` — deprecated alias of `pkg/config`) |
| `pkg/outbox/*` | `cmd/platform-events` (reference CLI) |
| `pkg/config/*` | `pkg/outbox/migrations/*.sql` file names (schema applied via `ApplySchema`) |

Import only `pkg/events` and `pkg/outbox` from consumer services. Do not import `internal/` — Go enforces this boundary for external modules.

### Guarantees

- **MAJOR `v1`:** We avoid breaking changes in `pkg/*` within `v1.x`. When breaking changes are required, we release `v2.0.0` and document migration in CHANGELOG.
- **MINOR:** Safe to upgrade with `go get` without code changes unless you opt into new features.
- **PATCH:** Drop-in replacement; upgrade recommended for security fixes.

### Envelope wire format guarantees

The `Envelope` JSON wire format has its own stability contract, independent of Go API compatibility. Within `v1.x`:

| Class | Fields | Guarantee |
|-------|--------|-----------|
| **Stable** | `id`, `type`, `source`, `time` | Always present on the wire; never removed, renamed, or changed in type/format |
| **Contextual** | `tenant_id`, `trace_id`, `correlation_id`, `specversion`, `subject`, `actor`, `dataschema`, `ip_address`, `user_agent` | Never removed; present when set; semantics of absent value frozen |
| **Externally governed** | `data` | Shape owned by the publishing service; library only validates well-formed JSON |

Wire keys follow CloudEvents naming; the Go fields are `Timestamp` (`time`), `SchemaVersion` (`specversion`), `SchemaID` (`dataschema`) and `Payload` (`data`). The full per-field table is in [ARCHITECTURE.md § Envelope compatibility guarantees](ARCHITECTURE.md#envelope-compatibility-guarantees).
| **Reserved** | Future optional fields | Added only in MINOR releases; always `omitempty`; never break existing consumers |

Any violation of these wire format guarantees — removing a stable field or changing `id` format — constitutes a MAJOR bump even if the Go API is unchanged.

See [ARCHITECTURE.md § Envelope compatibility guarantees](ARCHITECTURE.md#envelope-compatibility-guarantees) for the full per-field specification.

## Supported releases

| Version | Status | Go module | Supported until |
|---------|--------|-----------|-----------------|
| `v1.6.x` | **Current** | `@v1.6.0` | Active; patch releases as needed |
| `v1.0.x` – `v1.5.x` | Superseded | `@v1.5.0` … `@v1.0.0` | Upgrade to `v1.6.x` — MINOR releases are backward compatible (read the `[1.6.0]` upgrade notes) |
| `< v1.0.0` | — | — | No tagged releases before `v1.0.0` |

When a new **MAJOR** line ships (e.g. `v2`), the previous major receives **security fixes only** for a period defined by the platform team (typically 6 months after `v2.0.0`).

## Consume a release

This is a **private module**. Before running `go get`, configure Go to bypass the public proxy and checksum database:

```bash
go env -w GOPRIVATE=github.com/BCBP-SOLUTIONS-FZC-LLC/*
```

Set the same variable in CI pipelines that build consuming services. See [README.md § Integrating into a service](./README.md#1-prerequisites) for full authentication setup (SSH key vs PAT).

Pin in your service `go.mod`:

```bash
go get github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events@v1.0.0
```

| Pin style | Use when |
|-----------|----------|
| `@v1.0.0` | Production; exact reproducibility |
| `@v1.0.3` | Production; latest patch on `1.0` |
| `@v1.2.0` | Accept new minors on `1.x` (still SemVer-safe) |
| `@latest` | Experiments only; not recommended for prod |

Go resolves versions from **Git tags** pushed to this private repository — there is no public module proxy involved.

## Maintainer release process

The release workflow (`.github/workflows/release.yml`) automates validation and GitHub Release creation. Your job is to prepare the commit and push the tag.

1. **Merge** all changes for the release to `main`.

2. **Update CHANGELOG.md:** move `[Unreleased]` entries into a new `## [X.Y.Z] - YYYY-MM-DD` section.
   The release workflow reads this section to populate the GitHub Release body.

3. **Run `make ci` locally** to confirm everything is green before tagging:
   ```bash
   make ci   # tidy + vet + lint + test-ci (race) + build
   ```

4. **Create and push an annotated tag** — this triggers the release workflow automatically:
   ```bash
   git tag -a v1.0.0 -m "v1.0.0"
   git push origin v1.0.0
   ```

5. **The release workflow** (triggered by the tag) will:
   - Re-run both validation gates (`Validate / Test`, `Validate / Quality`) at the exact tagged commit.
   - Verify the tag matches the checkout and that `CHANGELOG.md` has the `## [X.Y.Z]` section.
   - Cross-compile the reference CLI for 5 platforms, push the reference-CLI image to GHCR (`vX.Y.Z`, `vX.Y`, `vX`, `latest`), Trivy-scan it (CRITICAL/HIGH fail the release), and attach an SBOM, SLSA provenance and a Cosign signature.
   - Create a **GitHub Release** with the CHANGELOG section as release notes plus the binaries, checksums, SBOM and provenance.

6. **Notify consumers** (Slack/ADR) with upgrade notes if MINOR or MAJOR.

### Pre-release tags (optional)

| Tag pattern | Meaning |
|-------------|---------|
| `v1.1.0-rc.1` | Release candidate; not for production unless approved |
| `v1.1.0-beta.1` | Early integration testing |

## Compatibility matrix (library ↔ Go)

| platform-events | Go (`go.mod`) |
|-----------------|---------------|
| `v1.0.x` | `1.26+` |

Consumer services must use the same or newer Go toolchain as stated in this module's `go.mod`.

## Related files

| File | Purpose |
|------|---------|
| [CHANGELOG.md](CHANGELOG.md) | User-facing history per version |
| [README.md](#versioning-and-releases) | Summary table and quick links |
| [CONTRIBUTING.md](CONTRIBUTING.md) | Development guide and PR checklist |
| [.github/workflows/release.yml](.github/workflows/release.yml) | Automated release pipeline |
| [go.mod](go.mod) | Module path and minimum Go version |
