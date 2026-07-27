# Security policy

## Supported versions

| Version | Supported |
|---------|-----------|
| `v1.0.x` (current) | Yes — security fixes released as patch versions |
| `< v1.0.0` | No |

When a new major line (e.g. `v2`) ships, the previous major receives security fixes only for six months after the new major's release.

## Reporting a vulnerability

**Do not open a public GitHub issue for security vulnerabilities.**

Report security issues by email to **vijay@bcbpsolutions.com** with the subject line `[platform-events] Security vulnerability`.

Include:

- A description of the vulnerability and its potential impact.
- Steps to reproduce or a proof-of-concept (a minimal Go test is sufficient).
- The version(s) affected.
- Any suggested fix, if you have one.

### Response timeline

| Step | Target |
|------|--------|
| Initial acknowledgement | 48 hours |
| Severity assessment | 5 business days |
| Patch release (critical/high) | 14 days |
| Public disclosure | After patch ships |

We follow responsible disclosure. Reporters will be credited in release notes unless anonymity is requested.

## Trust model and known limitations

This library publishes to AWS SNS and consumes from AWS SQS using credentials supplied by the consuming service. It connects to PostgreSQL for the outbox runner. It does not independently authenticate to any external system.

| Assumption | Implication |
|-----------|-------------|
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` are treated as secrets | Store in a secrets manager or environment variable; never commit to source control. Use instance roles (IRSA) in production. |
| `DATABASE_URL` is treated as a secret | Store in a secrets manager; never commit. Run migrations under a separate migration role if possible. |
| HMAC keys passed to `Sign` / `Verify` are sourced from a secrets manager | Keys < 32 bytes are rejected at call time (`ErrKeyTooShort`). Rotate keys using versioned secrets. |
| `Envelope.TenantID` and `Envelope.TraceID` originate from a trusted `RequestContext` (e.g. platform-gincommon) | If these fields are populated from untrusted input without validation, GUC injection and trace linking can be spoofed. |
| SQS messages are delivered by AWS infrastructure | The library does not verify message authenticity beyond JSON parsing. Use `VerifyEnvelope` with a shared HMAC key for cross-service authentication when required. |
| The `Envelope` wire format is a cross-language contract with `platform-eventcommon` (Python) | A change that desynchronises the two implementations' JSON encoding or HMAC canonicalisation is a correctness and security issue, not just a compatibility one — see the `interop` CI job. |

## Scope

The following are in scope for vulnerability reports:

- HMAC bypass or timing attacks in `Sign` / `Verify` / `SignEnvelope` / `VerifyEnvelope`
- Credential leakage in log output (DSN, AWS keys, tenant ID in structured log fields)
- Outbox GUC injection spoofing through unsanitised `Envelope.TenantID` values
- Denial-of-service conditions in the outbox runner or SQS consumer loop
- RLS bypass through incorrect GUC lifecycle management in the SQS consumer context
- Deserialisation vulnerabilities in `ParseEnvelope` or payload parsing

Out of scope: theoretical attacks requiring full control of AWS infrastructure or the Postgres server, issues in transitive dependencies unrelated to this library's functionality, and issues in the `cmd/platform-events` reference CLI.
