# Security Policy

## Supported Versions

| Version | Supported |
|---------|-----------|
| 1.x     | ✅ Active |

Older major versions are not patched. Upgrade to the latest 1.x release.

## Reporting a Vulnerability

**Do not open a public GitHub issue for security vulnerabilities.**

Email: vijay@bcbpsolutions.com  
Subject: `[platform-events] Security vulnerability`

Include in your report:
- Description of the vulnerability and affected component
- Steps to reproduce
- Potential impact (credential leakage, RLS bypass, HMAC/signature bypass, event forgery, privilege escalation, DoS, etc.)
- Suggested fix or patch (if any)

### Response timeline

| Step | Target |
|------|--------|
| Initial acknowledgement | 48 hours |
| Severity assessment | 5 business days |
| Patch release (critical/high) | 14 days |
| Public disclosure | After patch ships |

We follow responsible disclosure. We will credit reporters in release notes unless anonymity is requested.

See [../SECURITY.md](../SECURITY.md) for this library's trust model and known sensitive surface area (HMAC canonicalisation, GUC injection via `TenantID`, cross-language wire-format contract with `platform-eventcommon`).
