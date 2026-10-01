# HMAC helpers

Signing and verification, and when HMAC is (and isn't) the right tool. One of the detailed guides linked from the [project README](../../README.md#contributing).

---

## HMAC helpers

### When to use HMAC

HMAC-SHA256 provides **message authenticity and integrity** — proof that the payload was produced by a party that holds the shared key and has not been tampered with in transit. It does **not** provide confidentiality (the payload is still plaintext) or replay protection (a captured signature remains valid indefinitely unless you add a nonce or expiry check).

| Scenario | HMAC required | Why |
|----------|:---:|-----|
| Receiving webhooks from external systems (Stripe, GitHub, etc.) | **Yes** | The channel is the public internet; the SNS/SQS IAM boundary does not apply |
| Cross-service call over an internal HTTP endpoint (not SNS/SQS) | **Yes** | Service-to-service HTTP has no built-in message-level auth; HMAC fills that gap |
| Zero-trust internal network where service identity is not enforced at the infra layer | **Yes** | HMAC adds app-layer authentication even when mTLS or IRSA is absent |
| Events flowing exclusively over SNS → SQS within the same AWS account and IAM boundary | **No** | SNS delivery is authenticated by IAM policies; the message cannot be injected or tampered with by an unauthorised caller |
| Events signed by the outbox runner and delivered to an SQS queue you own | **No** | The publisher (outbox runner) is operating under your service's IRSA role; IAM controls who may publish |

**Rules:**
- Always use `VerifyEnvelope` (not `Verify`) when checking a full envelope — it re-serialises the envelope fields in a fixed order before hashing, so envelope field order on the wire does not matter. The `data` payload is only compacted, not key-sorted: a hop that re-serialises the payload with a different key order breaks the signature.
- Do not verify HMAC in a separate goroutine or after the handler context has branched — a failed verify must reject the message before any side effects occur.
- Rotate keys by accepting both the current and previous key for a short window (one deploy cycle), then dropping the old key.
- Keys must be ≥ 32 bytes. Store them in AWS Secrets Manager or SSM Parameter Store; never in environment variables checked into source control.

```go
key := []byte(os.Getenv("WEBHOOK_SECRET")) // must be ≥ 32 bytes

// Sign
sig, err := events.Sign(key, payload)

// Verify — constant-time; returns false on mismatch, never panics
if !events.Verify(key, payload, sig) {
    return errors.New("invalid signature")
}

// Sign/verify a full envelope (serialises to canonical JSON first)
sig, err := events.SignEnvelope(key, env)

ok, err := events.VerifyEnvelope(key, env, sig)
if err != nil || !ok {
    return errors.New("envelope signature invalid")
}
```

> `Verify` uses `hmac.Equal` (constant-time) — never replace with string `==`. `VerifyEnvelope` returns `(false, nil)` on mismatch and `(false, err)` on malformed input — callers must check both return values.

**Key length:** `Sign` returns `("", ErrKeyTooShort)` for keys < 32 bytes. Callers that ignore the error emit an empty signature, which `Verify` rejects — the system degrades safely.

