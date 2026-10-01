# Event envelope

The `Envelope[T]` wire format, construction options, serialisation and payload typing. One of the detailed guides linked from the [project README](../../README.md#contributing).

---

## Event envelope

`Envelope[T]` is the canonical wire format for all inter-service events. Terminology used consistently throughout this document:

| Term | Meaning | Example |
|---|---|---|
| **event** | The logical domain occurrence and the `Envelope` that carries it | "publish an event", "the event's `TenantID`" |
| **message** | The SNS/SQS infrastructure unit — what the broker delivers, retries, and deletes | "SQS message", "DeleteMessage", "`MessageGroupID`" |
| **outbox record** | A row in `outbox_events` or `outbox_dead_letters` — the durable Postgres representation of an event pending publish | "claim outbox records", "failed outbox record" |

```json
{
  "id":             "01926e4f-...",
  "type":           "iam.user.created",
  "source":         "platform-iam",
  "specversion":    "1",
  "tenant_id":      "acme",
  "trace_id":       "4bf92f3577b34da6a3ce929d0e0e4736",
  "correlation_id": "...",
  "subject":        "users/01926e4f-...",
  "actor":          "admin@acme.com",
  "ip_address":     "203.0.113.42",
  "user_agent":     "Mozilla/5.0 (compatible; XPert/1.0)",
  "dataschema":     "550e8400-e29b-41d4-a716-446655440000",
  "time":           "2026-05-27T12:00:00Z",
  "data":           { ... }
}
```

### Creating envelopes

```go
type UserCreatedPayload struct {
    UserID string `json:"user_id"`
    Email  string `json:"email"`
}

// From an HTTP handler — always carry trace and tenant from the RequestContext.
rc, ok := gincommon.RequestContext(c) // returns (*RequestContext, bool) — false for unauthenticated requests
if !ok {
    return errors.New("missing request context")
}
env := events.NewEnvelope("iam.user.created", "platform-iam", UserCreatedPayload{
    UserID: user.ID,
    Email:  user.Email,
},
    events.WithTenantID(rc.TenantID),
    events.WithTraceID(rc.TraceID),    // REQUIRED for cross-service trace continuity — see note below
    events.WithSchemaVersion("1"),
)
```

> ⚠️ **Always pass `WithTraceID(rc.TraceID)` from the request context.** If `TraceID` is omitted:
>
> - The OTel `sqs.receive` span on the consumer side has no parent — it starts a new disconnected trace, severing the link back to the originating HTTP request
> - A production incident that begins as an HTTP 500 in service A and manifests as a data anomaly in service B becomes impossible to correlate in Tempo or Grafana without manual log searching
> - The `trace_id` field in handler log lines will be empty, breaking the log-to-trace pivot that structured logging provides
>
> `rc.TraceID` is available via `gincommon.GetRequestContext(c).TraceID` in any HTTP handler. For background jobs where there is no inbound HTTP trace, generate a new trace ID at job startup and pass it consistently across all events emitted in that job run — this at minimum groups all events from the same run together.

**System-level events** (background jobs, scheduled tasks, cross-tenant operations) that are not scoped to any single tenant should use `WithSystemTenant()` instead:

```go
// Background job — not associated with any specific tenant.
env := events.NewEnvelope("billing.invoices.generated", "billing-worker",
    json.RawMessage(payload),
    events.WithSystemTenant(),   // sets TenantID = "system"
)
```

> ⚠️ **Use `WithSystemTenant()` only when the event genuinely has no tenant scope** — scheduled batch jobs, cross-tenant reconciliation, or platform-level operational events. Do not use it to avoid looking up a tenant ID, to simplify a code path, or because the tenant is "unknown at call time" (that last case means the upstream context is missing and should be fixed, not papered over).
> Misuse bypasses the tenant isolation that `platform-pgcommon`'s RLS policies enforce on the consumer side. A consumer handler that receives a `system` tenant ID will execute DB queries without a tenant GUC set, which causes RLS policies to either reject the query or return the wrong row set, depending on your policy definition. The failure is silent at the event level and only surfaces as incorrect data in the consuming service.

**`source` field contract:** identifies the producing service. Rules:

| Rule | Detail |
|---|---|
| Stable and globally unique | Use the canonical service name: `platform-iam`, `billing-service`, `inventory-worker`. It must be unique across all services in the organisation |
| Immutable across deployments | Do not derive it from hostname, pod name, or any runtime variable — it must be the same value in every environment |
| Environment-free | Never append `-dev`, `-staging`, `-prod`, or a region suffix. The environment is implicit in which AWS account/topic the event lands on; embedding it in `source` makes Loki/Tempo queries environment-specific and breaks dashboards when you promote code |
| Set as a constant | Define it once as a package-level constant in the service (`const serviceName = "platform-iam"`) and pass it to `NewEnvelope` and `events.MetricsIdentity.Service` from that single source of truth |

`source` is used for debugging, tracing (`sns.publish` span attribute), Prometheus metric labels, and consumer routing. Changing it severs observability continuity — historical log queries, dashboards, and alert rules that filter on `source` will silently stop matching.

**`event_type` convention:** `<domain>.<entity>.<past-tense-verb>[.v<N>]` — e.g. `iam.user.created`, `billing.invoice.settled`. The `.v<N>` segment is **only added for breaking payload changes** (v1 is implicit).

**Event types are immutable once published.** Additive fields (tagged `json:",omitempty"`) are allowed in the same type. Removing, renaming, changing the type, or changing the meaning of an existing field requires a new event type (e.g. `iam.user.created.v2`). See [EVENT_SCHEMA_GOVERNANCE.md § Event versioning](../../EVENT_SCHEMA_GOVERNANCE.md#event-versioning) for the full three-tier model and decision guide.

**Consumers ignore unknown fields by default (Go JSON behaviour).** This is a safety guarantee — do not disable it. `json.Decoder.DisallowUnknownFields()` turns every non-breaking producer addition into a consumer runtime failure. If you find yourself reaching for strict decoding to catch typos in field names, write a struct-tag linter instead.

**`WithSchemaVersion`** records the payload contract version in the envelope. Set `"1"` at inception. Increment **only** when both conditions are true: (1) a new optional field was added **and** (2) at least one consumer needs to branch logic based on whether that field is present. Do not increment for fields that are purely additive and whose absence consumers will handle identically to their presence (e.g. a display-only label that is simply rendered or ignored):

```go
env := events.NewEnvelope("iam.user.created.v2", "platform-iam", payload,
    events.WithTenantID(rc.TenantID),
    events.WithTraceID(rc.TraceID),
    events.WithSchemaVersion("1"), // v2 event type starts a new schema series at "1"
)
```

Consumers that receive an unrecognised schema version (`SchemaVersion`, wire key `specversion`) should log a warning and delete the message rather than silently misparsing it. If it is absent, treat it as `"1"` for backward compatibility.

### Serialisation

```go
// Marshal to JSON wire format.
data, err := env.JSON()

// Unmarshal and validate required fields.
env, err := events.ParseEnvelope[UserCreatedPayload](data)
```

### Payload typing

`Envelope[T any]` is generic, but two concrete forms appear throughout the codebase with different purposes:

| Form | When to use |
|------|-------------|
| `Envelope[json.RawMessage]` | Publishing (`Publisher.Publish` requires this form); handler dispatch (the `Handler` func type uses this); routing layers that inspect headers but not the payload; audit loggers; DLQ handlers that forward without parsing |
| `Envelope[YourStruct]` | Direct serialisation via `env.JSON()` in tests; `ParseEnvelope[YourStruct]` when you own the event type and want the payload pre-parsed; mock injection in unit tests with `mock.Inject(env)` |

**The transport boundary always uses `json.RawMessage`.** The `Publisher` and `Handler` interfaces are both fixed to `json.RawMessage` — this is intentional. It keeps the routing layer (SQS consumer, outbox runner) free of business-type imports and avoids a dependency on the payload struct definition.

Typed payload access happens *inside* the handler after the transport layer has delivered the envelope:

```go
// ✅ Correct pattern — transport receives json.RawMessage; handler parses to typed struct
func handleUserCreated(ctx context.Context, env events.Envelope[json.RawMessage]) error {
    var payload UserCreatedPayload
    if err := json.Unmarshal(env.Payload, &payload); err != nil {
        return fmt.Errorf("unmarshal UserCreatedPayload: %w", err)
    }
    // payload is now typed; env.ID / env.TenantID / env.TraceID are always available directly
    return repo.CreateUser(ctx, payload)
}

// ✅ Also correct — ParseEnvelope when consuming outside a Handler context
env, err := events.ParseEnvelope[UserCreatedPayload](rawBytes)
// env.Payload is now UserCreatedPayload (not json.RawMessage)
```

**When publishing**, always marshal to `json.RawMessage` before calling `Publish` or `Enqueue`:

```go
// ✅ Correct
raw, err := json.Marshal(UserCreatedPayload{UserID: user.ID, Email: user.Email})
if err != nil { ... }
env := events.NewEnvelope("iam.user.created", "platform-iam", json.RawMessage(raw),
    events.WithTenantID(rc.TenantID),
    events.WithTraceID(rc.TraceID),
    events.WithSchemaVersion("1"),
)

// ❌ Wrong — NewEnvelope[UserCreatedPayload] produces Envelope[UserCreatedPayload],
// which cannot be passed to Publisher.Publish or outbox.Enqueue without explicit conversion
env := events.NewEnvelope("iam.user.created", "platform-iam", UserCreatedPayload{...})
publisher.Publish(ctx, env) // compile error: Envelope[UserCreatedPayload] ≠ Envelope[json.RawMessage]
```

**Routing handlers** that need to inspect the event type and forward without parsing should stay with `json.RawMessage` throughout — no unmarshal cost, no coupling to payload structs:

```go
// Fan-out router — dispatches by event_type without ever touching the payload
func route(ctx context.Context, env events.Envelope[json.RawMessage]) error {
    switch env.Type {
    case "iam.user.created":
        return iamConsumer.Publish(ctx, env)   // forward as-is
    case "billing.invoice.settled":
        return billingConsumer.Publish(ctx, env)
    default:
        return nil // unknown type; discard
    }
}
```

### Envelope ID

`Envelope.ID` is a **UUID v7** (time-ordered, collation-friendly in Postgres B-tree indexes). Use it as an idempotency key on the consumer side.

### Envelope compatibility guarantees

The `id`, `type`, `source`, and `time` wire fields are **stable** — always present, never removed or renamed, format frozen within `v1.x`. The remaining fields (`tenant_id`, `trace_id`, `correlation_id`, `specversion`, `subject`, `actor`, `dataschema`, `ip_address`, `user_agent`) are **contextual** — present when set, never removed. The library may add new optional fields in MINOR releases; existing consumers are unaffected. See [ARCHITECTURE.md § Envelope compatibility guarantees](../../ARCHITECTURE.md#envelope-compatibility-guarantees) for the full per-field stability class table and the `v1.x` never-break list.

