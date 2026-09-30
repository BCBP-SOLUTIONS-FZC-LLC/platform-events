# Event Schema Governance

This document defines the rules for designing, evolving, and retiring event schemas in BCBP platform services that use `platform-events`.

It applies to:
- All `Envelope.Payload` Go structs defined in consuming services
- The `event_type` naming convention
- The `schema_version` envelope field
- The cross-service event type registry

> **Envelope wrapper stability** (field names, types, presence rules) is a library-level guarantee documented in [ARCHITECTURE.md § Envelope compatibility guarantees](ARCHITECTURE.md#envelope-compatibility-guarantees). This document governs payload content, not the wrapper.

---

## Contents

- [Core principles](#core-principles)
- [Event versioning](#event-versioning)
- [Event type naming](#event-type-naming)
- [Schema versioning with event_type](#schema-versioning-with-event_type)
- [Schema version field](#schema-version-field)
- [Payload evolution rules](#payload-evolution-rules)
  - [Payload content anti-patterns](#payload-content-anti-patterns)
- [Breaking vs non-breaking changes](#breaking-vs-non-breaking-changes-decision-tree)
- [Migration window pattern](#migration-window-pattern)
- [Consumer compatibility contract](#consumer-compatibility-contract)
- [Event type registry](#event-type-registry)
  - [Registry enforcement](#registry-enforcement)
  - [Event owner responsibilities](#event-owner-responsibilities)
- [Deprecation and sunset process](#deprecation-and-sunset-process)

---

## Core principles

1. **Events are facts, not commands.** An event records something that already happened — it is an immutable statement of past truth, not an instruction. Name event types in the past tense (`user.created`, not `create.user`). Producers do not direct consumers; consumers decide independently how to react. This has two concrete implications: (a) consumers must be resilient to receiving the same fact more than once — idempotency is not optional; (b) a consumer can be added, removed, or changed without any producer modification — the producer has no knowledge of who is listening.
2. **Use the outbox for any event tied to a database write.** Calling `publisher.Publish` directly for transactional events introduces a silent crash window — the DB write commits but the event is lost if the process dies before SNS receives it. See [Publishing guide § Publishing rules](docs/guides/publishing.md#publishing-rules) for the full decision table.
3. **Consumers ignore unknown fields by default (Go JSON behaviour). This is a safety guarantee — do not disable it.** Go's `encoding/json` silently discards unrecognised fields; a new optional field from the producer is a no-op. `json.Decoder.DisallowUnknownFields()` removes this safety net and turns every Tier 1 producer addition into a consumer runtime error.
4. **Producers must not silently change field semantics.** Renaming, removing, or changing the type of a field is always a breaking change, regardless of whether JSON serialisation succeeds.
5. **Breaking changes require a new versioned event type.** Never mutate an existing event type in an incompatible way. Mint a new type (e.g. `iam.user.created.v2`) and run both in parallel during the migration window.
6. **Use `schema_version` to signal the payload contract.** Publishers set it; consumers assert it. An unrecognised version is rejected, not silently misparsed.
7. **Every event type has a single owner.** Ownership lives in the event type registry (this file). The owner is the only team that may publish that type; consumers are read-only.
8. **An event payload is a public API contract.** Treat payload changes with the same rigour as HTTP API versioning: additive changes are non-breaking, anything else requires a versioned type. A payload consumed by three services has three implicit API clients — breaking it silently is no different from returning a 500 on a GET endpoint with no warning.

---

## Event versioning

### The immutability rule

**Event types are immutable once published.** Existing consumers depend on the exact payload shape of a published event type. Mutating it in a breaking way silently corrupts consumer state with no compile-time or runtime warning — JSON unmarshalling succeeds but fields carry wrong or zero values.

> If `iam.user.created` exists in production, its payload contract is frozen. You may add new optional fields. You may not remove, rename, or change the type or meaning of existing fields.

### Three-tier model

Every schema change falls into exactly one tier:

| Tier | Change | Action required | Consumers |
|------|--------|-----------------|-----------|
| **1 — No-op** | Add a new optional field tagged `json:",omitempty"` | Bump `schema_version` **only if** at least one consumer needs to branch logic on the field's presence; otherwise no action required | Existing consumers ignore the new field automatically — Go's `json.Unmarshal` discards unknown fields by default |
| **2 — New event type** | Remove, rename, change type, change semantics, or make a field required | Mint `iam.user.created.v2`; run both types in parallel during migration window | v1 consumers never receive v2 events; they subscribe to different queue filter policies |
| **3 — New event type series** | The v2 payload itself needs a breaking change | Mint `iam.user.created.v3` | Same process as Tier 2 |

**No partial tiers exist.** There is no "soft breaking change" — every mutation is either additive (Tier 1) or requires a new event type (Tier 2+).

### What this means for consumers

Consumers must be written to accept any Tier 1 evolution without a code change:

```go
// ✅ Correct — tolerates new fields added in future schema versions
type UserCreatedPayload struct {
    UserID string `json:"user_id"` // always present
    Email  string `json:"email"`   // always present
    Role   string `json:"role,omitempty"` // optional; absent on v1 events
}

// ✅ Correct — unknown fields silently discarded; never use DisallowUnknownFields
var payload UserCreatedPayload
_ = json.Unmarshal(env.Payload, &payload)

// ❌ Wrong — breaks on any new field the producer adds
dec := json.NewDecoder(bytes.NewReader(env.Payload))
dec.DisallowUnknownFields() // never do this on event payloads
```

`json.Decoder.DisallowUnknownFields()` must never be used on event payloads. It turns every Tier 1 (non-breaking) producer change into a consumer runtime error.

### Quick decision guide

```
Does the change preserve the existing payload contract?
│
├── Yes — adding a new optional field
│         └── Tier 1: add field with omitempty
│                     bump schema_version only if a consumer must branch on its presence
│
├── Possibly — changing an existing field
│   ├── Widening a numeric type (int32 → int64)
│   │         └── Tier 1: safe in JSON; bump schema_version only if consumers branch on the difference
│   ├── Removing, renaming, or changing JSON type
│   │         └── Tier 2: new event type required
│   └── Changing units or semantics (cents → dollars)
│             └── Tier 2: new event type required (no compile error protects you)
│
└── No — incompatible change
          └── Tier 2: mint iam.your.event.v<N+1>
```

When in doubt, **mint a new event type**. Running two event types in parallel for a few weeks costs less than debugging a silent data corruption incident.

### Key references

| Topic | Where |
|-------|-------|
| Naming format (`iam.user.created.v2`) | [Event type naming](#event-type-naming) |
| What counts as breaking | [Payload evolution rules](#payload-evolution-rules) · [Breaking vs non-breaking changes](#breaking-vs-non-breaking-changes-decision-tree) |
| How to migrate consumers safely | [Migration window pattern](#migration-window-pattern) |
| `schema_version` field usage | [Schema version field](#schema-version-field) |
| Deprecating old event types | [Deprecation and sunset process](#deprecation-and-sunset-process) |

---

## Event type naming

Format: `<domain>.<entity>.<past-tense-verb>[.v<N>]`

| Segment | Rules | Examples |
|---------|-------|---------|
| `domain` | Short service/domain name, lowercase, no hyphens | `iam`, `billing`, `inventory`, `comms` |
| `entity` | Singular noun, lowercase | `user`, `invoice`, `shipment`, `webhook` |
| `verb` | Past-tense, lowercase | `created`, `updated`, `deleted`, `settled`, `dispatched` |
| `.v<N>` | **Only present when N ≥ 2.** Version 1 has no segment. | `iam.user.created` (v1), `iam.user.created.v2` (v2) |

**Good:**
```
iam.user.created
billing.invoice.settled
inventory.shipment.dispatched
iam.user.created.v2          ← breaking payload change; v1 consumers unaffected
```

**Bad:**
```
UserCreated                  ← PascalCase, no domain
iam.userCreated              ← camelCase entity
billing.invoice.update       ← present tense
iam.user.created.v1          ← never use .v1; v1 is implicit
```

### Consumer matching

Consumers match event types using `strings.HasPrefix` for prefix routing or `==` for exact matching. Both patterns work correctly with versioned types because `iam.user.created.v2` does **not** have `iam.user.created` as a prefix.

```go
// Handles both v1 and v2 explicitly:
switch env.Type {
case "iam.user.created":
    handleV1(env)
case "iam.user.created.v2":
    handleV2(env)
}

// Handles all iam.user.* events (all verbs, all versions):
if strings.HasPrefix(env.Type, "iam.user.") { ... }
```

---

## Schema versioning with event_type

When a breaking payload change is required, **do not modify the existing event type**. Instead:

1. Register a new type with the next version segment (e.g. `iam.user.created.v2`).
2. Publish **both** the old and new event types from the producer during the migration window.
3. Update all consumers to handle the new type.
4. Once all consumers are on the new type, stop publishing the old type and begin the sunset timer.

The old event type follows the [deprecation and sunset process](#deprecation-and-sunset-process).

### Why not semantic versioning in the payload?

`event_type` versioning is for **routing** — SQS subscription filter policies match on `EventType` message attributes. Bumping `.v2` in the type means a consumer that only subscribes to `iam.user.created` never receives v2 messages; there is no deserialization surprise.

The `schema_version` field handles the complementary concern: fine-grained payload contract assertion inside a single event type series (e.g. v1 payloads that add new optional fields).

---

## Schema version field

`Envelope.SchemaVersion` carries the payload contract version. It is optional (`omitempty`) and backward-compatible with older envelopes that do not set it.

### Publisher contract

- Set `WithSchemaVersion("1")` for every new event type at inception.
- Increment to `"2"`, `"3"`, etc. **only when both conditions hold:** a new optional field was added **and** at least one consumer must branch logic based on whether that field is present. Do not increment for purely additive fields that all consumers will handle the same way regardless of presence (zero-value / omitted = same outcome).
- For **breaking** changes, mint a new event type (`.v2`) AND set `WithSchemaVersion("1")` again on the new series (the v2 event type starts at schema version 1).

```go
// Initial publish — explicit schema version
env := events.NewEnvelope(
    "iam.user.created",
    "platform-iam",
    UserCreatedPayload{UserID: "u123", Email: "alice@acme.com"},
    events.WithTenantID(rc.TenantID),
    events.WithTraceID(rc.TraceID),
    events.WithSchemaVersion("1"),
)

// Adding an optional field (non-breaking): bump schema_version to "2"
env := events.NewEnvelope(
    "iam.user.created",
    "platform-iam",
    UserCreatedPayload{UserID: "u123", Email: "alice@acme.com", Role: "admin"},
    events.WithTenantID(rc.TenantID),
    events.WithTraceID(rc.TraceID),
    events.WithSchemaVersion("2"),
)
```

### Consumer contract

- If you depend on a specific field introduced in schema version N, assert `env.SchemaVersion`:

```go
func handleUserCreated(ctx context.Context, env events.Envelope[json.RawMessage]) error {
    // Accept any schema version — handle unknown versions defensively
    if env.SchemaVersion != "" && env.SchemaVersion != "1" && env.SchemaVersion != "2" {
        // Unknown future version: log and skip rather than silently misparse
        logger.Warn(ctx, "unrecognised schema_version, skipping",
            zap.String("event_type", env.Type),
            zap.String("schema_version", env.SchemaVersion),
        )
        return nil // message will be deleted; no retry
    }
    ...
}
```

- If `SchemaVersion` is absent (empty string), treat it as `"1"` for backward compatibility.
- **Never** fail hard on a `SchemaVersion` you don't recognise unless the consumer has a strict security requirement. Log a warning and delete the message to avoid poison-pill retries.

---

## Payload evolution rules

### Always allowed (non-breaking)

| Change | Rule |
|--------|------|
| Add a new optional field | Tag with `json:",omitempty"`. Existing consumers ignore it. |
| Add a new enum value | Consumers must treat unknown enum values as a no-op or use a default. |
| Widen a numeric type | `int32` → `int64` is safe in JSON (no integer overflow on well-formed values). |
| Add a new event type | No consumers receive it until they explicitly subscribe. |

### Never allowed without a new event type (breaking)

| Change | Why |
|--------|-----|
| Remove a field | Consumers that read the field receive zero-value silently. |
| Rename a field | Equivalent to remove + add: old name becomes zero-value. |
| Change a field's JSON type | e.g. `string` → `number` causes unmarshal errors. |
| Change field semantics | e.g. `amount` changes from cents to dollars — no compile error, silent data corruption. |
| Make an optional field required | Producers that publish both old and new consumers break old consumers that don't set it. |
| Tighten enum values | Removing an enum value breaks consumers that already store and replay the removed value. |

### Required `omitempty` discipline

All payload struct fields that are not unconditionally present must be tagged `json:",omitempty"`. This prevents consumers from distinguishing "field was absent" from "field was zero-value", which is the source of most silent breakage.

```go
// Good
type UserCreatedPayload struct {
    UserID string `json:"user_id"`            // always present
    Email  string `json:"email"`              // always present
    Role   string `json:"role,omitempty"`     // optional, added in schema v2
}

// Bad — missing omitempty on optional field
type UserCreatedPayload struct {
    UserID string `json:"user_id"`
    Role   string `json:"role"`              // zero-value "" silently set on v1 events
}
```

### Payload content anti-patterns

Well-formed JSON is not enough — payload content design determines whether events remain small, stable, and consumer-agnostic over time.

| Anti-pattern | Why it is wrong | Correct approach |
|---|---|---|
| **Embedding the full DB row** | Couples the event schema directly to your database schema. Every column rename or type change becomes a breaking event change. Row size grows with the table, eventually hitting the 240 KB limit. | Send the primary key (`user_id`, `order_id`) and the fields that drove the event (e.g. `new_status`, `amount_cents`). Consumers fetch full state via the API or a shared read model if they need more. |
| **Including base64 file blobs** | Binary content encoded as base64 inflates size by ~33%. A 180 KB file becomes a 240 KB payload — at the enforced limit, before the envelope overhead. | Store the file in S3; put the bucket + key in the payload. Set a presigned URL expiry that outlasts `SQS_VISIBILITY_TIMEOUT` + expected handler duration. |
| **Sending entire object graphs** | Nested parent → child → grandchild structures couple multiple domain entities into one event. Adding a field anywhere in the graph risks a breaking change. Consumers that only care about the leaf entity must parse the whole tree. | Flatten to the fields relevant to the event. If a consumer genuinely needs the parent, let it look it up by ID. |
| **Denormalising computed fields** | Including derived data (e.g. `full_name` from `first_name + last_name`, `total_price` from line items) means the payload is inconsistent the moment the source formula changes. | Send the source fields. Let each consumer compute derived values according to its own rules. |
| **Including internal system IDs consumers don't own** | Exposing internal shard keys, sequence IDs, or partition identifiers couples consumers to your internal storage topology. | Use stable, externally meaningful identifiers (`user_id`, `tenant_id`, `order_id`). Internal IDs are an implementation detail. |
| **Timestamps in local time or ambiguous timezone** | Consumers in different regions will misinterpret the value. Timezone abbreviations (`EST`, `IST`) are not unique across the world. | Use `time.Time` marshalled to RFC3339Nano UTC, which is what `Envelope.Timestamp` already does. Apply the same rule to any timestamp field in the payload. |

**Design principle: events carry minimal context, not full state.** An event answers "what happened and to which entity?" — not "what does the entity look like now?" If consumers need full current state, they should call the owning service's read API or subscribe to a snapshot-style event that the producer explicitly designs for read-model hydration.

```go
// ✅ Correct — minimal context
type OrderShippedPayload struct {
    OrderID        string    `json:"order_id"`
    ShippedAt      time.Time `json:"shipped_at"`
    TrackingNumber string    `json:"tracking_number,omitempty"`
}

// ❌ Wrong — full DB row embedded in event
type OrderShippedPayload struct {
    OrderID        string         `json:"order_id"`
    CustomerID     string         `json:"customer_id"`
    Items          []LineItem     `json:"items"`          // entire order graph
    BillingAddress Address        `json:"billing_address"` // full nested struct
    ShippingAddress Address       `json:"shipping_address"`
    TotalCents     int64          `json:"total_cents"`
    Discount       *DiscountModel `json:"discount"`
    // ... 20 more fields from the orders table
}
```

See [Outbox guide § Payload size guidelines](docs/guides/outbox.md#payload-size-guidelines) for the size limits and the S3 reference pattern for large binary payloads.

---

## Breaking vs non-breaking changes: decision tree

```
Is the field/type being changed?
├── Adding a new optional field with omitempty → NON-BREAKING (bump schema_version)
├── Adding a new required field               → BREAKING (new event type)
├── Removing any field                        → BREAKING (new event type)
├── Renaming a field                          → BREAKING (new event type)
├── Changing a field's JSON type              → BREAKING (new event type)
├── Changing field semantics (units, meaning) → BREAKING (new event type)
└── Adding a new event type entirely          → NON-BREAKING
```

When in doubt: **mint a new event type**. Running two event types in parallel for a migration window is safer than breaking a consumer.

---

## Migration window pattern

When a breaking change is unavoidable:

```
Week 0: Register iam.user.created.v2 in the event type registry.
        Publisher emits BOTH iam.user.created (v1) and iam.user.created.v2.
        New consumers subscribe to v2.

Week 1–2: All services that consume iam.user.created are updated to also
          handle iam.user.created.v2 (or switch exclusively to v2).
          Verify via Grafana that iam.user.created consumer lag is zero and
          all consumers are processing iam.user.created.v2.

Week 3+: Stop publishing iam.user.created (v1).
         Mark v1 as deprecated in this file with the sunset date.
         After the sunset date: remove v1 SQS subscriptions and filter policies.
```

**Duration:** The migration window must be at minimum **two deployment cycles** for all consuming services. For shared infrastructure services, allow four weeks.

---

## Consumer compatibility contract

Consumers must follow these rules to remain resilient to future producer changes:

| Rule | Implementation |
|------|---------------|
| Ignore unknown fields | Go's `json.Unmarshal` does this automatically — no action required. Do not override it with `json.Decoder.DisallowUnknownFields()`; that turns the default safe behaviour into a fragile one. |
| Treat absent optional fields as zero-value | Check `omitempty`-tagged fields for zero value before use. |
| Handle unknown enum values | Use a `default` case or skip-unknown pattern. |
| Assert `schema_version` only when necessary | Check it when a field introduced in schema N is required for correctness. |
| Do not assume event ordering | Events may arrive out-of-order across the SNS/SQS boundary. Do not use `Envelope.Timestamp` for strict ordering across services — clock skew between producer hosts means timestamps from different services have no guaranteed order relationship. For strict ordering, use FIFO queues with a stable `MessageGroupID`. `Envelope.Timestamp` is acceptable for approximate display-level sorting within a single service's events. |
| Idempotency on `Envelope.ID` | The outbox runner guarantees at-least-once delivery. A handler may be called more than once for the same event. Use `env.ID` as the idempotency key — the recommended pattern is `INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING` inside the same transaction as the side-effect write. See [Consuming guide § Implementing idempotency](docs/guides/consuming.md#implementing-idempotency). |

---

## Event type registry

The canonical registry of all platform event types. Update this table when registering a new type or deprecating an old one.

| Event Type | Owner Service | Schema Version | Payload Struct | Status |
|-----------|--------------|----------------|---------------|--------|
| *(none registered — add entries here as services adopt platform-events)* | — | — | — | — |

<!--
EXAMPLE ROW (copy and fill in for each new event type):

| `iam.user.created` | platform-iam | 1 | `UserCreatedPayload` (iam/events/user.go) | active |

Fields:
- Event Type:      full dotted name, e.g. `iam.user.created` or `billing.invoice.settled.v2`
- Owner Service:   the service that publishes this type (only it may call NewEnvelope with this type)
- Schema Version:  current schema_version value set at all publish call sites
- Payload Struct:  Go type and file path in the owner's repo
- Status:          active | deprecated:YYYY-MM-DD | retired
-->

### Registry enforcement

> **A new event type MUST NOT be published to a production SNS topic until all three conditions are met:**
>
> 1. It has a row in the table above (registered)
> 2. An owner service is named (accountable)
> 3. `WithSchemaVersion("1")` is set at all publish call sites (contract declared)
>
> **An unregistered event type is considered invalid.** Consumers that receive an unregistered type should log a warning and drop the message — they cannot assert ownership, schema stability, or deprecation status for a type that has no registry entry.

| Condition | Why it is not optional |
|---|---|
| Registered | Without a registry entry, there is no way to know if a type is in production, who owns it, or whether it is safe to consume |
| Owner defined | Ownership determines who is accountable for payload stability, migration windows, and deprecation notices — an ownerless type has no one to notify consumers of breaking changes |
| `schema_version` set | An absent `schema_version` is ambiguously treated as `"1"` — acceptable for existing events in the wild, but a new type published without it cannot be distinguished from a legacy pre-governance event |

**Enforcement in code review:** the [PR checklist in CONTRIBUTING.md](CONTRIBUTING.md#pr-checklist) includes a registry check. Reviewers must reject any PR that introduces a new `NewEnvelope(eventType, ...)` call site without a corresponding registry entry in this file within the same PR.

### To register a new event type

1. Add a row to the table above (in the same PR as the publish call site — not a separate follow-up).
2. Get approval from the platform team.
3. Add `WithSchemaVersion("1")` to all publish call sites.
4. Document the payload struct fields (required vs optional) in the row or a linked doc.

### Event owner responsibilities

Ownership is not a label — it is an ongoing commitment. The service named as owner in the registry is solely responsible for the following throughout the event type's lifetime:

| Responsibility | What it means in practice |
|---|---|
| **Schema stability** | Payload changes must follow the three-tier model. No field removals, renames, or semantic changes without minting a new versioned type. All additive fields tagged `json:",omitempty"`. |
| **Schema version maintenance** | Increment `schema_version` only when a new optional field requires consumers to branch on its presence. Do not increment for purely additive fields consumers handle identically whether present or absent. Reset to `"1"` when a new versioned event type is minted. |
| **Migration planning** | Before publishing a breaking change (new event type), notify all consuming teams and agree on a migration window of at minimum two deployment cycles. |
| **Deprecation communication** | Announce deprecated event types via the platform Slack channel or ADR at least two weeks before sunset. Update the registry table with `deprecated: YYYY-MM-DD` on the same day. |
| **Backward compatibility during the migration window** | Publish both the old and new event type simultaneously for the full duration of the migration window. Do not stop publishing the old type until all consumers have confirmed they have migrated. |
| **Consumer awareness** | Maintain a list of known consumers in the registry row or linked doc. Ownership transfer requires notifying all known consumers. |

**Consumers must not rely on undocumented behaviour.** If your handler depends on a field, a value range, or a semantic guarantee that is not documented in the registry, raise a documentation PR against `EVENT_SCHEMA_GOVERNANCE.md` before shipping the consumer. Undocumented behaviour can change without notice and without triggering a version bump — it is not covered by the owner's stability commitment.

**Ownership transfer:** if a service is deprecated or the owning team changes, ownership must be explicitly transferred in a PR that updates the registry row and notifies all known consumers. An event type with no active owner is considered unmaintained and may be sunset by the platform team on 30 days' notice.

---

## Deprecation and sunset process

| Phase | Action |
|-------|--------|
| **Deprecated** | Mark the event type in the registry with `deprecated: YYYY-MM-DD`. Publisher continues to emit it. No new consumers may subscribe. |
| **Sunset warning** | Two weeks before sunset: alert all consuming teams via Slack/ADR. Remove SQS filter policy subscriptions for the old type. |
| **Sunset** | Stop publishing the event type. Remove from the registry (or mark `retired`). |

A deprecated event type receives no further schema changes. If a fix is required during the deprecation window, it is applied to the replacement type only.

---

## Related files

| File | Purpose |
|------|---------|
| [README.md](docs/guides/envelope.md#event-envelope) | Event envelope API reference |
| [ARCHITECTURE.md § Envelope compatibility guarantees](ARCHITECTURE.md#envelope-compatibility-guarantees) | Field stability classes, what the library reserves, `v1.x` never-break list |
| [ARCHITECTURE.md](ARCHITECTURE.md) | Wire format, invariants, observable signals |
| [VERSIONING.md](VERSIONING.md) | Library SemVer rules (separate from event schema versioning) |
| [CONTRIBUTING.md](CONTRIBUTING.md) | PR checklist includes schema governance steps |
| [CHANGELOG.md](CHANGELOG.md) | Per-version library changes |
