package outbox

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/adapter/outbound/outboxstore"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/events"
)

// Enqueue inserts a serialised envelope into the outbox_events table within the
// caller's transaction. No publish happens at insert time — the Runner delivers
// asynchronously.
//
// Use with pgcommon.RunInTx so the business write and the outbox insert share one
// transaction boundary (commit-or-rollback together):
//
//	pgcommon.RunInTx(ctx, pool, pgcommon.TxOptions{}, func(ctx context.Context, tx pgcommon.Tx) error {
//	    _ = repo.Save(ctx, tx, record)
//	    return outbox.Enqueue(ctx, tx, envelope)
//	})
func Enqueue(ctx context.Context, tx pgcommon.Tx, env events.Envelope[json.RawMessage]) error {
	return enqueue(ctx, tx, env, "")
}

// MaxOrderingKeyLen bounds an EnqueueOrdered ordering key.
const MaxOrderingKeyLen = 256

// EnqueueOrdered is Enqueue with an ordering key — typically the aggregate the
// event is about (e.g. "user/<id>", or env.Subject). Records with the same key
// are published one at a time, in enqueue order, across all runner replicas:
// a record enqueued while an earlier one of its key is unpublished waits, and
// becomes due when that one is published or moved to outbox_dead_letters.
// Records enqueued with Enqueue are unaffected. Requires outbox migration 010.
//
// Enqueue order is the order of the INSERTs (a sequence), so two transactions
// enqueuing for the same key must not interleave: take the aggregate's row
// lock (e.g. the business UPDATE of that row, or SELECT … FOR UPDATE) BEFORE
// calling EnqueueOrdered, so the second transaction's insert happens after the
// first commits. On a FIFO topic, derive WithMessageGroupID from the same key
// so SNS keeps the order the runner established.
//
// A failing head record holds its key until it is published or dead-lettered
// after MaxAttempts (platform_outbox_ordering_blocked_events counts the
// records waiting); a replayed dead letter joins the back of its key.
func EnqueueOrdered(ctx context.Context, tx pgcommon.Tx, env events.Envelope[json.RawMessage], orderingKey string) error {
	if orderingKey == "" {
		return fmt.Errorf("outbox: EnqueueOrdered requires a non-empty ordering key — use Enqueue for unordered events")
	}
	if len(orderingKey) > MaxOrderingKeyLen || !utf8.ValidString(orderingKey) || strings.ContainsRune(orderingKey, '\x00') {
		return fmt.Errorf("outbox: ordering key must be valid UTF-8 without null bytes, at most %d bytes", MaxOrderingKeyLen)
	}
	return enqueue(ctx, tx, env, orderingKey)
}

func enqueue(ctx context.Context, tx pgcommon.Tx, env events.Envelope[json.RawMessage], orderingKey string) error {
	if tx == nil {
		return fmt.Errorf("outbox: transaction must not be nil — use pgcommon.RunInTx to obtain a transaction")
	}
	if env.ID == "" {
		return fmt.Errorf("outbox: Enqueue requires a non-empty envelope ID — use events.NewEnvelope to construct envelopes")
	}
	if env.Type == "" {
		return fmt.Errorf("outbox: Enqueue requires a non-empty envelope Type")
	}
	if env.Source == "" {
		return fmt.Errorf("outbox: Enqueue requires a non-empty envelope Source")
	}
	if env.Timestamp.IsZero() {
		return fmt.Errorf("outbox: Enqueue requires a non-zero Timestamp — use events.NewEnvelope to construct envelopes")
	}
	if strings.ContainsRune(env.ID, '\x00') || strings.ContainsRune(env.Type, '\x00') || strings.ContainsRune(env.Source, '\x00') ||
		strings.ContainsRune(env.TenantID, '\x00') || strings.ContainsRune(env.TraceID, '\x00') {
		return fmt.Errorf("outbox: envelope fields (ID, Type, Source, TenantID, TraceID) must not contain null bytes")
	}
	// The ID is stored in a uuid column and read back in canonical form, while
	// publish failures are reported under the ID in the payload: anything but
	// the canonical spelling would make a failed publish look delivered.
	if id, err := uuid.Parse(env.ID); err != nil || id.String() != env.ID {
		return fmt.Errorf("outbox: envelope ID %q must be a canonical lowercase UUID — use events.NewEnvelope to construct envelopes", env.ID)
	}
	b, err := json.Marshal(env)
	if err != nil {
		return err
	}
	// The payload column is jsonb, which rejects the \u0000 escape: the
	// INSERT would fail and roll back the caller's business transaction on
	// every retry. Reject it here with a clear error instead.
	if containsJSONNul(b) {
		return fmt.Errorf("outbox: envelope contains a NUL character (\\u0000) — Postgres jsonb cannot store it; strip it from the payload and string fields")
	}
	// SNS message size limit is 256 KB. Reject early to avoid persisting records
	// that will always fail at publish time and burn outbox attempt budget.
	// A publisher codec (WithCodec) re-encodes the payload at publish time —
	// base64 alone adds about a third — so with a codec keep payloads well
	// below this; an envelope that grows past SNS's limit fails permanently
	// and is dead-lettered after MaxAttempts.
	const maxEnvelopeBytes = 240 * 1024
	if len(b) > maxEnvelopeBytes {
		return fmt.Errorf("outbox: serialised envelope is %d bytes — exceeds safe SNS limit (%d bytes); reduce payload size", len(b), maxEnvelopeBytes)
	}
	now := time.Now().UTC()
	return outboxstore.InsertRecord(ctx, tx, domain.OutboxRecord{
		ID:          env.ID,
		EventType:   env.Type,
		Payload:     b,
		TenantID:    env.TenantID,
		TraceID:     env.TraceID,
		CreatedAt:   now,
		ScheduledAt: now,
		OrderingKey: orderingKey,
	})
}

// containsJSONNul reports whether JSON text b contains the escape \u0000 — an
// unescaped backslash followed by u0000 (a literal "\\u0000" in a string is
// two characters, backslash and "u0000", and is fine).
func containsJSONNul(b []byte) bool {
	for i := 0; i < len(b); {
		j := bytes.Index(b[i:], []byte(`\u0000`))
		if j < 0 {
			return false
		}
		k := i + j
		n := 0
		for p := k; p >= 0 && b[p] == '\\'; p-- {
			n++
		}
		if n%2 == 1 {
			return true
		}
		i = k + 1
	}
	return false
}
