package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

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
	if strings.ContainsRune(env.ID, '\x00') || strings.ContainsRune(env.Type, '\x00') || strings.ContainsRune(env.Source, '\x00') {
		return fmt.Errorf("outbox: envelope fields (ID, Type, Source) must not contain null bytes")
	}
	b, err := json.Marshal(env)
	if err != nil {
		return err
	}
	// SNS message size limit is 256 KB. Reject early to avoid persisting records
	// that will always fail at publish time and burn outbox attempt budget.
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
	})
}
