package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

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
//	pgcommon.RunInTx(ctx, pool, pgx.TxOptions{}, func(ctx context.Context, tx pgx.Tx) error {
//	    _ = repo.Save(ctx, tx, record)
//	    return outbox.Enqueue(ctx, tx, envelope)
//	})
func Enqueue(ctx context.Context, tx pgx.Tx, env events.Envelope[json.RawMessage]) error {
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
