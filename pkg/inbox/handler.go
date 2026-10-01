package inbox

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/adapter/outbound/metrics"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/events"
)

// Ledger is what Handler needs from a Store (satisfied by *Store; a fake in
// tests).
type Ledger interface {
	IsProcessed(ctx context.Context, eventID uuid.UUID) (bool, error)
	MarkProcessed(ctx context.Context, eventID uuid.UUID) error
	Consumer() string
}

// Handler wraps next with deduplication against ledger:
//
//   - the envelope ID must parse as a UUID, else an error (→ retry / DLQ);
//   - an ID already recorded is acknowledged without calling next, and
//     counted in platform_duplicate_messages_total{queue,event_type} (and the
//     legacy events_inbox_duplicates_total{consumer});
//   - otherwise next runs, and the ID is recorded only when next returns nil
//     without having dead-lettered the message (SendToDLQ), so a redriven
//     message is processed.
//
// The check, the handler and the record are separate transactions: two
// copies of a message handled concurrently (WithConcurrency > 1) can both
// run, and a handler whose writes committed is rerun if the record then
// fails. When the handler's effects are Postgres writes, use [Store.Process]
// instead — it claims the ID in the handler's own transaction, so the writes
// happen exactly once.
//
// A ledger read or write error is returned, so SQS redelivers.
func Handler(ledger Ledger, next events.Handler) events.Handler {
	return func(ctx context.Context, env events.Envelope[json.RawMessage]) error {
		id, err := parseEventID(env.ID)
		if err != nil {
			return err
		}
		seen, err := ledger.IsProcessed(ctx, id)
		if err != nil {
			return err
		}
		if seen {
			recordDuplicate(ctx, ledger.Consumer(), env.Type)
			return nil
		}
		if err := next(ctx, env); err != nil {
			return err
		}
		// A handler that dead-lettered the message (SendToDLQ, then nil) did
		// not process it: leave it unrecorded so a redrive after the fix runs.
		if port.DLQAttributionFromContext(ctx).Recorded() {
			return nil
		}
		return ledger.MarkProcessed(ctx, id)
	}
}

func recordDuplicate(ctx context.Context, consumer, eventType string) {
	metrics.RecordInboxDuplicate(consumer)
	queueURL := ""
	if src, ok := port.SourceMessageFromContext(ctx); ok {
		queueURL = src.QueueURL
	}
	metrics.IncDuplicate(queueURL, eventType)
}

func parseEventID(raw string) (uuid.UUID, error) {
	id, err := uuid.Parse(raw)
	if err != nil || id == uuid.Nil {
		return uuid.Nil, fmt.Errorf("inbox: envelope has invalid or missing id %q", raw)
	}
	return id, nil
}
