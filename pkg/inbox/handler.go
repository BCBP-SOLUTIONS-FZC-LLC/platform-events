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
//   - otherwise next runs, and the ID is recorded only when next returns nil.
//
// A ledger read or write error is returned, so SQS redelivers.
func Handler(ledger Ledger, next events.Handler) events.Handler {
	return func(ctx context.Context, env events.Envelope[json.RawMessage]) error {
		id, err := uuid.Parse(env.ID)
		if err != nil || id == uuid.Nil {
			return fmt.Errorf("inbox: envelope has invalid or missing id %q", env.ID)
		}
		seen, err := ledger.IsProcessed(ctx, id)
		if err != nil {
			return err
		}
		if seen {
			metrics.RecordInboxDuplicate(ledger.Consumer())
			queueURL := ""
			if src, ok := port.SourceMessageFromContext(ctx); ok {
				queueURL = src.QueueURL
			}
			metrics.IncDuplicate(queueURL, env.Type)
			return nil
		}
		if err := next(ctx, env); err != nil {
			return err
		}
		return ledger.MarkProcessed(ctx, id)
	}
}
