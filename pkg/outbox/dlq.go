package outbox

import "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/internal/core/domain"

// DLQFilter selects records in outbox_dead_letters for [Runner.ListDeadLetters],
// [Runner.ReprocessDeadLettersWith], and [Runner.DiscardDeadLetters].
// All fields are optional — a zero-value filter matches every row in the table.
//
// Example — replay only failed IAM events for a specific tenant:
//
//	filter := outbox.DLQFilter{
//	    EventType: "iam.user.created",
//	    TenantID:  "acme",
//	}
//	n, err := runner.ReprocessDeadLettersWith(ctx, filter, 100)
type DLQFilter = domain.DLQFilter

// DeadLetterRecord is a row from the outbox_dead_letters table — an event that
// exhausted MaxAttempts without a successful SNS publish. Inspect LastError and
// Attempts to determine root cause before calling ReprocessDeadLettersWith or
// DiscardDeadLetters.
type DeadLetterRecord = domain.DeadLetterRecord
