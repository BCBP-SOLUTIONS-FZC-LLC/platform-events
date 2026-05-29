// Package outbox provides the transactional outbox pattern for guaranteed at-least-once
// event delivery without dual-write risk.
//
// # How it works
//
// Instead of calling [events.Publisher.Publish] directly inside a business transaction,
// call [Enqueue] to write the event to the outbox_events table within the same transaction.
// If the transaction rolls back, the event is never committed. The [Runner] polls the
// table asynchronously and delivers events via the configured [events.Publisher].
//
// # Wiring
//
//  1. Apply the outbox schema once at startup with [ApplySchema].
//  2. Construct a [Runner] with a [pgcommon.Pool] and an [events.Publisher].
//  3. Call [Runner.Start] in a goroutine; it blocks until the context is cancelled.
//  4. Inside business transactions, call [Enqueue] alongside domain writes.
//
// Example:
//
//	pgcommon.RunInTx(ctx, pool, pgx.TxOptions{}, func(ctx context.Context, tx pgx.Tx) error {
//	    if err := repo.SaveUser(ctx, tx, user); err != nil {
//	        return err
//	    }
//	    return outbox.Enqueue(ctx, tx, envelope)
//	})
//
// # Delivery semantics
//
// The runner provides at-least-once delivery. [Envelope.ID] (UUID v7) is forwarded as
// the SNS MessageDeduplicationID on FIFO topics and as a message attribute on standard
// topics — consumers should use it as their idempotency key.
//
// Records that exhaust MaxAttempts are moved to the outbox_dead_letters table, which is
// queryable and retryable from standard SQL tooling.
package outbox
