// Package inbox provides consumer-side deduplication — the counterpart of
// package outbox — for at-least-once SQS delivery.
//
// SQS redelivers a message whenever its handler fails, its visibility
// timeout lapses, or it is redriven from a DLQ. Handlers must be idempotent;
// inbox makes that cheap by recording every successfully handled envelope ID
// per consumer in a processed_events table and skipping redeliveries.
//
// # Wiring
//
//  1. Apply the inbox schema once at startup with [ApplySchema] (own tracking
//     table [MigrationsTable], so it composes with domain and outbox
//     migrations on the same database).
//  2. Build a [Store] with a [pgcommon.Pool] and a stable consumer name.
//  3. Wrap the handler: events.NewSQSConsumer(cfg, inbox.Handler(store, h), ...).
//  4. Periodically call [Store.Prune] with the retention you need — at least
//     the DLQ's message retention, so a redriven message is still deduped.
//
// # Semantics
//
// [Handler] checks the ledger first (a duplicate is acknowledged without
// calling the handler, and counted in events_inbox_duplicates_total), calls
// the handler, and records the ID ONLY after the handler returns nil — a
// crash mid-handler therefore reprocesses on redelivery, which is why the
// handler itself must still be idempotent. The envelope ID must be a UUID
// (platform-events producers emit UUID v7); anything else is returned as an
// error so the message reaches the DLQ instead of being processed unguarded.
package inbox
