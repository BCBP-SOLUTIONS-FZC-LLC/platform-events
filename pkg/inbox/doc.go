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
//  3. Either wrap the handler — events.NewSQSConsumer(cfg, inbox.Handler(store, h), ...)
//     — or, when its effects are Postgres writes, run them through
//     [Store.Process] for exactly-once writes.
//  4. Periodically call [Store.Prune] with the retention you need — at least
//     as long as a duplicate can still arrive (the queue's retention).
//
// # Semantics
//
// [Handler] checks the ledger first (a duplicate is acknowledged without
// calling the handler, and counted in platform_duplicate_messages_total /
// events_inbox_duplicates_total), calls the handler, and records the ID ONLY
// after the handler returns nil. The check, the handler and the record are
// separate transactions, so a crash mid-handler, a failed record, or two
// copies handled concurrently can each run the handler again — it must still
// be idempotent.
//
// [Store.Process] closes those gaps for Postgres writes: the ID is claimed in
// the handler's own transaction, so the claim and the writes commit or roll
// back together and concurrent copies serialise on the claim.
//
// Neither records a message the handler dead-lettered (SendToDLQ, then nil),
// so redriving the DLQ after a fix processes it. The envelope ID must be a UUID
// (platform-events producers emit UUID v7); anything else is returned as an
// error so the message reaches the DLQ instead of being processed unguarded.
package inbox
