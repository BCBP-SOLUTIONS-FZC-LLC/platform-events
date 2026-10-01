-- Per-key ordering for records enqueued with outbox.EnqueueOrdered.
--
-- ordering_seq is drawn from a sequence when the row is INSERTed (not at
-- transaction start, as NOW() would be), so under the aggregate row lock the
-- documented EnqueueOrdered contract requires, sequence order is commit order.
-- A keyed record enqueued behind an unpublished record of its key waits with
-- scheduled_at = 'infinity' and is promoted when the key's head is published
-- or dead-lettered — claims never scan a key's waiting backlog.
--
-- All statements are metadata-only on existing rows (nullable columns, no
-- default); CREATE INDEX without CONCURRENTLY is intentional (see 009).
CREATE SEQUENCE IF NOT EXISTS outbox_events_ordering_seq;
ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS ordering_key TEXT;
ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS ordering_seq BIGINT;
ALTER TABLE outbox_dead_letters ADD COLUMN IF NOT EXISTS ordering_key TEXT;
CREATE INDEX IF NOT EXISTS idx_outbox_events_ordering
    ON outbox_events (ordering_key, ordering_seq)
    WHERE published_at IS NULL AND ordering_key IS NOT NULL;
