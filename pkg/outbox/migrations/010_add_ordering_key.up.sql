-- Strict per-key ordering (outbox.Config.StrictOrdering): records enqueued
-- with outbox.EnqueueOrdered carry an ordering key; with strict ordering the
-- runner publishes a key's records one at a time, oldest first. The partial
-- index serves the "is there an earlier unpublished record with this key?"
-- probe of the claim query and the blocked-events gauge.
--
-- NOTE: CREATE INDEX without CONCURRENTLY is intentional (see 009).
ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS ordering_key TEXT;
ALTER TABLE outbox_dead_letters ADD COLUMN IF NOT EXISTS ordering_key TEXT;
CREATE INDEX IF NOT EXISTS idx_outbox_events_ordering
    ON outbox_events (ordering_key, created_at, id)
    WHERE published_at IS NULL AND ordering_key IS NOT NULL;
