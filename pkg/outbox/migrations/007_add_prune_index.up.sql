-- Migration 007: add index to support efficient pruning of published records.
--
-- Published records are never deleted automatically; consuming services must call
-- Runner.PrunePublished periodically to prevent unbounded table growth.
-- This partial index makes the prune DELETE efficient by narrowing the scan to
-- rows where published_at IS NOT NULL.
--
-- NOTE: CREATE INDEX without CONCURRENTLY is intentional here — the partial index
-- only covers published rows, so an empty table at first install is the common case
-- and the lock duration is negligible. For services upgrading with a large number
-- of already-published rows, run this migration manually with CONCURRENTLY outside
-- a transaction before upgrading:
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_outbox_events_published_at
--   ON outbox_events (published_at) WHERE published_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_outbox_events_published_at
    ON outbox_events (published_at)
    WHERE published_at IS NOT NULL;
