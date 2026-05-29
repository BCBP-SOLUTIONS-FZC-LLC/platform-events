-- Replace the id-only partial index with a composite (scheduled_at, id) index so
-- the ClaimBatch query (WHERE published_at IS NULL AND scheduled_at <= NOW()
-- ORDER BY id) can satisfy both the filter and the sort without a post-index heap
-- scan on scheduled_at.
DROP INDEX IF EXISTS idx_outbox_events_pending;
CREATE INDEX IF NOT EXISTS idx_outbox_events_pending
    ON outbox_events (scheduled_at, id)
    WHERE published_at IS NULL;
