-- Replace the id-only partial index with a composite (scheduled_at, id) index so
-- ClaimBatch (WHERE published_at IS NULL AND scheduled_at <= NOW()
-- ORDER BY scheduled_at, id) can use the index for both the filter and the sort.
--
-- NOTE: CREATE INDEX without CONCURRENTLY is intentional. The platform-pgcommon
-- migrate runner executes migrations inside a transaction, and CONCURRENTLY cannot
-- be used inside a transaction block. For large tables in production, run this
-- migration manually with CONCURRENTLY outside a transaction before upgrading.
DROP INDEX IF EXISTS idx_outbox_events_pending;
CREATE INDEX IF NOT EXISTS idx_outbox_events_pending
    ON outbox_events (scheduled_at, id)
    WHERE published_at IS NULL;
