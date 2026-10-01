-- Replace the id-only partial index with a composite (scheduled_at, id) index so
-- ClaimBatch (WHERE published_at IS NULL AND scheduled_at <= NOW()
-- ORDER BY scheduled_at, id) can use the index for both the filter and the sort.
--
-- NOTE: CREATE INDEX without CONCURRENTLY is intentional. The platform-pgcommon
-- migrate runner executes migrations inside a transaction, and CONCURRENTLY cannot
-- be used inside a transaction block. For large tables in production, run this
-- migration manually with CONCURRENTLY outside a transaction before upgrading.
--
-- The rebuild runs only when the index does not already have the target shape:
-- DROP INDEX takes an ACCESS EXCLUSIVE lock on outbox_events for the whole
-- CREATE INDEX scan, and this migration is re-run once by services that move
-- their outbox tracking to outbox_migrations (see CHANGELOG 1.6.0) — or that
-- pre-created the index CONCURRENTLY as advised above.
DO $$
BEGIN
    IF coalesce(pg_get_indexdef(to_regclass('idx_outbox_events_pending')), '')
       NOT LIKE '%(scheduled_at, id) WHERE (published_at IS NULL)' THEN
        DROP INDEX IF EXISTS idx_outbox_events_pending;
        CREATE INDEX idx_outbox_events_pending
            ON outbox_events (scheduled_at, id)
            WHERE published_at IS NULL;
    END IF;
END
$$;
