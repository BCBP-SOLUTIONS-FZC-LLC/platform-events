-- Supports the oldest-pending-event age gauge: MIN(created_at) over unpublished
-- rows becomes a single index probe instead of a scan of the backlog.
--
-- NOTE: CREATE INDEX without CONCURRENTLY is intentional — platform-pgcommon's
-- migrate.Runner runs each file in a transaction. On a large existing outbox,
-- create it CONCURRENTLY by hand first; IF NOT EXISTS then makes this a no-op:
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_outbox_events_unpublished_created
--   ON outbox_events (created_at) WHERE published_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_outbox_events_unpublished_created
    ON outbox_events (created_at)
    WHERE published_at IS NULL;
