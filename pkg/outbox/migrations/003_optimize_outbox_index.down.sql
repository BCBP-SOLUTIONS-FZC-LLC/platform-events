DROP INDEX IF EXISTS idx_outbox_events_pending;
CREATE INDEX IF NOT EXISTS idx_outbox_events_pending
    ON outbox_events (id)
    WHERE published_at IS NULL;
