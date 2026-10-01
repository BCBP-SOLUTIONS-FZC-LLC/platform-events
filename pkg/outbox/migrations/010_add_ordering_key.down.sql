DROP INDEX IF EXISTS idx_outbox_events_ordering;
ALTER TABLE outbox_dead_letters DROP COLUMN IF EXISTS ordering_key;
ALTER TABLE outbox_events DROP COLUMN IF EXISTS ordering_key;
