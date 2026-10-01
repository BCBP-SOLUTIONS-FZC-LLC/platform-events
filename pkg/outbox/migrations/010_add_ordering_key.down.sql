-- Release ordered records still waiting behind their key's head first: the
-- pre-010 claim query (scheduled_at <= NOW()) never selects a row at
-- 'infinity' and nothing would promote it, so it would never be published.
-- Rolling back gives up per-key ordering for these records.
UPDATE outbox_events SET scheduled_at = NOW()
    WHERE published_at IS NULL AND scheduled_at = 'infinity';
DROP INDEX IF EXISTS idx_outbox_events_ordering;
ALTER TABLE outbox_dead_letters DROP COLUMN IF EXISTS ordering_key;
ALTER TABLE outbox_events DROP COLUMN IF EXISTS ordering_seq;
ALTER TABLE outbox_events DROP COLUMN IF EXISTS ordering_key;
DROP SEQUENCE IF EXISTS outbox_events_ordering_seq;
