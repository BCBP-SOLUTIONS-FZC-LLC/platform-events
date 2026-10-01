CREATE INDEX IF NOT EXISTS idx_outbox_dead_letters_failed_at
    ON outbox_dead_letters (failed_at DESC);
DROP INDEX IF EXISTS idx_outbox_dead_letters_failed_at_id;
