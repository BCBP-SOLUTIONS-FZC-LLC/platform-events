-- Support operational queries on outbox_dead_letters (e.g. "show failures in
-- the last hour for tenant X") without a full-table scan.
CREATE INDEX IF NOT EXISTS idx_outbox_dead_letters_failed_at
    ON outbox_dead_letters (failed_at DESC);
