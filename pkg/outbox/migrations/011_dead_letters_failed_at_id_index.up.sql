-- Dead-letter list / replay / discard order by (failed_at, id) — a total order,
-- so a list followed by a replay or discard selects the same rows. Replace the
-- failed_at-only index (004) with one that serves that sort and the
-- failed_at < $n filter (DLQFilter.FailedBefore, retention).
CREATE INDEX IF NOT EXISTS idx_outbox_dead_letters_failed_at_id
    ON outbox_dead_letters (failed_at, id);
DROP INDEX IF EXISTS idx_outbox_dead_letters_failed_at;
