-- Add DEFAULT NOW() to outbox_dead_letters.created_at so rows inserted
-- without an explicit created_at (e.g. via direct SQL tooling or future
-- schema changes) do not fail the NOT NULL constraint.
ALTER TABLE outbox_dead_letters ALTER COLUMN created_at SET DEFAULT NOW();
