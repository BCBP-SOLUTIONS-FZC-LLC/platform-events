-- ReprocessDeadLetters sorts by created_at (oldest-first) to replay events in
-- arrival order. Without this index every replay call does a full table scan.
CREATE INDEX IF NOT EXISTS idx_outbox_dead_letters_created_at
    ON outbox_dead_letters (created_at ASC);
