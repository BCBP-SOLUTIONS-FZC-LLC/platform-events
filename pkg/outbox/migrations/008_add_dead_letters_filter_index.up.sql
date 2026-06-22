-- Support ListDeadLetters / ReprocessDeadLettersWith / DiscardDeadLetters filter
-- queries on (event_type, tenant_id) without a full-table scan.
CREATE INDEX IF NOT EXISTS idx_outbox_dead_letters_event_type_tenant_id
    ON outbox_dead_letters (event_type, tenant_id);
