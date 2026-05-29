CREATE TABLE IF NOT EXISTS outbox_dead_letters (
    id         UUID        PRIMARY KEY,
    event_type TEXT        NOT NULL,
    payload    JSONB       NOT NULL,
    tenant_id  TEXT        NOT NULL DEFAULT '',
    trace_id   TEXT        NOT NULL DEFAULT '',
    attempts   INT         NOT NULL,
    last_error TEXT        NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL,
    failed_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
