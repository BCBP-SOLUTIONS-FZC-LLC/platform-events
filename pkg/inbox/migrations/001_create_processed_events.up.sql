-- Idempotency ledger for SQS consumers (the "inbox" side of at-least-once
-- delivery). One row per (event_id, consumer) successfully handled: the
-- composite key lets several consumer types share one table while deduping
-- independently. IF NOT EXISTS keeps adoption safe for a service whose own
-- migrations already created an identically-shaped table.
CREATE TABLE IF NOT EXISTS processed_events (
    event_id     uuid        NOT NULL,
    consumer     text        NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT processed_events_pkey PRIMARY KEY (event_id, consumer)
);

-- Backs retention pruning (Store.Prune).
CREATE INDEX IF NOT EXISTS idx_processed_events_processed_at ON processed_events (processed_at);
