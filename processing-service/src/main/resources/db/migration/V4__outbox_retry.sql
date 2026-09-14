ALTER TABLE processing_outbox ADD COLUMN next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now();
CREATE INDEX processing_outbox_due_idx ON processing_outbox (next_attempt_at, created_at) WHERE published_at IS NULL;
