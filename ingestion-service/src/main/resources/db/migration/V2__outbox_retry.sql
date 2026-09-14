ALTER TABLE ingestion_outbox ADD COLUMN next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now();
CREATE INDEX ingestion_outbox_due_idx ON ingestion_outbox (next_attempt_at, created_at) WHERE published_at IS NULL;
