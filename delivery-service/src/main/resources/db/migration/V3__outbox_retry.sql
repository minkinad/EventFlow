ALTER TABLE delivery_outbox ADD COLUMN next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now();
CREATE INDEX delivery_outbox_due_idx ON delivery_outbox (next_attempt_at, created_at) WHERE published_at IS NULL;
