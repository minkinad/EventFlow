-- Additive, idempotent upgrade for an existing ClickHouse volume. Old rows stay unowned.
ALTER TABLE eventflow.events ADD COLUMN IF NOT EXISTS tenant_id String DEFAULT '';
ALTER TABLE eventflow.events ADD COLUMN IF NOT EXISTS producer_id String DEFAULT '';
