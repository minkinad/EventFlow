CREATE DATABASE IF NOT EXISTS eventflow;

CREATE TABLE IF NOT EXISTS eventflow.events
(
    event_id UUID,
    event_type LowCardinality(String),
    pipeline LowCardinality(String),
    destination LowCardinality(String),
    payload String,
    processed_at DateTime64(3, 'UTC'),
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(processed_at)
ORDER BY (event_type, event_id, destination)
TTL toDateTime(processed_at) + INTERVAL 365 DAY DELETE;

-- Idempotent additive upgrade for existing local tables. Old rows remain unowned.
ALTER TABLE eventflow.events ADD COLUMN IF NOT EXISTS tenant_id String DEFAULT '';
ALTER TABLE eventflow.events ADD COLUMN IF NOT EXISTS producer_id String DEFAULT '';
