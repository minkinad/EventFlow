CREATE TABLE ingested_event (
    event_id UUID PRIMARY KEY,
    event_type VARCHAR(120) NOT NULL,
    source VARCHAR(120) NOT NULL,
    schema_version INTEGER NOT NULL CHECK (schema_version > 0),
    occurred_at TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    content_hash CHAR(64) NOT NULL,
    status VARCHAR(32) NOT NULL,
    CONSTRAINT ingested_event_status CHECK (status IN ('ACCEPTED', 'PUBLISHED'))
);

CREATE INDEX ingested_event_received_at_idx ON ingested_event (received_at DESC);
CREATE INDEX ingested_event_type_idx ON ingested_event (event_type, received_at DESC);

CREATE TABLE ingestion_outbox (
    id UUID PRIMARY KEY,
    aggregate_id UUID NOT NULL REFERENCES ingested_event(event_id),
    topic VARCHAR(255) NOT NULL,
    message_key VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    published_at TIMESTAMPTZ,
    claimed_by VARCHAR(100),
    claimed_until TIMESTAMPTZ,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error VARCHAR(2000)
);

CREATE INDEX ingestion_outbox_unpublished_idx
    ON ingestion_outbox (created_at)
    WHERE published_at IS NULL;
