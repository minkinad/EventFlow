CREATE TABLE schema_definition (
    name VARCHAR(120) PRIMARY KEY,
    definition JSONB NOT NULL,
    active BOOLEAN NOT NULL DEFAULT true,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE pipeline_definition (
    id UUID PRIMARY KEY,
    name VARCHAR(120) NOT NULL,
    version INTEGER NOT NULL CHECK (version > 0),
    event_type VARCHAR(120) NOT NULL,
    definition JSONB NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL,
    UNIQUE (name, version)
);

CREATE UNIQUE INDEX pipeline_one_active_event_type_idx
    ON pipeline_definition (event_type) WHERE enabled = true;
CREATE INDEX pipeline_event_type_idx
    ON pipeline_definition (event_type, version DESC) WHERE enabled = true;

CREATE TABLE processing_record (
    event_id UUID PRIMARY KEY,
    content_hash CHAR(64) NOT NULL,
    raw_message JSONB NOT NULL,
    status VARCHAR(32) NOT NULL,
    attempt INTEGER NOT NULL,
    lease_until TIMESTAMPTZ,
    pipeline_name VARCHAR(120),
    pipeline_version INTEGER,
    failure_code VARCHAR(120),
    failure_message VARCHAR(2000),
    first_seen_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT processing_status CHECK
        (status IN ('PROCESSING', 'RETRYABLE', 'SUCCEEDED', 'FAILED', 'REPLAY_REQUESTED'))
);

CREATE INDEX processing_record_status_idx ON processing_record (status, updated_at);

CREATE TABLE processing_dead_letter (
    id UUID PRIMARY KEY,
    event_id UUID NOT NULL,
    stage VARCHAR(80) NOT NULL,
    failure_code VARCHAR(120) NOT NULL,
    failure_message VARCHAR(2000),
    attempt INTEGER NOT NULL,
    original_message JSONB NOT NULL,
    status VARCHAR(32) NOT NULL,
    failed_at TIMESTAMPTZ NOT NULL,
    replayed_at TIMESTAMPTZ,
    CONSTRAINT processing_dlq_status CHECK
        (status IN ('OPEN', 'REPLAY_REQUESTED', 'REPLAY_FAILED', 'RESOLVED'))
);

CREATE INDEX processing_dlq_open_idx
    ON processing_dead_letter (failed_at) WHERE status IN ('OPEN', 'REPLAY_FAILED');

CREATE TABLE processing_outbox (
    id UUID PRIMARY KEY,
    aggregate_id UUID NOT NULL,
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

CREATE INDEX processing_outbox_unpublished_idx
    ON processing_outbox (created_at) WHERE published_at IS NULL;
