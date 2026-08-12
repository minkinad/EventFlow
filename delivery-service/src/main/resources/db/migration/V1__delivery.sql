CREATE TABLE delivery_message (
    command_id UUID PRIMARY KEY,
    event_id UUID NOT NULL,
    raw_message JSONB NOT NULL,
    received_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX delivery_message_event_idx ON delivery_message (event_id);

CREATE TABLE delivery_job (
    id UUID PRIMARY KEY,
    command_id UUID NOT NULL REFERENCES delivery_message(command_id),
    event_id UUID NOT NULL,
    target_type VARCHAR(40) NOT NULL,
    destination VARCHAR(1000) NOT NULL,
    target_options JSONB NOT NULL DEFAULT '{}'::jsonb,
    status VARCHAR(32) NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL,
    lease_owner VARCHAR(100),
    lease_until TIMESTAMPTZ,
    last_error VARCHAR(2000),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    delivered_at TIMESTAMPTZ,
    UNIQUE (command_id, target_type, destination),
    CONSTRAINT delivery_job_status CHECK
        (status IN ('PENDING', 'DELIVERING', 'RETRY', 'SUCCEEDED', 'DEAD'))
);

CREATE INDEX delivery_job_ready_idx
    ON delivery_job (next_attempt_at, created_at) WHERE status IN ('PENDING', 'RETRY');
CREATE INDEX delivery_job_event_idx ON delivery_job (event_id);

CREATE TABLE delivery_dead_letter (
    id UUID PRIMARY KEY,
    job_id UUID REFERENCES delivery_job(id),
    event_id UUID NOT NULL,
    target_type VARCHAR(40) NOT NULL,
    destination VARCHAR(1000) NOT NULL,
    failure_message VARCHAR(2000),
    original_message JSONB NOT NULL,
    status VARCHAR(32) NOT NULL,
    failed_at TIMESTAMPTZ NOT NULL,
    replayed_at TIMESTAMPTZ,
    CONSTRAINT delivery_dlq_status CHECK (status IN ('OPEN', 'REPLAYED', 'RESOLVED'))
);

CREATE INDEX delivery_dlq_open_idx ON delivery_dead_letter (failed_at) WHERE status='OPEN';

CREATE TABLE delivery_outbox (
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

CREATE INDEX delivery_outbox_unpublished_idx
    ON delivery_outbox (created_at) WHERE published_at IS NULL;

CREATE TABLE operational_event (
    event_id UUID NOT NULL,
    destination VARCHAR(255) NOT NULL,
    event_type VARCHAR(120) NOT NULL,
    pipeline VARCHAR(120) NOT NULL,
    payload JSONB NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL,
    delivered_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (event_id, destination)
);
