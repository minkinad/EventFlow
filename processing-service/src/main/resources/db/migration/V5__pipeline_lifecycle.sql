ALTER TABLE pipeline_definition ADD COLUMN state VARCHAR(20) NOT NULL DEFAULT 'DRAFT';
ALTER TABLE pipeline_definition ADD COLUMN revision BIGINT NOT NULL DEFAULT 0;
ALTER TABLE pipeline_definition ADD COLUMN updated_at TIMESTAMPTZ NOT NULL DEFAULT now();
UPDATE pipeline_definition SET state = CASE WHEN enabled THEN 'ACTIVE' ELSE 'DRAFT' END;
ALTER TABLE pipeline_definition ADD CONSTRAINT pipeline_state CHECK
    (state IN ('DRAFT', 'VALIDATED', 'ACTIVE', 'SUPERSEDED', 'DISABLED'));
ALTER TABLE pipeline_definition ADD CONSTRAINT pipeline_active_state CHECK (enabled = (state = 'ACTIVE'));

CREATE TABLE audit_log (
    id UUID PRIMARY KEY,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    tenant_id VARCHAR(120),
    actor VARCHAR(200) NOT NULL,
    action VARCHAR(80) NOT NULL,
    entity_type VARCHAR(80) NOT NULL,
    entity_id VARCHAR(200) NOT NULL,
    before_state JSONB,
    after_state JSONB,
    reason VARCHAR(1000) NOT NULL,
    trace_id VARCHAR(100)
);
CREATE INDEX audit_log_entity_idx ON audit_log(entity_type, entity_id, occurred_at DESC);
CREATE FUNCTION reject_audit_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'Audit history is append-only'; END $$;
CREATE TRIGGER audit_immutable BEFORE UPDATE OR DELETE ON audit_log
    FOR EACH ROW EXECUTE FUNCTION reject_audit_mutation();
