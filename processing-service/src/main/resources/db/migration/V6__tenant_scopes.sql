ALTER TABLE pipeline_definition ADD COLUMN tenant_id VARCHAR(120);
ALTER TABLE schema_definition ADD COLUMN tenant_id VARCHAR(120);
-- Only the built-in demo seed has known ownership. Other existing configuration stays inaccessible.
UPDATE pipeline_definition SET tenant_id='demo' WHERE id='10000000-0000-0000-0000-000000000001';
UPDATE schema_definition SET tenant_id='demo' WHERE name='order-v1';
ALTER TABLE schema_definition DROP CONSTRAINT schema_definition_pkey;
CREATE UNIQUE INDEX schema_tenant_name_idx ON schema_definition(tenant_id, name) NULLS NOT DISTINCT;
ALTER TABLE pipeline_definition DROP CONSTRAINT pipeline_definition_name_version_key;
CREATE UNIQUE INDEX pipeline_tenant_version_idx ON pipeline_definition(tenant_id, name, version) NULLS NOT DISTINCT;
DROP INDEX pipeline_one_active_event_type_idx;
DROP INDEX pipeline_event_type_idx;
CREATE UNIQUE INDEX pipeline_one_active_event_type_idx ON pipeline_definition(tenant_id, event_type) NULLS NOT DISTINCT WHERE enabled;
CREATE INDEX pipeline_event_type_idx ON pipeline_definition(tenant_id, event_type, version DESC) WHERE enabled;
ALTER TABLE processing_record ADD COLUMN tenant_id VARCHAR(120) GENERATED ALWAYS AS (raw_message->>'tenantId') STORED;
ALTER TABLE processing_record ADD COLUMN producer_id VARCHAR(200) GENERATED ALWAYS AS (raw_message->>'producerId') STORED;
ALTER TABLE processing_dead_letter ADD COLUMN tenant_id VARCHAR(120) GENERATED ALWAYS AS (original_message->>'tenantId') STORED;
CREATE INDEX processing_tenant_owner_idx ON processing_record(tenant_id, producer_id, first_seen_at DESC);
CREATE INDEX processing_dlq_tenant_idx ON processing_dead_letter(tenant_id, failed_at DESC);
