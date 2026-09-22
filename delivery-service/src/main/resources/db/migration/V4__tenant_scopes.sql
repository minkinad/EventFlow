ALTER TABLE delivery_message ADD COLUMN tenant_id VARCHAR(120) GENERATED ALWAYS AS (raw_message->>'tenantId') STORED;
ALTER TABLE delivery_message ADD COLUMN producer_id VARCHAR(200) GENERATED ALWAYS AS (raw_message->>'producerId') STORED;
ALTER TABLE delivery_dead_letter ADD COLUMN tenant_id VARCHAR(120) GENERATED ALWAYS AS (original_message->>'tenantId') STORED;
ALTER TABLE operational_event ADD COLUMN tenant_id VARCHAR(120);
ALTER TABLE operational_event ADD COLUMN producer_id VARCHAR(200);
CREATE INDEX delivery_message_tenant_idx ON delivery_message(tenant_id, producer_id, event_id);
CREATE INDEX delivery_dlq_tenant_idx ON delivery_dead_letter(tenant_id, failed_at DESC);
