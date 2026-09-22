-- Legacy records deliberately remain unowned; never infer ownership from old request metadata.
ALTER TABLE ingested_event ADD COLUMN tenant_id VARCHAR(120);
ALTER TABLE ingested_event ADD COLUMN producer_id VARCHAR(200);
CREATE INDEX ingestion_tenant_owner_idx ON ingested_event(tenant_id, producer_id, received_at DESC);
