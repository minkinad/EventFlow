ALTER TABLE delivery_job ADD COLUMN lease_token UUID;
ALTER TABLE delivery_dead_letter ADD COLUMN failure_code VARCHAR(120) NOT NULL DEFAULT 'DELIVERY_FAILED';
ALTER TABLE delivery_dead_letter ADD COLUMN attempt INTEGER NOT NULL DEFAULT 0;
CREATE INDEX delivery_job_expired_lease_idx ON delivery_job (lease_until) WHERE status = 'DELIVERING';
-- Stop old workers before upgrading: they do not enforce fencing tokens.
UPDATE delivery_job SET lease_until = now() WHERE status = 'DELIVERING';
