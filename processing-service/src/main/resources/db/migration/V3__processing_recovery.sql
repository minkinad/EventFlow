ALTER TABLE processing_record ADD COLUMN lease_token UUID;
ALTER TABLE processing_record ADD COLUMN next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now();
-- Existing workers must be stopped for the upgrade. Expire pre-fencing leases.
UPDATE processing_record SET lease_until = now() WHERE status = 'PROCESSING';
CREATE INDEX processing_lease_expiry_idx ON processing_record (lease_until)
    WHERE status = 'PROCESSING';
