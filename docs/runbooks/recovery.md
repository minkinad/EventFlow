# Queue and lease recovery

## Symptoms and impact

Outbox age grows, deliveries stay DELIVERING, or Kafka lag rises. Accepted events
remain in service-owned PostgreSQL; transport retries must not advance without a
durable outcome. A failing sink can leave a backlog or a terminal DLQ entry.

## Diagnosis

Inspect `/actuator/health/readiness`, `eventflow_outbox_pending`,
`eventflow_outbox_oldest_age_seconds`, delivery status and application logs. Liveness
is independent of destination availability. Query only the owning database:

```sql
-- Ingestion database
SELECT count(*), min(created_at) FROM ingestion_outbox WHERE published_at IS NULL;
-- Delivery database
SELECT status, count(*), min(next_attempt_at) FROM delivery_job GROUP BY status;
SELECT id, attempt, lease_until FROM delivery_job
WHERE status='DELIVERING' AND lease_until < now();
-- Processing database
SELECT status, failure_code, count(*) FROM processing_record GROUP BY status, failure_code;
```

## Mitigation and recovery

Restore PostgreSQL/Kafka/sink connectivity before replaying. Expired leases are
reclaimed automatically. Never clear tokens, mark jobs successful or advance Kafka
offsets manually. Stop pre-fencing binaries before the migration upgrade.
HTTP private-address failures require correcting the destination; do not widen the
allow-list to internal/metadata endpoints. A legitimate remote outage uses durable
backoff; HTTP 408/425/429/5xx retry, other statuses including 409 are terminal by default.

Single-record DLQ replay keeps eventId. Reconcile uncertain HTTP outcomes with the
receiver before replaying; an idempotency header is useful only when honored.
Do not delete inbox/job identity rows to force a retry.

## Verification and escalation

Run the smoke test, confirm oldest queue age decreases, and inspect an accepted
ID through the three status APIs. Compare PostgreSQL business-row count and
ClickHouse FINAL for that ID. A RESOLVED processing DLQ means processing succeeded,
not that all downstream deliveries completed. Escalate persistent growth, exhausted
attempts, unknown receiver outcomes or repeated lease loss to the service owner.

Do not run unbounded bulk replay. Retention remains manual pending an agreed
retry/replay horizon; database backups must include identity records and outboxes.
