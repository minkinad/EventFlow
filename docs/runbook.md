# Local and operational runbook

## Local start

```bash
docker compose up --build -d
docker compose ps
curl -fsS http://localhost:8080/actuator/health
curl -fsS http://localhost:8081/actuator/health
curl -fsS http://localhost:8082/actuator/health
```

Submit `docs/examples/order-created.json`, then inspect delivery state:

```bash
docker compose exec postgres psql -U eventflow -d eventflow_delivery \
  -c "select event_id, destination, status, attempt, last_error from delivery_job order by created_at desc;"

curl -u eventflow:eventflow \
  'http://localhost:8123/?query=SELECT%20event_id,event_type,pipeline%20FROM%20eventflow.events%20LIMIT%2010'
```

Resetting volumes destroys local data and is intentionally not part of normal startup. If a clean-room test is explicitly needed, stop the stack and use `docker compose down -v` after verifying that only disposable local volumes are in scope.

## Incident triage order

1. Confirm client impact and stop unsafe bulk replay/deploy activity.
2. Check health, request errors, oldest outbox age, Kafka lag, oldest delivery job, and DLQ rate.
3. Correlate one `eventId` through ingestion row, Kafka topic/offset, processing record, command, and delivery jobs.
4. Determine whether the bottleneck is capacity, dependency availability, poison data, or configuration.
5. Prefer restoring forward processing before replaying backlog.
6. Replay a small canary set, observe, then ramp gradually.

## Common symptoms

| Symptom | Check | Typical action |
|---|---|---|
| API returns `409` | compare canonical event content for the ID | fix producer ID reuse; never overwrite history |
| Ingestion outbox age grows | Kafka reachability/acks, DB locks | restore Kafka; scale relay only after diagnosis |
| Processing lag grows with low CPU | active lease/retry logs, enrichment circuit | fix dependency or disable/roll back affected pipeline |
| Processing DLQ spike | group by `failure_code` and pipeline version | roll back bad configuration or fix producer/schema |
| One delivery target backs up | circuit state, HTTP status, target latency | isolate/rate-limit target; processing can continue |
| ClickHouse duplicates visible | query without latest-version aggregation | use `FINAL` for diagnosis; keep idempotent version strategy |
