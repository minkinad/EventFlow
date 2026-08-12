# Reliability model

## Invariants

1. An accepted event exists durably in PostgreSQL before the API returns `202`.
2. Every newly accepted event has exactly one ingestion outbox row in the same local transaction.
3. Published Kafka records may be duplicated; consumers must remain correct when they are.
4. A Kafka offset is acknowledged only after the consumer's durable state is committed.
5. Every external delivery is represented by a durable job before network I/O starts.
6. Retries are bounded and observable; terminal failures are queryable and replayable.
7. Replay preserves the original `eventId` and original message. It is not a new business event.

## Failure matrix

| Failure point | Observable result | Recovery |
|---|---|---|
| API dies before DB commit | client gets timeout/error; no event | client retries same `eventId` |
| API dies after commit before response | event is durable; client sees timeout | retry returns duplicate `202` |
| Outbox relay dies before publish | unpublished row remains/lease expires | another poller claims it |
| Relay dies after publish before mark | Kafka receives duplicate | processing inbox removes effect |
| Processor dies before durable claim | Kafka offset is uncommitted | record is redelivered |
| Processor dies after claim | lease remains temporarily | error handler/redelivery waits; lease expires and is reclaimed |
| Processor commits output then dies before ack | delivery message may be republished | command/inbox dedup removes effect |
| Delivery consumer dies before jobs commit | offset uncommitted | command is redelivered |
| Worker dies before sink call | job lease expires | another worker retries |
| Worker dies after sink call before success mark | call repeats | sink upsert/idempotency key neutralizes duplicate |
| Redis unavailable | rate limiter degrades according to policy | PostgreSQL idempotency remains correct |
| Sink unavailable | only that target's jobs back up | exponential retry, circuit breaker, then DLQ |

## Retry taxonomy

Do not retry invalid JSON, schema violations, missing pipeline/schema, forbidden destinations, or idempotency conflicts. Retry timeouts, connection resets, HTTP 408/429/5xx, and temporary database/Kafka errors. A remote HTTP 4xx other than 408/409/425/429 is normally terminal.

There are three distinct retry layers:

- short in-call retry (milliseconds) handles transient network noise;
- durable delivery-job retry (seconds to minutes) handles dependency incidents;
- operator DLQ replay handles corrected data/configuration or long outages.

Combining all retries in a Kafka listener would block a partition and hide state from operators, so durable delivery retries are intentionally outside the listener.

Processing dependency failures are different: the record remains ordered within its Kafka partition, while a durable `RETRYABLE` processing state records the attempt and releases the lease. Redelivery reclaims that state; after the configured maximum attempt the event moves to processing DLQ. Terminal validation/configuration failures bypass retry.

## Backoff

Delivery uses exponential backoff capped at five minutes plus jitter. The retry timestamp is stored, so restarts do not reset the delay. After the configured attempt limit the job becomes `DEAD` and an immutable DLQ record is created.

## DLQ replay procedure

1. Classify and fix the cause; do not blindly replay a growing DLQ.
2. Select a bounded set by failure code, pipeline version, time range, and tenant.
3. Estimate downstream load and apply a replay rate limit.
4. Record operator identity and reason.
5. Replay with the same event ID.
6. Verify success rate, consumer lag, and sink health.
7. Mark the DLQ record resolved only after the expected downstream state is observed.

The current API supports single-record replay. Bulk replay, dry-run, RBAC, audit identity, and rate control are production roadmap items.

## Backup and disaster recovery

- PostgreSQL: continuous WAL archiving plus daily base backup; quarterly restore exercise.
- Kafka: replication across failure domains; retention is not a database backup.
- Pipeline/schema definitions: database backup plus reviewed export in source control for critical pipelines.
- ClickHouse: replicated production tables and object-storage backups when analytics recovery is required.
- Redis: reconstructible limiter/cache state; no strict RPO dependency.

Proposed targets after validation: operational PostgreSQL RPO 5 minutes/RTO 30 minutes; Kafka single-node/broker failure RPO 0 in a three-broker cluster; analytics RPO 1 hour/RTO 4 hours.
