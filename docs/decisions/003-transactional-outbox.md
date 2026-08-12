# ADR-003: Transactional outbox

- Status: Accepted
- Date: 2026-08-12

## Context

Writing business state to PostgreSQL and publishing to Kafka as two independent operations creates a dual-write failure window. XA/2PC adds coupling and is unavailable for many targets.

## Decision

Write state and an outbox row in one local PostgreSQL transaction. A relay claims rows with `FOR UPDATE SKIP LOCKED` and a lease, publishes with `acks=all`, and then marks them published. Consumers apply the same pattern for outgoing messages.

## Consequences

- A committed state change always has recoverable publication intent.
- Crash-after-publish can duplicate a message, so idempotent consumers remain mandatory.
- Outbox age becomes a first-class SLI.
- Rows need retention and indexes; polling load must be measured.
- CDC/Debezium is a future alternative if polling becomes a proven bottleneck, not a starting dependency.
