# ADR-001: Kafka as the event transport

- Status: Accepted
- Date: 2026-08-12

## Context

EventFlow needs durable buffering, producer/consumer decoupling, per-key ordering, replay, consumer groups, and horizontal processing. Direct synchronous calls would couple availability and turn every downstream outage into ingestion downtime. A database queue is operationally simpler at small scale but offers weaker independent replay and ecosystem tooling.

## Decision

Use Kafka between ingestion, processing, and delivery. Key records by `eventId`. Treat Kafka as a durable transport log, not as the authoritative business database. Use explicit topic/contract versions and consumer groups per logical stage.

## Consequences

- Services scale and fail independently; backlog is visible as lag.
- Ordering is guaranteed only inside one partition, so no global ordering is promised.
- Consumers must handle duplicates and poison messages.
- Production requires a multi-broker cluster, replication, security, retention planning, and partition-capacity discipline.
- Kafka Streams is not introduced until stateful streaming semantics justify the additional model.
