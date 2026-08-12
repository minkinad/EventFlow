# ADR-004: Durable layered idempotency

- Status: Accepted
- Date: 2026-08-12

## Context

Clients retry timeouts and Kafka redelivers records. A cache-only key can expire, be evicted, or disappear during failover. Silently accepting the same event ID with different content corrupts event identity.

## Decision

Bind `eventId` to a canonical content hash in PostgreSQL with a unique constraint. Return the original acceptance semantics for the same ID/content and `409` for the same ID/different content. Use durable processing records, delivery `commandId`, and per-target unique constraints downstream. Redis may accelerate checks and rate limits but is not authoritative.

## Consequences

- Correctness survives Redis loss and service restarts.
- Canonicalization rules are part of API semantics and need contract tests.
- Idempotency data requires a retention policy at least as long as the producer retry/replay horizon.
- Replay keeps the same event identity and explicitly transitions terminal records back to retryable state.
