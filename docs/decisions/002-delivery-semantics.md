# ADR-002: At-least-once transport with idempotent effects

- Status: Accepted
- Date: 2026-08-12

## Context

A crash can occur after a broker or external system accepts a write but before EventFlow persists confirmation. No atomic transaction spans PostgreSQL, Kafka, ClickHouse, and arbitrary HTTP services. Claiming global exactly-once delivery would therefore be misleading.

## Decision

Use at-least-once publication and consumption. Provide effectively-once business effects with durable inbox keys, unique constraints, upserts, stable external idempotency keys, and versioned ClickHouse rows.

## Consequences

- Duplicate transport records are normal and test cases must inject them.
- External endpoints that ignore idempotency keys may observe repeated calls.
- Delivery audit records distinguish attempts from final business effects.
- Kafka transactional/EOS features may optimize Kafka-to-Kafka stages later but do not replace sink idempotency.
