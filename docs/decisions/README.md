# Architecture decision records

| ADR | Decision | Status |
|---|---|---|
| [001](001-kafka-as-event-transport.md) | Kafka as event transport | Accepted |
| [002](002-delivery-semantics.md) | At-least-once plus idempotent effects | Accepted |
| [003](003-transactional-outbox.md) | Transactional outbox for DB-to-Kafka publication | Accepted |
| [004](004-idempotency.md) | Durable layered idempotency | Accepted |
| [005](005-postgresql-and-clickhouse.md) | PostgreSQL for operations, ClickHouse for analytics | Accepted |
| [006](006-versioned-pipelines.md) | Versioned declarative pipelines | Accepted |
| [007](007-three-service-boundary.md) | Three deployable services | Accepted |
| [008](008-fenced-recovery.md) | Fenced leases and durable recovery | Accepted |
| [009](009-http-egress-policy.md) | HTTP egress and local schema references | Accepted |
| [010](010-pipeline-lifecycle.md) | Revisioned lifecycle and side-effect-free preview | Accepted |

ADRs are immutable after acceptance. A changed decision gets a new ADR that supersedes the old one.
