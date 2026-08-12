# ADR-005: PostgreSQL for operations and ClickHouse for analytics

- Status: Accepted
- Date: 2026-08-12

## Context

Operational workflows require constraints, transactions, row locking, point lookup, and frequent status updates. Analytics requires high-volume append ingestion and columnar scans. Forcing one database to serve both workloads creates contention and compromises both models.

## Decision

Use PostgreSQL for service-owned operational state and ClickHouse for analytical event copies. Delivery to ClickHouse is asynchronous and does not participate in operational transactions.

## Consequences

- PostgreSQL enforces workflow correctness and powers replay/audit queries.
- ClickHouse supports analytical scans without loading operational databases.
- Analytics is eventually consistent and can lag or be rebuilt from retained events.
- Cross-store joins are application/analytics concerns, not request-path operations.
- ClickHouse duplicate reconciliation follows a versioned `ReplacingMergeTree` strategy.
