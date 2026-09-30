# Implementation roadmap

Checkpoint 2026-09-30: authenticated tenancy passes full clean verification with 68 tests,
including packaged E2E. Runtime results and remaining gates are recorded in
[the implementation report](implementation-report.md).

Implemented: fenced recovery, durable retries, strict build gates, HTTP egress protection,
revisioned pipelines, JWT/RBAC, explicit tenant/producer propagation and scoped reads/replays,
tenant-scoped schemas/pipelines, local Keycloak and atomic tenant/producer admission quotas.

The roadmap is organized as vertical increments. A capability is complete only when its failure path, tests, telemetry, and runbook exist.

## Increment 0 — architecture foundation (current)

- Java 21/Maven multi-module layout with three deployables.
- REST ingestion, PostgreSQL idempotency, transactional outbox relay.
- Kafka processing consumer, JSON Schema validation, configurable step engine.
- Durable delivery jobs with retry, circuit breaker, PostgreSQL/ClickHouse/HTTP adapters.
- Processing and delivery DLQ replay endpoints.
- Compose infrastructure and initial Prometheus/Grafana/OpenTelemetry setup.
- Architecture, reliability, ADRs, local example.

Build, infrastructure and packaged E2E verification run on Java 21 with Docker. Production deployment still requires infrastructure authentication/TLS and completion of the remaining release gates.

## Increment 1 — tested vertical slice

- Repository and service integration tests using PostgreSQL, Kafka, Redis, ClickHouse, and WireMock Testcontainers.
- End-to-end test from HTTP `202` to both PostgreSQL and ClickHouse sinks.
- Crash-window tests around every outbox/inbox boundary.
- Contract tests for all message versions.
- Maven Enforcer, Checkstyle/Spotless, JaCoCo thresholds, reproducible CI.

Definition of Done: `mvn verify` and an end-to-end Compose smoke test pass in CI; killing each service during load produces no missing business IDs.

## Increment 2 — pipeline control plane

- Pipeline-definition JSON Schema and semantic validation.
- Immutable schema names and revisioned draft/validate/dry-run/activate/rollback/disable lifecycle are implemented and covered by tests. Append-only pipeline history includes the authenticated tenant and actor.
- Implemented: dry-run endpoint with a supplied payload and no side effects; external enrichment is explicitly skipped.
- Schema compatibility checks.
- Optimistic locking and audited operator identity.
- Cache active definitions with explicit invalidation.

Definition of Done: an incompatible configuration cannot be activated; rollback is one atomic operation; every change is attributable.

## Increment 3 — production security

- Implemented: OAuth2 resource server, tenant-scoped claims and RBAC.
- Implemented: PRODUCER, PIPELINE_EDITOR, DLQ_OPERATOR, VIEWER and tenant-local ADMIN.
- Destination allow-list and DNS/IP egress controls.
- TLS/SASL, managed secrets, payload size/depth limits, log redaction.
- Implemented: atomic tenant/producer Redis token buckets; concurrency and storage quotas remain open.

Definition of Done: threat model reviewed; security integration tests cover cross-tenant and SSRF attempts; no default credential is accepted outside the local profile.

## Increment 4 — operational maturity

- Outbox/inbox retention jobs and table partitioning.
- Lag/oldest-age gauges, SLO dashboards and alert rules.
- Structured JSON logging with trace and event correlation.
- Bulk DLQ query/replay with rate control and audit reason.
- Graceful consumer draining and deployment runbooks.
- Load/soak/chaos tests and capacity report.

Definition of Done: SLOs are measurable, alerts have owners/runbooks, and a rolling deployment during peak test load causes no loss.

## Increment 5 — advanced routing

- Conditional routes using a constrained expression language.
- Batch-capable sink adapters and target-specific concurrency limits.
- Webhook signing and per-destination credentials.
- Optional CDC-based outbox relay evaluation if polling becomes a bottleneck.

Avoid adding new microservices unless independent ownership, scaling, or failure isolation is demonstrated with measurements.
