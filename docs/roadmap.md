# Implementation roadmap

The roadmap is organized as vertical increments. A capability is complete only when its failure path, tests, telemetry, and runbook exist.

## Increment 0 — architecture foundation (current)

- Java 21/Maven multi-module layout with three deployables.
- REST ingestion, PostgreSQL idempotency, transactional outbox relay.
- Kafka processing consumer, JSON Schema validation, configurable step engine.
- Durable delivery jobs with retry, circuit breaker, PostgreSQL/ClickHouse/HTTP adapters.
- Processing and delivery DLQ replay endpoints.
- Compose infrastructure and initial Prometheus/Grafana/OpenTelemetry setup.
- Architecture, reliability, ADRs, local example.

The reactor compiles, unit tests pass, executable JARs package, and the Compose model validates. Exit work still required: execute all Testcontainers and end-to-end tests with Docker Engine enabled on Java 21, and add API authentication before any non-local deployment.

## Increment 1 — tested vertical slice

- Repository and service integration tests using PostgreSQL, Kafka, Redis, ClickHouse, and WireMock Testcontainers.
- End-to-end test from HTTP `202` to both PostgreSQL and ClickHouse sinks.
- Crash-window tests around every outbox/inbox boundary.
- Contract tests for all message versions.
- Maven Enforcer, Checkstyle/Spotless, JaCoCo thresholds, reproducible CI.

Definition of Done: `mvn verify` and an end-to-end Compose smoke test pass in CI; killing each service during load produces no missing business IDs.

## Increment 2 — pipeline control plane

- Pipeline-definition JSON Schema and semantic validation.
- Immutable schema names and draft/validate/activate pipeline lifecycle are implemented; add rollback and audit history.
- Dry-run endpoint with a supplied event and no side effects.
- Schema compatibility checks.
- Optimistic locking and audited operator identity.
- Cache active definitions with explicit invalidation.

Definition of Done: an incompatible configuration cannot be activated; rollback is one atomic operation; every change is attributable.

## Increment 3 — production security

- OAuth2 resource server, tenant-scoped claims and RBAC.
- Dedicated roles: producer, pipeline-editor, DLQ-operator, viewer.
- Destination allow-list and DNS/IP egress controls.
- TLS/SASL, managed secrets, payload size/depth limits, log redaction.
- Per-tenant Redis token buckets plus concurrency and storage quotas.

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
