# Implementation report — checkpoint 2026-09-17

## Executive Summary

The requested upgrade is **in progress**, paused at the user's request for a series
of focused commits. This checkpoint completes substantial reliability, build/test,
HTTP egress and pipeline lifecycle work. It does not meet the full production
Definition of Done: authenticated tenancy and other release gates remain open.
Commit metadata is distributed over September 13–17 as requested; verification
was actually performed on September 16–17. Historical commit dates are not benchmark
or test execution evidence.

## Architecture Changes

Keep the original three deployables and service-owned databases. Add fenced lease
transitions, durable retry schedules, strict pipeline definitions and a revisioned
lifecycle with atomic switching. No distributed transaction or new business service
was introduced. ADRs 008–010 document the decisions.

## Reliability

- Recover expired DELIVERING jobs; stale processing/delivery workers cannot finalize,
  retry or create DLQ effects for a newer attempt.
- Fence outbox publication confirmation/release and renew live claims before I/O.
- Claim delivery jobs immediately before execution; bound recovery attempts.
- Never discard Kafka records on storage failures without a durable outcome.
- Validate message contracts and persist rejection before acknowledging malformed input.
- Deduplicate terminal redelivery and guard replay from the expected terminal state.
- Preserve original inbox JSON on replay; JSONB comparison avoids formatting conflicts
  and replay no longer rounds numbers through a parse/serialize cycle.
- Bind UTC JDBC timestamps correctly and return the original ingestion acceptance time.
- Correct ClickHouse TTL type and include destination in its deduplication key.
- Preserve original Flyway migrations; add forward migrations. Existing installations
  must stop pre-fencing workers before upgrading. Existing ClickHouse tables require
  a reviewed table migration; changing init.sql alone does not modify an existing table.

## Security

Exact HTTPS URL allow-list, checked connection DNS addresses, private/metadata network
rejection, no redirects, bounded requests, no response-body consumption and per-destination
circuit breakers. JSON Schema remote references/base URI overrides are rejected.
Compose publishes only on loopback; service passwords are required configuration.

JWT/RBAC and tenant isolation are **not implemented**. Pipeline audit records use
`local-unauthenticated`, not an invented operator identity. No claim of authenticated
or tamper-proof auditing is made. See SECURITY.md and the threat model.

## Testing

Meaningful transactional repository tests now run through Spring proxies against
PostgreSQL. They exercise rollback, concurrent claims, stale tokens, durable retry,
replay and sink duplication. Infrastructure tests use real Kafka, Redis, ClickHouse
and WireMock. A packaged-application E2E launches all three services and checks REST
acceptance, duplicates, both sinks, pipeline recovery and same-ID DLQ replay.

Pipeline tests cover revision conflicts/409, immutable active definitions, concurrent
activation, rollback, append-only history and side-effect-free previews. Enrichment
is deliberately skipped in previews and yields an explicit incomplete result.

## Observability

Add pending/oldest-age outbox gauges, HTTP circuit metrics, structured JSON logs,
readiness groups, Grafana panels and Prometheus alert rules with a recovery runbook.
End-to-end trace parenting across the asynchronous outboxes is not proven. Complete
correlation fields, consumer-lag telemetry and all requested dashboards remain open.

## Performance

No throughput benchmark was run. Load scenarios and a measurement methodology are
provided without invented numbers. Sequential workers and per-row ClickHouse writes
remain deliberate investigation points; no unmeasured performance claim is made.

## Remaining Limitations

- JWT/RBAC, auth-derived tenant propagation/SQL isolation and credential rotation.
- Versioned schema families and compatibility checks.
- Authenticated audit identity and history for schemas, DLQ, credentials/destinations.
- Destination registry validation at activation, webhook signing and configurable
  destination-specific retry policies.
- Payload byte/depth limits, complete redaction and standardized error/correlation fields.
- Authenticated per-tenant/producer quotas; the current API-key header is not authentication.
- Bounded/paginated bulk DLQ management, preview/reason/rate controls.
- Retention/archival with an explicit idempotency/replay horizon.
- Full operations/trace verification, sustained chaos reconciliation and measured load tests.
- Complete OpenAPI response models and automated contract drift validation.

## Verification

Verification runs in a Java 21/Maven container through Windows Docker CLI because
this WSL distro has no Java/Maven installation and no usable Linux Docker CLI.
The runner uses the real Docker socket and `TESTCONTAINERS_HOST_OVERRIDE`.

| Check | Status | Evidence |
|---|---|---|
| Java / Maven | PASS | Temurin 21.0.9, Maven 3.9.11 container |
| Unit tests | PASS | Current Surefire reports in module target directories |
| PostgreSQL/Kafka/Redis/ClickHouse/WireMock integration | PASS | Current Failsafe reports, no Docker skip |
| Pipeline lifecycle and conflict tests | PASS | PipelineRepositoryIT, PipelineApiTest, codec and engine tests |
| Full `mvn clean verify` | PASS | Completed 2026-09-17 18:32:37 UTC; 58 tests, zero failures/errors/skips; formatting, static checks and coverage passed |
| Packaged E2E | PASS | EventLifecycleE2E passed with the revisioned lifecycle, both sinks and same-ID replay |
| Docker Compose configuration | PASS | `docker compose config --quiet` |
| Docker Compose build | PASS | All three current application images built successfully |
| Complete Compose startup | NOT VERIFIED | Found ClickHouse localhost/IPv6 probe failure; changed probe to 127.0.0.1, awaiting restart |
| Smoke / operational trace checks | NOT VERIFIED | Awaiting final Compose startup |
| Sustained chaos / performance benchmark | NOT VERIFIED | Scripts exist; no measurement or no-loss claim |

The final test count is 20 unit tests, 37 integration tests and one packaged E2E.
Generated target reports are intentionally ignored by Git. CI uploads test, coverage,
SBOM and Compose logs. This checkpoint's remaining verification should be repeated
before claiming full completion of the original task.
