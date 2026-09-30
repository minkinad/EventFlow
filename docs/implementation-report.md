# Implementation report — checkpoint 2026-09-30

## Executive Summary

The original upgrade remains **in progress**. This increment adds authenticated
multi-tenant API access to the earlier reliability and pipeline lifecycle work.
JWT roles, tenant/producer ownership, scoped SQL queries, local Keycloak and admission
quotas are implemented as one vertical slice. Remaining production gates are listed
below; the whole original Definition of Done is not claimed complete.

The eight prior commits were dated September 13–17 as requested. This increment is
organized into eight thematic commits dated September 21–30 at the user's request.
Those assigned commit dates are not test execution evidence. Final verification is
dated September 30; focused integration checks also ran on September 29.

## Architecture Changes

Keep three deployables and service-owned databases. `common` remains wire DTOs;
`security` is an explicitly imported library for resource-server validation and RBAC.
Ingestion and operations use different expected audiences. JWT tenant/subject become
persisted ownership and travel in EventEnvelope/DeliveryCommand v2 on separate Kafka
topics. Workers take explicit tenant arguments, with no request-context dependency.

Pipeline/schema names, active-version uniqueness and activation locks are tenant scoped.
Event UUIDs remain globally unique to preserve existing inbox/outbox and sink keys;
foreign producer/tenant reuse returns 404. Old event rows remain unowned and inaccessible
rather than being assigned a tenant from untrusted historical metadata. ADR 011 and the
[upgrade guide](security/authentication.md) document this choice and migration boundaries.

## Reliability

Earlier fenced leases, expired-job recovery, durable retry deadlines, guarded replay,
transactional inbox/outbox transitions and stable sink idempotency remain in place.
The full regression suite exercises these behaviors with the new identity fields.
Existing Flyway migrations are preserved; additive migrations introduce ownership and
configuration scopes. ClickHouse has an idempotent additive tenant-column upgrade.

The local smoke loop now retries startup connection resets. Before upgrading the
existing demo, its unpublished ingestion/processing outboxes, pending delivery work
and open DLQs were checked and found empty. Existing volumes were preserved.

## Security

- Verify RS256 signature, issuer, service audience, expiry/not-before, tenant and subject.
  Missing audience is rejected with 401. Unknown roles grant no privileges.
- Explicit PRODUCER, PIPELINE_EDITOR, DLQ_OPERATOR, VIEWER and ADMIN permissions;
  ADMIN is tenant-local. Minimal probes are anonymous; metrics require a read role.
- Producers can read only their own event status in all services. Tenant predicates
  protect status, replay, schemas and pipeline lifecycle mutations.
- Pipeline audit history records the verified actor/tenant without configuration secrets.
- External enrichment uses a tenant-scoped URI from the durable envelope; payload
  fields cannot change it. The external backend must enforce that namespace and service
  authentication; this repository does not implement that backend.
- Redis atomically checks tenant and producer quotas using server time. Fail-closed
  is the default and returns 503 on outage; quota exhaustion returns 429. Explicit
  fail-open adds a degradation header; outcomes have bounded metrics.
- X-API-Key was never a validated credential and is now ignored. Machine clients use
  OAuth2 client credentials. Local Keycloak fixtures, smoke, load and metrics clients
  are documented; no unsigned authentication bypass exists.
- Earlier HTTP egress allow-list, connection-time DNS/IP filtering, redirect denial,
  response bounds and local-only schema references remain covered by tests.

## Testing

The security module tests actual RSA-signed JWTs through the filter chain and JWKS
endpoint, including wrong signature/issuer/audience, missing claims, expiry, future
not-before and privilege escalation. PostgreSQL tests cover tenant/owner reads,
foreign replay, independent schema names and active pipelines. Redis tests cover
concurrency and shared tenant quotas; outage tests verify admission and error privacy.

Packaged application E2E uses signed JWTs against its own JWKS server and checks
401/403, forged request ownership fields, cross-tenant 404s, same-tenant producer
privacy, pipeline isolation, both sinks and processing DLQ recovery. These supplement
existing rollback, stale lease, durable retry and sink deduplication regressions.

## Observability

JSON logs, OTLP infrastructure, outbox gauges/alerts and circuit metrics remain.
Prometheus now obtains/refreshes OAuth2 credentials for protected metrics endpoints.
Admission metrics distinguish allowed, limited, fail_open and fail_closed outcomes.
Full trace parenting/correlation across asynchronous outboxes is still not proven.

## Performance

No throughput benchmark was run. The k6 script requires a token; documented methodology
distinguishes authentication failures, quota rejections, accepted throughput and completed
effects. No capacity or performance number is inferred from unit/E2E timing.

## Remaining Limitations

- Schema families/version compatibility and activation-time destination registry validation.
- Complete immutable audit coverage for schemas, DLQ, credentials and destinations.
- Paginated/bounded DLQ management, replay reason/preview/rate controls.
- Payload byte/depth limits, complete redaction and standardized public errors.
- Tenant concurrency/storage quotas, database RLS defense in depth, retention/replay horizon.
- Kafka/database TLS and ACLs, managed identity-provider credentials, webhook signing/rotation.
- Full trace/correlation verification, sustained chaos reconciliation and measured load tests.
- Complete OpenAPI response models and automated contract-drift validation.

Kafka writers and direct database/analytics credentials are trusted infrastructure
identities, not tenant-facing API credentials. Compose remains a local development
environment with public fixture secrets. JWT alone does not make it a production topology.

## Verification

Java/Maven run in the verification container against the real Docker socket. This WSL
environment now has a usable Linux Docker CLI. Reports are under module `target/`
directories; generated reports and SBOMs are intentionally ignored by Git. Durable local
command logs are under `.verification/`, also ignored, so a WSL restart does not erase them.

| Check | Status | Evidence |
|---|---|---|
| Java / Maven | PASS | Temurin 21.0.9, Maven 3.9.11 |
| Compose configuration | PASS | `docker compose config --quiet` |
| Keycloak issuer/role/tenant configuration | PASS | Local client-credentials token claims inspected without printing token |
| Existing ClickHouse upgrade | PASS | Additive tenant/producer columns applied; existing data preserved |
| Full clean verification, packaged E2E | PASS | `mvn -B -ntp clean verify`: 68 tests, zero failures/errors/skips; finished 2026-09-30 08:24:14 UTC; coverage, formatting, Checkstyle and SBOM gates passed |
| Compose images | PASS | All three application images built from the current implementation |
| Authenticated Compose smoke | PASS | Event `4f04ebe8-5995-4317-8221-cb2251563f0a`: duplicate acceptance retains timestamp, processing succeeds, both deliveries succeed, ClickHouse FINAL count is 1 |
| Keycloak access isolation | PASS | 19 runtime checks: anonymous 401, producer mutation 403, foreign tenant/producer status 404 in all three services, own tenant 200; metrics require a read role |
| Prometheus OAuth2 scraping | PASS | All three application targets report `up` with empty lastError |
| Full trace propagation / sustained chaos / benchmark | NOT VERIFIED | No unsupported runtime or throughput claim |

The updated Compose stack is running with its existing volumes. September 30 command
logs, per-suite test totals and runtime access/scrape results are saved in `.verification/`.
