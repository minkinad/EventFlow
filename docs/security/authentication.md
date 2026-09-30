# Authentication and tenancy

Every business API requires an OAuth2 JWT bearer token. The services verify RS256
signatures against the configured JWKS endpoint and validate issuer, service audience,
expiry, not-before, `sub` (1–200 characters), and `tenant_id` (1–120 identifier characters).
Roles come only from the signed `roles` array; unknown roles grant no access.
Production must supply `JWT_ISSUER` and `JWT_JWK_SET_URI`; no unsigned/dev bypass exists.
Use a trusted HTTPS issuer/JWKS endpoint outside the isolated local environment.

Ingestion defaults to audience `eventflow-ingestion`; processing and delivery use
`eventflow-operations`. `JWT_AUDIENCE` can override each resource server's audience.
Issue producer tokens for both audiences only when they need downstream status access.
Browser sessions, cookies and form login are not used; CSRF protection is disabled
for these stateless bearer-token APIs. Health readiness/liveness remain anonymous
without details. Metrics require VIEWER or ADMIN; other actuator endpoints are denied.
Metrics describe the deployment in aggregate, not a tenant-filtered business view;
restrict the metrics route to trusted monitoring/operators in deployment.

| Role | Allowed operations within the token tenant |
|---|---|
| PRODUCER | Submit events; read status of its own events in all three services |
| PIPELINE_EDITOR | Create/read/edit/validate/dry-run/activate/rollback/disable pipelines; publish schemas |
| DLQ_OPERATOR | Read event status and replay eligible dead letters |
| VIEWER | Read operational event/pipeline status and metrics |
| ADMIN | All implemented business operations; no cross-tenant bypass |

Tenant is derived from verified `tenant_id`; producer is the stable JWT `sub`.
Request fields, headers, source and metadata cannot set either identity.
Tenant/producer travel in EventEnvelope and DeliveryCommand v2. Background workers
use that durable identity explicitly, not a request ThreadLocal. Kafka and service
DBs form a trusted boundary and need transport authentication and ACLs in deployment.
External users must never be given raw Kafka write access.

The optional trusted enrichment backend now receives
`GET /api/v1/tenants/{tenant}/enrichments/{source}/{customerId}`. The tenant comes
from the durable envelope, even if payload fields attempt to override it. The
backend must enforce this namespace; deploy it with service authentication/network
isolation. The repository does not include or certify that external backend.

Event UUIDs remain globally unique, preserving existing storage and sink idempotency.
A foreign tenant or another producer cannot reuse an existing UUID: ingestion returns
404 without revealing content, owner or original acceptance time. The original producer
gets duplicate acceptance for identical content or 409 for changed content. Tenant
predicates protect status and replay; pipeline/schema names and activation locks are
scoped by tenant. This is application-enforced row isolation, not PostgreSQL RLS.
Database and analytics credentials are infrastructure privileges, not tenant API credentials.

## Local credentials

Compose imports `infrastructure/keycloak/eventflow-realm.json` into Keycloak.
These client secrets are deliberately public, local-only fixtures. Production must
use a separately managed realm, credentials, TLS and Kafka/database access policies.

| Client | Tenant | Role | Local secret |
|---|---|---|---|
| demo-producer | demo | PRODUCER | demo-producer-local-only |
| demo-admin | demo | ADMIN | demo-admin-local-only |
| other-admin | other | ADMIN | other-admin-local-only |
| metrics | demo | VIEWER | metrics-local-only |

```bash
export EVENTFLOW_TOKEN=$(python3 scripts/local-token.py)
# For pipeline/replay operations:
export EVENTFLOW_TOKEN=$(CLIENT_ID=demo-admin python3 scripts/local-token.py)
curl -H "Authorization: Bearer $EVENTFLOW_TOKEN" http://localhost:8081/api/v1/pipelines/10000000-0000-0000-0000-000000000001
```

The script uses client credentials, not a password grant. Tokens last five minutes.
The smoke script fetches its own admin token unless EVENTFLOW_TOKEN is supplied.
Prometheus obtains and refreshes its metrics token through OAuth2 client credentials.
The Keycloak admin console uses `local-admin` / `local-admin-only` in Compose only.

`X-API-Key` was previously an arbitrary rate-bucket label, never an authenticated
credential. It is now ignored. Machine clients use OAuth2 client credentials instead
of introducing a second credential store. Redis enforces producer and tenant buckets
atomically with its own clock. Fail-closed is the default: Redis outage returns 503;
quota exhaustion returns 429. Both include Retry-After. Explicit fail-open admits
with X-RateLimit-Degraded and a bounded outcome metric, without affecting idempotency.

## Upgrade from tenant-less v1

1. Pause producers and drain v1 outboxes, consumer lag and delivery work with the old
   binaries. Resolve or explicitly archive old DLQs before switching. Take backups.
2. Stop old workers; apply the forward Flyway migrations with the new applications.
   Old event records retain NULL ownership and are inaccessible through tenant APIs.
   No tenant is inferred from legacy metadata. Only the built-in demo pipeline/schema
   seeds map to `demo`; review this mapping before upgrading customized installations.
3. New publications use separate `.v2` topics and contractVersion 2. There is no
   automatic republishing or tenant assignment of v1 messages. Retain old topics/data
   until an operator-approved ownership/backfill plan is executed.
4. Apply the additive ClickHouse statements in `infrastructure/clickhouse/tenant-columns.sql`
   before starting new delivery workers. Existing volume initialization does not rerun SQL.
5. Configure issuer/JWKS/audiences and obtain new tokens. Run the smoke test and verify
   negative access tests. Do not delete volumes to resolve a migration problem.

A signing-key rotation under the same issuer is handled by the JWKS decoder. Subject
changes create a new producer identity; migrate ownership only through a reviewed
administrative procedure. Access-token revocation/credential lifecycle is owned by
the identity provider. HTTP webhook signing, schema compatibility, full audit coverage,
replay quotas and database RLS remain separate work.
