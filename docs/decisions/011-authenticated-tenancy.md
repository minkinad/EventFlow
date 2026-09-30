# ADR 011: JWT resource servers and explicit tenant scope

Status: Accepted. Supersedes the unauthenticated audit-context limitation in ADR 010.

## Context

Public business APIs had no trusted producer or operator identity. X-API-Key only
selected an arbitrary Redis bucket. Global pipeline/schema keys and unscoped reads
could not isolate customers. Async workers must not depend on request thread state.

## Decision

Keep three deployable services. An explicitly imported `security` library provides
OAuth2 resource-server validation, role rules and access to verified claims; `common`
remains wire DTOs only. Use separate ingestion/operations audiences and signed tenant,
subject and role claims. ADMIN is tenant-local. Unknown endpoints are denied.

Persist the authenticated tenant/producer at ingestion and propagate them in Kafka
contract v2 on separate topics. Workers use explicit tenant arguments. SQL predicates
scope operational reads/replays; pipeline/schema uniqueness and activation locks are
per tenant. Retain globally unique event UUIDs for existing inbox/outbox and sink keys;
foreign UUID reuse returns 404. No request-supplied identity is trusted.

Use Keycloak client credentials for the local demo and machine producers. Remove the
unauthenticated API-key bucket convention. Redis atomically checks tenant and producer
buckets using server time, with explicit fail-open/fail-closed behavior. Legacy data
is unowned and inaccessible until reviewed; it is not silently assigned a tenant.

## Alternatives

Separate deployments per tenant provide stronger infrastructure isolation but require
more operational resources. PostgreSQL RLS can add defense in depth; connection-pool
session/transaction state must be carefully managed and is not implied here. A second
API-key registry adds rotation, auditing and secret-management responsibilities without
a demonstrated requirement; OAuth2 client credentials cover the existing use case.

## Consequences

Every caller needs a token; local scripts and metrics use client credentials. Upgrades
must drain v1 work and explicitly plan ownership of legacy records. Kafka remains a
trusted transport boundary and production needs ACLs/TLS. Schema compatibility, complete
audit coverage, signed webhooks and tenant resource/storage quotas are not solved by JWT.
