# Threat model

Scope: three application services, Kafka, service-owned PostgreSQL databases,
Redis limiter, ClickHouse, Keycloak and external HTTP receivers. Business APIs require
verified JWT claims. Local fixture credentials and infrastructure remain development-only.

| Threat | Current control | Remaining requirement |
|---|---|---|
| Producer spoofing / stolen key | JWT subject/tenant, client credentials; X-API-Key ignored | Managed identity-provider credentials, rotation and revocation |
| JWT privilege escalation | RS256, issuer/audience/lifetime and required claims checked; explicit role allow-list | Identity-provider administration, HTTPS and key lifecycle |
| Cross-tenant read/change/replay | Auth-derived tenant/owner in v2 contracts; scoped SQL reads/replay/configuration; ADMIN stays tenant-local | Optional RLS; per-tenant storage/concurrency quotas |
| SSRF / DNS rebinding / redirect escape | Exact HTTPS URL allow-list; connection uses checked DNS answers; no redirects | Egress network controls; enforce the tenant namespace and service authentication at the trusted enrichment backend |
| Schema-reference SSRF | Only local references; fixed dialect; no base URI overrides | Compatibility and resource-complexity limits |
| Replay duplicates | Stable event identity, fenced state transitions, PostgreSQL sink uniqueness | Authenticated operator reason, immutable audit, bounded bulk preview/replay |
| Kafka tampering / unsupported contracts | Contract validation and durable rejection; content conflicts do not overwrite inbox | Kafka ACLs, TLS/SASL and authenticated producers |
| DLQ abuse | Tenant/RBAC guards and terminal-state single replay, no unrestricted bulk API | Replay actor audit, reason and rate controls |
| Webhook forgery / replay attacks | Stable Idempotency-Key; HTTPS | HMAC signing, timestamp window and secret rotation at receiver |
| Resource exhaustion | JWT tenant/producer admission quotas; HTTP time bounds; bounded fan-out and retry | REST byte/depth limits, concurrency/storage quotas and table retention |
| Data leakage | Generic malformed/publication error text; no deliberate event payload logging | Unified error redaction; encrypt/retain DLQ storage appropriately; complete redaction |

Internal infrastructure is assumed trusted in the demo. Do not expose it publicly.
The HTTP adapter cannot guarantee receiver idempotency. PostgreSQL fencing cannot
undo a network request from a suspended worker. These limits are part of the contract.
