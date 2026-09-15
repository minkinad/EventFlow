# Threat model

Scope: three application services, Kafka, service-owned PostgreSQL databases,
Redis limiter, ClickHouse and external HTTP receivers. The local demo has no
trusted authentication context. Network-reachable APIs must be considered public.

| Threat | Current control | Remaining requirement |
|---|---|---|
| Producer spoofing / stolen key | Loopback-only Compose publishing | JWT or hashed/rotatable API credentials; X-API-Key is not authentication |
| JWT privilege escalation | No JWT implementation to rely on | Issuer/audience/algorithm validation and tested role mappings |
| Cross-tenant read/change/replay | No tenant isolation | Auth-derived tenant in contracts, keys, SQL predicates and negative-path tests |
| SSRF / DNS rebinding / redirect escape | Exact HTTPS URL allow-list; connection uses checked DNS answers; no redirects | Egress network controls; review trusted enrichment configuration |
| Schema-reference SSRF | Only local references; fixed dialect; no base URI overrides | Compatibility and resource-complexity limits |
| Replay duplicates | Stable event identity, fenced state transitions, PostgreSQL sink uniqueness | Authenticated operator reason, immutable audit, bounded bulk preview/replay |
| Kafka tampering / unsupported contracts | Contract validation and durable rejection; content conflicts do not overwrite inbox | Kafka ACLs, TLS/SASL and authenticated producers |
| DLQ abuse | Guarded single replay from terminal state, no unrestricted bulk API | RBAC, actor audit and replay rate controls |
| Webhook forgery / replay attacks | Stable Idempotency-Key; HTTPS | HMAC signing, timestamp window and secret rotation at receiver |
| Resource exhaustion | HTTP request/response time bounds; bounded fan-out and business retry | REST byte/depth limits, authenticated quotas and table retention |
| Data leakage | Generic malformed/publication error text; no deliberate event payload logging | Unified error redaction; protect operational status APIs and DLQ storage |

Internal infrastructure is assumed trusted in the demo. Do not expose it publicly.
The HTTP adapter cannot guarantee receiver idempotency. PostgreSQL fencing cannot
undo a network request from a suspended worker. These limits are part of the contract.
