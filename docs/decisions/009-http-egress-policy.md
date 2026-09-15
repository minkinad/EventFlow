# ADR-009: Explicit HTTP egress policy

Status: Accepted, 2026-09-16.

## Context

A pipeline administrator can choose a webhook URL. HTTPS alone does not prevent
SSRF, DNS rebinding, redirects into a private network or an unlimited response body.

## Decision

The delivery service defaults to an empty exact-URL allow-list. Operators configure
at most 100 HTTPS destinations, without user information, query strings or fragments,
using `eventflow.http.allowed-destinations` (comma-separated). The connection's
Apache HttpClient resolver checks every address and supplies those same addresses
to the connection manager. Private, local, link-local, multicast, metadata, shared
address space and selected reserved/transition ranges are denied.

Disable redirects and automatic HTTP client retries. A durable job owns retries.
Use 2-second connect/pool timeouts, a 5-second response timeout and a 10-second
cancellation deadline. Ignore response bodies and abort before close to avoid
unbounded response draining. Preserve the eventId as Idempotency-Key.

Circuit breakers are per configured destination. Metric labels contain a stable
hash of the URL and are bounded by the allow-list size. No event identifiers are
metric labels. JSON Schema compilation only supports document-local references;
remote schemas and base URI overrides are rejected.

## Alternatives

- Resolve, validate, then use the default resolver: introduces a DNS rebinding race.
- Allow arbitrary HTTPS: reaches private networks and metadata endpoints.
- Follow redirects: permits an approved destination to select unapproved targets.
- Shared breaker for every URL: one outage interrupts unrelated endpoints.

## Consequences

This intentionally trades HTTP connection reuse for bounded response handling.
A successful HTTP status is the receiver's acknowledgement, not proof of its
business transaction. Remote idempotency is still a requirement for effectively-once
HTTP effects. HMAC signing, credential rotation and destination-specific retry
policies remain follow-up work. The fixed enrichment service is a trusted operator
configuration and needs a separate destination registry before generalizing it.
