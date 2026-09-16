# ADR-010: Revisioned pipeline lifecycle and side-effect-free dry-run

Status: Accepted, 2026-09-17. Supplements ADR-006.

## Context

Immutable business versions do not prevent two operators from editing a draft or
activating it while validation of an older revision is in progress. Operators need
a way to inspect routing without executing production enrichment or deliveries.

## Decision

Keep business `version` separate from mutable `revision`. Pipeline states are
DRAFT, VALIDATED, ACTIVE, SUPERSEDED and DISABLED. Editing a draft invalidates its
validation. Every mutation requires the current revision. Stale revision or invalid
lifecycle state returns HTTP 409. Schema validation operates before DTO conversion,
so unknown fields and inapplicable step parameters cannot disappear silently.

Activation requires VALIDATED; rollback selects an explicitly named SUPERSEDED
version. PostgreSQL advisory transaction locks serialize switches per event type;
a unique partial index remains the final constraint. The old and new versions and
an append-only audit entry change in one transaction. Rollback revalidates references.

Dry-run takes the selected definition and a test payload, performs validation and
routing, returns step snapshots/errors/duration, and never invokes enrichment. A
skipped enrichment is an explicit warning and `valid=false`, not a fabricated result.
Dry-run writes no outbox, delivery job, lifecycle state or audit record.

## Alternatives

- Validate only at creation: later edits invalidate the check.
- Use business version as revision: conflates event reproducibility with concurrent edits.
- Run enrichment in preview: creates network side effects and an SSRF/exhaustion path.
- Table-wide activation lock: unnecessarily serializes independent event types.

## Consequences

Existing ACTIVE seed definitions migrate unchanged; inactive versions become DRAFT.
Clients must explicitly validate and supply revisions before activation. The API
currently runs without authentication in local development, so audit actor is
honestly recorded as `local-unauthenticated`; tenant is null. This history is not an
authenticated operator audit until JWT/tenancy is implemented. Database owners can
bypass triggers, so append-only is not a claim of tamper resistance against a DBA.
