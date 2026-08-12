# ADR-007: Three deployable service boundaries

- Status: Accepted
- Date: 2026-08-12

## Context

Validator, enricher, router, outbox relay, and DLQ processor could each be separate services. That would multiply repositories/deployments, network failure modes, contract evolution, and local infrastructure without independent teams or proven scaling needs.

## Decision

Deploy three services: ingestion, processing, and delivery. Keep pipeline steps as in-process strategy implementations and outbox/DLQ workers inside the owning service. Share only wire contracts.

## Consequences

- The system remains understandable and operable by a small team.
- Pipeline ordering and transaction boundaries are explicit.
- Each major workload still scales and fails independently.
- A component may be extracted only when measurements demonstrate a distinct scaling profile, security boundary, or ownership model.
