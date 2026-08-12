# ADR-006: Versioned declarative pipelines with a fixed step registry

- Status: Accepted
- Date: 2026-08-12

## Context

Adding a Java deployment for every validation or routing change is slow. Loading arbitrary scripts/classes from configuration creates security, performance, and supportability risks. Processing must also remain reproducible after a configuration change.

## Decision

Store immutable, versioned JSON pipeline definitions. Definitions reference an allow-list of Java implementations such as `validate`, `enrich`, and `route`. Persist the selected pipeline version with every processing result. Activation is a control-plane operation separate from editing.

## Consequences

- Common changes become configuration operations with audit/rollback potential.
- New step behavior still requires reviewed Java code and deployment.
- Definitions need structural and semantic validation, compatibility checks, dry-run, RBAC, and audit history.
- External enrichment makes a step non-deterministic; captured version/trace and durable retries are therefore essential.
