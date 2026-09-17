# Architecture and code audit — 2026-09-16

Baseline: `c1a154f` (clean working tree). Reviewed all tracked production/test Java,
POMs, migrations, application configuration, Compose, infrastructure, CI and docs.
ADRs live in `docs/decisions`, not `docs/adr`. `CONTRIBUTING.md` was linked but absent.
Java/Maven were absent from PATH; Docker Desktop integration was unavailable.
Initial findings below are code-review findings, not claims of passing runtime tests.
Implementation status is updated at the end of this increment.

## Critical

| ID / location | Problem and failure scenario | Required correction | Status |
|---|---|---|---|
| C1 `DeliveryJobRepository.claim` | Only PENDING/RETRY are selected. A killed worker leaves DELIVERING permanently stranded despite lease expiration. | Reclaim expired DELIVERING with a fresh fencing token; indexed expiry scan. | Implemented; see checkpoint report/tests |
| C2 `ProcessingRepository`, `DeliveryJobRepository` | Completion/retry/dead updates check status only. An expired worker can finish a newer worker's attempt, publish a different command or create a spurious DLQ. | Unique token per claim; compare token and live lease on every transition; transactionally gate side effects. | Implemented; see checkpoint report/tests |
| C3 `KafkaConsumerConfiguration`, delivery listener | Default recovery eventually logs/discards unhandled records. During database failure there is no durable handoff, yet consumer can advance. | Retry transport/storage failures indefinitely; bounded business failures must commit DLQ before ack. | Implemented; see checkpoint report/tests |
| C4 all REST APIs, `RateLimitFilter` | No authentication. X-API-Key is an arbitrary limiter identity, not a credential. Anyone can change pipelines, replay or read payload diagnostics; changing the header bypasses limits. | JWT/RBAC, authenticated tenant scoping across contracts and DB; local-only exposure until implemented. | Open release gate |
| C5 `ExternalHttpDeliveryAdapter`, schema validator | Arbitrary HTTPS destination can reach private/metadata networks; schema references can trigger network fetches. HTTP timeouts are absent. | Allow-listed destinations, resolver/connection-level IP policy, no redirects, bounded timeouts; disable remote schema references. | HTTP egress and schema references hardened; trusted enrichment configuration remains a limitation |

## High

| ID / location | Problem and failure scenario | Required correction | Status |
|---|---|---|---|
| H1 `ProcessingCoordinator.sha256` | Hashes raw JSON bytes. PostgreSQL JSONB changes whitespace/key order on replay, producing a content conflict for the same event. | Compare stored JSONB with incoming JSONB; preserve original inbox JSON for replay. | Implemented; see checkpoint report/tests |
| H2 `ProcessingRepository.claim/replay` | FAILED redelivery returns BUSY forever; old DLQ can replay a different active/successful record; content-conflict DLQ is marked PROCESSING and is replayable. | Terminal duplicate handling, restricted reclaim predicate, guarded replay from FAILED only, non-replayable conflict stage. | Implemented; see checkpoint report/tests |
| H3 outbox relays | Finalization/release does not verify lease owner; whole sequential batch leases expire while earlier network calls block. | Fresh token per claim, fence renew/finalization/release, refresh before I/O, stop safely on interrupt. | Implemented; see checkpoint report/tests |
| H4 JDBC timestamp bindings | Repositories pass `Instant` directly to JDBC; pgjdbc cannot infer its SQL type. Original DB tests were never exercised here. | Bind UTC OffsetDateTime consistently and exercise real PostgreSQL. | Implemented; see checkpoint report/tests |
| H5 repository tests | Direct `new Repository(...)` bypasses Spring @Transactional; tests cannot prove atomicity even when Docker is available. Docker silently skips tests. | Transactional Spring proxies or real Spring context, deliberate rollback/crash tests, strict IT execution. | Implemented; see checkpoint report/tests |
| H6 `DeliveryCommandListener`, processing decoder | Valid JSON with missing identities/unsupported contract versions bypasses parse-error handling, then loops or gets discarded. Duplicate commandId with changed content is silently accepted. | Validate wire contracts, durable rejection, conflict detection, no ack on persistence failure. | Implemented; see checkpoint report/tests |
| H7 `PipelineRepository.activate` | Concurrent activation lacks scope lock and revision; unique index prevents two actives but causes conflicts without a control-plane lifecycle. | Serialized activation per event type, revision checks, validate/dry-run/rollback and audit. | Implemented for current single-scope model; authenticated tenancy remains open |
| H8 ClickHouse DDL/adapter | ORDER BY excludes destination, so two destinations may collapse; JSONEachRow timestamp parsing settings unspecified. | Include destination in identity, explicit best-effort parsing, real adapter IT; existing volume migration documented. | Implemented; see checkpoint report/tests |
| H9 schema/destination management | No tenant model, immutable audit, compatibility validation, bulk replay controls, webhook signing or bounded payload policy. | Complete security/control-plane vertical increments before deployment. | Open |

## Medium

| ID / location | Problem and failure scenario | Required correction | Status |
|---|---|---|---|
| M1 root POM/CI | No enforced toolchain, formatting, static checks, coverage, IT phase or SBOM; green build may mean skipped DB tests. | Strict Java 21/Maven, pinned build plugins, Failsafe, JaCoCo, Spotless/static analysis, SBOM, CI evidence. | Implemented; mvn clean verify passed (58 tests) |
| M2 delivery retry | HTTP 409 always retried; cap applied before jitter; terminal failures labelled exhausted; replayed DLQ never resolves. | Explicit retry statuses, bounded jitter, accurate failure codes, resolve after success. | Implemented; see checkpoint report/tests |
| M3 rate limiter | Client clock used by Lua, arbitrary identity, outage fail-closed becomes 500, no degradation metrics. | Server clock, authenticated identity, explicit 503 and metrics; Redis remains optional for correctness. | Open |
| M4 observability | Dashboard is only initial counters; no backlog age, alerts, structured logging, end-to-end tracing evidence or operational readiness groups. | Add queue telemetry and runbooks, verify actual traces; never claim exporter config proves tracing. | Partial: queue gauges, JSON logging and alerts added; trace correlation remains open |
| M5 storage | No retention. Deleting dedup state casually reintroduces effects; expiry scan index missing. | Bounded retention design tied to replay horizon; index lease expiry, measure before partitioning. | Partial |
| M6 Compose/Docker | Services lack readiness healthchecks, Kafka lacks named data volume, development passwords default in service config, ports bind publicly. | Explicit local config, loopback ports, durable Kafka volume, readiness probes and image checks. | Partial: loopback ports, Kafka volume, required passwords and probes; final startup pending |
| M7 ingestion response | Duplicate returns new acceptedAt instead of original acceptance time. | Return durable received_at. | Implemented; see checkpoint report/tests |

## Low

| ID / location | Problem and failure scenario | Required correction | Status |
|---|---|---|---|
| L1 docs | Missing contribution guide; reliability text sometimes describes future mechanisms as present. | Document measured verification and unresolved release gates. | Implemented; see checkpoint report/tests |
| L2 configuration | Many scattered @Value fields, duplicated relay logic, no validated bounds. | Targeted configuration refactoring after correctness; preserve service ownership. | Open |
| L3 performance | No measured throughput, sink batching or capacity evidence. | Reproducible workload scripts and honest environment/results report. | Open |

## Good existing decisions

- Three deployables with service-owned PostgreSQL databases; no cross-service SQL.
- Local transactional outbox and durable delivery jobs; Kafka acknowledgement follows repository calls.
- Unique ingestion identity and PostgreSQL upsert sink; Redis is not the correctness authority.
- `SKIP LOCKED` claims and partial polling indexes are a sound starting point.
- Pipeline implementation allow-list, immutable schemas by name, active-version unique index.
- Explicit at-least-once contract and documented ClickHouse eventual deduplication.
- Flyway, Java records, constructor injection, non-root runtime image, manual Kafka acknowledgement.

## Scope and sequencing

Complete the data-loss/lease/idempotency corrections and meaningful tests first,
as requested in section 64. Do not implement fragments of tenancy/authentication
that imply an isolation guarantee the data model does not yet enforce. This audit
and the implementation report retain remaining release gates explicitly.

## Checkpoint

See [implementation-report.md](implementation-report.md) for the pause on 2026-09-17,
actual verification evidence and outstanding release gates. New real ClickHouse tests
also found an invalid DateTime64 TTL expression, corrected to DateTime.
