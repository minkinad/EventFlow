# ADR-008: Fenced leases and durable recovery

Status: Accepted, 2026-09-16. Supplements ADR-002/003/004.

## Context

A lease expiry does not stop the old process. A resumed worker can still write after
another worker claims the same row. Sequentially leasing a large delivery batch also
consumes retry attempts before some jobs have even started. Broker retries cannot
safely discard records while PostgreSQL is unavailable.

## Decision

Use a fresh UUID fencing token for every processing/delivery claim. Completion,
retry and terminal transitions compare row identity, token, current status and
lease deadline. Outbox publication confirmation and release compare the per-claim
owner token. Renew a live claim before external I/O; do not revive an expired claim.
Delivery claims one job immediately before execution, up to the configured poll budget.

Commit completion with its outgoing message or DLQ in one PostgreSQL transaction.
A lost lease cannot create an outgoing command or DLQ. Store retry deadlines in
PostgreSQL. Bound business attempts; keep infrastructure redelivery indefinite,
with capped Kafka backoff and a recoverer that cannot log-and-discard.

Compare processing input as JSONB, including on replay. Copy replay content from
the durable inbox without a JSON parse/serialize round trip that could round numbers.
Keep eventId unchanged. FAILED transport duplicates are acknowledged; only an
explicit guarded replay reopens failed processing.

## Alternatives

- Status-only updates: cannot distinguish old and current workers.
- Long leases alone: delay recovery without preventing stale writes.
- Database locks across network I/O: tie database capacity to sink latency.
- Kafka-only retries: cannot represent independent durable sink backoff.

## Consequences

No transaction spans Kafka and a sink. A network effect may happen after a lease
expires, so sink idempotency remains essential. UUID fencing protects database
state, not arbitrary remote systems. A crash on the last permitted sink attempt
can leave an uncertain remote outcome; recovery records a terminal failure rather
than allowing unlimited calls. Operators must reconcile that outcome before replay.

Stop old workers before applying the fencing migrations: old binaries do not
supply tokens. Existing PROCESSING/DELIVERING leases are explicitly expired by the
migration. Do not run mixed pre-fencing and post-fencing workers.

Retain identity records for the entire accepted retry/replay horizon. No automatic
inbox/job deletion is added until that horizon and archival policy are defined.
