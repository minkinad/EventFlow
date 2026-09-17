# Performance methodology

No throughput or latency benchmark has been executed for this increment. The
1,000 accepted events/second figure is an offered-load experiment target, not a result.

The verification environment uses Docker Desktop 4.45.0 / Engine 28.3.3 on a Windows
host with WSL Ubuntu. Maven runs in `maven:3.9.11-eclipse-temurin-21`; the Docker VM
reports approximately 7.6 GiB RAM. Tests include fresh infrastructure startup, so
suite duration must not be interpreted as application throughput.

## Reproduction

1. Start the demo and run `make smoke`.
2. Set the local rate limit above the intended load; record capacity/refill policy.
3. Run `RATE=10 DURATION=30s k6 run scripts/load/events.js` as a canary.
4. Repeat at increasing offered load, then `DUPLICATES=true`. Save k6 JSON/summary
   artifacts and the exact revision, CPU/RAM/storage, JDK, images and configuration.
5. During a separate failure workload, stop a sink or Kafka, restore it, and measure
   accepted IDs against final sink IDs plus unresolved DLQ, backlog drain time and
   duplicate business effects. Preserve accepted IDs outside the failed process.

Measure acceptance p50/p95/p99, errors/429, achieved accepted throughput, queue age,
Kafka lag, DB pool/locks, sink throughput and process CPU/GC. Acceptance alone is not
end-to-end throughput. Compare ClickHouse with FINAL, never physical row count.

## Review findings, not measured bottlenecks

- Delivery claims just before I/O to avoid expiring queued leases; workers are
  sequential per instance. Measure before introducing bounded concurrency.
- Outbox batch publication is synchronous; expired batch entries are skipped and
  reclaimed. Database round trips and broker acknowledgement latency may dominate.
- ClickHouse currently inserts one row per request; batching needs an explicit
  partial-failure/idempotency design.
- HTTP response bodies are ignored and connections aborted for bounded handling;
  measure before adding safe bounded connection reuse.
- Partial indexes cover due outbox rows and expired delivery leases.
- No partitioning until retained tables, vacuum time and measured queries justify
  it. Start review around tens of millions of retained rows or failed latency/maintenance
  objectives; this is an investigation trigger, not a universal capacity limit.
