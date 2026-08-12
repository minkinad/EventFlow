# EventFlow

EventFlow is a Java 21 event-processing platform that accepts events over REST, publishes them reliably through Kafka, executes configurable processing pipelines, and delivers results to operational, analytical, or external systems.

The repository is deliberately limited to three deployable services. Reliability and operability are treated as product features: transactional outbox/inbox, idempotency, bounded retries, DLQ replay, rate limiting, metrics, traces, and integration tests.

## Architecture at a glance

```text
Client
  │ REST + Idempotency-Key/eventId
  ▼
Ingestion Service ── PostgreSQL (event + outbox in one transaction)
  │ outbox relay
  ▼
Kafka: eventflow.events.raw (key = eventId)
  │
  ▼
Processing Service ── PostgreSQL (pipelines, schemas, inbox/outbox, DLQ)
  │ validate → enrich → route
  ▼
Kafka: eventflow.events.delivery
  │
  ▼
Delivery Service ── PostgreSQL (delivery inbox/jobs and audit)
  ├── PostgreSQL sink
  ├── ClickHouse
  └── External HTTP API (Idempotency-Key + circuit breaker)
```

The system provides **at-least-once transport** and **effectively-once business processing**. Kafka duplicates are expected and neutralized by durable inbox/idempotency records. Exact-once claims are intentionally avoided for external HTTP systems.

## Repository

```text
eventflow/
├── common/                 # versioned wire contracts only
├── ingestion-service/      # REST ingestion and raw-event outbox
├── processing-service/     # schemas, configurable pipelines and DLQ
├── delivery-service/       # durable delivery jobs and sink adapters
├── infrastructure/         # Docker, PostgreSQL, ClickHouse, observability
├── docs/                   # architecture, reliability, runbooks and ADRs
├── compose.yaml
└── pom.xml
```

## Local development

Prerequisites: Java 21, Maven 3.9+, Docker with Compose v2.

```bash
mvn clean verify
docker compose up --build
```

Submit the sample event:

```bash
curl -i http://localhost:8080/api/v1/events \
  -H 'Content-Type: application/json' \
  -H 'X-API-Key: local-dev-key' \
  -d @docs/examples/order-created.json
```

Useful endpoints:

- Ingestion API: `http://localhost:8080`
- Processing administration API: `http://localhost:8081`
- Delivery/DLQ administration API: `http://localhost:8082`
- Kafka UI: `http://localhost:8088`
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000` (`admin` / `admin` locally)
- ClickHouse HTTP: `http://localhost:8123`

Start with [architecture.md](docs/architecture.md), then read [reliability.md](docs/reliability.md) and the [implementation roadmap](docs/roadmap.md). Architectural trade-offs are captured in [ADRs](docs/decisions/README.md).
Development and review rules are in [CONTRIBUTING.md](CONTRIBUTING.md).

## Engineering rules

- PostgreSQL is authoritative; Redis is never required to prove correctness.
- Kafka message keys are stable `eventId` values, preserving per-event ordering.
- Database changes and outgoing messages share a local transaction through an outbox.
- Consumers acknowledge Kafka only after their durable inbox/state transaction commits.
- Every retry has a limit, backoff, jitter, metrics, and a terminal DLQ path.
- No service reads or writes another service's schema.
- Wire contracts are backward-compatible and carry an explicit `contractVersion`.

Event progress can be investigated without direct database access:

```text
GET :8080/api/v1/events/{eventId}
GET :8081/api/v1/events/{eventId}
GET :8082/api/v1/events/{eventId}/deliveries
```

Pipeline changes use a draft/activate lifecycle: `POST /api/v1/pipelines` creates an inactive version and `POST /api/v1/pipelines/{id}/activate` performs the guarded switch.

This is an architecture-first foundation. See the roadmap for the exact Definition of Done of each increment.
