# Contributing

Use Java 21, Maven 3.9.9+ (3.x) and Docker Engine. Run `mvn spotless:apply`, then
`mvn clean verify`. The latter includes real Testcontainers tests and a packaged
application E2E test; missing Docker is a failure, never a silent skip.
`mvn test` runs unit tests only. `-DskipITs` is an explicit local shortcut and is
not release verification; coverage may fail without integration tests.

Use `*Test`, `*IT`, and `*E2E`. For database correctness tests instantiate repositories
through the provided transactional test proxy, or a Spring application context.
Calling a repository constructor directly does not activate `@Transactional`.

Preserve service database ownership, stable event IDs and at-least-once semantics.
Add new Flyway migrations instead of editing applied migrations. Changes to lease
protocols need stale-owner and crash recovery tests. Capture material decisions
in `docs/decisions` and update the audit/roadmap with evidence, not intended behavior.

Never report a benchmark without workload, environment and measured results. Never
claim global exactly-once delivery. Do not commit generated target directories or secrets.
