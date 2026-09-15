# Security policy

Only the current development branch is maintained. No released version is certified
for production use. The demo APIs currently have **no authentication or tenant isolation**;
run them only on a trusted local machine. Compose binds published ports to loopback.

Report a vulnerability through GitHub's private vulnerability reporting for this
repository, if enabled. Otherwise contact the repository owner privately before
publishing exploit details. Do not put credentials or customer payloads in issues.

Service database passwords are required environment variables. Compose credentials
and Grafana defaults are disposable local demo values; do not reuse them in deployments.
Do not commit `.env`, tokens or raw API keys. `X-API-Key` is currently only a limiter
bucket identifier; it does not authenticate a producer.

HTTP delivery requires an explicit exact-URL allow-list and connection-time public
address validation. Redirects, remote JSON Schema references and unbounded HTTP
responses are not allowed. See [the threat model](docs/security/threat-model.md).

`mvn clean verify` generates a CycloneDX inventory at `target/bom.json` and runs
unit, infrastructure and packaged application tests. An SBOM is an inventory, not
an assurance that dependencies are free from vulnerabilities. Dependency convergence
is enforced; major dependency upgrades still require behavioral verification.

Release gates: JWT/RBAC, authenticated tenant propagation and row scoping, payload
limits, audit identity, webhook signing, secret rotation and infrastructure TLS/ACLs.
