# Security policy

Only the current development branch is maintained. No released version is certified
for production use. APIs require JWT authentication, role authorization and tenant scope.
Compose uses public development credentials and binds published ports to loopback;
run it only on a trusted local machine. See [authentication](docs/security/authentication.md).

Report a vulnerability through GitHub's private vulnerability reporting for this
repository, if enabled. Otherwise contact the repository owner privately before
publishing exploit details. Do not put credentials or customer payloads in issues.

Service database passwords are required environment variables. Compose credentials
and Grafana defaults are disposable local demo values; do not reuse them in deployments.
Do not commit `.env`, tokens or private keys. `X-API-Key` is ignored; producers use
OAuth2 client credentials. Production requires an explicit JWT issuer and JWKS endpoint.
No development authentication bypass or production credential default is provided.

HTTP delivery requires an explicit exact-URL allow-list and connection-time public
address validation. Redirects, remote JSON Schema references and unbounded HTTP
responses are not allowed. See [the threat model](docs/security/threat-model.md).

`mvn clean verify` generates a CycloneDX inventory at `target/bom.json` and runs
unit, infrastructure and packaged application tests. An SBOM is an inventory, not
an assurance that dependencies are free from vulnerabilities. Dependency convergence
is enforced; major dependency upgrades still require behavioral verification.

Remaining release gates: payload byte/depth limits, complete audit coverage, webhook
signing, managed credential rotation, replay quotas and infrastructure TLS/ACLs.
Tenant isolation is enforced by application SQL predicates, not database RLS. Kafka
writers and direct database/analytics credentials are trusted infrastructure identities.
