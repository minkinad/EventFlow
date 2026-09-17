#!/usr/bin/env bash
# Local crash/restart smoke exercise. Does not claim accepted-ID reconciliation under load.
set -euo pipefail
cd "$(dirname "$0")/../.."
service=${1:?Specify ingestion-service, processing-service, delivery-service, kafka, postgres or clickhouse}
case "$service" in ingestion-service|processing-service|delivery-service|kafka|postgres|clickhouse) ;; *) exit 2;; esac
restore() { docker compose up -d "$service"; }
trap restore EXIT
docker compose kill -s SIGKILL "$service"
sleep 5
restore
trap - EXIT
./scripts/smoke-test.sh
