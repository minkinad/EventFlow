#!/usr/bin/env bash
set -euo pipefail
exec "$(dirname "$0")/fail-service.sh" delivery-service
