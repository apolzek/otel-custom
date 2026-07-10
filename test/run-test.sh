#!/usr/bin/env bash
# End-to-end test: telemetrygen (8 services) -> otelcol-custom -> Kafka (6 partitions),
# then a verifier asserts every service.name is confined to exactly one partition.
set -euo pipefail
cd "$(dirname "$0")"

KEEP=${KEEP:-0}

cleanup() {
  if [ "$KEEP" != "1" ]; then
    echo "--- tearing down (set KEEP=1 to keep the stack running) ---"
    docker compose --profile verify down -v --remove-orphans
  fi
}
trap cleanup EXIT

echo "--- starting kafka + collector ---"
docker compose up -d --build --wait kafka collector

echo "--- generating traces (8 services x 100 traces x 2 workers x 4 spans) ---"
docker compose up \
  telemetrygen-checkout telemetrygen-payments telemetrygen-inventory \
  telemetrygen-shipping telemetrygen-frontend telemetrygen-recommendations \
  telemetrygen-notifications telemetrygen-auth

echo "--- verifying partition affinity per service.name ---"
docker compose --profile verify run --build --rm verifier
