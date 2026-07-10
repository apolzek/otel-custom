#!/usr/bin/env bash
# Scale test for partition_traces_by_resource_attributes:
#   scenario A: 1000 spans across 20 distinct service.name  -> topic otlp_spans
#               (partition key: service.name)
#   scenario B: 1000 spans across 10 service.name x 2 environments, each
#               resource carrying 3 extra attributes                 -> topic otlp_spans_multi
#               (partition key: service.name + deployment.environment
#                + service.namespace + cloud.region)
# A consumer-side verifier decodes every partition and asserts each group is
# confined to exactly one partition, writing JSON reports to test/results/
# for manual validation. Set DISCORD_WEBHOOK to post the evidence to Discord.
set -euo pipefail
cd "$(dirname "$0")"

TELEMETRYGEN_IMAGE=ghcr.io/open-telemetry/opentelemetry-collector-contrib/telemetrygen:v0.156.0
NETWORK=otel-custom-partition-test_default
mkdir -p results

echo "--- (re)starting kafka + collector with clean topics ---"
docker compose --profile verify down -v --remove-orphans
docker compose up -d --build --wait kafka collector

# gen ENDPOINT SERVICE [extra telemetrygen args...]
# 25 traces x (1 parent + 1 child span) = 50 spans per invocation.
gen() {
  local endpoint=$1 service=$2
  shift 2
  docker run --rm --network "$NETWORK" "$TELEMETRYGEN_IMAGE" traces \
    --otlp-endpoint "$endpoint" --otlp-insecure \
    --service "$service" --traces 25 --child-spans 1 --workers 1 "$@" \
    >/dev/null 2>&1
}

echo "--- scenario A: 20 service.name x 50 spans = 1000 spans -> otlp_spans ---"
pids=()
for i in $(seq -w 1 20); do
  gen collector:4317 "svc-$i" &
  pids+=($!)
done
for pid in "${pids[@]}"; do wait "$pid"; done
echo "scenario A load sent"

echo "--- scenario B: (10 service.name x 2 envs) x 50 spans = 1000 spans -> otlp_spans_multi ---"
pids=()
for i in $(seq -w 1 10); do
  region="us-east-$((10#$i % 2 + 1))"
  for environment in prod staging; do
    gen collector:4319 "app-$i" \
      --otlp-attributes "deployment.environment=\"$environment\"" \
      --otlp-attributes "service.namespace=\"shop\"" \
      --otlp-attributes "cloud.region=\"$region\"" &
    pids+=($!)
  done
done
for pid in "${pids[@]}"; do wait "$pid"; done
echo "scenario B load sent"

echo "--- verifying scenario A (group by service.name) ---"
KAFKA_TOPIC=otlp_spans \
GROUP_BY=service.name \
OUTPUT_JSON=/results/scenario-a-by-service.json \
  docker compose --profile verify run --build --rm verifier \
  | tee results/scenario-a-by-service.txt

echo "--- verifying scenario B (group by service.name + 3 attributes) ---"
KAFKA_TOPIC=otlp_spans_multi \
GROUP_BY=service.name,deployment.environment,service.namespace,cloud.region \
OUTPUT_JSON=/results/scenario-b-multiattr.json \
  docker compose --profile verify run --rm verifier \
  | tee results/scenario-b-multiattr.txt

echo
echo "JSON reports for manual validation:"
ls -l results/*.json

if [ -n "${DISCORD_WEBHOOK:-}" ]; then
  echo "--- posting evidence to discord ---"
  summary_a=$(grep -E "^consumed|^PASS|^FAIL" results/scenario-a-by-service.txt | head -3)
  summary_b=$(grep -E "^consumed|^PASS|^FAIL" results/scenario-b-multiattr.txt | head -3)
  payload=$(SUMMARY_A="$summary_a" SUMMARY_B="$summary_b" python3 - << 'PY'
import json
import os

content = (
    "\U0001F9EA **[otel-custom] Scale test results**\n\n"
    "**Scenario A** — 1000 spans, 20 service.name, key=[service.name], topic otlp_spans:\n"
    "```\n" + os.environ["SUMMARY_A"] + "\n```\n"
    "**Scenario B** — 1000 spans, 10 service.name x 2 envs + 3 attributes, "
    "key=[service.name, deployment.environment, service.namespace, cloud.region], topic otlp_spans_multi:\n"
    "```\n" + os.environ["SUMMARY_B"] + "\n```\n"
    "Full verifier output + JSON reports attached (also saved in test/results/)."
)
print(json.dumps({"content": content}))
PY
)
  curl -sS -X POST "$DISCORD_WEBHOOK" \
    -F "payload_json=$payload" \
    -F "file1=@results/scenario-a-by-service.json;type=application/json" \
    -F "file2=@results/scenario-b-multiattr.json;type=application/json" \
    -F "file3=@results/scenario-a-by-service.txt;type=text/plain" \
    -F "file4=@results/scenario-b-multiattr.txt;type=text/plain" \
    -o /dev/null -w "discord: HTTP %{http_code}\n"
fi

echo "--- done (stack left running; docker compose down -v to clean up) ---"
