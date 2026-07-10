# otel-custom

Custom OpenTelemetry Collector distribution implementing
[opentelemetry-collector-contrib#49550](https://github.com/open-telemetry/opentelemetry-collector-contrib/issues/49550):
**partition Kafka trace messages by resource attributes** (e.g. `service.name`),
so every span of a given service always lands on the same Kafka partition.

This enables horizontally scaled *stateful* consumers behind Kafka — e.g. a
second collector tier running the spanmetrics connector, where cumulative
counters/histograms are only correct if each replica always sees the same
services.

## The feature

Implemented as a small patch to the upstream contrib `kafkaexporter`
(forked at `v0.156.0`, see [components/kafkaexporter/PATCH.md](components/kafkaexporter/PATCH.md)),
mirroring the existing `partition_metrics_by_resource_attributes` /
`partition_logs_by_resource_attributes` options, but with explicit attribute
selection so multiple attributes can be combined (like the loadbalancing
exporter's `routing_key: resource` + `routing_attributes`):

```yaml
exporters:
  kafka:
    brokers: [kafka:9092]
    partition_traces_by_resource_attributes: [service.name]
    # or combine attributes:
    # partition_traces_by_resource_attributes: [service.name, deployment.environment]
```

Semantics:

- Spans are grouped per resource; the Kafka record key is a hash
  (`pdatautil.MapHash`) of the values of the configured resource attributes.
  Same values → same key → same partition.
- Attributes absent from a resource are ignored; if none are present the key
  is left unset and the record falls back to the client's default partitioning.
- Mutually exclusive with `partition_traces_by_id` and
  `traces::message_key_from_metadata_key` (validated at startup).
- No effect for Jaeger encodings (they always key by trace ID), matching the
  existing `partition_traces_by_id` behavior.

## Repository layout

```
components/kafkaexporter/  fork of contrib exporter/kafkaexporter v0.156.0 + the feature (with tests)
collector/                 OCB builder config, collector config, Dockerfile
test/                      docker-compose end-to-end scenario + partition verifier
upstream/                  changelog entry (.chloggen) ready for the upstream PR
```

## Build the collector

```sh
go install go.opentelemetry.io/collector/cmd/builder@v0.156.0
cd collector
builder --config builder-config.yaml
./_build/otelcol-custom --config config.yaml
```

Or via Docker (context must be the repo root):

```sh
docker build -f collector/Dockerfile -t otelcol-custom .
```

## End-to-end test

`test/run-test.sh` spins up, via docker compose:

1. **Kafka** (single-node KRaft, `apache/kafka:3.9.1`) with topic `otlp_spans`
   pre-created with **6 partitions** (auto-creation disabled on purpose);
2. the **custom collector** (OTLP in → kafka exporter with
   `partition_traces_by_resource_attributes: [service.name]`, batch processor
   deliberately in the pipeline to prove mixed-service payloads are split
   correctly);
3. **telemetrygen** for 8 different `service.name`s (checkout, payments,
   inventory, shipping, frontend, recommendations, notifications, auth), 100
   traces × 2 workers × 4 spans each;
4. a **verifier** that consumes every partition, decodes the OTLP payloads and
   asserts each `service.name` appears in exactly **one** partition.

```sh
cd test
./run-test.sh          # tears the stack down at the end
KEEP=1 ./run-test.sh   # keeps kafka/collector running for inspection
```

Expected verifier output:

```
SERVICE.NAME             PARTITIONS   SPANS PER PARTITION
auth                     1            p3=800    [OK]
checkout                 1            p0=800    [OK]
...
PASS: every service.name is confined to exactly one partition
```

## Unit tests (contrib standards)

```sh
cd components/kafkaexporter
go test ./...
```

New coverage in the fork: config loading of the new option, validation of the
mutually exclusive combinations, same-service → same-key (across resources
that differ in other attributes), multi-attribute keys, missing-attribute →
nil key fallback, and the Jaeger-encoding override.

## Upstreaming

The patch is intentionally shaped as an upstream PR against
`exporter/kafkaexporter`: minimal diff, follows existing naming/behavior
conventions, includes tests, README, `config.schema.yaml`, and a `.chloggen`
entry (`upstream/`). See `components/kafkaexporter/PATCH.md` for the exact
file-by-file delta.

## License

The `components/kafkaexporter` directory is derived from
[opentelemetry-collector-contrib](https://github.com/open-telemetry/opentelemetry-collector-contrib)
and remains Apache-2.0.
