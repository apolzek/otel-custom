# How to integrate the patched kafka exporter into a custom Collector build

This guide shows how to build your own OpenTelemetry Collector distribution
that ships the forked `kafkaexporter` from this repository (which adds
`partition_traces_by_resource_attributes`, see
[components/kafkaexporter/PATCH.md](components/kafkaexporter/PATCH.md)).

The official way to assemble a custom Collector is the
[OpenTelemetry Collector Builder (OCB)](https://opentelemetry.io/docs/collector/custom-collector/).
OCB reads a manifest (`builder-config.yaml`), generates a `main.go` with the
components you list, and compiles the binary. A Go `replace` directive is what
swaps the upstream exporter for the local fork.

## Prerequisites

- Go >= 1.25
- OCB matching the Collector core version used by the exporter fork. The fork
  is based on contrib `v0.156.0`, which pairs with core `v0.156.0` / `v1.62.0`:

```sh
go install go.opentelemetry.io/collector/cmd/builder@v0.156.0
```

> Keeping OCB, core modules, and contrib modules on the same minor version is
> not optional — mixed versions routinely fail to compile.

## 1. Write the builder manifest

The manifest in this repo ([collector/builder-config.yaml](collector/builder-config.yaml))
is a minimal working example:

```yaml
dist:
  module: github.com/apolzek/otel-custom/collector
  name: otelcol-custom
  version: 0.156.0-dev
  output_path: ./_build

receivers:
  - gomod: go.opentelemetry.io/collector/receiver/otlpreceiver v0.156.0

processors:
  - gomod: go.opentelemetry.io/collector/processor/batchprocessor v0.156.0

exporters:
  - gomod: go.opentelemetry.io/collector/exporter/debugexporter v0.156.0
  - gomod: github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter v0.156.0

extensions:
  - gomod: github.com/open-telemetry/opentelemetry-collector-contrib/extension/healthcheckextension v0.156.0

replaces:
  - github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter => ../../components/kafkaexporter
```

Key points:

- The exporter is still declared with its **upstream module path and version**
  (`.../exporter/kafkaexporter v0.156.0`). That keeps the component name
  (`kafka`) and every transitive dependency resolution identical to upstream.
- The `replaces` entry redirects that module to the local fork. **The path is
  relative to `dist::output_path`** (the directory where OCB writes the
  generated `go.mod`), not to the manifest file. With `output_path: ./_build`
  under `collector/`, the fork at `components/kafkaexporter` is two levels up:
  `../../components/kafkaexporter`.
- An absolute path also works and is less surprising in CI:
  `=> /src/components/kafkaexporter`.
- Add any other receivers/processors/exporters your pipelines need; anything
  not listed will not exist in the binary.

## 2. Build and verify

```sh
cd collector
builder --config builder-config.yaml
```

Verify the binary actually carries the fork:

```sh
# the component must be registered
./_build/otelcol-custom components | grep -A2 'name: kafka'

# the new option must pass config validation
./_build/otelcol-custom validate --config config.yaml
```

If `validate` rejects `partition_traces_by_resource_attributes`, the replace
did not take effect — check the relative path (mistakes silently fall back to
the upstream module only when the path exists but points elsewhere; a missing
path fails the build with `replacement directory does not exist`).

## 3. Configure the exporter

```yaml
exporters:
  kafka:
    brokers: [kafka:9092]
    traces:
      topic: otlp_spans
    partition_traces_by_resource_attributes:
      - service.name
      # combine attributes for a finer sharding key if needed:
      # - deployment.environment

service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [batch]
      exporters: [kafka]
```

Operational notes:

- **Create the topic with multiple partitions yourself.** Broker-side
  auto-creation defaults to 1 partition, which makes the feature useless and
  is easy to miss. The e2e test disables auto-creation on purpose.
- The default `record_partitioner` (`sticky_key` with the `sarama_compat`
  hasher) is what maps key → partition. Any key-hashing strategy preserves
  the guarantee "same key ⇒ same partition"; `round_robin`/`least_backup`
  ignore keys and must not be combined with this feature.
- The number of partitions bounds consumer parallelism: N partitions serve at
  most N consumer instances in one group. Services are distributed over
  partitions by hash, so expect some partitions to carry more than one
  service (that is fine — each service still sticks to a single partition).
- Repartitioning a topic (changing partition count) changes the key → partition
  mapping and will re-shuffle services across consumers. Stateful consumers
  (e.g. spanmetrics) should expect a reset when that happens.
- Mutually exclusive with `partition_traces_by_id` and
  `traces::message_key_from_metadata_key`; Jaeger encodings ignore the option
  (they always key by trace ID).

## 4. Docker image (optional)

[collector/Dockerfile](collector/Dockerfile) does the same build in a
multi-stage image. The build context must be the repository root so both
`collector/` and `components/` are visible:

```sh
docker build -f collector/Dockerfile -t otelcol-custom .
```

## 5. Consuming with a git fork instead of a local path

If your build pipeline cannot vendor this repository next to the manifest,
point the replace at a git fork of contrib instead:

```yaml
replaces:
  - github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter => github.com/<you>/opentelemetry-collector-contrib/exporter/kafkaexporter <commit-or-pseudo-version>
```

The target must be a Go module (the fork keeps upstream's module path layout),
and the referenced commit must contain the patch.

## 6. Smoke test

The repo ships an end-to-end scenario that proves partition affinity with
real Kafka + telemetrygen traffic:

```sh
cd test
./run-test.sh
```

Expected final output:

```
PASS: every service.name is confined to exactly one partition
```
