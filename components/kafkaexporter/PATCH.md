# Fork notice

This directory is a fork of the upstream OpenTelemetry Collector Contrib
`exporter/kafkaexporter` module, pinned at release **v0.156.0**
(tag `exporter/kafkaexporter/v0.156.0`, commit `41e24cd516dd69a5b4277465cdb2ff4ef0676f49`),
extracted from the Go module proxy.

Upstream: https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter/kafkaexporter

License: Apache-2.0 (unchanged, same as upstream).

## Local changes vs upstream v0.156.0

Implements https://github.com/open-telemetry/opentelemetry-collector-contrib/issues/49550 —
partition trace messages by resource attributes (e.g. `service.name`), so all
spans of a service land on the same Kafka partition. This enables horizontally
scaled stateful consumers (e.g. spanmetrics) behind Kafka.

New config option:

```yaml
exporters:
  kafka:
    partition_traces_by_resource_attributes: [service.name]
```

Multiple attributes may be combined; the record key is the hash
(`pdatautil.MapHash`) of the values of the configured resource attributes,
mirroring the existing `partition_metrics_by_resource_attributes` /
`partition_logs_by_resource_attributes` behavior, but with explicit attribute
selection as proposed in the issue.

Files changed:

- `go.mod`: removed relative `replace` directives (siblings are resolved from
  the Go module proxy at v0.156.0) — build plumbing only, not part of the
  upstream proposal.
- `config.go`: new `PartitionTracesByResourceAttributes []string` field
  (`partition_traces_by_resource_attributes`), validation: mutually exclusive
  with `partition_traces_by_id` and `traces::message_key_from_metadata_key`.
- `kafka_exporter.go`: traces `partitionData` splits per resource and derives
  the record key via new helper `resourceAttributesHashKey`; disabled for
  Jaeger encodings (which always key by trace ID), like
  `partition_traces_by_id`.
- `config_test.go`, `kafka_exporter_test.go`, `testdata/config.yaml`,
  `testdata/config-partitioning-failed.yaml`: tests for config loading,
  validation errors, key derivation (same service → same key, multi-attribute,
  missing attribute → nil key, Jaeger override).
- `README.md`, `config.schema.yaml`: documentation.

The upstream changelog entry for the eventual PR lives at
`../../upstream/chloggen-kafkaexporter-partition-traces-by-resource-attributes.yaml`.
