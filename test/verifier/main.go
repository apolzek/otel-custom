// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Command verifier consumes OTLP trace messages from a Kafka topic and
// verifies that every group of resource attribute values (GROUP_BY, default
// service.name) is confined to exactly one partition, i.e. that the kafka
// exporter's partition_traces_by_resource_attributes feature routes all spans
// sharing the configured attribute values to the same partition. Optionally
// writes a JSON report (OUTPUT_JSON) for manual validation.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

type groupReport struct {
	Attributes map[string]string `json:"attributes"`
	Partitions map[string]int    `json:"spans_per_partition"`
	Spans      int               `json:"spans"`
	OK         bool              `json:"ok"`
}

type report struct {
	Topic               string         `json:"topic"`
	GroupBy             []string       `json:"group_by"`
	ConsumedAt          time.Time      `json:"consumed_at"`
	Records             int            `json:"records"`
	KeyedRecords        int            `json:"keyed_records"`
	RecordsPerPartition map[string]int `json:"records_per_partition"`
	Groups              []groupReport  `json:"groups"`
	Pass                bool           `json:"pass"`
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func durationEnv(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			fmt.Fprintf(os.Stderr, "invalid %s=%q: %v\n", key, v, err)
			os.Exit(2)
		}
		return d
	}
	return fallback
}

func main() {
	brokers := strings.Split(env("KAFKA_BROKERS", "kafka:9092"), ",")
	topic := env("KAFKA_TOPIC", "otlp_spans")
	groupBy := strings.Split(env("GROUP_BY", "service.name"), ",")
	outputJSON := os.Getenv("OUTPUT_JSON")
	quietPeriod := durationEnv("QUIET_PERIOD", 15*time.Second)
	timeout := durationEnv("TIMEOUT", 3*time.Minute)

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create kafka client: %v\n", err)
		os.Exit(2)
	}
	defer client.Close()

	unmarshaler := &ptrace.ProtoUnmarshaler{}
	// composite group key -> partition -> span count
	groups := map[string]map[int32]int{}
	groupAttrs := map[string]map[string]string{}
	partitions := map[int32]int{}
	records := 0
	keyedRecords := 0

	fmt.Printf("consuming topic %q from %v, grouping by %v (quiet period %s, timeout %s)\n",
		topic, brokers, groupBy, quietPeriod, timeout)

	deadline := time.Now().Add(timeout)
	lastRecord := time.Now()
	for time.Now().Before(deadline) && time.Since(lastRecord) < quietPeriod {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		fetches := client.PollFetches(ctx)
		cancel()
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, fe := range errs {
				if fe.Err == context.DeadlineExceeded || fe.Err == context.Canceled {
					continue
				}
				fmt.Fprintf(os.Stderr, "fetch error on %s/%d: %v\n", fe.Topic, fe.Partition, fe.Err)
			}
		}
		fetches.EachRecord(func(r *kgo.Record) {
			lastRecord = time.Now()
			records++
			if len(r.Key) > 0 {
				keyedRecords++
			}
			partitions[r.Partition]++
			traces, err := unmarshaler.UnmarshalTraces(r.Value)
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to unmarshal record at %s/%d offset %d: %v\n",
					r.Topic, r.Partition, r.Offset, err)
				return
			}
			for _, rs := range traces.ResourceSpans().All() {
				attrs := map[string]string{}
				keyParts := make([]string, 0, len(groupBy))
				for _, attrKey := range groupBy {
					value := "<absent>"
					if v, ok := rs.Resource().Attributes().Get(attrKey); ok {
						value = v.AsString()
					}
					attrs[attrKey] = value
					keyParts = append(keyParts, attrKey+"="+value)
				}
				key := strings.Join(keyParts, "|")
				spans := 0
				for _, ss := range rs.ScopeSpans().All() {
					spans += ss.Spans().Len()
				}
				if groups[key] == nil {
					groups[key] = map[int32]int{}
					groupAttrs[key] = attrs
				}
				groups[key][r.Partition] += spans
			}
		})
	}

	fmt.Printf("\nconsumed %d records (%d with a partition key) across %d partitions\n\n",
		records, keyedRecords, len(partitions))

	if records == 0 {
		fmt.Fprintln(os.Stderr, "FAIL: no records consumed")
		os.Exit(1)
	}

	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	rep := report{
		Topic:               topic,
		GroupBy:             groupBy,
		ConsumedAt:          time.Now().UTC(),
		Records:             records,
		KeyedRecords:        keyedRecords,
		RecordsPerPartition: map[string]int{},
		Pass:                true,
	}
	for id, n := range partitions {
		rep.RecordsPerPartition[fmt.Sprintf("%d", id)] = n
	}

	fmt.Printf("%-72s %-12s %s\n", "GROUP", "PARTITIONS", "SPANS PER PARTITION")
	for _, key := range keys {
		parts := groups[key]
		ids := make([]int32, 0, len(parts))
		for id := range parts {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		var detail []string
		spans := 0
		perPartition := map[string]int{}
		for _, id := range ids {
			detail = append(detail, fmt.Sprintf("p%d=%d", id, parts[id]))
			spans += parts[id]
			perPartition[fmt.Sprintf("%d", id)] = parts[id]
		}
		ok := len(parts) == 1
		if !ok {
			rep.Pass = false
		}
		status := "OK"
		if !ok {
			status = "FAIL"
		}
		fmt.Printf("%-72s %-12d %-24s [%s]\n", key, len(parts), strings.Join(detail, " "), status)
		rep.Groups = append(rep.Groups, groupReport{
			Attributes: groupAttrs[key],
			Partitions: perPartition,
			Spans:      spans,
			OK:         ok,
		})
	}

	if outputJSON != "" {
		data, err := json.MarshalIndent(rep, "", "  ")
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to marshal report: %v\n", err)
			os.Exit(2)
		}
		if err := os.WriteFile(outputJSON, append(data, '\n'), 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "failed to write %s: %v\n", outputJSON, err)
			os.Exit(2)
		}
		fmt.Printf("\nJSON report written to %s\n", outputJSON)
	}

	fmt.Println()
	if !rep.Pass {
		fmt.Println("FAIL: at least one group was spread across multiple partitions")
		os.Exit(1)
	}
	if len(partitions) < 2 {
		fmt.Println("WARN: all groups landed on a single partition; " +
			"partition affinity holds, but the run cannot demonstrate distribution " +
			"(try more groups or fewer partitions)")
	}
	fmt.Println("PASS: every group is confined to exactly one partition")
}
