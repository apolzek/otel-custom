// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Command verifier consumes OTLP trace messages from a Kafka topic and
// verifies that every service.name is confined to exactly one partition,
// i.e. that the kafka exporter's partition_traces_by_resource_attributes
// feature routes all spans of a service to the same partition.
package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

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
	// service.name -> partition -> span count
	services := map[string]map[int32]int{}
	partitions := map[int32]int{}
	records := 0
	keyedRecords := 0

	fmt.Printf("consuming topic %q from %v (quiet period %s, timeout %s)\n",
		topic, brokers, quietPeriod, timeout)

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
				service := "<no service.name>"
				if v, ok := rs.Resource().Attributes().Get("service.name"); ok {
					service = v.Str()
				}
				spans := 0
				for _, ss := range rs.ScopeSpans().All() {
					spans += ss.Spans().Len()
				}
				if services[service] == nil {
					services[service] = map[int32]int{}
				}
				services[service][r.Partition] += spans
			}
		})
	}

	fmt.Printf("\nconsumed %d records (%d with a partition key) across %d partitions\n\n",
		records, keyedRecords, len(partitions))

	if records == 0 {
		fmt.Fprintln(os.Stderr, "FAIL: no records consumed")
		os.Exit(1)
	}

	names := make([]string, 0, len(services))
	for name := range services {
		names = append(names, name)
	}
	sort.Strings(names)

	failed := false
	fmt.Printf("%-24s %-12s %s\n", "SERVICE.NAME", "PARTITIONS", "SPANS PER PARTITION")
	for _, name := range names {
		parts := services[name]
		ids := make([]int32, 0, len(parts))
		for id := range parts {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		var detail []string
		for _, id := range ids {
			detail = append(detail, fmt.Sprintf("p%d=%d", id, parts[id]))
		}
		status := "OK"
		if len(parts) != 1 {
			status = "FAIL"
			failed = true
		}
		fmt.Printf("%-24s %-12d %-40s [%s]\n", name, len(parts), strings.Join(detail, " "), status)
	}

	fmt.Println()
	if failed {
		fmt.Println("FAIL: at least one service.name was spread across multiple partitions")
		os.Exit(1)
	}
	if len(partitions) < 2 {
		fmt.Println("WARN: all services landed on a single partition; " +
			"partition affinity holds, but the run cannot demonstrate distribution " +
			"(try more services or fewer partitions)")
	}
	fmt.Println("PASS: every service.name is confined to exactly one partition")
}
