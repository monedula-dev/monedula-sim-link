package mcpserver

import (
	"context"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/monedula-dev/monedula-sim-link/mapping"
	"github.com/monedula-dev/monedula-sim-link/simurl"
)

// startFake spins an in-process fake Kafka cluster (franz-go kfake) with the
// given seed topics and produces a fixed set of records onto chosen partitions,
// returning the bootstrap addresses. It mirrors the snapshot/cmd fixtures so
// the cluster_to_url tool is exercised end to end over the real client path.
func startFake(t *testing.T, opts []kfake.Opt, records []*kgo.Record) []string {
	t.Helper()
	cluster, err := kfake.NewCluster(opts...)
	if err != nil {
		t.Fatalf("kfake: %v", err)
	}
	t.Cleanup(cluster.Close)

	if len(records) > 0 {
		producer, err := kgo.NewClient(
			kgo.SeedBrokers(cluster.ListenAddrs()...),
			kgo.RecordPartitioner(kgo.ManualPartitioner()),
		)
		if err != nil {
			t.Fatalf("producer: %v", err)
		}
		defer producer.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := producer.ProduceSync(ctx, records...).FirstErr(); err != nil {
			t.Fatalf("seed produce: %v", err)
		}
	}
	return cluster.ListenAddrs()
}

func standardFake(t *testing.T) []string {
	return startFake(t,
		[]kfake.Opt{
			kfake.NumBrokers(3),
			kfake.SeedTopics(2, "orders"),
			kfake.SeedTopics(3, "payments"),
			kfake.SeedTopics(1, "_hidden"),
		},
		[]*kgo.Record{
			{Topic: "orders", Partition: 0, Value: []byte("a")},
			{Topic: "orders", Partition: 0, Value: []byte("b")},
			{Topic: "orders", Partition: 1, Value: []byte("c")},
			{Topic: "payments", Partition: 2, Value: []byte("d")},
		},
	)
}

// TestClusterToURLEndToEnd drives the tool over the in-memory MCP transport
// against a kfake cluster and asserts the URL's decoded action log, the size
// param and the name tables match the faked cluster.
func TestClusterToURLEndToEnd(t *testing.T) {
	brokers := standardFake(t)
	s := newSession(t)

	res := callTool(t, s, "cluster_to_url", map[string]any{"brokers": brokers})
	var out ClusterToURLOutput
	structured(t, res, &out)

	// Same shape the CLI end-to-end test asserts: RF = min(3, brokers) = 3,
	// orders 2 partitions (primary), payments 3 (extra), _hidden excluded.
	// kfake's default topic config resolves retention.ms=604800000 (7d) for
	// every topic (min.insync.replicas=1, cleanup.policy=delete); the tail
	// carries the primary's (index 10) and payments' (index 0) real
	// DescribeConfigs values, retention.bytes stays empty (kfake's -1/disabled
	// sentinel is treated as unset, see snapshot/kafka.go).
	const wantSize = "3x2x3xpayments~3~3~1~d~604800000~~~xxxxxxxxxx1~d~604800000~~"
	if out.Size != wantSize {
		t.Fatalf("size = %q, want %q", out.Size, wantSize)
	}
	if out.Cluster != "" {
		t.Fatalf("single-dc must omit the cluster param, got %q", out.Cluster)
	}
	if got := sizeParamOf(t, out.URL); got != wantSize {
		t.Fatalf("URL size param = %q, want %q", got, wantSize)
	}

	// Decode the URL's actions and assert the deterministic log: template
	// producer removed, then per-topic keyed produce bursts (no reassign/kill
	// since 3 brokers at RF 3 are all in-sync).
	entries, err := simurl.DecodeActionsFromURL(actionsParamOf(t, out.URL))
	if err != nil {
		t.Fatalf("actions decode: %v", err)
	}
	if len(entries) != len(out.DecodedActions) {
		t.Fatalf("URL has %d actions but echo has %d", len(entries), len(out.DecodedActions))
	}
	var kinds []string
	producesPerTopicPartition := map[string]int{}
	for _, e := range out.DecodedActions {
		kinds = append(kinds, e.Kind)
		if e.Kind == "produce" {
			if e.Key == nil {
				t.Fatalf("keyless produce echoed: %+v", e)
			}
			count := 2
			if e.Topic == "payments" {
				count = 3
			}
			pid := mapping.PartitionForKey(*e.Key, count)
			producesPerTopicPartition[e.Topic+"/"+strconv.Itoa(pid)]++
		}
	}
	wantKinds := "remove_producer,add_producer,produce,produce,produce,remove_producer,add_producer,produce,remove_producer"
	if strings.Join(kinds, ",") != wantKinds {
		t.Fatalf("action kinds = %v, want %v", kinds, wantKinds)
	}
	wantProduces := map[string]int{"orders/0": 2, "orders/1": 1, "payments/2": 1}
	if len(producesPerTopicPartition) != len(wantProduces) {
		t.Fatalf("stray produces: %v", producesPerTopicPartition)
	}
	for k, want := range wantProduces {
		if producesPerTopicPartition[k] != want {
			t.Fatalf("produce distribution = %v, want %v", producesPerTopicPartition, wantProduces)
		}
	}

	// Name tables echo the real→simulator identity mapping for this fixture.
	// kfake numbers brokers from node id 0, mapped to broker-1..broker-3.
	if got := nameTable(out.BrokerNames); got["0"] != "broker-1" || got["2"] != "broker-3" {
		t.Fatalf("broker names = %v", got)
	}
	if got := nameTable(out.TopicNames); got["orders"] != "orders" || got["payments"] != "payments" {
		t.Fatalf("topic names = %v", got)
	}
}

// TestClusterToURLSelectionAndErrors covers exact selection plus the actionable
// error cases: empty selection, over-cap without clamp, and a bad regex.
func TestClusterToURLSelectionAndErrors(t *testing.T) {
	brokers := standardFake(t)
	s := newSession(t)

	// Exact selection narrows to a single topic (payments becomes the primary
	// placeholder note; orders is dropped).
	res := callTool(t, s, "cluster_to_url", map[string]any{
		"brokers": brokers,
		"topics":  []string{"orders"},
	})
	var out ClusterToURLOutput
	structured(t, res, &out)
	if nameTable(out.TopicNames)["orders"] != "orders" {
		t.Fatalf("exact selection topic table = %v", out.TopicNames)
	}
	if _, ok := nameTable(out.TopicNames)["payments"]; ok {
		t.Fatalf("payments should not be selected: %v", out.TopicNames)
	}

	// Empty selection: a regex that matches no topic must be an actionable
	// error, never a silent empty link.
	res = callTool(t, s, "cluster_to_url", map[string]any{
		"brokers":     brokers,
		"topicsRegex": "no-such-topic-xyz",
	})
	wantToolError(t, res, "matched nothing")

	// Bad regex is rejected up front.
	res = callTool(t, s, "cluster_to_url", map[string]any{
		"brokers":     brokers,
		"topicsRegex": "(unclosed",
	})
	wantToolError(t, res, "regexp")
}

// TestClusterToURLConsumerGroup joins a real consumer group on the fake
// cluster (test scaffolding only — cluster_to_url itself never joins a group
// or commits an offset) and forces its committed offsets down to 0 so the
// group has lag on every partition, then asserts the emitted URL carries an
// add_group + add_consumer + the earliest autoOffsetReset override, and that
// GroupNames echoes the mapping.
func TestClusterToURLConsumerGroup(t *testing.T) {
	brokers := standardFake(t)

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup("analytics"),
		kgo.ConsumeTopics("orders"),
		kgo.ClientID("orders-consumer-1"),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	defer consumer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	seen := map[int32]bool{}
	for len(seen) < 2 {
		fetches := consumer.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			t.Fatalf("poll fetches: %v", errs)
		}
		fetches.EachRecord(func(r *kgo.Record) { seen[r.Partition] = true })
	}
	// Force the committed offset back to 0 on both partitions: guaranteed lag
	// regardless of how many records this poll happened to fetch.
	committed := make(chan error, 1)
	consumer.CommitOffsets(ctx, map[string]map[int32]kgo.EpochOffset{
		"orders": {0: {Epoch: -1, Offset: 0}, 1: {Epoch: -1, Offset: 0}},
	}, func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, _ *kmsg.OffsetCommitResponse, err error) {
		committed <- err
	})
	select {
	case err := <-committed:
		if err != nil {
			t.Fatalf("commit: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("commit did not complete")
	}

	s := newSession(t)
	res := callTool(t, s, "cluster_to_url", map[string]any{"brokers": brokers})
	var out ClusterToURLOutput
	structured(t, res, &out)

	if nameTable(out.GroupNames)["analytics"] != "analytics" {
		t.Fatalf("group names = %v", out.GroupNames)
	}
	var sawAddGroup, sawAddConsumer, sawEarliestReset bool
	for _, e := range out.DecodedActions {
		switch e.Kind {
		case "add_group":
			if e.GroupID == "analytics" {
				sawAddGroup = true
			}
		case "add_consumer":
			if e.GroupID == "analytics" && e.MemberID != "" {
				sawAddConsumer = true
			}
		case "config_change":
			if e.Path == "group.analytics.consumer.autoOffsetReset" && e.Value != nil && *e.Value == "earliest" {
				sawEarliestReset = true
			}
		}
	}
	if !sawAddGroup {
		t.Fatalf("no add_group for analytics: %+v", out.DecodedActions)
	}
	if !sawAddConsumer {
		t.Fatalf("no add_consumer for analytics: %+v", out.DecodedActions)
	}
	if !sawEarliestReset {
		t.Fatalf("no earliest autoOffsetReset override despite forced lag: %+v", out.DecodedActions)
	}
}

// TestClusterToURLOverCap proves the cap policy: a topic past the 24-partition
// cap fails with the violation unless clamp is set.
func TestClusterToURLOverCap(t *testing.T) {
	brokers := startFake(t,
		[]kfake.Opt{
			kfake.NumBrokers(3),
			kfake.SeedTopics(2, "orders"),
			kfake.SeedTopics(30, "big"),
		},
		nil,
	)
	s := newSession(t)

	res := callTool(t, s, "cluster_to_url", map[string]any{"brokers": brokers})
	wantToolError(t, res, "exceeds", "24")

	// clamp trims the over-cap topic deterministically and succeeds.
	res = callTool(t, s, "cluster_to_url", map[string]any{"brokers": brokers, "clamp": true})
	var out ClusterToURLOutput
	structured(t, res, &out)
	if !strings.HasPrefix(out.Size, "3x2x3x") {
		t.Fatalf("clamped size = %q", out.Size)
	}
	var clamped bool
	for _, w := range out.Warnings {
		if strings.Contains(w, "big") && strings.Contains(w, "24") {
			clamped = true
		}
	}
	if !clamped {
		t.Fatalf("clamp should warn about the trimmed topic: %v", out.Warnings)
	}
}

// TestClusterToURLWideSelection proves a selection wider than the topic cap is
// not a failure: the listed topic is kept, the free slots are ranked, and a
// warning names what was kept and dropped.
func TestClusterToURLWideSelection(t *testing.T) {
	brokers := startFake(t,
		[]kfake.Opt{
			kfake.NumBrokers(3),
			kfake.SeedTopics(1, "t1", "t2", "t3", "t4", "t5", "t6", "t7", "t8"),
		},
		nil,
	)
	s := newSession(t)

	res := callTool(t, s, "cluster_to_url", map[string]any{
		"brokers": brokers, "topics": []string{"t8"}, "topicsRegex": "^t",
	})
	var out ClusterToURLOutput
	structured(t, res, &out)
	kept := map[string]bool{}
	for _, m := range out.TopicNames {
		kept[m.Real] = true
	}
	if len(kept) != 6 || !kept["t8"] || kept["t6"] || kept["t7"] {
		t.Fatalf("want t1..t5 plus the listed t8, got %v", out.TopicNames)
	}
	var warned bool
	for _, w := range out.Warnings {
		if strings.Contains(w, "t8 (listed in --topics)") && strings.Contains(w, "dropping t6, t7") {
			warned = true
		}
	}
	if !warned {
		t.Fatalf("want a keep/drop warning, got %v", out.Warnings)
	}
}

func sizeParamOf(t *testing.T, rawURL string) string {
	t.Helper()
	u, err := url.Parse(rawURL)
	if err != nil {
		t.Fatalf("generated URL does not parse: %v", err)
	}
	return u.Query().Get("size")
}

func nameTable(m []NameMapping) map[string]string {
	out := make(map[string]string, len(m))
	for _, nm := range m {
		out[nm.Real] = nm.Simulator
	}
	return out
}
