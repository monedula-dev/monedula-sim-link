package snapshot

import (
	"context"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// startFake spins an in-process fake Kafka cluster (franz-go kfake): 3
// brokers, orders(2 partitions) / payments(3) / _hidden(1), with a few records
// placed on chosen partitions via the manual partitioner.
func startFake(t *testing.T) *kfake.Cluster {
	t.Helper()
	cluster, err := kfake.NewCluster(
		kfake.NumBrokers(3),
		kfake.SeedTopics(2, "orders"),
		kfake.SeedTopics(3, "payments"),
		kfake.SeedTopics(1, "_hidden"),
	)
	if err != nil {
		t.Fatalf("kfake: %v", err)
	}
	t.Cleanup(cluster.Close)

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
	records := []*kgo.Record{
		{Topic: "orders", Partition: 0, Value: []byte("a")},
		{Topic: "orders", Partition: 0, Value: []byte("b")},
		{Topic: "orders", Partition: 1, Value: []byte("c")},
		{Topic: "payments", Partition: 2, Value: []byte("d")},
	}
	if err := producer.ProduceSync(ctx, records...).FirstErr(); err != nil {
		t.Fatalf("seed produce: %v", err)
	}
	return cluster
}

func fetch(t *testing.T, cluster *kfake.Cluster, sel Selection) (ClusterSnapshot, []string, error) {
	t.Helper()
	client, err := Connect(ConnectConfig{Brokers: cluster.ListenAddrs()})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(client.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return client.Fetch(ctx, sel)
}

func TestFetchSnapshotsFakeCluster(t *testing.T) {
	cluster := startFake(t)
	snap, warnings, err := fetch(t, cluster, Selection{})
	if err != nil {
		t.Fatalf("fetch: %v (warnings %v)", err, warnings)
	}
	if len(snap.Brokers) != 3 {
		t.Fatalf("brokers: %+v", snap.Brokers)
	}
	for _, b := range snap.Brokers {
		if !b.Online {
			t.Fatalf("broker %s should be online", b.ID)
		}
	}
	if len(snap.Topics) != 2 || snap.Topics[0].Name != "orders" || snap.Topics[1].Name != "payments" {
		t.Fatalf("default selection must be the sorted non-internal topics: %+v", snap.Topics)
	}
	orders := snap.Topics[0]
	if len(orders.Partitions) != 2 {
		t.Fatalf("orders partitions: %+v", orders.Partitions)
	}
	for _, p := range orders.Partitions {
		if len(p.Replicas) == 0 || len(p.ISR) == 0 || p.Leader == "" {
			t.Fatalf("placement not captured: %+v", p)
		}
	}
	if orders.Partitions[0].LatestOffset != 2 || orders.Partitions[1].LatestOffset != 1 {
		t.Fatalf("orders offsets: %+v", orders.Partitions)
	}
	payments := snap.Topics[1]
	if payments.Partitions[2].LatestOffset != 1 || payments.Partitions[0].LatestOffset != 0 {
		t.Fatalf("payments offsets: %+v", payments.Partitions)
	}
	if orders.Partitions[0].EarliestOffset != 0 {
		t.Fatalf("earliest should be 0: %+v", orders.Partitions[0])
	}
}

func TestFetchSelectionRules(t *testing.T) {
	cluster := startFake(t)

	// Exact name opts an internal topic in.
	snap, _, err := fetch(t, cluster, Selection{Exact: []string{"_hidden"}})
	if err != nil {
		t.Fatalf("exact internal: %v", err)
	}
	if len(snap.Topics) != 1 || snap.Topics[0].Name != "_hidden" {
		t.Fatalf("topics: %+v", snap.Topics)
	}

	// Regex never matches internal topics.
	snap, _, err = fetch(t, cluster, Selection{Regex: regexp.MustCompile(".*")})
	if err != nil {
		t.Fatalf("regex: %v", err)
	}
	if len(snap.Topics) != 2 {
		t.Fatalf("regex should skip _hidden: %+v", snap.Topics)
	}
}

// TestFetchMissingTopicDoesNotAutoCreate is the read-only guarantee test: a
// failed exact selection must not create the topic (the Metadata request is
// sent with AllowAutoTopicCreation=false; kfake honors the flag).
func TestFetchMissingTopicDoesNotAutoCreate(t *testing.T) {
	cluster := startFake(t)
	_, _, err := fetch(t, cluster, Selection{Exact: []string{"never-created"}})
	if err == nil || !strings.Contains(err.Error(), "never-created") {
		t.Fatalf("want missing-topic error, got %v", err)
	}
	snap, _, err := fetch(t, cluster, Selection{})
	if err != nil {
		t.Fatalf("refetch: %v", err)
	}
	for _, topic := range snap.Topics {
		if topic.Name == "never-created" {
			t.Fatal("metadata lookup auto-created a topic — read-only guarantee broken")
		}
	}
}

// TestFetchTopicConfigs asserts DescribeConfigs values land on Topic.Config,
// both the fake cluster's own defaults (retention.ms, cleanup.policy=delete,
// min.insync.replicas) and an explicit AlterConfigs override the test applies
// directly (bypassing monedula-sim-link entirely — the tool itself never alters a
// config; this only proves fetchTopicConfigs reads back what is actually set).
func TestFetchTopicConfigs(t *testing.T) {
	cluster := startFake(t)

	admin, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...))
	if err != nil {
		t.Fatalf("admin client: %v", err)
	}
	defer admin.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req := kmsg.NewPtrAlterConfigsRequest()
	res := kmsg.NewAlterConfigsRequestResource()
	res.ResourceType = kmsg.ConfigResourceTypeTopic
	res.ResourceName = "payments"
	for _, kv := range [][2]string{{"cleanup.policy", "compact"}, {"min.insync.replicas", "2"}} {
		c := kmsg.NewAlterConfigsRequestResourceConfig()
		c.Name = kv[0]
		v := kv[1]
		c.Value = &v
		res.Configs = append(res.Configs, c)
	}
	req.Resources = append(req.Resources, res)
	if _, err := req.RequestWith(ctx, admin); err != nil {
		t.Fatalf("seed alter configs: %v", err)
	}

	snap, _, err := fetch(t, cluster, Selection{})
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	var orders, payments *Topic
	for i := range snap.Topics {
		switch snap.Topics[i].Name {
		case "orders":
			orders = &snap.Topics[i]
		case "payments":
			payments = &snap.Topics[i]
		}
	}
	if orders == nil || payments == nil {
		t.Fatalf("topics: %+v", snap.Topics)
	}
	if orders.Config.CleanupPolicy != "delete" {
		t.Errorf("orders cleanup.policy = %q, want the fake cluster's default \"delete\"", orders.Config.CleanupPolicy)
	}
	if orders.Config.RetentionMs == nil || *orders.Config.RetentionMs <= 0 {
		t.Errorf("orders retention.ms = %v, want a positive default", orders.Config.RetentionMs)
	}
	if payments.Config.CleanupPolicy != "compact" {
		t.Errorf("payments cleanup.policy = %q, want the altered \"compact\"", payments.Config.CleanupPolicy)
	}
	if payments.Config.MinInSyncReplicas == nil || *payments.Config.MinInSyncReplicas != 2 {
		t.Errorf("payments min.insync.replicas = %v, want 2", payments.Config.MinInSyncReplicas)
	}
}

// TestFetchGroups joins a real consumer group against the fake cluster (test
// scaffolding only — the tool itself never joins a group or commits an
// offset), lets it commit, then asserts Fetch's read-only ListGroups /
// DescribeGroups / OffsetFetch capture its state, member and committed
// offsets, restricted to the selected topics.
func TestFetchGroups(t *testing.T) {
	cluster := startFake(t)

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(cluster.ListenAddrs()...),
		kgo.ConsumerGroup("analytics"),
		kgo.ConsumeTopics("orders"),
		kgo.ClientID("orders-consumer-1"),
	)
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	defer consumer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// The 3 seeded orders records span 2 partitions; poll until both are seen
	// (a single round trip may only return a subset).
	seen := map[int32]bool{}
	for len(seen) < 2 {
		fetches := consumer.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			t.Fatalf("poll fetches: %v", errs)
		}
		fetches.EachRecord(func(r *kgo.Record) { seen[r.Partition] = true })
	}
	if err := consumer.CommitUncommittedOffsets(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	snap, _, err := fetch(t, cluster, Selection{})
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(snap.Groups) != 1 {
		t.Fatalf("groups: %+v", snap.Groups)
	}
	g := snap.Groups[0]
	if g.ID != "analytics" {
		t.Fatalf("group id = %q", g.ID)
	}
	if len(g.Members) != 1 || g.Members[0].ClientID != "orders-consumer-1" {
		t.Fatalf("members: %+v", g.Members)
	}
	offs, ok := g.Offsets["orders"]
	if !ok || len(offs) == 0 {
		t.Fatalf("want committed offsets on orders, got %+v", g.Offsets)
	}
	for p, off := range offs {
		if off <= 0 {
			t.Errorf("orders/%d committed offset = %d, want > 0 (consumer read the seeded records)", p, off)
		}
	}
	if _, ok := g.Offsets["payments"]; ok {
		t.Fatalf("group never subscribed to payments, want no entry: %+v", g.Offsets)
	}

	// Read-only: fetching twice in a row must not itself advance anything the
	// consumer didn't already do (no OffsetCommit/JoinGroup was issued by Fetch).
	snap2, _, err := fetch(t, cluster, Selection{})
	if err != nil {
		t.Fatalf("refetch: %v", err)
	}
	if len(snap2.Groups) != 1 || snap2.Groups[0].Offsets["orders"][0] != offs[0] {
		t.Fatalf("a second read-only Fetch changed group state: %+v vs %+v", snap2.Groups, g.Offsets)
	}
}
