//go:build e2e

package e2e

import (
	"context"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/monedula-dev/monedula-sim-link/actionlog"
	"github.com/monedula-dev/monedula-sim-link/internal/emit"
	"github.com/monedula-dev/monedula-sim-link/simurl"
	"github.com/monedula-dev/monedula-sim-link/snapshot"
)

// takeSnapshot runs the same read-only snapshot the CLI and MCP tool use.
func takeSnapshot(t *testing.T, topics ...string) (snapshot.ClusterSnapshot, []string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	snap, warnings, err := emit.Snapshot(ctx, emit.ConnectParams{Brokers: brokers(), Topics: topics})
	if err != nil {
		t.Fatalf("snapshot: %v (warnings %v)", err, warnings)
	}
	return snap, warnings
}

func findTopic(t *testing.T, snap snapshot.ClusterSnapshot, name string) snapshot.Topic {
	t.Helper()
	for _, tp := range snap.Topics {
		if tp.Name == name {
			return tp
		}
	}
	t.Fatalf("topic %s missing from snapshot", name)
	return snapshot.Topic{}
}

func findGroup(t *testing.T, snap snapshot.ClusterSnapshot, id string) snapshot.Group {
	t.Helper()
	for _, g := range snap.Groups {
		if g.ID == id {
			return g
		}
	}
	t.Fatalf("group %s missing from snapshot (have %d groups)", id, len(snap.Groups))
	return snapshot.Group{}
}

func sameSet(a, b []string) bool {
	a, b = slices.Clone(a), slices.Clone(b)
	sort.Strings(a)
	sort.Strings(b)
	return slices.Equal(a, b)
}

func TestSnapshotBrokers(t *testing.T) {
	snap, warnings := takeSnapshot(t)
	if len(warnings) > 0 {
		t.Errorf("unexpected warnings on a healthy cluster: %v", warnings)
	}
	want := []snapshot.Broker{
		{ID: "1", Rack: "rack-a", Online: true},
		{ID: "2", Rack: "rack-b", Online: true},
		{ID: "3", Rack: "rack-c", Online: true},
	}
	if !slices.Equal(snap.Brokers, want) {
		t.Fatalf("brokers = %+v, want %+v", snap.Brokers, want)
	}
}

// TestSnapshotDefaultSelection: the default selection is every non-internal
// topic. On a fresh cluster that is exactly the fixture, whatever internal
// topics the distribution adds (__consumer_offsets, Confluent's _confluent-*).
func TestSnapshotDefaultSelection(t *testing.T) {
	snap, _ := takeSnapshot(t)
	var got []string
	for _, tp := range snap.Topics {
		got = append(got, tp.Name)
	}
	if !slices.Equal(got, fixtureTopics) {
		t.Fatalf("default selection = %v, want %v", got, fixtureTopics)
	}
}

func TestSnapshotExactSelectionIncludesInternal(t *testing.T) {
	snap, _ := takeSnapshot(t, internalTopic, "orders")
	if len(snap.Topics) != 2 || snap.Topics[0].Name != internalTopic || snap.Topics[1].Name != "orders" {
		t.Fatalf("exact selection = %+v", snap.Topics)
	}
}

func TestSnapshotPlacementAndOffsets(t *testing.T) {
	snap, _ := takeSnapshot(t)
	cases := []struct {
		topic   string
		rf      int
		records map[int32]int
		parts   int
	}{
		{"orders", 3, ordersRecords, 3},
		{"payments", 2, paymentsRecords, 2},
		{"audit.log", 1, auditRecords, 1},
	}
	for _, c := range cases {
		tp := findTopic(t, snap, c.topic)
		if len(tp.Partitions) != c.parts {
			t.Fatalf("%s: %d partitions, want %d", c.topic, len(tp.Partitions), c.parts)
		}
		for i, p := range tp.Partitions {
			if p.ID != i {
				t.Fatalf("%s: partitions out of order: %+v", c.topic, tp.Partitions)
			}
			if len(p.Replicas) != c.rf {
				t.Errorf("%s/%d: replicas %v, want RF %d", c.topic, p.ID, p.Replicas, c.rf)
			}
			if !sameSet(p.ISR, p.Replicas) {
				t.Errorf("%s/%d: ISR %v != replicas %v on a healthy cluster", c.topic, p.ID, p.ISR, p.Replicas)
			}
			if !slices.Contains(p.Replicas, p.Leader) {
				t.Errorf("%s/%d: leader %q not among replicas %v", c.topic, p.ID, p.Leader, p.Replicas)
			}
			want := int64(c.records[int32(p.ID)])
			if p.EarliestOffset != 0 || p.LatestOffset != want {
				t.Errorf("%s/%d: offsets [%d, %d), want [0, %d)", c.topic, p.ID, p.EarliestOffset, p.LatestOffset, want)
			}
		}
	}
}

func TestSnapshotTopicConfigs(t *testing.T) {
	snap, _ := takeSnapshot(t)

	orders := findTopic(t, snap, "orders").Config
	if orders.CleanupPolicy != "delete" || orders.MinInSyncReplicas == nil || *orders.MinInSyncReplicas != 2 ||
		orders.RetentionMs == nil || *orders.RetentionMs != 86400000 {
		t.Errorf("orders config = %s", describeConfig(orders))
	}
	if payments := findTopic(t, snap, "payments").Config; payments.CleanupPolicy != "compact" {
		t.Errorf("payments config = %s", describeConfig(payments))
	}
	if audit := findTopic(t, snap, "audit.log").Config; audit.RetentionBytes == nil || *audit.RetentionBytes != 1048576 {
		t.Errorf("audit.log config = %s", describeConfig(audit))
	}
}

func describeConfig(c snapshot.TopicConfig) string {
	var b strings.Builder
	b.WriteString("cleanup=" + c.CleanupPolicy)
	if c.MinInSyncReplicas != nil {
		b.WriteString(" minISR=" + itoa(int64(*c.MinInSyncReplicas)))
	}
	if c.RetentionMs != nil {
		b.WriteString(" retention.ms=" + itoa(*c.RetentionMs))
	}
	if c.RetentionBytes != nil {
		b.WriteString(" retention.bytes=" + itoa(*c.RetentionBytes))
	}
	return b.String()
}

func TestSnapshotConsumerGroups(t *testing.T) {
	snap, _ := takeSnapshot(t)

	billing := findGroup(t, snap, billingGroup)
	if billing.State != "Stable" || len(billing.Members) != 1 || billing.Members[0].ClientID != billingClientID {
		t.Errorf("billing = %+v, want Stable with one %s member", billing, billingClientID)
	}
	assertOffsets(t, billingGroup, billing.Offsets, "orders", billingCommits)

	reporting := findGroup(t, snap, reportingGroup)
	if reporting.State != "Empty" || len(reporting.Members) != 0 {
		t.Errorf("reporting = %+v, want Empty with no members", reporting)
	}
	assertOffsets(t, reportingGroup, reporting.Offsets, "payments", reportingCommits)
}

func assertOffsets(t *testing.T, group string, got map[string]map[int]int64, topic string, want map[int32]int64) {
	t.Helper()
	if len(got) != 1 || len(got[topic]) != len(want) {
		t.Errorf("%s offsets = %v, want only %s %v", group, got, topic, want)
		return
	}
	for p, o := range want {
		if got[topic][int(p)] != o {
			t.Errorf("%s offsets = %v, want %s %v", group, got, topic, want)
		}
	}
}

// buildURL runs the mapping + self-validation pipeline over a fresh snapshot.
func buildURL(t *testing.T) *emit.Result {
	t.Helper()
	snap, _ := takeSnapshot(t)
	res, err := emit.Build(snap, emit.BuildOptions{})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	return res
}

// TestURLMapsTheCluster checks what the link will show: the cluster shape,
// the renamed topic, synthetic data volume capped per partition, and both
// groups, with only the lagging one reset to earliest.
func TestURLMapsTheCluster(t *testing.T) {
	res := buildURL(t)

	if !strings.HasPrefix(res.Size, "3x3x3x") {
		t.Errorf("size = %q, want 3 brokers and orders at 3 partitions x RF 3", res.Size)
	}
	for _, extra := range []string{"audit-log~1~1~", "payments~2~2~1~c~"} {
		if !strings.Contains(res.Size, extra) {
			t.Errorf("size = %q, missing extra topic %q", res.Size, extra)
		}
	}
	if res.Cluster != "" {
		t.Errorf("cluster = %q, want the single-dc default", res.Cluster)
	}
	if res.BrokerNames["1"] != "broker-1" || res.BrokerNames["3"] != "broker-3" {
		t.Errorf("broker names = %v", res.BrokerNames)
	}
	if res.TopicNames["audit.log"] != "audit-log" || res.TopicNames["orders"] != "orders" {
		t.Errorf("topic names = %v", res.TopicNames)
	}

	produces := map[string]int{}
	groups := map[string]bool{}
	consumers := map[string]int{}
	var resets, kills, slowdowns []string
	for _, e := range res.DecodedActions {
		switch a := e.Action.(type) {
		case actionlog.ProduceRecord:
			produces[a.Topic]++
		case actionlog.AddGroup:
			groups[a.GroupID] = true
		case actionlog.AddConsumer:
			consumers[a.GroupID]++
		case actionlog.ConfigChange:
			if strings.HasSuffix(a.Path, ".consumer.autoOffsetReset") {
				resets = append(resets, a.Path+"="+a.Value)
			}
		case actionlog.KillBroker:
			kills = append(kills, a.BrokerID)
		case actionlog.SetReplicaSpeed:
			slowdowns = append(slowdowns, a.BrokerID)
		}
	}
	// min(records, 8) per partition.
	wantProduces := map[string]int{"orders": 7, "payments": 4, "audit-log": 8}
	for topic, n := range wantProduces {
		if produces[topic] != n {
			t.Errorf("produce actions = %v, want %v", produces, wantProduces)
			break
		}
	}
	billing, reporting := res.GroupNames[billingGroup], res.GroupNames[reportingGroup]
	if billing == "" || reporting == "" || !groups[billing] || !groups[reporting] {
		t.Errorf("groups = %v (names %v), want billing and reporting", groups, res.GroupNames)
	}
	if consumers[billing] != 1 || consumers[reporting] != 0 {
		t.Errorf("add_consumer per group = %v, want one billing member only", consumers)
	}
	if want := []string{"group." + billing + ".consumer.autoOffsetReset=earliest"}; !slices.Equal(resets, want) {
		t.Errorf("offset resets = %v, want %v (billing lags, reporting is caught up)", resets, want)
	}
	if len(kills) > 0 || len(slowdowns) > 0 {
		t.Errorf("healthy cluster emitted kills %v / slowdowns %v", kills, slowdowns)
	}
}

// TestURLRoundTrips decodes the actions param out of the URL string itself
// and re-encodes it: the link carries exactly the log the tool built.
func TestURLRoundTrips(t *testing.T) {
	res := buildURL(t)
	u, err := url.Parse(res.URL)
	if err != nil {
		t.Fatalf("parse %q: %v", res.URL, err)
	}
	if got := u.Query().Get("scenario"); got != "free" {
		t.Errorf("scenario = %q, want free", got)
	}
	if got := u.Query().Get("size"); got != res.Size {
		t.Errorf("size param = %q, want %q", got, res.Size)
	}
	decoded, err := simurl.DecodeActionsFromURL(u.Query().Get("actions"))
	if err != nil {
		t.Fatalf("decode actions: %v", err)
	}
	want, _ := actionlog.Encode(res.Entries)
	got, err := actionlog.Encode(decoded)
	if err != nil || got != want {
		t.Fatalf("URL actions decode to\n%s\nwant\n%s (err %v)", got, want, err)
	}
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }
