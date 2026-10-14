package mapping

import (
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/monedula-dev/monedula-sim-link/actionlog"
	"github.com/monedula-dev/monedula-sim-link/snapshot"
)

// snapshotAt is the fixed snapshot time every producers test measures from.
var snapshotAt = time.Date(2026, time.October, 14, 12, 0, 0, 0, time.UTC)

// wrote builds a ProducerWrite whose last write was `ago` before snapshotAt.
func wrote(id int64, ago time.Duration) snapshot.ProducerWrite {
	return snapshot.ProducerWrite{ProducerID: id, LastTimestamp: snapshotAt.Add(-ago).UnixMilli(), CurrentTxnStartOffset: -1}
}

// producersCluster is a healthy 3-broker cluster with three topics, so topic
// choice has something to choose between: orders (the primary, 3 partitions),
// payments (2) and audit (1). Every partition is on the default placement and
// holds no records, so the log carries no keyed replay and the producers are the
// only thing after the template removals.
func producersCluster(writes map[string]map[int][]snapshot.ProducerWrite) snapshot.ClusterSnapshot {
	part := func(id int) snapshot.Partition {
		return snapshot.Partition{ID: id, Leader: "1", Replicas: []string{"1", "2", "3"}, ISR: []string{"1", "2", "3"}}
	}
	snap := snapshot.ClusterSnapshot{
		Brokers: []snapshot.Broker{{ID: "1", Online: true}, {ID: "2", Online: true}, {ID: "3", Online: true}},
	}
	for _, spec := range []struct {
		name  string
		parts int
	}{{"orders", 3}, {"payments", 2}, {"audit", 1}} {
		t := snapshot.Topic{Name: spec.name}
		for i := 0; i < spec.parts; i++ {
			p := part(i)
			p.Producers = writes[spec.name][i]
			t.Partitions = append(t.Partitions, p)
		}
		snap.Topics = append(snap.Topics, t)
	}
	snap.Producers = &snapshot.ProducersRead{At: snapshotAt}
	return snap
}

// producerEntries returns the encoded AP:pid-... entries, plus any config change
// on a pid- producer (there must be none: add_producer's default is already
// idempotent).
func producerEntries(t *testing.T, res *Result) []string {
	t.Helper()
	var out []string
	for _, e := range res.Entries {
		enc, err := actionlog.EncodeEntry(e)
		if err != nil {
			t.Fatal(err)
		}
		if strings.HasPrefix(enc, "AP:pid-") || strings.HasPrefix(enc, "C:producers.pid-") {
			out = append(out, enc[:strings.LastIndex(enc, "@")])
		}
	}
	return out
}

func mapProducersCluster(t *testing.T, snap snapshot.ClusterSnapshot, opts Options) *Result {
	t.Helper()
	res, err := Map(snap, opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := actionlog.Verify(res.Entries); err != nil {
		t.Fatal(err)
	}
	return res
}

// TestProducersAddedWithDefaultConfig: one producer that wrote to two partitions
// of orders is added once, named pid-<id>, pinned to orders, and nothing else:
// add_producer creates it with the simulator's default config (acks=all,
// idempotence), so no config change follows.
func TestProducersAddedWithDefaultConfig(t *testing.T) {
	snap := producersCluster(map[string]map[int][]snapshot.ProducerWrite{
		"orders": {0: {wrote(1001, time.Minute)}, 1: {wrote(1001, 2*time.Minute)}},
	})
	res := mapProducersCluster(t, snap, Options{})
	want := []string{"AP:pid-1001:orders"}
	if got := producerEntries(t, res); !reflect.DeepEqual(got, want) {
		t.Fatalf("producer entries = %v, want %v", got, want)
	}
	for _, e := range res.Entries {
		if _, ok := e.Action.(actionlog.ConfigChange); ok {
			enc, _ := actionlog.EncodeEntry(e)
			t.Errorf("unexpected config change %s: a new producer is idempotent already", enc)
		}
	}
	if len(res.Warnings) != 0 {
		t.Errorf("a clean read warns about nothing, got %v", res.Warnings)
	}
}

// TestProducersAreOptIn: a snapshot taken without the producers read maps
// exactly as before, even if its partitions carry stray producer data.
func TestProducersAreOptIn(t *testing.T) {
	snap := producersCluster(map[string]map[int][]snapshot.ProducerWrite{
		"orders": {0: {wrote(1001, time.Minute)}},
	})
	snap.Producers = nil
	res := mapProducersCluster(t, snap, Options{})
	if got := producerEntries(t, res); len(got) != 0 {
		t.Fatalf("no producers read, yet the log has %v", got)
	}
	if len(res.Warnings) != 0 {
		t.Errorf("warnings = %v, want none", res.Warnings)
	}
}

// TestProducersGoAfterTheKeyedReplayAndGroups: the replay's transient producers
// and the consumer groups come out exactly as without producers, and the
// persistent ones are appended after them, so the replay is not disturbed and at
// most 4 producers are ever alive.
func TestProducersGoAfterTheKeyedReplayAndGroups(t *testing.T) {
	build := func(withProducers bool) snapshot.ClusterSnapshot {
		snap := producersCluster(map[string]map[int][]snapshot.ProducerWrite{
			"orders": {0: {wrote(1001, time.Minute)}},
		})
		snap.Topics[0].Partitions[0].LatestOffset = 3 // records to replay on orders
		snap.Topics[1].Partitions[0].LatestOffset = 2 // and on payments
		snap.Groups = []snapshot.Group{{
			ID: "billing", Members: []snapshot.GroupMember{{ID: "m1", ClientID: "w1"}},
			Offsets: map[string]map[int]int64{"orders": {0: 1}},
		}}
		if !withProducers {
			snap.Producers = nil
		}
		return snap
	}
	without := mapProducersCluster(t, build(false), Options{})
	with := mapProducersCluster(t, build(true), Options{})

	if len(with.Entries) != len(without.Entries)+1 {
		t.Fatalf("entries: %d with producers, %d without, want 1 more", len(with.Entries), len(without.Entries))
	}
	for i, e := range without.Entries {
		a, _ := actionlog.EncodeEntry(e)
		b, _ := actionlog.EncodeEntry(with.Entries[i])
		if a != b {
			t.Fatalf("entry %d changed by --producers: %s vs %s", i, a, b)
		}
	}
	// The kp producers were all removed before the first persistent one is added.
	live, maxLive := 0, 0
	for _, e := range with.Entries {
		switch e.Action.(type) {
		case actionlog.AddProducer:
			live++
		case actionlog.RemoveProducer:
			live--
		}
		if live > maxLive {
			maxLive = live
		}
	}
	if maxLive > MaxProducers {
		t.Errorf("up to %d producers alive at once, the engine renders %d", maxLive, MaxProducers)
	}
	last := with.Entries[len(with.Entries)-1]
	if ap, ok := last.Action.(actionlog.AddProducer); !ok || ap.ProducerID != "pid-1001" {
		t.Errorf("last entry = %+v, want AP pid-1001", last.Action)
	}
	if last.At <= without.Entries[len(without.Entries)-1].At {
		t.Errorf("producers at %d are not after the last mapped action at %d", last.At, without.Entries[len(without.Entries)-1].At)
	}
}

// TestProducersRankedByPartitionsThenRecencyThenId.
func TestProducersRankedByPartitionsThenRecencyThenId(t *testing.T) {
	snap := producersCluster(map[string]map[int][]snapshot.ProducerWrite{
		"orders": {
			0: {wrote(10, 5*time.Minute), wrote(20, 4*time.Minute), wrote(30, 3*time.Minute), wrote(40, 9*time.Minute)},
			1: {wrote(10, 5*time.Minute), wrote(20, 4*time.Minute), wrote(30, 3*time.Minute)},
			2: {wrote(10, 6*time.Minute), wrote(40, 8*time.Minute)},
		},
		"payments": {0: {wrote(10, 7*time.Minute), wrote(50, 2*time.Minute)}},
	})
	// partitions written: 10 -> 4, 20 -> 2, 30 -> 2, 40 -> 2, 50 -> 1.
	// 20, 30 and 40 tie on partitions: 30 wrote most recently (3m), then 20 (4m),
	// then 40 (8m). The cap keeps 4, so 50 is dropped.
	res := mapProducersCluster(t, snap, Options{})
	var ids []string
	for _, e := range producerEntries(t, res) {
		if strings.HasPrefix(e, "AP:") {
			ids = append(ids, strings.Split(e, ":")[1])
		}
	}
	if want := []string{"pid-10", "pid-30", "pid-20", "pid-40"}; !reflect.DeepEqual(ids, want) {
		t.Fatalf("producer order = %v, want %v", ids, want)
	}
}

func TestProducersTieBreaksByProducerId(t *testing.T) {
	// Identical footprint and timestamp: the lower id comes first.
	snap := producersCluster(map[string]map[int][]snapshot.ProducerWrite{
		"orders": {0: {wrote(7, time.Minute), wrote(3, time.Minute)}},
	})
	res := mapProducersCluster(t, snap, Options{})
	got := producerEntries(t, res)
	if want := []string{"AP:pid-3:orders", "AP:pid-7:orders"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("entries = %v, want %v (pid-3 before pid-7)", got, want)
	}
}

// TestProducersCappedAtFour: over 4 active producers the top 4 stay and one
// warning names what was kept and dropped.
func TestProducersCappedAtFour(t *testing.T) {
	var all []snapshot.ProducerWrite
	for id := int64(1); id <= 7; id++ {
		all = append(all, wrote(id, time.Minute))
	}
	// Producer 7 also wrote a second partition, so it outranks the rest.
	snap := producersCluster(map[string]map[int][]snapshot.ProducerWrite{
		"orders": {0: all, 1: {wrote(7, time.Minute)}},
	})
	res := mapProducersCluster(t, snap, Options{})
	var ids []string
	for _, e := range producerEntries(t, res) {
		if strings.HasPrefix(e, "AP:") {
			ids = append(ids, strings.Split(e, ":")[1])
		}
	}
	if want := []string{"pid-7", "pid-1", "pid-2", "pid-3"}; !reflect.DeepEqual(ids, want) {
		t.Fatalf("kept = %v, want %v", ids, want)
	}
	if !hasWarning(res, "7 producers", "last 15m", "at most 4", "pid-7 (2 partitions)", "dropping pid-4, pid-5, pid-6") {
		t.Fatalf("want one cap warning naming kept and dropped, got %v", res.Warnings)
	}
}

// TestProducersNaming: pid-<id> for the whole int64 range stays inside the 40
// characters the generated ids obey and survives the codec.
func TestProducersNaming(t *testing.T) {
	const maxID = int64(9223372036854775807)
	snap := producersCluster(map[string]map[int][]snapshot.ProducerWrite{
		"orders": {0: {wrote(0, time.Minute), wrote(maxID, 2*time.Minute)}},
	})
	res := mapProducersCluster(t, snap, Options{})
	got := producerEntries(t, res)
	want := []string{"AP:pid-0:orders", "AP:pid-9223372036854775807:orders"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("entries = %v, want %v", got, want)
	}
	if n := len(producerSimID(maxID)); n != 23 || n > maxSimIDLen {
		t.Errorf("longest producer id has %d chars, want 23 (limit %d)", n, maxSimIDLen)
	}
}

// TestProducersTopicChoice: a producer is pinned to the rendered topic it wrote
// to on the most partitions; a tie goes to the earlier topic in emission order,
// the primary first, then alphabetical.
func TestProducersTopicChoice(t *testing.T) {
	cases := []struct {
		name   string
		writes map[string]map[int][]snapshot.ProducerWrite
		want   string
	}{
		{
			"most partitions wins",
			map[string]map[int][]snapshot.ProducerWrite{
				"orders":   {0: {wrote(1, time.Minute)}},
				"payments": {0: {wrote(1, time.Minute)}, 1: {wrote(1, time.Minute)}},
			},
			"payments",
		},
		{
			"tie goes to the primary",
			map[string]map[int][]snapshot.ProducerWrite{
				"orders":   {0: {wrote(1, time.Minute)}, 1: {wrote(1, time.Minute)}},
				"payments": {0: {wrote(1, time.Minute)}, 1: {wrote(1, time.Minute)}},
			},
			"orders",
		},
		{
			"tie between extras goes alphabetical",
			map[string]map[int][]snapshot.ProducerWrite{
				"payments": {0: {wrote(1, time.Minute)}},
				"audit":    {0: {wrote(1, time.Minute)}},
			},
			"audit",
		},
		{
			"only active partitions count",
			map[string]map[int][]snapshot.ProducerWrite{
				"orders":   {0: {wrote(1, 3*time.Hour)}, 1: {wrote(1, 3*time.Hour)}, 2: {wrote(1, 3*time.Hour)}},
				"payments": {0: {wrote(1, time.Minute)}},
			},
			"payments",
		},
	}
	for _, c := range cases {
		res := mapProducersCluster(t, producersCluster(c.writes), Options{})
		got := producerEntries(t, res)
		if len(got) != 1 || got[0] != "AP:pid-1:"+c.want {
			t.Errorf("%s: entries = %v, want the producer pinned to %s", c.name, got, c.want)
		}
	}
}

// TestProducersWindow: a write is active when it is within the window before the
// snapshot (the boundary included, and a write stamped after the snapshot by a
// broker clock that runs ahead); an older one and an unknown timestamp are left
// out. Equal footprints rank by recency, so the future-stamped producer is first.
func TestProducersWindow(t *testing.T) {
	ids := func(res *Result, t *testing.T) []string {
		var out []string
		for _, e := range producerEntries(t, res) {
			if strings.HasPrefix(e, "AP:") {
				out = append(out, strings.Split(e, ":")[1])
			}
		}
		return out
	}
	unknown := snapshot.ProducerWrite{ProducerID: 4, LastTimestamp: -1, CurrentTxnStartOffset: -1}
	future := snapshot.ProducerWrite{ProducerID: 5, LastTimestamp: snapshotAt.Add(time.Minute).UnixMilli(), CurrentTxnStartOffset: -1}
	snap := func() snapshot.ClusterSnapshot {
		return producersCluster(map[string]map[int][]snapshot.ProducerWrite{
			"orders": {0: {
				wrote(1, 14*time.Minute+59*time.Second), // inside the default window
				wrote(2, 15*time.Minute),                // exactly on the boundary
				wrote(3, 15*time.Minute+time.Millisecond),
				unknown,
				future, // the broker clock runs ahead of the reader's
			}},
		})
	}

	res := mapProducersCluster(t, snap(), Options{})
	if got, want := ids(res, t), []string{"pid-5", "pid-1", "pid-2"}; !reflect.DeepEqual(got, want) {
		t.Errorf("default window: %v, want %v", got, want)
	}

	res = mapProducersCluster(t, snap(), Options{ProducersWindow: time.Minute})
	if got, want := ids(res, t), []string{"pid-5"}; !reflect.DeepEqual(got, want) {
		t.Errorf("1m window: %v, want %v", got, want)
	}

	res = mapProducersCluster(t, snap(), Options{ProducersWindow: time.Hour})
	if got, want := ids(res, t), []string{"pid-5", "pid-1", "pid-2", "pid-3"}; !reflect.DeepEqual(got, want) {
		t.Errorf("1h window: %v, want %v", got, want)
	}
}

func TestProducersNoneActiveSaysSo(t *testing.T) {
	snap := producersCluster(map[string]map[int][]snapshot.ProducerWrite{
		"orders": {0: {wrote(1, 2*time.Hour)}},
	})
	res := mapProducersCluster(t, snap, Options{ProducersWindow: 30 * time.Minute})
	if got := producerEntries(t, res); len(got) != 0 {
		t.Fatalf("entries = %v", got)
	}
	if !hasWarning(res, "--producers", "no producer wrote", "last 30m", "idempotent or transactional", "no producers") {
		t.Fatalf("want a no-producers note, got %v", res.Warnings)
	}
}

func TestProducersOnlyRenderedTopicsCount(t *testing.T) {
	// 8 extra topics (over the cap of 6): the two least eventful are dropped.
	// Producers that wrote only to a dropped topic have nowhere to be pinned.
	snap := threeBrokerSnap()
	snap.Topics = nil
	for i := 0; i < MaxExtraTopics+2; i++ {
		name := "t" + strconv.Itoa(i)
		p := snapshot.Partition{ID: 0, Leader: "1", Replicas: []string{"1", "2", "3"}, ISR: []string{"1", "2", "3"}}
		if i == MaxExtraTopics+1 {
			p.Producers = []snapshot.ProducerWrite{wrote(99, time.Minute)} // t7: dropped by the ranking
		}
		if i == 0 {
			p.Producers = []snapshot.ProducerWrite{wrote(1, time.Minute)}
		}
		snap.Topics = append(snap.Topics, snapshot.Topic{Name: name, Partitions: []snapshot.Partition{p}})
	}
	snap.Producers = &snapshot.ProducersRead{At: snapshotAt}
	res := mapProducersCluster(t, snap, Options{})
	if _, rendered := res.TopicNames["t7"]; rendered {
		t.Fatal("test setup: t7 should have been dropped by the topic cap")
	}
	got := producerEntries(t, res)
	if want := []string{"AP:pid-1:t0"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("entries = %v, want only the producer of a rendered topic %v", got, want)
	}
}

// TestProducersOpenTransactionStaysIdempotent: the URL grammar has no
// transactional producer, so one with an open transaction is added as an
// idempotent one, with a warning, and no begin_transaction.
func TestProducersOpenTransactionStaysIdempotent(t *testing.T) {
	txn := wrote(2002, 10*time.Second)
	txn.CurrentTxnStartOffset = 41
	snap := producersCluster(map[string]map[int][]snapshot.ProducerWrite{
		"orders": {0: {txn}, 1: {txn}},
	})
	res := mapProducersCluster(t, snap, Options{})
	want := []string{"AP:pid-2002:orders"}
	if got := producerEntries(t, res); !reflect.DeepEqual(got, want) {
		t.Fatalf("entries = %v, want %v", got, want)
	}
	for _, e := range res.Entries {
		if enc, _ := actionlog.EncodeEntry(e); strings.HasPrefix(enc, "BT:") || strings.Contains(enc, "transactionalId") {
			t.Errorf("a transaction action slipped in: %s", enc)
		}
	}
	if !hasWarning(res, "producer 2002", "pid-2002", "open transaction on orders/0 and 1 more", "idempotent") {
		t.Fatalf("want the transaction warning, got %v", res.Warnings)
	}
}

// TestProducersReadFailuresWarnAndKeepTheLink: a refused or unsupported read is a
// warning and a link without producers, never an error.
func TestProducersReadFailuresWarnAndKeepTheLink(t *testing.T) {
	cases := []struct {
		name     string
		read     snapshot.ProducersRead
		writes   map[string]map[int][]snapshot.ProducerWrite
		wantAP   []string // producers still mapped, by AP entry
		wantWarn [][]string
	}{
		{
			name:     "unsupported",
			read:     snapshot.ProducersRead{Unsupported: true},
			wantWarn: [][]string{{"--producers", "does not support DescribeProducers", "Kafka 2.8", "no producers"}},
		},
		{
			name:     "cluster authorization failed",
			read:     snapshot.ProducersRead{ClusterDenied: true},
			wantWarn: [][]string{{"--producers", "CLUSTER_AUTHORIZATION_FAILED", "no producers"}},
		},
		{
			name:     "every topic denied",
			read:     snapshot.ProducersRead{DeniedTopics: []string{"audit", "orders", "payments"}},
			wantWarn: [][]string{{"--producers", "READ", "TOPIC_AUTHORIZATION_FAILED", "audit, orders, payments", "the link has no producers"}},
		},
		{
			name:     "one topic denied keeps the producers of the others",
			read:     snapshot.ProducersRead{DeniedTopics: []string{"orders"}},
			writes:   map[string]map[int][]snapshot.ProducerWrite{"payments": {0: {wrote(5, time.Minute)}}},
			wantAP:   []string{"AP:pid-5:payments"},
			wantWarn: [][]string{{"--producers", "TOPIC_AUTHORIZATION_FAILED", "orders", "left out"}},
		},
		{
			name:     "a denied topic that is not rendered is not mentioned",
			read:     snapshot.ProducersRead{DeniedTopics: []string{"elsewhere"}},
			writes:   map[string]map[int][]snapshot.ProducerWrite{"payments": {0: {wrote(5, time.Minute)}}},
			wantAP:   []string{"AP:pid-5:payments"},
			wantWarn: nil,
		},
		{
			name:     "other failures",
			read:     snapshot.ProducersRead{Errors: []string{"NOT_LEADER_OR_FOLLOWER (6) on orders/0, orders/1"}},
			writes:   map[string]map[int][]snapshot.ProducerWrite{"payments": {0: {wrote(5, time.Minute)}}},
			wantAP:   []string{"AP:pid-5:payments"},
			wantWarn: [][]string{{"--producers", "DescribeProducers failed", "NOT_LEADER_OR_FOLLOWER (6) on orders/0, orders/1", "left out"}},
		},
	}
	for _, c := range cases {
		snap := producersCluster(c.writes)
		read := c.read
		read.At = snapshotAt
		snap.Producers = &read
		res := mapProducersCluster(t, snap, Options{})
		var aps []string
		for _, e := range producerEntries(t, res) {
			if strings.HasPrefix(e, "AP:") {
				aps = append(aps, e)
			}
		}
		if !reflect.DeepEqual(aps, c.wantAP) {
			t.Errorf("%s: producers = %v, want %v", c.name, aps, c.wantAP)
		}
		if len(res.Warnings) != len(c.wantWarn) {
			t.Errorf("%s: warnings = %v, want %d", c.name, res.Warnings, len(c.wantWarn))
			continue
		}
		for i, frags := range c.wantWarn {
			if !strings.Contains(res.Warnings[i], frags[0]) || !hasWarning(&Result{Warnings: res.Warnings[i : i+1]}, frags...) {
				t.Errorf("%s: warning %q lacks one of %v", c.name, res.Warnings[i], frags)
			}
		}
	}
}

func TestWindowString(t *testing.T) {
	for in, want := range map[time.Duration]string{
		15 * time.Minute:             "15m",
		time.Hour:                    "1h",
		90 * time.Second:             "1m30s",
		45 * time.Second:             "45s",
		2*time.Hour + 30*time.Minute: "2h30m",
		24 * time.Hour:               "24h",
		time.Hour + 5*time.Minute + 3*time.Second: "1h5m3s",
	} {
		if got := windowString(in); got != want {
			t.Errorf("windowString(%s) = %q, want %q", in, got, want)
		}
	}
}
