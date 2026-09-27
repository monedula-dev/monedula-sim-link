package mapping

import (
	"strconv"
	"strings"
	"testing"

	"github.com/monedula-dev/monedula-sim-link/actionlog"
	"github.com/monedula-dev/monedula-sim-link/snapshot"
)

// threeBrokerSnap builds a healthy 3-broker cluster whose single topic sits
// exactly on the simulator's default placement (fixture PLACEMENT 3 3 3: every
// RF-3 replica set is {1,2,3}).
func threeBrokerSnap() snapshot.ClusterSnapshot {
	return snapshot.ClusterSnapshot{
		Brokers: []snapshot.Broker{
			{ID: "1", Online: true}, {ID: "2", Online: true}, {ID: "3", Online: true},
		},
		Topics: []snapshot.Topic{{
			Name: "orders",
			Partitions: []snapshot.Partition{
				{ID: 0, Leader: "1", Replicas: []string{"1", "2", "3"}, ISR: []string{"1", "2", "3"}, EarliestOffset: 0, LatestOffset: 5},
				{ID: 1, Leader: "2", Replicas: []string{"2", "3", "1"}, ISR: []string{"2", "3", "1"}, EarliestOffset: 2, LatestOffset: 4},
				{ID: 2, Leader: "3", Replicas: []string{"3", "1", "2"}, ISR: []string{"3", "1", "2"}, EarliestOffset: 0, LatestOffset: 0},
			},
		}},
	}
}

// actionsOfKind filters entries via their encoded form's kind prefix.
func actionsOfKind(t *testing.T, entries []actionlog.Entry, kind string) []string {
	t.Helper()
	var out []string
	for _, e := range entries {
		enc, err := actionlog.EncodeEntry(e)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		if strings.HasPrefix(enc, kind+":") {
			out = append(out, enc)
		}
	}
	return out
}

func TestMapHappyPathOnDefaultPlacement(t *testing.T) {
	res, err := Map(threeBrokerSnap(), Options{MaxRecordsPerPartition: 8})
	if err != nil {
		t.Fatal(err)
	}
	if res.Size != "3x3x3" {
		t.Errorf("size = %q, want 3x3x3", res.Size)
	}
	if res.Cluster != "" {
		t.Errorf("cluster = %q, want single-dc default (empty)", res.Cluster)
	}
	if got := actionsOfKind(t, res.Entries, "RA"); len(got) != 0 {
		t.Errorf("no reassign expected on default placement, got %v", got)
	}
	if got := actionsOfKind(t, res.Entries, "K"); len(got) != 0 {
		t.Errorf("no kills expected, got %v", got)
	}
	if got := actionsOfKind(t, res.Entries, "SR"); len(got) != 0 {
		t.Errorf("no slow replicas expected, got %v", got)
	}
	// Template producer removed first, at t=500.
	first, _ := actionlog.EncodeEntry(res.Entries[0])
	if first != "RP:p1@500" {
		t.Errorf("first entry = %q, want RP:p1@500", first)
	}
	// Data volume: p0 has 5 records, p1 has 2 (4-2), p2 is empty ⇒ 7 produces.
	produces := actionsOfKind(t, res.Entries, "P")
	if len(produces) != 7 {
		t.Fatalf("want 7 produces, got %d: %v", len(produces), produces)
	}
	// Every produce key must hash to its intended partition.
	wantPerPartition := map[int]int{0: 5, 1: 2}
	gotPerPartition := map[int]int{}
	for _, e := range res.Entries {
		p, ok := e.Action.(actionlog.ProduceRecord)
		if !ok {
			continue
		}
		if p.Topic != "orders" || p.Key == nil {
			t.Fatalf("produce must target orders with a key: %+v", p)
		}
		gotPerPartition[PartitionForKey(*p.Key, 3)]++
	}
	for pid, want := range wantPerPartition {
		if gotPerPartition[pid] != want {
			t.Errorf("partition %d: %d produces, want %d (distribution %v)", pid, gotPerPartition[pid], want, gotPerPartition)
		}
	}
	// The synthetic producer is added before and removed after its produces.
	if got := actionsOfKind(t, res.Entries, "AP"); len(got) != 1 || got[0] != "AP:kp1:orders@750" {
		t.Errorf("AP entries: %v", got)
	}
	rps := actionsOfKind(t, res.Entries, "RP")
	if len(rps) != 2 || !strings.HasPrefix(rps[1], "RP:kp1@") {
		t.Errorf("RP entries: %v", rps)
	}
	if err := actionlog.Verify(res.Entries); err != nil {
		t.Fatal(err)
	}
}

func TestMapMaxRecordsCap(t *testing.T) {
	res, err := Map(threeBrokerSnap(), Options{MaxRecordsPerPartition: 2})
	if err != nil {
		t.Fatal(err)
	}
	if got := len(actionsOfKind(t, res.Entries, "P")); got != 4 { // 2 + 2 + 0
		t.Fatalf("want 4 produces at cap 2, got %d", got)
	}
}

func TestMapReassignOffDefaultPlacement(t *testing.T) {
	snap := snapshot.ClusterSnapshot{
		Brokers: []snapshot.Broker{
			{ID: "1", Online: true}, {ID: "2", Online: true}, {ID: "3", Online: true},
			{ID: "4", Online: true}, {ID: "5", Online: true},
		},
		Topics: []snapshot.Topic{{
			Name: "orders",
			Partitions: []snapshot.Partition{
				// Default for 5 brokers rf2 is {1,2} (fixture PLACEMENT 5 7 2); this
				// sits on {4,5} ⇒ reassign.
				{ID: 0, Leader: "4", Replicas: []string{"4", "5"}, ISR: []string{"4", "5"}},
				// Default for partition 1 is {2,3}; this matches as a SET (order is
				// preference only) so no reassign is emitted.
				{ID: 1, Leader: "3", Replicas: []string{"3", "2"}, ISR: []string{"3", "2"}},
			},
		}},
	}
	res, err := Map(snap, Options{})
	if err != nil {
		t.Fatal(err)
	}
	ras := actionsOfKind(t, res.Entries, "RA")
	if len(ras) != 1 || !strings.HasPrefix(ras[0], "RA:orders:0:broker-4.broker-5@") {
		t.Fatalf("RA entries: %v", ras)
	}
}

func TestMapKillsAndSlowsForFailures(t *testing.T) {
	snap := snapshot.ClusterSnapshot{
		Brokers: []snapshot.Broker{
			{ID: "1", Online: true}, {ID: "2", Online: true}, {ID: "3", Online: false},
		},
		Topics: []snapshot.Topic{{
			Name: "orders",
			Partitions: []snapshot.Partition{
				// Broker 3 is offline per metadata; broker 4 exists only in the
				// replica list (ghost ⇒ synthesized offline); broker 2 is assigned but
				// out of the ISR while alive ⇒ slowed.
				{ID: 0, Leader: "1", Replicas: []string{"1", "2", "3", "4"}, ISR: []string{"1"}, EarliestOffset: 0, LatestOffset: 3},
			},
		}},
	}
	res, err := Map(snap, Options{})
	if err != nil {
		t.Fatal(err)
	}
	kills := actionsOfKind(t, res.Entries, "K")
	if len(kills) != 2 || !strings.HasPrefix(kills[0], "K:broker-3@") || !strings.HasPrefix(kills[1], "K:broker-4@") {
		t.Fatalf("kill entries: %v", kills)
	}
	slows := actionsOfKind(t, res.Entries, "SR")
	if len(slows) != 1 || !strings.HasPrefix(slows[0], "SR:broker-2:1@") {
		t.Fatalf("slow entries: %v", slows)
	}
	// The leader is alive, so produces still flow.
	if got := len(actionsOfKind(t, res.Entries, "P")); got != 3 {
		t.Fatalf("want 3 produces, got %d", got)
	}
	if len(res.Warnings) == 0 {
		t.Fatal("expected approximation warnings")
	}
}

func TestMapSkipsProducesWithoutLiveReplica(t *testing.T) {
	snap := snapshot.ClusterSnapshot{
		Brokers: []snapshot.Broker{
			{ID: "1", Online: true}, {ID: "2", Online: false},
		},
		Topics: []snapshot.Topic{{
			Name: "orders",
			Partitions: []snapshot.Partition{
				{ID: 0, Leader: "", Replicas: []string{"2"}, ISR: nil, EarliestOffset: 0, LatestOffset: 9},
			},
		}},
	}
	res, err := Map(snap, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if got := actionsOfKind(t, res.Entries, "P"); len(got) != 0 {
		t.Fatalf("produces to a dead partition can never ack; got %v", got)
	}
	found := false
	for _, w := range res.Warnings {
		if strings.Contains(w, "skipping its produces") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected a skip warning, got %v", res.Warnings)
	}
}

func TestMapBrokerCap(t *testing.T) {
	snap := snapshot.ClusterSnapshot{}
	for i := 1; i <= 11; i++ {
		snap.Brokers = append(snap.Brokers, snapshot.Broker{ID: strconv.Itoa(i), Online: true})
	}
	snap.Topics = []snapshot.Topic{{Name: "orders", Partitions: []snapshot.Partition{{ID: 0, Replicas: []string{"1"}, ISR: []string{"1"}}}}}
	if _, err := Map(snap, Options{}); err == nil || !strings.Contains(err.Error(), "--clamp") {
		t.Fatalf("want cap failure mentioning --clamp, got %v", err)
	}
	res, err := Map(snap, Options{Clamp: true})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(res.Size, "10x") {
		t.Fatalf("size %q should render 10 brokers", res.Size)
	}
	// Numeric order: ids 1..10 kept (2 sorts after 10 lexicographically, not here).
	if _, kept := res.BrokerNames["11"]; kept {
		t.Fatalf("broker 11 should be dropped: %v", res.BrokerNames)
	}
	if res.BrokerNames["10"] != "broker-10" {
		t.Fatalf("numeric sort broken: %v", res.BrokerNames)
	}
}

func TestMapTopicAndPartitionCaps(t *testing.T) {
	snap := snapshot.ClusterSnapshot{
		Brokers: []snapshot.Broker{{ID: "1", Online: true}},
	}
	// 8 extra topics (a..h): two over the cap of 6.
	for _, n := range []string{"a", "b", "c", "d", "e", "f", "g", "h"} {
		snap.Topics = append(snap.Topics, snapshot.Topic{Name: n, Partitions: []snapshot.Partition{{ID: 0, Replicas: []string{"1"}, ISR: []string{"1"}}}})
	}
	if _, err := Map(snap, Options{}); err == nil {
		t.Fatal("want topic-cap failure without --clamp")
	}
	res, err := Map(snap, Options{Clamp: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.TopicNames) != 6 {
		t.Fatalf("want 6 kept topics, got %v", res.TopicNames)
	}
	if _, kept := res.TopicNames["g"]; kept {
		t.Fatalf("alphabetical clamp should drop g/h: %v", res.TopicNames)
	}

	// 30 partitions: ids ≥ 24 dropped under clamp.
	wide := snapshot.ClusterSnapshot{Brokers: []snapshot.Broker{{ID: "1", Online: true}}}
	var parts []snapshot.Partition
	for i := 0; i < 30; i++ {
		parts = append(parts, snapshot.Partition{ID: i, Replicas: []string{"1"}, ISR: []string{"1"}, LatestOffset: 1})
	}
	wide.Topics = []snapshot.Topic{{Name: "orders", Partitions: parts}}
	if _, err := Map(wide, Options{}); err == nil {
		t.Fatal("want partition-cap failure without --clamp")
	}
	resWide, err := Map(wide, Options{Clamp: true})
	if err != nil {
		t.Fatal(err)
	}
	if resWide.Size != "1x24x1" {
		t.Fatalf("size %q, want 1x24x1", resWide.Size)
	}
	if got := len(actionsOfKind(t, resWide.Entries, "P")); got != 24 {
		t.Fatalf("want 24 produces (one per kept partition), got %d", got)
	}
}

func TestMapPhantomPrimaryAndRename(t *testing.T) {
	snap := snapshot.ClusterSnapshot{
		Brokers: []snapshot.Broker{{ID: "1", Online: true}, {ID: "2", Online: true}},
		Topics: []snapshot.Topic{{
			Name: "billing.events",
			Partitions: []snapshot.Partition{
				{ID: 0, Leader: "1", Replicas: []string{"1", "2"}, ISR: []string{"1", "2"}, EarliestOffset: 0, LatestOffset: 2},
			},
		}},
	}
	res, err := Map(snap, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Size != "2x1x1xbilling-events~1~2~1~d~~~~" {
		t.Fatalf("size = %q", res.Size)
	}
	if res.TopicNames["billing.events"] != "billing-events" {
		t.Fatalf("rename map: %v", res.TopicNames)
	}
	for _, e := range res.Entries {
		if p, ok := e.Action.(actionlog.ProduceRecord); ok && p.Topic != "billing-events" {
			t.Fatalf("produce targets %q, want sanitized name", p.Topic)
		}
	}
	renamed := false
	for _, w := range res.Warnings {
		if strings.Contains(w, "renamed") {
			renamed = true
		}
	}
	if !renamed {
		t.Fatalf("want a rename warning, got %v", res.Warnings)
	}
}

func TestMapNoBrokersFails(t *testing.T) {
	if _, err := Map(snapshot.ClusterSnapshot{}, Options{}); err == nil {
		t.Fatal("want error for empty snapshot")
	}
}

// snapWithGroup returns threeBrokerSnap() with one consumer group attached to
// "orders", committed at the given per-partition offsets (partitions absent
// from the map have no committed offset).
func snapWithGroup(group snapshot.Group) snapshot.ClusterSnapshot {
	snap := threeBrokerSnap()
	snap.Groups = []snapshot.Group{group}
	return snap
}

func TestMapConsumerGroupCaughtUp(t *testing.T) {
	res, err := Map(snapWithGroup(snapshot.Group{
		ID:      "analytics",
		State:   "Stable",
		Members: []snapshot.GroupMember{{ID: "m-1", ClientID: "worker-1"}},
		Offsets: map[string]map[int]int64{"orders": {0: 5, 1: 4, 2: 0}}, // == LatestOffset everywhere
	}), Options{})
	if err != nil {
		t.Fatal(err)
	}
	ag := actionsOfKind(t, res.Entries, "AG")
	if len(ag) != 1 || !strings.HasPrefix(ag[0], "AG:analytics:orders@") {
		t.Fatalf("add_group: %v", ag)
	}
	ac := actionsOfKind(t, res.Entries, "AC")
	if len(ac) != 1 || !strings.Contains(ac[0], "analytics:worker-1") {
		t.Fatalf("add_consumer: %v", ac)
	}
	if cc := actionsOfKind(t, res.Entries, "C"); len(cc) != 0 {
		t.Fatalf("caught-up group should not need an autoOffsetReset override, got %v", cc)
	}
	if res.GroupNames["analytics"] != "analytics" {
		t.Fatalf("GroupNames: %v", res.GroupNames)
	}
}

func TestMapConsumerGroupWithLag(t *testing.T) {
	res, err := Map(snapWithGroup(snapshot.Group{
		ID:      "analytics",
		State:   "Stable",
		Members: []snapshot.GroupMember{{ID: "m-1", ClientID: "worker-1"}},
		Offsets: map[string]map[int]int64{"orders": {0: 1, 1: 4, 2: 0}}, // partition 0 lags by 4
	}), Options{})
	if err != nil {
		t.Fatal(err)
	}
	cc := actionsOfKind(t, res.Entries, "C")
	if len(cc) != 1 || !strings.Contains(cc[0], "group.analytics.consumer.autoOffsetReset:earliest") {
		t.Fatalf("want an earliest autoOffsetReset override, got %v", cc)
	}
	lagWarned := false
	for _, w := range res.Warnings {
		if strings.Contains(w, "lag") {
			lagWarned = true
		}
	}
	if !lagWarned {
		t.Fatalf("want a lag warning, got %v", res.Warnings)
	}
}

func TestMapConsumerGroupNoMatchingTopicSkipped(t *testing.T) {
	res, err := Map(snapWithGroup(snapshot.Group{
		ID:      "connect-sink",
		Members: []snapshot.GroupMember{{ID: "m-1"}},
		Offsets: map[string]map[int]int64{"never-selected": {0: 1}},
	}), Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(actionsOfKind(t, res.Entries, "AG")) != 0 {
		t.Fatalf("group on an unselected topic must not be emitted: %v", res.Entries)
	}
	if _, ok := res.GroupNames["connect-sink"]; ok {
		t.Fatalf("skipped group must be absent from GroupNames: %v", res.GroupNames)
	}
	skipped := false
	for _, w := range res.Warnings {
		if strings.Contains(w, "connect-sink") && strings.Contains(w, "skipping") {
			skipped = true
		}
	}
	if !skipped {
		t.Fatalf("want a skip warning, got %v", res.Warnings)
	}
}

func TestMapConsumerGroupsCapClamped(t *testing.T) {
	snap := threeBrokerSnap()
	for i := 0; i < MaxGroups+2; i++ {
		id := "g" + strconv.Itoa(i)
		snap.Groups = append(snap.Groups, snapshot.Group{ID: id, Offsets: map[string]map[int]int64{"orders": {0: 5}}})
	}
	res, err := Map(snap, Options{Clamp: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.GroupNames) != MaxGroups {
		t.Fatalf("want %d groups kept, got %d: %v", MaxGroups, len(res.GroupNames), res.GroupNames)
	}
	if _, err := Map(snap, Options{}); err == nil {
		t.Fatal("without --clamp, exceeding MaxGroups should fail")
	}
}

func TestMapConsumerGroupMembersCappedAndDeduped(t *testing.T) {
	members := make([]snapshot.GroupMember, MaxGroupMembers+2)
	for i := range members {
		members[i] = snapshot.GroupMember{ID: "m-" + strconv.Itoa(i), ClientID: "worker"} // all share one ClientID
	}
	res, err := Map(snapWithGroup(snapshot.Group{
		ID: "analytics", Members: members,
		Offsets: map[string]map[int]int64{"orders": {0: 5, 1: 4, 2: 0}},
	}), Options{})
	if err != nil {
		t.Fatal(err)
	}
	ac := actionsOfKind(t, res.Entries, "AC")
	if len(ac) != MaxGroupMembers {
		t.Fatalf("want %d add_consumer entries (capped), got %d: %v", MaxGroupMembers, len(ac), ac)
	}
	seen := map[string]bool{}
	for _, e := range ac {
		if seen[e] {
			t.Fatalf("duplicate add_consumer entry despite distinct real members: %v", ac)
		}
		seen[e] = true
	}
}

func TestMapTopicConfigInSizeSlug(t *testing.T) {
	snap := snapshot.ClusterSnapshot{
		Brokers: []snapshot.Broker{{ID: "1", Online: true}, {ID: "2", Online: true}, {ID: "3", Online: true}},
		Topics: []snapshot.Topic{
			{
				Name: "orders",
				Config: snapshot.TopicConfig{
					CleanupPolicy: "compact", MinInSyncReplicas: intp(1), RetentionMs: i64p(3600000),
				},
				Partitions: []snapshot.Partition{
					{ID: 0, Leader: "1", Replicas: []string{"1", "2", "3"}, ISR: []string{"1", "2", "3"}},
				},
			},
			{
				Name: "payments",
				Config: snapshot.TopicConfig{
					CleanupPolicy: "compact,delete", MinInSyncReplicas: intp(2), RetentionBytes: i64p(500000),
				},
				Partitions: []snapshot.Partition{
					{ID: 0, Leader: "1", Replicas: []string{"1", "2"}, ISR: []string{"1", "2"}},
				},
			},
		},
	}
	res, err := Map(snap, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.Size, "payments~1~2~2~cd~~500000~~") {
		t.Fatalf("extra topic config missing from size: %q", res.Size)
	}
	if !strings.Contains(res.Size, "1~c~3600000~~") {
		t.Fatalf("primary topic config missing from size: %q", res.Size)
	}
}

func intp(v int) *int { return &v }
