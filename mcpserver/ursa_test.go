package mcpserver

import (
	"strings"
	"testing"

	"github.com/monedula-dev/monedula-sim-link/simurl"
)

// TestShapeUrsa covers the ursa-3az shape: the default omits size, a shaped
// slug keeps orders diskless with the flag d at tail index 17 unless the shape
// turns it off, and diskless and disk topics sit side by side. Each slug here is
// a SIZE-IMPL fixture the TypeScript codec re-encodes unchanged.
func TestShapeUrsa(t *testing.T) {
	for _, c := range []struct {
		name  string
		shape *ClusterShape
		want  string
	}{
		{"no shape", nil, ""},
		{"empty shape", &ClusterShape{}, ""},
		{"preset values", &ClusterShape{Brokers: 3, Topics: []TopicShape{{Name: "orders", Partitions: 3, ReplicationFactor: 1}}}, ""},
		{"diskless said explicitly", &ClusterShape{Topics: []TopicShape{{Name: "orders", Partitions: 3, ReplicationFactor: 1, DisklessEnable: boolp(true)}}}, ""},
		{"four brokers", &ClusterShape{Brokers: 4}, "4x3x1xxxxxxxxxxxxxxxxxxd"},
		{"six brokers six partitions", &ClusterShape{Brokers: 6, Topics: []TopicShape{{Name: "orders", Partitions: 6, ReplicationFactor: 1}}}, "6x6x1xxxxxxxxxxxxxxxxxxd"},
		{"orders a disk topic at RF 3", &ClusterShape{Topics: []TopicShape{{Name: "orders", Partitions: 3, ReplicationFactor: 3, DisklessEnable: boolp(false)}}}, "3x3x3"},
		{"orders a disk topic at RF 1", &ClusterShape{Topics: []TopicShape{{Name: "orders", Partitions: 3, ReplicationFactor: 1, DisklessEnable: boolp(false)}}}, "3x3x1"},
		{"compacted disk orders", &ClusterShape{Topics: []TopicShape{{Name: "orders", Partitions: 3, ReplicationFactor: 3, MinInSyncReplicas: intp(3), CleanupPolicy: "compact", DisklessEnable: boolp(false)}}}, "3x3x3xxxxxxxxxxx3~c~~~"},
		{"timing", &ClusterShape{DisklessTiming: &DisklessTimingShape{CommitIntervalMs: intp(250), UploadMs: intp(900)}}, "3x3x1xxxxxxxxxxxxxxxxxxd250.900"},
		{"disk orders, diskless extra, timing", &ClusterShape{
			Topics:         []TopicShape{{Name: "orders", Partitions: 3, ReplicationFactor: 3, DisklessEnable: boolp(false)}, {Name: "ev", Partitions: 2, ReplicationFactor: 1, DisklessEnable: boolp(true)}},
			DisklessTiming: &DisklessTimingShape{UploadMs: intp(900)},
		}, "3x3x3xev~2~1~1~d~~~~~1xxxxxxxxxxxxxxxxx.900"},
		{"compacted disk extra", &ClusterShape{Topics: []TopicShape{{Name: "ev", Partitions: 3, ReplicationFactor: 3, MinInSyncReplicas: intp(2), CleanupPolicy: "compact"}}}, "3x3x1xev~3~3~2~c~~~~xxxxxxxxxxxxxxxxxd"},
		{"diskless extra", &ClusterShape{Topics: []TopicShape{{Name: "ev", Partitions: 3, ReplicationFactor: 1, DisklessEnable: boolp(true)}}}, "3x3x1xev~3~1~1~d~~~~~1xxxxxxxxxxxxxxxxxd"},
		{"disk extra on five brokers", &ClusterShape{Brokers: 5, Topics: []TopicShape{{Name: "orders", Partitions: 4, ReplicationFactor: 1}, {Name: "audit", Partitions: 2, ReplicationFactor: 2}}}, "5x4x1xaudit~2~2~1~d~~~~xxxxxxxxxxxxxxxxxd"},
		{"rack-aware", &ClusterShape{RackAware: true}, "3x3x1xxxx1xxxxxxxxxxxxxxd"},
		{"registry", &ClusterShape{SchemaRegistry: &SchemaRegistryShape{InstancesPerDc: 1}}, "3x3x1xxxxxxxxxxxxxxx2xxxd"},
	} {
		t.Run(c.name, func(t *testing.T) {
			cluster, size, _, err := shapeToParams("ursa-3az", c.shape)
			if err != nil {
				t.Fatalf("shapeToParams: %v", err)
			}
			if cluster != "ursa-3az" || size != c.want {
				t.Errorf("cluster %q size %q, want ursa-3az %q", cluster, size, c.want)
			}
		})
	}

	// Fewer than three brokers: payments is created at a lower RF, and a note
	// says so.
	_, size, notes, err := shapeToParams("ursa-3az", &ClusterShape{Brokers: 2})
	if err != nil || size != "2x3x1xxxxxxxxxxxxxxxxxxd" || !strings.Contains(strings.Join(notes, "\n"), "payments") {
		t.Errorf("2 brokers: size %q notes %v err %v", size, notes, err)
	}
}

func TestShapeUrsaValidation(t *testing.T) {
	for _, c := range []struct {
		name    string
		shape   *ClusterShape
		needles []string
	}{
		{"diskless orders at RF 3", &ClusterShape{Topics: []TopicShape{{Name: "orders", Partitions: 3, ReplicationFactor: 3}}}, []string{"primary 'orders'", "replicationFactor 3", "Ursa forces replication factor 1 on diskless topics", "disklessEnable false"}},
		{"diskless extra at RF 2", &ClusterShape{Topics: []TopicShape{{Name: "ev", Partitions: 1, ReplicationFactor: 2, DisklessEnable: boolp(true)}}}, []string{"\"ev\"", "Ursa forces replication factor 1"}},
		{"compacted diskless orders", &ClusterShape{Topics: []TopicShape{{Name: "orders", Partitions: 3, ReplicationFactor: 1, CleanupPolicy: "compact"}}}, []string{"primary 'orders'", "Ursa does not support compacted diskless topics yet"}},
		{"compacted diskless extra", &ClusterShape{Topics: []TopicShape{{Name: "ev", Partitions: 1, ReplicationFactor: 1, CleanupPolicy: "compact,delete", DisklessEnable: boolp(true)}}}, []string{"\"ev\"", "compacted diskless"}},
		{"payments taken", &ClusterShape{Topics: []TopicShape{{Name: "payments", Partitions: 1, ReplicationFactor: 1}}}, []string{"\"payments\"", "already has a topic"}},
		{"seven brokers", &ClusterShape{Brokers: 7}, []string{"7 total brokers", "ursa-3az cap of 6"}},
		{"seven partitions", &ClusterShape{Topics: []TopicShape{{Name: "orders", Partitions: 7, ReplicationFactor: 1}}}, []string{"partitions 7", "cap of 6"}},
		{"timing without a diskless topic", &ClusterShape{
			Topics:         []TopicShape{{Name: "orders", Partitions: 3, ReplicationFactor: 3, DisklessEnable: boolp(false)}},
			DisklessTiming: &DisklessTimingShape{UploadMs: intp(900)},
		}, []string{"shape.disklessTiming", "requires a diskless topic"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, _, _, err := shapeToParams("ursa-3az", c.shape)
			if err == nil {
				t.Fatalf("expected an error")
			}
			for _, n := range c.needles {
				if !strings.Contains(err.Error(), n) {
					t.Errorf("error %q does not mention %q", err, n)
				}
			}
		})
	}
}

// TestBuildURLUrsa builds the ursa-3az preset end to end: the default cluster
// with a broker kill, restart, and a pause and resume of a broker added after
// the topics were placed (it holds no payments replica), echoed with the
// playground's own codes.
func TestBuildURLUrsa(t *testing.T) {
	s := newSession(t)
	res := callTool(t, s, "build_playground_url", map[string]any{
		"cluster": "ursa-3az",
		"actions": []map[string]any{
			{"kind": "produce", "at": 500, "producerId": "p-a", "topic": "orders", "key": "k1"},
			{"kind": "add_broker", "at": 1000, "brokerId": "broker-4", "rack": "az-a"},
			{"kind": "pause_broker", "at": 2000, "brokerId": "broker-4"},
			{"kind": "resume_broker", "at": 4000, "brokerId": "broker-4"},
			{"kind": "kill_broker", "at": 6000, "brokerId": "broker-1"},
			{"kind": "restart_broker", "at": 9000, "brokerId": "broker-1"},
			{"kind": "start_object_storage", "at": 10000},
		},
	})
	var out BuildPlaygroundURLOutput
	structured(t, res, &out)
	if out.Cluster != "ursa-3az" || out.Size != "" || !strings.HasPrefix(out.URL, simurl.DefaultBaseURL) || !strings.Contains(out.URL, "cluster=ursa-3az") {
		t.Errorf("cluster %q size %q URL %q, want the ursa-3az default", out.Cluster, out.Size, out.URL)
	}
	if len(out.Notes) != 0 {
		t.Errorf("notes %v, want none", out.Notes)
	}
	kinds := []string{"produce", "add_broker", "pause_broker", "resume_broker", "kill_broker", "restart_broker", "start_object_storage"}
	if len(out.DecodedActions) != len(kinds) {
		t.Fatalf("echoed %+v", out.DecodedActions)
	}
	for i, k := range kinds {
		if out.DecodedActions[i].Kind != k {
			t.Errorf("echoed[%d] kind %q, want %q", i, out.DecodedActions[i].Kind, k)
		}
	}
	if !strings.Contains(actionsParamOf(t, out.URL), "BP:broker-4@2000,BU:broker-4@4000") {
		t.Errorf("URL actions param %q lacks the BP/BU codes", actionsParamOf(t, out.URL))
	}

	// A shape keeps orders diskless through the size's flag.
	res = callTool(t, s, "build_playground_url", map[string]any{"cluster": "ursa-3az", "shape": map[string]any{"brokers": 6}})
	structured(t, res, &out)
	if out.Size != "6x3x1xxxxxxxxxxxxxxxxxxd" || !strings.Contains(out.URL, "size=6x3x1xxxxxxxxxxxxxxxxxxd") {
		t.Errorf("6 brokers: size %q URL %q", out.Size, out.URL)
	}
}

// TestBuildURLUrsaRefusals: the actions the playground's engine refuses on
// ursa-3az are refused here with the engine's own copy, and a pause whose
// outcome depends on replica placement gets a note.
func TestBuildURLUrsaRefusals(t *testing.T) {
	s := newSession(t)
	build := func(shape map[string]any, actions ...map[string]any) *BuildPlaygroundURLOutput {
		args := map[string]any{"cluster": "ursa-3az", "actions": actions}
		if shape != nil {
			args["shape"] = shape
		}
		res := callTool(t, s, "build_playground_url", args)
		if res.IsError {
			t.Fatalf("unexpected error: %s", errText(res))
		}
		var out BuildPlaygroundURLOutput
		structured(t, res, &out)
		return &out
	}
	refused := func(shape map[string]any, needles []string, actions ...map[string]any) {
		t.Helper()
		args := map[string]any{"cluster": "ursa-3az", "actions": actions}
		if shape != nil {
			args["shape"] = shape
		}
		wantToolError(t, callTool(t, s, "build_playground_url", args), needles...)
	}

	for _, kind := range []string{"stop_object_storage", "slow_object_storage"} {
		refused(nil, []string{"actions[0]", kind, "refused on ursa-3az", ursaStorageRefusal},
			map[string]any{"kind": kind, "at": 1000})
	}

	// Every starting broker of a cluster of at most 3 holds payments replicas.
	for _, b := range []string{"broker-1", "broker-2", "broker-3"} {
		refused(nil, []string{"actions[0]", "pause_broker", ursaPauseRule, b + " holds disk-topic replicas", "payments-0, payments-1, payments-2", "add_broker"},
			map[string]any{"kind": "pause_broker", "at": 1000, "brokerId": b})
	}
	refused(map[string]any{"brokers": 2}, []string{"replication factor 2 on 2 broker(s)"},
		map[string]any{"kind": "pause_broker", "at": 1000, "brokerId": "broker-2"})

	// A reassignment before the pause may have moved the replicas: a note.
	out := build(nil,
		map[string]any{"kind": "reassign_partition", "at": 1000, "topic": "payments", "partition": 0, "replicas": []string{"broker-1", "broker-2"}},
		map[string]any{"kind": "pause_broker", "at": 2000, "brokerId": "broker-3"})
	if !strings.Contains(strings.Join(out.Notes, "\n"), "does not compute replica placement") {
		t.Errorf("notes %v, want the placement note", out.Notes)
	}
	// The same reassignment after the pause does not help.
	refused(nil, []string{"actions[1]", ursaPauseRule},
		map[string]any{"kind": "reassign_partition", "at": 3000, "topic": "payments", "partition": 0, "replicas": []string{"broker-1", "broker-2"}},
		map[string]any{"kind": "pause_broker", "at": 2000, "brokerId": "broker-3"})

	// On more than 3 brokers the placement decides: a note, no refusal.
	out = build(map[string]any{"brokers": 6}, map[string]any{"kind": "pause_broker", "at": 1000, "brokerId": "broker-6"})
	if len(out.Notes) != 1 || !strings.Contains(out.Notes[0], "pause_broker broker-6") {
		t.Errorf("notes %v, want the placement note", out.Notes)
	}

	// Share groups: refused on a diskless topic, fine on a disk topic.
	refused(nil, []string{"actions[0]", "add_share_group", "\"orders\" is diskless", ursaShareGroupRefusal},
		map[string]any{"kind": "add_share_group", "at": 1000, "groupId": "sg", "topic": "orders"})
	build(nil, map[string]any{"kind": "add_share_group", "at": 1000, "groupId": "sg", "topic": "payments"})
	build(map[string]any{"topics": []map[string]any{{"name": "orders", "partitions": 3, "replicationFactor": 3, "disklessEnable": false}}},
		map[string]any{"kind": "add_share_group", "at": 1000, "groupId": "sg", "topic": "orders"})

	// Other clusters are untouched by these checks.
	res := callTool(t, s, "build_playground_url", map[string]any{
		"cluster": "kafscale-3b",
		"actions": []map[string]any{{"kind": "pause_broker", "at": 1000, "brokerId": "broker-1"}, {"kind": "stop_object_storage", "at": 2000}},
	})
	if res.IsError {
		t.Errorf("kafscale-3b: %s", errText(res))
	}
}

func TestInitialBrokerIndex(t *testing.T) {
	for _, c := range []struct {
		id    string
		total int
		n     int
		ok    bool
	}{
		{"broker-1", 3, 1, true},
		{"broker-3", 3, 3, true},
		{"broker-4", 3, 0, false},
		{"broker-0", 3, 0, false},
		{"broker-01", 3, 0, false},
		{"b-1", 3, 0, false},
		{"broker-", 3, 0, false},
	} {
		n, ok := initialBrokerIndex(c.id, c.total)
		if n != c.n || ok != c.ok {
			t.Errorf("%s of %d: %d %v, want %d %v", c.id, c.total, n, ok, c.n, c.ok)
		}
	}
}
