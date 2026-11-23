package mcpserver

import (
	"strings"
	"testing"

	"github.com/monedula-dev/monedula-sim-link/simurl"
)

// TestShapeWarpstream covers the warpstream-3az shape: the default omits size,
// past one Agent per zone the slug carries the per-Agent zone cycle at tail
// index 4, and every topic is diskless at RF 1. Each slug here is a SIZE-IMPL
// fixture the TypeScript codec re-encodes unchanged.
func TestShapeWarpstream(t *testing.T) {
	for _, c := range []struct {
		name  string
		shape *ClusterShape
		want  string
	}{
		{"no shape", nil, ""},
		{"empty shape", &ClusterShape{}, ""},
		{"preset values", &ClusterShape{Brokers: 3, Topics: []TopicShape{{Name: "orders", Partitions: 3, ReplicationFactor: 1}}}, ""},
		{"diskless flag changes nothing", &ClusterShape{Topics: []TopicShape{{Name: "orders", Partitions: 3, ReplicationFactor: 1, DisklessEnable: boolp(true)}}}, ""},
		{"four Agents", &ClusterShape{Brokers: 4}, "4x3x1xxxxxaz-a!az-b!az-c!az-a"},
		{"five Agents four partitions", &ClusterShape{Brokers: 5, Topics: []TopicShape{{Name: "orders", Partitions: 4, ReplicationFactor: 1}}}, "5x4x1xxxxxaz-a!az-b!az-c!az-a!az-b"},
		{"six Agents six partitions", &ClusterShape{Brokers: 6, Topics: []TopicShape{{Name: "orders", Partitions: 6, ReplicationFactor: 1}}}, "6x6x1xxxxxaz-a!az-b!az-c!az-a!az-b!az-c"},
		{"timing", &ClusterShape{DisklessTiming: &DisklessTimingShape{CommitIntervalMs: intp(250), UploadMs: intp(900)}}, "3x3x1xxxxxxxxxxxxxxxxxx250.900"},
		{"extra topic is diskless", &ClusterShape{Topics: []TopicShape{{Name: "ev", Partitions: 2, ReplicationFactor: 1}}}, "3x3x1xev~2~1~1~d~~~~~1"},
		{"extra topic, four Agents, upload time", &ClusterShape{
			Brokers:        4,
			Topics:         []TopicShape{{Name: "orders", Partitions: 2, ReplicationFactor: 1}, {Name: "ev", Partitions: 1, ReplicationFactor: 1}},
			DisklessTiming: &DisklessTimingShape{UploadMs: intp(900)},
		}, "4x2x1xev~1~1~1~d~~~~~1xxxxaz-a!az-b!az-c!az-axxxxxxxxxxxxx.900"},
		{"registry", &ClusterShape{SchemaRegistry: &SchemaRegistryShape{InstancesPerDc: 1}}, "3x3x1xxxxxxxxxxxxxxx2"},
		{"rack-aware", &ClusterShape{RackAware: true}, "3x3x1xxxx1x"},
	} {
		t.Run(c.name, func(t *testing.T) {
			cluster, size, _, err := shapeToParams("warpstream-3az", c.shape)
			if err != nil {
				t.Fatalf("shapeToParams: %v", err)
			}
			if cluster != "warpstream-3az" || size != c.want {
				t.Errorf("cluster %q size %q, want warpstream-3az %q", cluster, size, c.want)
			}
		})
	}
}

func TestShapeWarpstreamValidation(t *testing.T) {
	for _, c := range []struct {
		name    string
		shape   *ClusterShape
		needles []string
	}{
		{"two Agents", &ClusterShape{Brokers: 2}, []string{"2 total brokers", "warpstream-3az minimum of 3", "az-a, az-b, az-c"}},
		{"seven Agents", &ClusterShape{Brokers: 7}, []string{"7 total brokers", "warpstream-3az cap of 6"}},
		{"orders at RF 3", &ClusterShape{Topics: []TopicShape{{Name: "orders", Partitions: 3, ReplicationFactor: 3}}}, []string{"primary 'orders'", "replicationFactor 3", warpstreamNoReplicasReason}},
		{"extra at RF 2", &ClusterShape{Topics: []TopicShape{{Name: "ev", Partitions: 1, ReplicationFactor: 2}}}, []string{"\"ev\"", "replication factor 1"}},
		{"compacted orders", &ClusterShape{Topics: []TopicShape{{Name: "orders", Partitions: 3, ReplicationFactor: 1, CleanupPolicy: "compact"}}}, []string{"primary 'orders'", "does not model file compaction"}},
		{"compacted extra", &ClusterShape{Topics: []TopicShape{{Name: "ev", Partitions: 1, ReplicationFactor: 1, CleanupPolicy: "compact,delete"}}}, []string{"\"ev\"", "cleanup.policy=delete"}},
		{"seven partitions", &ClusterShape{Topics: []TopicShape{{Name: "orders", Partitions: 7, ReplicationFactor: 1}}}, []string{"partitions 7", "cap of 6"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, _, _, err := shapeToParams("warpstream-3az", c.shape)
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

// TestBuildURLWarpstream builds the warpstream-3az preset end to end: the
// control plane stopped and started, an Agent killed and restarted once the
// control plane is back, and a reassignment to one Agent.
func TestBuildURLWarpstream(t *testing.T) {
	s := newSession(t)
	res := callTool(t, s, "build_playground_url", map[string]any{
		"cluster": "warpstream-3az",
		"actions": []map[string]any{
			{"kind": "produce", "at": 500, "producerId": "p-a", "topic": "orders", "key": "k1"},
			{"kind": "stop_control_plane", "at": 1000},
			{"kind": "kill_broker", "at": 2000, "brokerId": "broker-2"},
			{"kind": "start_control_plane", "at": 4000},
			{"kind": "restart_broker", "at": 5000, "brokerId": "broker-2"},
			{"kind": "reassign_partition", "at": 6000, "topic": "orders", "partition": 0, "replicas": []string{"broker-3"}},
			{"kind": "start_object_storage", "at": 7000},
		},
	})
	var out BuildPlaygroundURLOutput
	structured(t, res, &out)
	if out.Cluster != "warpstream-3az" || out.Size != "" || !strings.HasPrefix(out.URL, simurl.DefaultBaseURL) || !strings.Contains(out.URL, "cluster=warpstream-3az") {
		t.Errorf("cluster %q size %q URL %q, want the warpstream-3az default", out.Cluster, out.Size, out.URL)
	}
	if len(out.Notes) != 0 {
		t.Errorf("notes %v, want none", out.Notes)
	}
	kinds := []string{"produce", "stop_control_plane", "kill_broker", "start_control_plane", "restart_broker", "reassign_partition", "start_object_storage"}
	if len(out.DecodedActions) != len(kinds) {
		t.Fatalf("echoed %+v", out.DecodedActions)
	}
	for i, k := range kinds {
		if out.DecodedActions[i].Kind != k {
			t.Errorf("echoed[%d] kind %q, want %q", i, out.DecodedActions[i].Kind, k)
		}
	}
	if p := actionsParamOf(t, out.URL); !strings.Contains(p, "CP:0@1000") || !strings.Contains(p, "CP:1@4000") {
		t.Errorf("URL actions param %q lacks the CP codes", p)
	}

	// Four Agents: the slug carries the zone cycle.
	res = callTool(t, s, "build_playground_url", map[string]any{"cluster": "warpstream-3az", "shape": map[string]any{"brokers": 4}})
	structured(t, res, &out)
	if out.Size != "4x3x1xxxxxaz-a!az-b!az-c!az-a" {
		t.Errorf("4 Agents: size %q URL %q", out.Size, out.URL)
	}
}

// TestBuildURLWarpstreamRefusals: the actions the playground's engine refuses
// on warpstream-3az are refused here with the engine's own copy.
func TestBuildURLWarpstreamRefusals(t *testing.T) {
	s := newSession(t)
	refused := func(needles []string, actions ...map[string]any) {
		t.Helper()
		wantToolError(t, callTool(t, s, "build_playground_url", map[string]any{"cluster": "warpstream-3az", "actions": actions}), needles...)
	}
	ok := func(actions ...map[string]any) {
		t.Helper()
		res := callTool(t, s, "build_playground_url", map[string]any{"cluster": "warpstream-3az", "actions": actions})
		if res.IsError {
			t.Errorf("unexpected error: %s", errText(res))
		}
	}

	for _, kind := range []string{"stop_object_storage", "slow_object_storage"} {
		refused([]string{"actions[0]", kind, "refused on warpstream-3az", warpstreamStorageRefusal},
			map[string]any{"kind": kind, "at": 1000})
	}

	// An Agent cannot start while the control plane is down.
	refused([]string{"actions[2]", "restart_broker", warpstreamAgentStartRefusal},
		map[string]any{"kind": "kill_broker", "at": 1000, "brokerId": "broker-1"},
		map[string]any{"kind": "stop_control_plane", "at": 2000},
		map[string]any{"kind": "restart_broker", "at": 3000, "brokerId": "broker-1"})
	refused([]string{"actions[1]", "add_broker", warpstreamAgentStartRefusal},
		map[string]any{"kind": "stop_control_plane", "at": 1000},
		map[string]any{"kind": "add_broker", "at": 2000, "brokerId": "broker-4", "rack": "az-a"})
	// Replay order is by at, not list order: the stop listed last still comes first.
	refused([]string{"actions[0]", "add_broker"},
		map[string]any{"kind": "add_broker", "at": 2000, "brokerId": "broker-4", "rack": "az-a"},
		map[string]any{"kind": "stop_control_plane", "at": 1000})
	// After start_control_plane, or before the stop, an Agent starts.
	ok(map[string]any{"kind": "kill_broker", "at": 1000, "brokerId": "broker-1"},
		map[string]any{"kind": "restart_broker", "at": 1500, "brokerId": "broker-1"},
		map[string]any{"kind": "stop_control_plane", "at": 2000},
		map[string]any{"kind": "start_control_plane", "at": 3000},
		map[string]any{"kind": "add_broker", "at": 4000, "brokerId": "broker-4", "rack": "az-a"})

	// No replicas: a reassign to more than one Agent, and any RF change.
	refused([]string{"actions[0]", "reassign_partition", warpstreamNoReplicasReason},
		map[string]any{"kind": "reassign_partition", "at": 1000, "topic": "orders", "partition": 0, "replicas": []string{"broker-1", "broker-2"}})
	refused([]string{"actions[0]", "change_replication_factor", warpstreamNoReplicasReason},
		map[string]any{"kind": "change_replication_factor", "at": 1000, "topic": "orders", "targetReplicationFactor": 1})

	refused([]string{"actions[0]", "add_share_group", warpstreamShareGroupRefusal},
		map[string]any{"kind": "add_share_group", "at": 1000, "groupId": "sg", "topic": "orders"})

	// Other clusters are untouched by these checks.
	res := callTool(t, s, "build_playground_url", map[string]any{
		"cluster": "automq-3az",
		"actions": []map[string]any{{"kind": "stop_control_plane", "at": 1000}, {"kind": "add_broker", "at": 2000, "brokerId": "broker-4"}, {"kind": "stop_object_storage", "at": 3000}},
	})
	if res.IsError {
		t.Errorf("automq-3az: %s", errText(res))
	}
}
