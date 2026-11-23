package mcpserver

import (
	"strings"
	"testing"
)

// TestShapeRedpanda covers the redpanda-3az shape: the default omits size,
// topics keep their own replication factor and carry no diskless flag, and a
// diskless timing applies with no diskless topic (every topic is a cloud topic
// by the cluster). Each slug here is a SIZE-IMPL fixture the TypeScript codec
// re-encodes unchanged.
func TestShapeRedpanda(t *testing.T) {
	for _, c := range []struct {
		name  string
		shape *ClusterShape
		want  string
	}{
		{"no shape", nil, ""},
		{"empty shape", &ClusterShape{}, ""},
		{"preset values", &ClusterShape{Brokers: 3, Topics: []TopicShape{{Name: "orders", Partitions: 6, ReplicationFactor: 3}}}, ""},
		{"five brokers", &ClusterShape{Brokers: 5}, "5x6x3"},
		{"nine brokers", &ClusterShape{Brokers: 9}, "9x6x3"},
		{"one broker", &ClusterShape{Brokers: 1, Topics: []TopicShape{{Name: "orders", Partitions: 3, ReplicationFactor: 1}}}, "1x3x1"},
		{"min.ISR", &ClusterShape{Topics: []TopicShape{{Name: "orders", Partitions: 4, ReplicationFactor: 3, MinInSyncReplicas: intp(3)}}}, "3x4x3xxxxxxxxxxx3~~~~"},
		{"extra topic", &ClusterShape{Topics: []TopicShape{{Name: "ev", Partitions: 2, ReplicationFactor: 3, MinInSyncReplicas: intp(2)}}}, "3x6x3xev~2~3~2~d~~~~"},
		{"timing", &ClusterShape{DisklessTiming: &DisklessTimingShape{CommitIntervalMs: intp(250), UploadMs: intp(900)}}, "3x6x3xxxxxxxxxxxxxxxxxx250.900"},
		{"upload time", &ClusterShape{DisklessTiming: &DisklessTimingShape{UploadMs: intp(900)}}, "3x6x3xxxxxxxxxxxxxxxxxx.900"},
		{"rack-aware", &ClusterShape{RackAware: true}, "3x6x3xxxx1x"},
		{"registry", &ClusterShape{SchemaRegistry: &SchemaRegistryShape{InstancesPerDc: 1}}, "3x6x3xxxxxxxxxxxxxxx2"},
	} {
		t.Run(c.name, func(t *testing.T) {
			cluster, size, _, err := shapeToParams("redpanda-3az", c.shape)
			if err != nil {
				t.Fatalf("shapeToParams: %v", err)
			}
			if cluster != "redpanda-3az" || size != c.want {
				t.Errorf("cluster %q size %q, want redpanda-3az %q", cluster, size, c.want)
			}
		})
	}

	// Fewer brokers than the preset's RF: orders follows them, with a note.
	_, size, notes, err := shapeToParams("redpanda-3az", &ClusterShape{Brokers: 2})
	if err != nil || size != "2x6x2" || len(notes) != 1 {
		t.Errorf("2 brokers: size %q notes %v err %v", size, notes, err)
	}
}

func TestShapeRedpandaValidation(t *testing.T) {
	for _, c := range []struct {
		name    string
		shape   *ClusterShape
		needles []string
	}{
		{"diskless orders", &ClusterShape{Topics: []TopicShape{{Name: "orders", Partitions: 6, ReplicationFactor: 3, DisklessEnable: boolp(true)}}}, []string{"primary 'orders'", "disklessEnable", "every topic is a cloud topic"}},
		{"diskless extra", &ClusterShape{Topics: []TopicShape{{Name: "ev", Partitions: 1, ReplicationFactor: 1, DisklessEnable: boolp(true)}}}, []string{"\"ev\"", "redpanda.storage.mode=cloud"}},
		{"compacted orders", &ClusterShape{Topics: []TopicShape{{Name: "orders", Partitions: 6, ReplicationFactor: 3, CleanupPolicy: "compact"}}}, []string{"primary 'orders'", "Redpanda compacts cloud topics in L1"}},
		{"compacted extra", &ClusterShape{Topics: []TopicShape{{Name: "ev", Partitions: 1, ReplicationFactor: 1, CleanupPolicy: "compact,delete"}}}, []string{"\"ev\"", "cleanup.policy=delete"}},
		{"ten brokers", &ClusterShape{Brokers: 10}, []string{"10 total brokers", "redpanda-3az cap of 9"}},
		{"seven partitions", &ClusterShape{Topics: []TopicShape{{Name: "orders", Partitions: 7, ReplicationFactor: 3}}}, []string{"partitions 7", "cap of 6"}},
		{"RF above brokers", &ClusterShape{Topics: []TopicShape{{Name: "orders", Partitions: 6, ReplicationFactor: 4}}}, []string{"replicationFactor 4", "maximum 3"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, _, _, err := shapeToParams("redpanda-3az", c.shape)
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

// TestBuildURLRedpandaRefusals: remove_broker on a broker that holds replicas
// is refused with the engine's copy where the request shows it, and noted
// where replica placement decides. Object storage actions apply on Redpanda.
func TestBuildURLRedpandaRefusals(t *testing.T) {
	s := newSession(t)
	build := func(shape map[string]any, actions ...map[string]any) *BuildPlaygroundURLOutput {
		t.Helper()
		args := map[string]any{"cluster": "redpanda-3az", "actions": actions}
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
		args := map[string]any{"cluster": "redpanda-3az", "actions": actions}
		if shape != nil {
			args["shape"] = shape
		}
		wantToolError(t, callTool(t, s, "build_playground_url", args), needles...)
	}

	// The preset: orders at RF 3 on 3 brokers puts a replica on every broker.
	for _, b := range []string{"broker-1", "broker-2", "broker-3"} {
		refused(nil, []string{"actions[0]", "remove_broker", b + " holds replicas of every partition of orders", redpandaRemoveBrokerReason},
			map[string]any{"kind": "remove_broker", "at": 1000, "brokerId": b})
	}
	// An extra topic at the full replication factor does the same.
	refused(map[string]any{"brokers": 4, "topics": []map[string]any{{"name": "ev", "partitions": 1, "replicationFactor": 4}}},
		[]string{"every partition of ev", "replication factor 4 on 4 broker(s)"},
		map[string]any{"kind": "remove_broker", "at": 1000, "brokerId": "broker-4"})

	// A reassignment first, or more brokers than the RF: a note.
	out := build(nil,
		map[string]any{"kind": "reassign_partition", "at": 1000, "topic": "orders", "partition": 0, "replicas": []string{"broker-1", "broker-2"}},
		map[string]any{"kind": "remove_broker", "at": 2000, "brokerId": "broker-3"})
	if !strings.Contains(strings.Join(out.Notes, "\n"), "does not compute replica placement") {
		t.Errorf("notes %v, want the placement note", out.Notes)
	}
	out = build(map[string]any{"brokers": 5}, map[string]any{"kind": "remove_broker", "at": 1000, "brokerId": "broker-5"})
	if len(out.Notes) != 1 || !strings.Contains(out.Notes[0], "remove_broker broker-5") {
		t.Errorf("notes %v, want the placement note", out.Notes)
	}
	// A broker added later holds nothing: no note.
	out = build(nil,
		map[string]any{"kind": "add_broker", "at": 1000, "brokerId": "broker-4", "rack": "az-a"},
		map[string]any{"kind": "remove_broker", "at": 2000, "brokerId": "broker-4"})
	if len(out.Notes) != 0 {
		t.Errorf("notes %v, want none", out.Notes)
	}

	// Object storage outages are modelled on Redpanda.
	out = build(nil,
		map[string]any{"kind": "stop_object_storage", "at": 1000},
		map[string]any{"kind": "slow_object_storage", "at": 3000},
		map[string]any{"kind": "start_object_storage", "at": 5000})
	if p := actionsParamOf(t, out.URL); p != "OS:0@1000,OS:s@3000,OS:1@5000" {
		t.Errorf("URL actions param %q", p)
	}
}
