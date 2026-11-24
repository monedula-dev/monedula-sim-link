package mcpserver

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func boolp(b bool) *bool { return &b }

// TestShapeStreamhouseMatchesTypeScriptFixtures replays the SIZE-SINK lines of
// mapping/testdata/ts-fixtures.txt (v3.0 Streamhouse, next), dumped from the
// real TypeScript codec: the shape the playground decodes from a slug must
// come back out of shapeToParams as the slug the playground itself writes, or
// no size at all where it omits one. Lines without a Schema Registry are not
// replayed: the tool always keeps the preset's registry.
func TestShapeStreamhouseMatchesTypeScriptFixtures(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "mapping", "testdata", "ts-fixtures.txt"))
	if err != nil {
		t.Fatalf("read fixtures: %v", err)
	}
	checked, noRegistry := 0, 0
	for _, line := range strings.Split(strings.ReplaceAll(string(raw), "\r\n", "\n"), "\n") {
		if !strings.HasPrefix(line, "SIZE-SINK ") {
			continue
		}
		head, js, ok := strings.Cut(strings.TrimPrefix(line, "SIZE-SINK "), " :: ")
		fields := strings.Split(head, " ")
		if !ok || len(fields) != 3 || fields[1] != "=>" {
			t.Fatalf("malformed SIZE-SINK fixture: %q", line)
		}
		want := fields[2]
		var sz struct {
			BrokersPerZone    []int `json:"brokersPerZone"`
			Partitions        int   `json:"partitions"`
			ReplicationFactor int   `json:"replicationFactor"`
			ExtraTopics       []struct {
				Name              string `json:"name"`
				Partitions        int    `json:"partitions"`
				ReplicationFactor int    `json:"replicationFactor"`
				MinInSyncReplicas int    `json:"minInSyncReplicas"`
				CleanupPolicy     string `json:"cleanupPolicy"`
			} `json:"extraTopics"`
			SchemaRegistry   bool `json:"schemaRegistry"`
			SrInstancesPerDc int  `json:"srInstancesPerDc"`
			Streamhouse      struct {
				Workers          int  `json:"workers"`
				TasksMax         int  `json:"tasksMax"`
				CommitIntervalMs int  `json:"commitIntervalMs"`
				CommitTimeoutMs  int  `json:"commitTimeoutMs"`
				EvolveSchema     bool `json:"evolveSchema"`
			} `json:"streamhouse"`
		}
		if err := json.Unmarshal([]byte(js), &sz); err != nil {
			t.Fatalf("%s: size json: %v", head, err)
		}
		if !sz.SchemaRegistry {
			noRegistry++
			continue
		}
		k := sz.Streamhouse
		shape := &ClusterShape{
			BrokersPerZone: sz.BrokersPerZone,
			Topics:         []TopicShape{{Name: "orders", Partitions: sz.Partitions, ReplicationFactor: sz.ReplicationFactor}},
			SchemaRegistry: &SchemaRegistryShape{InstancesPerDc: sz.SrInstancesPerDc},
			ConnectSink: &ConnectSinkShape{
				Workers: intp(k.Workers), TasksMax: intp(k.TasksMax),
				CommitIntervalMs: intp(k.CommitIntervalMs), CommitTimeoutMs: intp(k.CommitTimeoutMs),
				EvolveSchema: boolp(k.EvolveSchema),
			},
		}
		for _, e := range sz.ExtraTopics {
			shape.Topics = append(shape.Topics, TopicShape{
				Name: e.Name, Partitions: e.Partitions, ReplicationFactor: e.ReplicationFactor,
				MinInSyncReplicas: intp(e.MinInSyncReplicas), CleanupPolicy: e.CleanupPolicy,
			})
		}
		if want == "(default)" {
			want = ""
		}
		cluster, size, _, err := shapeToParams("streamhouse", shape)
		if err != nil {
			t.Errorf("%s: shapeToParams: %v", head, err)
			continue
		}
		if cluster != "streamhouse" || size != want {
			t.Errorf("%s: cluster %q size %q, want %q", head, cluster, size, want)
		}
		checked++
	}
	if checked != 19 || noRegistry != 3 {
		t.Fatalf("replayed %d SIZE-SINK fixtures and skipped %d without a registry, want 19 and 3", checked, noRegistry)
	}
}

// TestShapeStreamhouse covers what the tool writes for the Streamhouse preset:
// the preset's registry in every slug it writes, the sink segment, and the
// tasksMax the playground lowers on a 1-partition orders.
func TestShapeStreamhouse(t *testing.T) {
	for _, c := range []struct {
		name  string
		shape *ClusterShape
		want  string
		note  string
	}{
		{"no shape", nil, "", ""},
		{"empty shape", &ClusterShape{}, "", ""},
		{"explicit preset", &ClusterShape{Brokers: 3, SchemaRegistry: &SchemaRegistryShape{}, ConnectSink: &ConnectSinkShape{Workers: intp(2), TasksMax: intp(2), CommitIntervalMs: intp(2000), CommitTimeoutMs: intp(6000), EvolveSchema: boolp(true)}}, "", ""},
		// A bare "4x3x3" would drop the preset's registry, so the "1" stays.
		{"four brokers", &ClusterShape{Brokers: 4}, "4x3x3xxxxxxxxxxxxxxx1", ""},
		// The sink example of playground-url-api.md §11.3, with the preset's
		// registry kept (the doc's own slug has none).
		{"doc sink", &ClusterShape{ConnectSink: &ConnectSinkShape{Workers: intp(3), TasksMax: intp(3), CommitIntervalMs: intp(5000), CommitTimeoutMs: intp(10000), EvolveSchema: boolp(false)}}, "3x3x3xxxxxxxxxxxxxxx1xxxx3.3.5000.10000.0", ""},
		{"evolve off only", &ClusterShape{ConnectSink: &ConnectSinkShape{EvolveSchema: boolp(false)}}, "3x3x3xxxxxxxxxxxxxxx1xxxx2.2.2000.6000.0", ""},
		{"timeout below interval", &ClusterShape{ConnectSink: &ConnectSinkShape{CommitIntervalMs: intp(10000), CommitTimeoutMs: intp(2000)}}, "3x3x3xxxxxxxxxxxxxxx1xxxx2.2.10000.2000.1", ""},
		{"one partition", &ClusterShape{Topics: []TopicShape{{Name: "orders", Partitions: 1, ReplicationFactor: 3}}}, "3x1x3xxxxxxxxxxxxxxx1xxxx2.1.2000.6000.1", "tasksMax lowered to 1"},
		{"sr instances", &ClusterShape{SchemaRegistry: &SchemaRegistryShape{InstancesPerDc: 2}}, "3x3x3xxxxxxxxxxxxxxx3", "sr-dc-1-2"},
		{"compacted extra", &ClusterShape{Topics: []TopicShape{{Name: "audit", Partitions: 2, ReplicationFactor: 3, CleanupPolicy: "compact"}}}, "3x3x3xaudit~2~3~1~c~~~~xxxxxxxxxxxxxx1", ""},
		{"max", &ClusterShape{Brokers: 9, Topics: []TopicShape{{Name: "orders", Partitions: 6, ReplicationFactor: 3}}, ConnectSink: &ConnectSinkShape{Workers: intp(4), TasksMax: intp(6), CommitIntervalMs: intp(30000), CommitTimeoutMs: intp(60000)}}, "9x6x3xxxxxxxxxxxxxxx1xxxx4.6.30000.60000.1", ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			cluster, size, notes, err := shapeToParams("streamhouse", c.shape)
			if err != nil {
				t.Fatalf("shapeToParams: %v", err)
			}
			if cluster != "streamhouse" || size != c.want {
				t.Errorf("cluster %q size %q, want %q", cluster, size, c.want)
			}
			if c.note != "" && !strings.Contains(strings.Join(notes, "\n"), c.note) {
				t.Errorf("notes %v do not mention %q", notes, c.note)
			}
		})
	}
}

func TestShapeStreamhouseValidation(t *testing.T) {
	for _, c := range []struct {
		name    string
		cluster string
		shape   *ClusterShape
		needles []string
	}{
		{"workers 0", "streamhouse", &ClusterShape{ConnectSink: &ConnectSinkShape{Workers: intp(0)}}, []string{"shape.connectSink", "workers 0", "1..4"}},
		{"workers 5", "streamhouse", &ClusterShape{ConnectSink: &ConnectSinkShape{Workers: intp(5)}}, []string{"workers 5"}},
		{"tasks over partitions", "streamhouse", &ClusterShape{ConnectSink: &ConnectSinkShape{TasksMax: intp(4)}}, []string{"tasksMax 4", "1..3", "orders"}},
		{"interval step", "streamhouse", &ClusterShape{ConnectSink: &ConnectSinkShape{CommitIntervalMs: intp(2250)}}, []string{"commitIntervalMs 2250", "steps of 500"}},
		{"interval low", "streamhouse", &ClusterShape{ConnectSink: &ConnectSinkShape{CommitIntervalMs: intp(500)}}, []string{"commitIntervalMs 500"}},
		{"timeout high", "streamhouse", &ClusterShape{ConnectSink: &ConnectSinkShape{CommitTimeoutMs: intp(60500)}}, []string{"commitTimeoutMs 60500"}},
		{"sink elsewhere", "single-dc", &ClusterShape{ConnectSink: &ConnectSinkShape{Workers: intp(3)}}, []string{"single-dc", "streamhouse only"}},
		{"control topic", "streamhouse", &ClusterShape{Topics: []TopicShape{{Name: "control-iceberg", Partitions: 1, ReplicationFactor: 1}}}, []string{"\"control-iceberg\"", "internal topic"}},
		{"diskless extra", "streamhouse", &ClusterShape{Topics: []TopicShape{{Name: "ev", Partitions: 1, ReplicationFactor: 1, DisklessEnable: boolp(true)}}}, []string{"disklessEnable", "classic topics only"}},
		{"diskless primary", "streamhouse", &ClusterShape{Topics: []TopicShape{{Name: "orders", Partitions: 3, ReplicationFactor: 3, DisklessEnable: boolp(true)}}}, []string{"primary 'orders'", "classic topics only"}},
		{"diskless timing", "streamhouse", &ClusterShape{DisklessTiming: &DisklessTimingShape{UploadMs: intp(300)}}, []string{"shape.disklessTiming", "requires a diskless topic"}},
		{"too many brokers", "streamhouse", &ClusterShape{Brokers: 10}, []string{"10 total brokers", "cap of 9"}},
		{"too many partitions", "streamhouse", &ClusterShape{Topics: []TopicShape{{Name: "orders", Partitions: 7, ReplicationFactor: 3}}}, []string{"partitions 7", "cap of 6"}},
		{"unknown cluster lists it", "streamhous", nil, []string{"automq-3az, warpstream-3az, redpanda-3az, ursa-3az, nisshi-3b, streamhouse"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, _, _, err := shapeToParams(c.cluster, c.shape)
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

// TestBuildURLStreamhouse builds the doc's Connect example end to end through
// the MCP tool, with an Iceberg op, and previews the doc's IO examples.
func TestBuildURLStreamhouse(t *testing.T) {
	s := newSession(t)
	res := callTool(t, s, "build_playground_url", map[string]any{
		"cluster": "streamhouse",
		"shape":   map[string]any{"connectSink": map[string]any{"workers": 3, "tasksMax": 3}},
		"actions": []map[string]any{
			{"kind": "iceberg_op", "at": 5000, "icebergOp": map[string]any{"op": "set_properties", "table": "orders", "updates": map[string]any{"write.target-file-size-bytes": "134217728"}, "removals": []any{}}},
			{"kind": "kill_connect_worker", "at": 8000, "workerId": "connect-1"},
			{"kind": "restart_connect_worker", "at": 20000, "workerId": "connect-1"},
			{"kind": "restart_connector", "at": 25000, "connectorName": "orders-sink"},
		},
	})
	var out BuildPlaygroundURLOutput
	structured(t, res, &out)
	if out.Cluster != "streamhouse" || out.Size != "3x3x3xxxxxxxxxxxxxxx1xxxx3.3.2000.6000.1" {
		t.Errorf("cluster %q size %q", out.Cluster, out.Size)
	}
	// The MCP input is a JSON object, so its keys arrive unordered and are
	// written sorted; the playground keeps whatever order the link has.
	want := `IO:{"op":"set_properties"~c"removals":[]~c"table":"orders"~c"updates":{"write.target-file-size-bytes":"134217728"}}@5000,KCW:connect-1@8000,RCW:connect-1@20000,RCN:orders-sink@25000`
	if got := actionsParamOf(t, out.URL); got != want {
		t.Errorf("URL actions param\n  got  %q\n  want %q", got, want)
	}
	if len(out.DecodedActions) != 4 ||
		out.DecodedActions[0].Kind != "iceberg_op" || out.DecodedActions[0].IcebergOp["table"] != "orders" ||
		out.DecodedActions[1].Kind != "kill_connect_worker" || out.DecodedActions[1].WorkerID != "connect-1" ||
		out.DecodedActions[2].Kind != "restart_connect_worker" || out.DecodedActions[2].WorkerID != "connect-1" ||
		out.DecodedActions[3].Kind != "restart_connector" || out.DecodedActions[3].ConnectorName != "orders-sink" {
		t.Errorf("echoed %+v", out.DecodedActions)
	}

	// Required fields, and an op request the playground would skip.
	for _, c := range []struct {
		action map[string]any
		needle string
	}{
		{map[string]any{"kind": "kill_connect_worker", "at": 1}, "workerId"},
		{map[string]any{"kind": "restart_connect_worker", "at": 1}, "workerId"},
		{map[string]any{"kind": "restart_connector", "at": 1}, "connectorName"},
		{map[string]any{"kind": "iceberg_op", "at": 1}, "icebergOp"},
		{map[string]any{"kind": "iceberg_op", "at": 1, "icebergOp": map[string]any{"op": "rollback", "table": "orders"}}, "shape"},
		{map[string]any{"kind": "iceberg_op", "at": 1, "icebergOp": map[string]any{"op": "drop_table"}}, "unknown op"},
	} {
		wantToolError(t, callTool(t, s, "build_playground_url", map[string]any{
			"cluster": "streamhouse",
			"actions": []map[string]any{c.action},
		}), c.needle)
	}

	// validate_actions previews the doc's examples verbatim and drops what the
	// playground drops.
	res = callTool(t, s, "validate_actions", map[string]any{"actions": `IO:{"op":"append"~c"input":{"table":"orders"~c"dataFiles":[{"specId":0~c"partition":{}~c"recordCount":100~c"sizeBytes":4096~c"stats":{}}]}}@6000,KCW:connect-1@8000,RCW:@9000,IO:{"op":"catalog_up"~c"x":1}@9500,RCN:orders-sink@25000`})
	var v ValidateActionsOutput
	structured(t, res, &v)
	if len(v.Entries) != 3 || len(v.Dropped) != 2 {
		t.Fatalf("entries %+v dropped %+v, want 3 kept and 2 dropped", v.Entries, v.Dropped)
	}
	if v.Entries[0].Kind != "iceberg_op" || v.Entries[0].IcebergOp["op"] != "append" || v.Entries[2].Kind != "restart_connector" {
		t.Errorf("previewed %+v", v.Entries)
	}
}
