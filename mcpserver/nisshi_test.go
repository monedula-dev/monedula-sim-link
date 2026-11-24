package mcpserver

import (
	"strings"
	"testing"

	"github.com/monedula-dev/monedula-sim-link/simurl"
)

// TestShapeNisshi covers the nisshi-3b shape: the default omits size, the S3
// engine is an `s` and a PostgreSQL maintenance interval an `m<ms>` at tail
// index 17, there is no primary diskless flag, and every extra topic carries
// the diskless `~1`. Each slug here is a SIZE-IMPL fixture the TypeScript codec
// re-encodes unchanged.
func TestShapeNisshi(t *testing.T) {
	for _, c := range []struct {
		name  string
		shape *ClusterShape
		want  string
	}{
		{"no shape", nil, ""},
		{"empty shape", &ClusterShape{}, ""},
		{"preset values", &ClusterShape{Brokers: 3, Topics: []TopicShape{{Name: "orders", Partitions: 3, ReplicationFactor: 1}}}, ""},
		{"postgres said explicitly", &ClusterShape{Nisshi: &NisshiShape{Engine: "postgres"}}, ""},
		{"diskless said explicitly", &ClusterShape{Topics: []TopicShape{{Name: "orders", Partitions: 3, ReplicationFactor: 1, DisklessEnable: boolp(true)}}}, ""},
		{"one broker one partition", &ClusterShape{Brokers: 1, Topics: []TopicShape{{Name: "orders", Partitions: 1, ReplicationFactor: 1}}}, "1x1x1"},
		{"five brokers six partitions", &ClusterShape{Brokers: 5, Topics: []TopicShape{{Name: "orders", Partitions: 6, ReplicationFactor: 1}}}, "5x6x1"},
		{"s3", &ClusterShape{Nisshi: &NisshiShape{Engine: "s3"}}, "3x3x1xxxxxxxxxxxxxxxxxxs"},
		{"s3 with upload", &ClusterShape{Nisshi: &NisshiShape{Engine: "s3"}, DisklessTiming: &DisklessTimingShape{UploadMs: intp(900)}}, "3x3x1xxxxxxxxxxxxxxxxxxs.900"},
		{"postgres with upload", &ClusterShape{DisklessTiming: &DisklessTimingShape{UploadMs: intp(900)}}, "3x3x1xxxxxxxxxxxxxxxxxx.900"},
		{"maintenance", &ClusterShape{Nisshi: &NisshiShape{MaintenanceMs: intp(30000)}}, "3x3x1xxxxxxxxxxxxxxxxxxm30000"},
		{"maintenance at the floor", &ClusterShape{Nisshi: &NisshiShape{Engine: "postgres", MaintenanceMs: intp(2000)}}, "3x3x1xxxxxxxxxxxxxxxxxxm2000"},
		{"maintenance at the ceiling", &ClusterShape{Nisshi: &NisshiShape{MaintenanceMs: intp(600000)}}, "3x3x1xxxxxxxxxxxxxxxxxxm600000"},
		{"upload and maintenance", &ClusterShape{Nisshi: &NisshiShape{MaintenanceMs: intp(30000)}, DisklessTiming: &DisklessTimingShape{UploadMs: intp(900)}}, "3x3x1xxxxxxxxxxxxxxxxxx.900m30000"},
		{"extra topic", &ClusterShape{Topics: []TopicShape{{Name: "ev", Partitions: 3, ReplicationFactor: 1}}}, "3x3x1xev~3~1~1~d~~~~~1"},
		{"compacted extras", &ClusterShape{Topics: []TopicShape{
			{Name: "ev", Partitions: 2, ReplicationFactor: 1, CleanupPolicy: "compact,delete"},
			{Name: "log", Partitions: 1, ReplicationFactor: 1},
		}}, "3x3x1xev~2~1~1~cd~~~~~1!log~1~1~1~d~~~~~1"},
		{"extra topic on s3 with upload", &ClusterShape{
			Topics:         []TopicShape{{Name: "ev", Partitions: 3, ReplicationFactor: 1}},
			Nisshi:         &NisshiShape{Engine: "s3"},
			DisklessTiming: &DisklessTimingShape{UploadMs: intp(900)},
		}, "3x3x1xev~3~1~1~d~~~~~1xxxxxxxxxxxxxxxxxs.900"},
		{"compacted orders", &ClusterShape{Topics: []TopicShape{{Name: "orders", Partitions: 3, ReplicationFactor: 1, CleanupPolicy: "compact"}}}, "3x3x1xxxxxxxxxxx~c~~~"},
		{"registry", &ClusterShape{SchemaRegistry: &SchemaRegistryShape{InstancesPerDc: 1}}, "3x3x1xxxxxxxxxxxxxxx2"},
		{"registry and maintenance", &ClusterShape{
			Brokers:        4,
			Topics:         []TopicShape{{Name: "orders", Partitions: 2, ReplicationFactor: 1}},
			SchemaRegistry: &SchemaRegistryShape{},
			Nisshi:         &NisshiShape{MaintenanceMs: intp(15000)},
		}, "4x2x1xxxxxxxxxxxxxxx1xxxm15000"},
		// The playground builds an RF above 1 as 1 (with min.ISR 1): so does
		// the shape, with a note (TestShapeNisshiNotes).
		{"orders at RF 3", &ClusterShape{Topics: []TopicShape{{Name: "orders", Partitions: 3, ReplicationFactor: 3}}}, ""},
		{"compacted orders at RF 3 min.ISR 3", &ClusterShape{Topics: []TopicShape{{Name: "orders", Partitions: 3, ReplicationFactor: 3, MinInSyncReplicas: intp(3), CleanupPolicy: "compact"}}}, "3x3x1xxxxxxxxxxx~c~~~"},
		{"compacted extra at RF 3 min.ISR 2", &ClusterShape{Topics: []TopicShape{{Name: "ev", Partitions: 3, ReplicationFactor: 3, MinInSyncReplicas: intp(2), CleanupPolicy: "compact"}}}, "3x3x1xev~3~1~1~c~~~~~1"},
	} {
		t.Run(c.name, func(t *testing.T) {
			cluster, size, _, err := shapeToParams("nisshi-3b", c.shape)
			if err != nil {
				t.Fatalf("shapeToParams: %v", err)
			}
			if cluster != "nisshi-3b" || size != c.want {
				t.Errorf("cluster %q size %q, want nisshi-3b %q", cluster, size, c.want)
			}
		})
	}
}

// TestShapeNisshiNotes: what the playground rewrites or stores without using
// is said in a note, and the caller's shape is left as it was.
func TestShapeNisshiNotes(t *testing.T) {
	for _, c := range []struct {
		name    string
		shape   *ClusterShape
		needles []string
	}{
		{"orders at RF 3", &ClusterShape{Topics: []TopicShape{{Name: "orders", Partitions: 3, ReplicationFactor: 3, MinInSyncReplicas: intp(2)}}},
			[]string{`topic "orders": replicationFactor 3 and minInSyncReplicas 2 built as 1`, nisshiNoReplicasReason}},
		{"extra at RF 2", &ClusterShape{Topics: []TopicShape{{Name: "ev", Partitions: 1, ReplicationFactor: 2}}},
			[]string{`topic "ev": replicationFactor 2 built as 1`}},
		{"diskless off", &ClusterShape{Topics: []TopicShape{{Name: "ev", Partitions: 1, ReplicationFactor: 1, DisklessEnable: boolp(false)}}},
			[]string{`topic "ev": disklessEnable false changes nothing`}},
		{"upload on postgres", &ClusterShape{DisklessTiming: &DisklessTimingShape{UploadMs: intp(900)}},
			[]string{"uploadMs 900 is the S3 engine's batch PUT", "PostgreSQL engine does not use it"}},
		{"compaction on s3", &ClusterShape{Nisshi: &NisshiShape{Engine: "s3"}, Topics: []TopicShape{{Name: "orders", Partitions: 3, ReplicationFactor: 1, CleanupPolicy: "compact"}, {Name: "ev", Partitions: 1, ReplicationFactor: 1, CleanupPolicy: "compact,delete"}}},
			[]string{`topic "orders": cleanupPolicy compact is stored but not applied on Nisshi's S3 engine`, `topic "ev": cleanupPolicy compact,delete is stored but not applied`}},
	} {
		t.Run(c.name, func(t *testing.T) {
			before := *firstTopic(c.shape)
			_, _, notes, err := shapeToParams("nisshi-3b", c.shape)
			if err != nil {
				t.Fatalf("shapeToParams: %v", err)
			}
			joined := strings.Join(notes, "\n")
			for _, n := range c.needles {
				if !strings.Contains(joined, n) {
					t.Errorf("notes %q do not mention %q", joined, n)
				}
			}
			if after := *firstTopic(c.shape); after.ReplicationFactor != before.ReplicationFactor || (before.MinInSyncReplicas != nil && *after.MinInSyncReplicas != *before.MinInSyncReplicas) {
				t.Errorf("the caller's shape changed: %+v, was %+v", after, before)
			}
		})
	}
	// Compaction on PostgreSQL is applied by its maintenance sweep: no note.
	_, _, notes, err := shapeToParams("nisshi-3b", &ClusterShape{Topics: []TopicShape{{Name: "orders", Partitions: 3, ReplicationFactor: 1, CleanupPolicy: "compact"}}})
	if err != nil || len(notes) != 0 {
		t.Errorf("compaction on postgres: notes %v err %v, want none", notes, err)
	}
}

// firstTopic is the shape's first topic, or an empty one, for the note tests.
func firstTopic(s *ClusterShape) *TopicShape {
	if len(s.Topics) == 0 {
		return &TopicShape{}
	}
	return &s.Topics[0]
}

func TestShapeNisshiValidation(t *testing.T) {
	for _, c := range []struct {
		name    string
		cluster string
		shape   *ClusterShape
		needles []string
	}{
		{"six brokers", "nisshi-3b", &ClusterShape{Brokers: 6}, []string{"6 total brokers", "nisshi-3b cap of 5"}},
		{"seven partitions", "nisshi-3b", &ClusterShape{Topics: []TopicShape{{Name: "orders", Partitions: 7, ReplicationFactor: 1}}}, []string{"partitions 7", "cap of 6"}},
		{"unknown engine", "nisshi-3b", &ClusterShape{Nisshi: &NisshiShape{Engine: "sqlite"}}, []string{"shape.nisshi.engine \"sqlite\"", "postgres or s3"}},
		{"maintenance on s3", "nisshi-3b", &ClusterShape{Nisshi: &NisshiShape{Engine: "s3", MaintenanceMs: intp(30000)}}, []string{"shape.nisshi.maintenanceMs", "S3 engine runs no maintenance sweep"}},
		{"maintenance too short", "nisshi-3b", &ClusterShape{Nisshi: &NisshiShape{MaintenanceMs: intp(1000)}}, []string{"maintenanceMs 1000", "[2000, 600000]"}},
		{"maintenance too long", "nisshi-3b", &ClusterShape{Nisshi: &NisshiShape{MaintenanceMs: intp(700000)}}, []string{"maintenanceMs 700000", "[2000, 600000]"}},
		{"commit interval", "nisshi-3b", &ClusterShape{DisklessTiming: &DisklessTimingShape{CommitIntervalMs: intp(250), UploadMs: intp(900)}}, []string{"shape.disklessTiming", "commitIntervalMs", "no commit interval", "uploadMs only"}},
		{"min.ISR above RF 1", "nisshi-3b", &ClusterShape{Topics: []TopicShape{{Name: "ev", Partitions: 1, ReplicationFactor: 1, MinInSyncReplicas: intp(2)}}}, []string{"minInSyncReplicas 2"}},
		{"RF 0", "nisshi-3b", &ClusterShape{Topics: []TopicShape{{Name: "orders", Partitions: 3, ReplicationFactor: 0}}}, []string{"replicationFactor 0"}},
		{"nisshi on another cluster", "ursa-3az", &ClusterShape{Nisshi: &NisshiShape{Engine: "s3"}}, []string{"shape.nisshi", "\"ursa-3az\" is not Nisshi", "nisshi-3b only"}},
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

func TestBuildURLNisshi(t *testing.T) {
	s := newSession(t)
	res := callTool(t, s, "build_playground_url", map[string]any{
		"cluster": "nisshi-3b",
		"actions": []map[string]any{
			{"kind": "produce", "at": 500, "producerId": "p-orders", "topic": "orders", "key": "k1"},
			{"kind": "stop_coordinator_db", "at": 1000},
			{"kind": "kill_broker", "at": 2000, "brokerId": "broker-2"},
			{"kind": "start_coordinator_db", "at": 4000},
			{"kind": "restart_broker", "at": 5000, "brokerId": "broker-2"},
			{"kind": "change_replication_factor", "at": 6000, "topic": "orders", "targetReplicationFactor": 1},
			{"kind": "config_change", "at": 7000, "path": "topics.orders.cleanupPolicy", "value": "compact"},
			{"kind": "config_change", "at": 8000, "path": "topics.orders.partitions", "value": "3"},
		},
	})
	var out BuildPlaygroundURLOutput
	structured(t, res, &out)
	if out.Cluster != "nisshi-3b" || out.Size != "" || !strings.HasPrefix(out.URL, simurl.DefaultBaseURL) || !strings.Contains(out.URL, "cluster=nisshi-3b") {
		t.Errorf("cluster %q size %q URL %q, want the nisshi-3b default", out.Cluster, out.Size, out.URL)
	}
	if len(out.Notes) != 0 {
		t.Errorf("notes %v, want none", out.Notes)
	}
	if len(out.DecodedActions) != 8 {
		t.Fatalf("echoed %+v", out.DecodedActions)
	}
	if p := actionsParamOf(t, out.URL); !strings.Contains(p, "DB:0@1000") || !strings.Contains(p, "DB:1@4000") {
		t.Errorf("URL actions param %q lacks the DB codes", p)
	}

	// The S3 engine: object storage actions, and the size carries `s`.
	res = callTool(t, s, "build_playground_url", map[string]any{
		"cluster": "nisshi-3b",
		"shape":   map[string]any{"nisshi": map[string]any{"engine": "s3"}},
		"actions": []map[string]any{
			{"kind": "stop_object_storage", "at": 1000},
			{"kind": "slow_object_storage", "at": 2000},
			{"kind": "start_object_storage", "at": 3000},
			{"kind": "stop_coordinator_db", "at": 4000},
		},
	})
	structured(t, res, &out)
	if out.Size != "3x3x1xxxxxxxxxxxxxxxxxxs" {
		t.Errorf("s3: size %q URL %q", out.Size, out.URL)
	}
	if p := actionsParamOf(t, out.URL); !strings.Contains(p, "OS:0@1000") || !strings.Contains(p, "OS:s@2000") || !strings.Contains(p, "OS:1@3000") {
		t.Errorf("URL actions param %q lacks the OS codes", p)
	}
	if len(out.Notes) != 1 || !strings.Contains(out.Notes[0], "actions[3] stop_coordinator_db: does nothing on the S3 engine") {
		t.Errorf("s3 notes %v", out.Notes)
	}

	// start_object_storage on PostgreSQL does nothing: a note.
	res = callTool(t, s, "build_playground_url", map[string]any{
		"cluster": "nisshi-3b",
		"actions": []map[string]any{{"kind": "start_object_storage", "at": 1000}},
	})
	structured(t, res, &out)
	if len(out.Notes) != 1 || !strings.Contains(out.Notes[0], "start_object_storage: does nothing on the PostgreSQL engine") {
		t.Errorf("postgres notes %v", out.Notes)
	}
}

// TestBuildURLNisshiRefusals: the actions the playground's engine refuses on
// nisshi-3b are refused here with the engine's own copy.
func TestBuildURLNisshiRefusals(t *testing.T) {
	s := newSession(t)
	refusedOn := func(shape map[string]any, needles []string, actions ...map[string]any) {
		t.Helper()
		in := map[string]any{"cluster": "nisshi-3b", "actions": actions}
		if shape != nil {
			in["shape"] = shape
		}
		wantToolError(t, callTool(t, s, "build_playground_url", in), needles...)
	}
	refused := func(needles []string, actions ...map[string]any) {
		t.Helper()
		refusedOn(nil, needles, actions...)
	}

	refused([]string{"actions[0]", "pause_broker", "refused on nisshi-3b", nisshiPauseRefusal, "admin_rejected"},
		map[string]any{"kind": "pause_broker", "at": 1000, "brokerId": "broker-1"})
	for _, kind := range []string{"stop_object_storage", "slow_object_storage"} {
		refused([]string{"actions[0]", kind, nisshiPgStorageRefusal, "shape.nisshi.engine s3"},
			map[string]any{"kind": kind, "at": 1000})
	}
	refused([]string{"actions[1]", "reassign_partition", nisshiNoReplicasReason},
		map[string]any{"kind": "kill_broker", "at": 500, "brokerId": "broker-1"},
		map[string]any{"kind": "reassign_partition", "at": 1000, "topic": "orders", "partition": 0, "replicas": []string{"broker-2"}})
	refused([]string{"actions[0]", "change_replication_factor", nisshiNoReplicasReason},
		map[string]any{"kind": "change_replication_factor", "at": 1000, "topic": "orders", "targetReplicationFactor": 2})
	refused([]string{"actions[0]", "add_share_group", nisshiShareGroupRefusal},
		map[string]any{"kind": "add_share_group", "at": 1000, "groupId": "sg", "topic": "orders"})
	refused([]string{"actions[0]", "topics.orders.partitions=4", nisshiPartitionsRefusal, "config_rejected"},
		map[string]any{"kind": "config_change", "at": 1000, "path": "topics.orders.partitions", "value": "4"})
	refusedOn(map[string]any{"topics": []map[string]any{{"name": "ev", "partitions": 2, "replicationFactor": 1}}},
		[]string{"topics.ev.partitions=3", nisshiPartitionsRefusal},
		map[string]any{"kind": "config_change", "at": 1000, "path": "topics.ev.partitions", "value": "3"})
	refused([]string{"topics.orders.remoteStorageEnable=true", nisshiOneKindRefusal},
		map[string]any{"kind": "config_change", "at": 1000, "path": "topics.orders.remoteStorageEnable", "value": "true"})
	refused([]string{"topics.orders.disklessEnable=false", nisshiOneKindRefusal},
		map[string]any{"kind": "config_change", "at": 1000, "path": "topics.orders.disklessEnable", "value": "false"})

	// The same actions on another cluster are not Nisshi's to refuse.
	res := callTool(t, s, "build_playground_url", map[string]any{
		"cluster": "kafscale-3b",
		"actions": []map[string]any{{"kind": "pause_broker", "at": 1000, "brokerId": "broker-1"}, {"kind": "stop_object_storage", "at": 2000}},
	})
	if res.IsError {
		t.Errorf("kafscale-3b: %s", errText(res))
	}
}
