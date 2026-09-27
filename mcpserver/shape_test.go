package mcpserver

import (
	"strings"
	"testing"
)

func intp(n int) *int { return &n }

// TestShapeDocExamples pins the generated size slugs to the worked examples
// asserted verbatim by the doc conformance test (§14), so this generator and
// the playground codec cannot drift apart silently.
func TestShapeDocExamples(t *testing.T) {
	cases := []struct {
		name        string
		cluster     string
		shape       *ClusterShape
		wantCluster string
		wantSize    string
	}{
		{"nil shape default", "", nil, "", ""},
		{"single-dc default shape", "single-dc", &ClusterShape{Brokers: 3}, "", ""},
		{"§14.2 6x4x3", "", &ClusterShape{Brokers: 6, Topics: []TopicShape{{Name: "orders", Partitions: 4, ReplicationFactor: 3}}}, "", "6x4x3"},
		{
			"§14.3 extra topics", "",
			&ClusterShape{Brokers: 6, Topics: []TopicShape{
				{Name: "orders", Partitions: 4, ReplicationFactor: 3},
				{Name: "payments", Partitions: 6, ReplicationFactor: 3, MinInSyncReplicas: intp(2), CleanupPolicy: "delete"},
				{Name: "audit", Partitions: 2, ReplicationFactor: 2, MinInSyncReplicas: intp(1), CleanupPolicy: "compact"},
			}},
			"", "6x4x3xpayments~6~3~2~d~~~~!audit~2~2~1~c~~~~",
		},
		{"§14.5 rack-aware", "", &ClusterShape{Brokers: 4, RackAware: true}, "", "4x3x3xxxx1x"},
		{"active-passive default", "active-passive", nil, "active-passive", ""},
		{"active-passive default zones", "active-passive", &ClusterShape{BrokersPerZone: []int{2, 2}}, "active-passive", ""},
		{"active-passive custom", "active-passive", &ClusterShape{BrokersPerZone: []int{3, 2}}, "active-passive", "3-2x1x2"},
		{"stretched-3 custom", "stretched-3", &ClusterShape{BrokersPerZone: []int{2, 2, 2}}, "stretched-3", "2-2-2x3x3"},
		{"diskless-3az default", "diskless-3az", nil, "diskless-3az", ""},
		{"diskless-3az default shape", "diskless-3az", &ClusterShape{Brokers: 6}, "diskless-3az", ""},
		{"diskless-3az custom brokers", "diskless-3az", &ClusterShape{Brokers: 8}, "diskless-3az", "8x3x3"},
		{
			"primary min.ISR tail", "",
			&ClusterShape{Brokers: 6, Topics: []TopicShape{{Name: "orders", Partitions: 4, ReplicationFactor: 3, MinInSyncReplicas: intp(2)}}},
			"", "6x4x3xxxxxxxxxxx2~~~~",
		},
		{
			"name with x escaped", "",
			&ClusterShape{Brokers: 4, Topics: []TopicShape{
				{Name: "orders", Partitions: 3, ReplicationFactor: 3},
				{Name: "tx-events", Partitions: 2, ReplicationFactor: 2},
			}},
			"", "4x3x3xt*-events~2~2~~~~~~",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cluster, size, _, err := shapeToParams(tc.cluster, tc.shape)
			if err != nil {
				t.Fatalf("shapeToParams: %v", err)
			}
			if cluster != tc.wantCluster {
				t.Errorf("cluster: got %q want %q", cluster, tc.wantCluster)
			}
			if size != tc.wantSize {
				t.Errorf("size: got %q want %q", size, tc.wantSize)
			}
		})
	}
}

// TestShapeDiskless covers the KIP-1163 draft (v2.0+) fields:
// an extra topic's disklessEnable (extraTopics 10th field) and the primary
// topic's disklessEnable (size tail index 17). Expected slugs are pinned to
// the equivalent mapping/size_test.go and ts-fixtures.txt cases so this
// generator and mapping.buildSize agree on the wire encoding.
func TestShapeDiskless(t *testing.T) {
	cases := []struct {
		name        string
		cluster     string
		shape       *ClusterShape
		wantCluster string
		wantSize    string
	}{
		{
			"extra topic disklessEnable", "",
			&ClusterShape{Brokers: 10, Topics: []TopicShape{
				{Name: "orders", Partitions: 1, ReplicationFactor: 1},
				{Name: "wal", Partitions: 4, ReplicationFactor: 3, MinInSyncReplicas: intp(1), CleanupPolicy: "delete", DisklessEnable: true},
			}},
			"", "10x1x1xwal~4~3~1~d~~~~~1",
		},
		{
			"primary disklessEnable flag only", "",
			&ClusterShape{Brokers: 5, Topics: []TopicShape{
				{Name: "orders", Partitions: 4, ReplicationFactor: 3, DisklessEnable: true},
			}},
			"", "5x4x3xxxxxxxxxxxxxxxxxxd",
		},
		{
			"diskless-3az extra topic", "diskless-3az",
			&ClusterShape{Brokers: 6, Topics: []TopicShape{
				{Name: "orders", Partitions: 3, ReplicationFactor: 3},
				{Name: "clicks2", Partitions: 3, ReplicationFactor: 3, MinInSyncReplicas: intp(2), CleanupPolicy: "delete", DisklessEnable: true},
			}},
			"diskless-3az", "6x3x3xclicks2~3~3~2~d~~~~~1",
		},
		{
			// Pinned to the ts-fixtures.txt SIZE line for
			// {brokersPerZone:[5],partitions:4,replicationFactor:3,
			//  primaryDiskless:true,disklessTiming:{commitIntervalMs:250,uploadMs:600}}.
			"primary disklessEnable + full timing", "",
			&ClusterShape{Brokers: 5, Topics: []TopicShape{
				{Name: "orders", Partitions: 4, ReplicationFactor: 3, DisklessEnable: true},
			}, DisklessTiming: &DisklessTimingShape{CommitIntervalMs: intp(250), UploadMs: intp(600)}},
			"", "5x4x3xxxxxxxxxxxxxxxxxxd250.600",
		},
		{
			// Pinned to the ts-fixtures.txt SIZE line for an extra diskless topic
			// with disklessTiming:{uploadMs:900} and no primary diskless flag.
			"extra topic disklessEnable + upload-only timing", "",
			&ClusterShape{Brokers: 5, Topics: []TopicShape{
				{Name: "orders", Partitions: 4, ReplicationFactor: 3},
				{Name: "wal", Partitions: 4, ReplicationFactor: 3, MinInSyncReplicas: intp(1), CleanupPolicy: "delete", DisklessEnable: true},
			}, DisklessTiming: &DisklessTimingShape{UploadMs: intp(900)}},
			"", "5x4x3xwal~4~3~1~d~~~~~1xxxxxxxxxxxxxxxxx.900",
		},
		{
			// The diskless-3az preset always materializes a diskless `clicks`
			// topic (see topologies["diskless-3az"].templateDiskless), so
			// disklessTiming is accepted even though this shape sets no
			// topic's disklessEnable itself. Pinned to the ts-fixtures.txt SIZE
			// line generated via clampFreePlaySize('diskless-3az', ...) +
			// encodeFreePlaySize for {brokersPerZone:[6],partitions:3,
			// replicationFactor:3,disklessTiming:{commitIntervalMs:500}}.
			"diskless-3az preset topic satisfies disklessTiming", "diskless-3az",
			&ClusterShape{DisklessTiming: &DisklessTimingShape{CommitIntervalMs: intp(500)}},
			"diskless-3az", "6x3x3xxxxxxxxxxxxxxxxxx500.",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cluster, size, _, err := shapeToParams(tc.cluster, tc.shape)
			if err != nil {
				t.Fatalf("shapeToParams: %v", err)
			}
			if cluster != tc.wantCluster {
				t.Errorf("cluster: got %q want %q", cluster, tc.wantCluster)
			}
			if size != tc.wantSize {
				t.Errorf("size: got %q want %q", size, tc.wantSize)
			}
		})
	}
}

// TestShapeDisklessTimingBoundary checks the [100, 5000] range is inclusive
// on both ends, mirroring DISKLESS_TIMING_MIN_MS/MAX_MS in
// freePlayTopologies.ts (validateDisklessTiming rejects only values strictly
// outside the range).
func TestShapeDisklessTimingBoundary(t *testing.T) {
	cases := []struct {
		name  string
		shape *ClusterShape
	}{
		{
			"commitIntervalMs 100 (min, inclusive)",
			&ClusterShape{Brokers: 3, Topics: []TopicShape{{Name: "orders", Partitions: 3, ReplicationFactor: 3, DisklessEnable: true}}, DisklessTiming: &DisklessTimingShape{CommitIntervalMs: intp(100)}},
		},
		{
			"commitIntervalMs 5000 (max, inclusive)",
			&ClusterShape{Brokers: 3, Topics: []TopicShape{{Name: "orders", Partitions: 3, ReplicationFactor: 3, DisklessEnable: true}}, DisklessTiming: &DisklessTimingShape{CommitIntervalMs: intp(5000)}},
		},
		{
			"uploadMs 100 (min, inclusive)",
			&ClusterShape{Brokers: 3, Topics: []TopicShape{{Name: "orders", Partitions: 3, ReplicationFactor: 3, DisklessEnable: true}}, DisklessTiming: &DisklessTimingShape{UploadMs: intp(100)}},
		},
		{
			"uploadMs 5000 (max, inclusive)",
			&ClusterShape{Brokers: 3, Topics: []TopicShape{{Name: "orders", Partitions: 3, ReplicationFactor: 3, DisklessEnable: true}}, DisklessTiming: &DisklessTimingShape{UploadMs: intp(5000)}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, _, err := shapeToParams("", tc.shape); err != nil {
				t.Fatalf("shapeToParams: %v", err)
			}
		})
	}
}

// TestShapeDefaultRFNote covers the one clamp we apply instead of erroring:
// the caller set only a small broker count, so the DEFAULT RF is lowered.
func TestShapeDefaultRFNote(t *testing.T) {
	_, size, notes, err := shapeToParams("", &ClusterShape{Brokers: 2})
	if err != nil {
		t.Fatalf("shapeToParams: %v", err)
	}
	if size != "2x3x2" {
		t.Errorf("size: got %q want 2x3x2", size)
	}
	if len(notes) != 1 || !strings.Contains(notes[0], "replication factor") {
		t.Errorf("expected an RF note, got %v", notes)
	}
}

func TestShapeValidationErrors(t *testing.T) {
	cases := []struct {
		name    string
		cluster string
		shape   *ClusterShape
		needles []string
	}{
		{"cap single-dc", "", &ClusterShape{Brokers: 12}, []string{"12", "10"}},
		{"cap mirror", "active-active", &ClusterShape{BrokersPerZone: []int{5, 5}}, []string{"10", "9"}},
		{"zone >= 1", "stretched-3", &ClusterShape{BrokersPerZone: []int{2, 0, 2}}, []string{"dc-b", ">= 1"}},
		{"both broker fields", "", &ClusterShape{Brokers: 3, BrokersPerZone: []int{3}}, []string{"not both"}},
		{"partition cap single-dc", "", &ClusterShape{Brokers: 6, Topics: []TopicShape{{Name: "orders", Partitions: 25, ReplicationFactor: 3}}}, []string{"25", "24"}},
		{"partition cap mirror", "active-active", &ClusterShape{BrokersPerZone: []int{2, 2}, Topics: []TopicShape{{Name: "orders", Partitions: 7, ReplicationFactor: 2}}}, []string{"7", "6"}},
		{"rf active-passive primary zone", "active-passive", &ClusterShape{BrokersPerZone: []int{2, 4}, Topics: []TopicShape{{Name: "orders", Partitions: 1, ReplicationFactor: 3}}}, []string{"replicationFactor 3", "2"}},
		{"rf active-active smaller zone", "active-active", &ClusterShape{BrokersPerZone: []int{4, 2}, Topics: []TopicShape{{Name: "orders", Partitions: 1, ReplicationFactor: 3}}}, []string{"replicationFactor 3", "2"}},
		{"too many extras", "", &ClusterShape{Brokers: 6, Topics: []TopicShape{
			{Name: "t1", Partitions: 1, ReplicationFactor: 1}, {Name: "t2", Partitions: 1, ReplicationFactor: 1},
			{Name: "t3", Partitions: 1, ReplicationFactor: 1}, {Name: "t4", Partitions: 1, ReplicationFactor: 1},
			{Name: "t5", Partitions: 1, ReplicationFactor: 1}, {Name: "t6", Partitions: 1, ReplicationFactor: 1},
			{Name: "t7", Partitions: 1, ReplicationFactor: 1},
		}}, []string{"7", "6", "MAX_FREE_PLAY_TOPICS"}},
		{"duplicate topic", "", &ClusterShape{Brokers: 3, Topics: []TopicShape{
			{Name: "audit", Partitions: 1, ReplicationFactor: 1},
			{Name: "audit", Partitions: 2, ReplicationFactor: 1},
		}}, []string{"topics[1]", "duplicate"}},
		{"minISR range", "", &ClusterShape{Brokers: 3, Topics: []TopicShape{{Name: "orders", Partitions: 3, ReplicationFactor: 2, MinInSyncReplicas: intp(3)}}}, []string{"minInSyncReplicas 3", "replicationFactor"}},
		{"bad cleanup", "", &ClusterShape{Brokers: 3, Topics: []TopicShape{{Name: "orders", Partitions: 3, ReplicationFactor: 3, CleanupPolicy: "purge"}}}, []string{"purge", "compact"}},
		{"stretched-3 strips", "stretched-3", &ClusterShape{BrokersPerZone: []int{1, 1, 1}, Topics: []TopicShape{{Name: "orders", Partitions: 4, ReplicationFactor: 3}}}, []string{"4", "3"}},
		{"cap diskless-3az", "diskless-3az", &ClusterShape{Brokers: 10}, []string{"10", "9"}},
		{"partition cap diskless-3az", "diskless-3az", &ClusterShape{Brokers: 6, Topics: []TopicShape{{Name: "orders", Partitions: 7, ReplicationFactor: 3}}}, []string{"7", "6"}},
		{"diskless + compact", "", &ClusterShape{Brokers: 3, Topics: []TopicShape{{Name: "orders", Partitions: 3, ReplicationFactor: 3, CleanupPolicy: "compact", DisklessEnable: true}}}, []string{"disklessEnable", "compact"}},
		{"diskless on mirror topology", "active-passive", &ClusterShape{BrokersPerZone: []int{2, 2}, Topics: []TopicShape{{Name: "orders", Partitions: 1, ReplicationFactor: 2, DisklessEnable: true}}}, []string{"disklessEnable", "mirror", "active-passive"}},
		{
			"disklessTiming without a diskless topic", "",
			&ClusterShape{Brokers: 3, Topics: []TopicShape{{Name: "orders", Partitions: 3, ReplicationFactor: 3}}, DisklessTiming: &DisklessTimingShape{CommitIntervalMs: intp(250)}},
			[]string{"disklessTiming", "disklessEnable"},
		},
		{
			"disklessTiming commitIntervalMs out of range", "",
			&ClusterShape{Brokers: 3, Topics: []TopicShape{{Name: "orders", Partitions: 3, ReplicationFactor: 3, DisklessEnable: true}}, DisklessTiming: &DisklessTimingShape{CommitIntervalMs: intp(50)}},
			[]string{"commitIntervalMs 50", "100", "5000"},
		},
		{
			"disklessTiming uploadMs out of range", "",
			&ClusterShape{Brokers: 3, Topics: []TopicShape{{Name: "orders", Partitions: 3, ReplicationFactor: 3, DisklessEnable: true}}, DisklessTiming: &DisklessTimingShape{UploadMs: intp(9000)}},
			[]string{"uploadMs 9000", "100", "5000"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, _, err := shapeToParams(tc.cluster, tc.shape)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			for _, n := range tc.needles {
				if !strings.Contains(err.Error(), n) {
					t.Errorf("error %q does not mention %q", err, n)
				}
			}
		})
	}
}
