package mcpserver

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
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
		// The primary topic's tail index 10 is canonical, the form the simulator
		// itself re-encodes to (clampPrimaryTopic drops a min.ISR equal to
		// max(1, RF-1) and cleanup delete). Each wanted slug below is a SIZE
		// fixed point of the real TypeScript codec.
		{
			"primary min.ISR below the default", "",
			&ClusterShape{Brokers: 6, Topics: []TopicShape{{Name: "orders", Partitions: 4, ReplicationFactor: 3, MinInSyncReplicas: intp(1)}}},
			"", "6x4x3xxxxxxxxxxx1~~~~",
		},
		{
			"primary min.ISR above the default", "",
			&ClusterShape{Brokers: 6, Topics: []TopicShape{{Name: "orders", Partitions: 4, ReplicationFactor: 3, MinInSyncReplicas: intp(3)}}},
			"", "6x4x3xxxxxxxxxxx3~~~~",
		},
		{
			"primary default min.ISR is left out", "",
			&ClusterShape{Brokers: 6, Topics: []TopicShape{{Name: "orders", Partitions: 4, ReplicationFactor: 3, MinInSyncReplicas: intp(2)}}},
			"", "6x4x3",
		},
		{
			"primary default min.ISR and delete are both left out", "",
			&ClusterShape{Brokers: 6, Topics: []TopicShape{{Name: "orders", Partitions: 4, ReplicationFactor: 3, MinInSyncReplicas: intp(2), CleanupPolicy: "delete"}}},
			"", "6x4x3",
		},
		{
			"primary delete alone is left out", "",
			&ClusterShape{Brokers: 6, Topics: []TopicShape{{Name: "orders", Partitions: 4, ReplicationFactor: 3, CleanupPolicy: "delete"}}},
			"", "6x4x3",
		},
		{
			"primary compact keeps the cleanup code only", "",
			&ClusterShape{Brokers: 6, Topics: []TopicShape{{Name: "orders", Partitions: 4, ReplicationFactor: 3, MinInSyncReplicas: intp(2), CleanupPolicy: "compact"}}},
			"", "6x4x3xxxxxxxxxxx~c~~~",
		},
		{
			"primary compact,delete with a non-default min.ISR", "",
			&ClusterShape{Brokers: 6, Topics: []TopicShape{{Name: "orders", Partitions: 4, ReplicationFactor: 3, MinInSyncReplicas: intp(3), CleanupPolicy: "compact,delete"}}},
			"", "6x4x3xxxxxxxxxxx3~cd~~~",
		},
		{
			"primary min.ISR 1 at RF 2 is the default", "",
			&ClusterShape{Brokers: 3, Topics: []TopicShape{{Name: "orders", Partitions: 1, ReplicationFactor: 2, MinInSyncReplicas: intp(1)}}},
			"", "3x1x2",
		},
		{
			"primary min.ISR 2 at RF 2 is not", "",
			&ClusterShape{Brokers: 3, Topics: []TopicShape{{Name: "orders", Partitions: 1, ReplicationFactor: 2, MinInSyncReplicas: intp(2)}}},
			"", "3x1x2xxxxxxxxxxx2~~~~",
		},
		{
			"primary compact beside rack-aware placement", "",
			&ClusterShape{Brokers: 4, RackAware: true, Topics: []TopicShape{{Name: "orders", Partitions: 3, ReplicationFactor: 3, CleanupPolicy: "compact"}}},
			"", "4x3x3xxxx1xxxxxxx~c~~~",
		},
		{
			"name with x escaped", "",
			&ClusterShape{Brokers: 4, Topics: []TopicShape{
				{Name: "orders", Partitions: 3, ReplicationFactor: 3},
				{Name: "tx-events", Partitions: 2, ReplicationFactor: 2},
			}},
			"", "4x3x3xt*-events~2~2~1~d~~~~",
		},
		{
			// An extra topic with no min.ISR or cleanup set is still written with the
			// defaults (1, delete): the simulator drops a chunk whose min.ISR field is
			// not a number, which used to lose such a topic from the link.
			"extra topic with nothing configured", "",
			&ClusterShape{Brokers: 6, Topics: []TopicShape{{Name: "audit", Partitions: 2, ReplicationFactor: 2}}},
			"", "6x3x3xaudit~2~2~1~d~~~~",
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

// TestStretched3PartitionCap pins the stretched-3 partition clamp to the
// simulator's maxFreePlayPartitions (src/sim/freePlayTopologies.ts). The wanted
// caps were produced by calling the real TypeScript function, not derived from a
// formula: the cap is the largest count in 1..6 that keeps the busiest broker at
// or under 3 partition strips, so it never exceeds 6. The closed form
// 3*brokers/rf this replaced allowed 9 partitions on 9 brokers at RF 3, and 6 on
// 9 brokers at RF 4, where the simulator clamps to 6 and 3.
func TestStretched3PartitionCap(t *testing.T) {
	cases := []struct {
		zones []int
		rf    int
		max   int
	}{
		{[]int{3, 3, 3}, 1, 6},
		{[]int{3, 3, 3}, 2, 6},
		{[]int{3, 3, 3}, 3, 6}, // 9 brokers, RF 3: 6 allowed, 7 rejected
		{[]int{3, 3, 3}, 4, 3},
		{[]int{3, 3, 3}, 9, 3},
		{[]int{1, 1, 1}, 1, 6},
		{[]int{1, 1, 1}, 2, 4},
		{[]int{1, 1, 1}, 3, 3}, // the template default
		{[]int{2, 2, 2}, 3, 6},
		{[]int{2, 2, 2}, 4, 3},
		{[]int{1, 2, 3}, 3, 6},
		{[]int{4, 3, 2}, 5, 3},
		{[]int{1, 1, 2}, 2, 5},
		{[]int{3, 3, 2}, 8, 3},
	}
	for _, tc := range cases {
		name := strings.Trim(strings.Join(strings.Fields(fmt.Sprint(tc.zones)), "-"), "[]") + " rf " + strconv.Itoa(tc.rf)
		t.Run(name, func(t *testing.T) {
			topo := topologies["stretched-3"]
			if got := maxPartitions(topo, tc.zones, tc.rf); got != tc.max {
				t.Fatalf("maxPartitions = %d, want %d", got, tc.max)
			}
			shape := func(partitions int) *ClusterShape {
				return &ClusterShape{BrokersPerZone: tc.zones, Topics: []TopicShape{{Name: "orders", Partitions: partitions, ReplicationFactor: tc.rf}}}
			}
			if _, _, _, err := shapeToParams("stretched-3", shape(tc.max)); err != nil {
				t.Errorf("%d partitions must be accepted: %v", tc.max, err)
			}
			_, _, _, err := shapeToParams("stretched-3", shape(tc.max+1))
			if err == nil {
				t.Fatalf("%d partitions must be rejected (cap %d)", tc.max+1, tc.max)
			}
			if !strings.Contains(err.Error(), strconv.Itoa(tc.max+1)) || !strings.Contains(err.Error(), "cap of "+strconv.Itoa(tc.max)) {
				t.Errorf("error %q should name the count and the cap", err)
			}
		})
	}
}

// TestShapePrimaryTailMatchesTypeScriptFixtures replays the SIZE fixtures the
// cluster mapping is pinned to (mapping/testdata/ts-fixtures.txt, dumped from the
// real TypeScript codec) through the declarative shape generator: for every
// single-zone size with no extra topic, retention or diskless field, a shape that
// spells out the primary's min.ISR and cleanup, defaults included, must produce
// the simulator's canonical slug (nothing at all when that is the topology
// default 3x3x3). Both Go entry points into tail index 10 share one helper; this
// proves the shape generator's inputs reach it.
func TestShapePrimaryTailMatchesTypeScriptFixtures(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "mapping", "testdata", "ts-fixtures.txt"))
	if err != nil {
		t.Fatalf("read fixtures: %v", err)
	}
	checked := 0
	for _, line := range strings.Split(strings.ReplaceAll(string(raw), "\r\n", "\n"), "\n") {
		if !strings.HasPrefix(line, "SIZE ") {
			continue
		}
		fields := strings.SplitN(strings.TrimPrefix(line, "SIZE "), " ", 4)
		if len(fields) != 4 || fields[1] != "=>" || !strings.HasPrefix(fields[3], ":: ") {
			t.Fatalf("malformed SIZE fixture: %q", line)
		}
		canonical := fields[2]
		var sz struct {
			BrokersPerZone    []int  `json:"brokersPerZone"`
			Partitions        int    `json:"partitions"`
			ReplicationFactor int    `json:"replicationFactor"`
			ExtraTopics       []any  `json:"extraTopics"`
			PrimaryMinIsr     *int   `json:"primaryMinIsr"`
			PrimaryCleanup    string `json:"primaryCleanup"`
			RetentionMs       *int64 `json:"primaryRetentionMs"`
			RetentionBytes    *int64 `json:"primaryRetentionBytes"`
			PrimaryDiskless   bool   `json:"primaryDiskless"`
			DisklessTiming    any    `json:"disklessTiming"`
			ConsumerGroups    []any  `json:"consumerGroups"`
		}
		if err := json.Unmarshal([]byte(strings.TrimPrefix(fields[3], ":: ")), &sz); err != nil {
			t.Fatalf("%s: fixture json: %v", canonical, err)
		}
		if len(sz.BrokersPerZone) != 1 || len(sz.ExtraTopics) > 0 || sz.RetentionMs != nil || sz.RetentionBytes != nil ||
			sz.PrimaryDiskless || sz.DisklessTiming != nil || len(sz.ConsumerGroups) > 0 {
			continue // beyond what the shape tool can express
		}
		primary := TopicShape{
			Name: "orders", Partitions: sz.Partitions, ReplicationFactor: sz.ReplicationFactor,
			MinInSyncReplicas: intp(max(1, sz.ReplicationFactor-1)), CleanupPolicy: "delete",
		}
		if sz.PrimaryMinIsr != nil {
			primary.MinInSyncReplicas = sz.PrimaryMinIsr
		}
		if sz.PrimaryCleanup != "" {
			primary.CleanupPolicy = sz.PrimaryCleanup
		}
		_, size, _, err := shapeToParams("", &ClusterShape{Brokers: sz.BrokersPerZone[0], Topics: []TopicShape{primary}})
		if err != nil {
			t.Errorf("%s: shapeToParams: %v", canonical, err)
			continue
		}
		want := canonical
		if canonical == "3x3x3" {
			want = "" // the topology default is omitted from the URL
		}
		if size != want {
			t.Errorf("shape for %s: size = %q, want %q", canonical, size, want)
		}
		checked++
	}
	if checked < 8 {
		t.Fatalf("only %d usable SIZE fixtures; was the fixture file reshaped?", checked)
	}
}

// TestShapeDisklessTimingMatchesTypeScriptFixtures replays the SIZE-DISKLESS-3AZ
// lines of mapping/testdata/ts-fixtures.txt, dumped from the real TypeScript
// codec with the diskless-3az topology: the preset's own diskless topic makes a
// timing alone valid there (single-dc drops it), and shapeToParams must write the
// same slug.
func TestShapeDisklessTimingMatchesTypeScriptFixtures(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "mapping", "testdata", "ts-fixtures.txt"))
	if err != nil {
		t.Fatalf("read fixtures: %v", err)
	}
	checked := 0
	for _, line := range strings.Split(strings.ReplaceAll(string(raw), "\r\n", "\n"), "\n") {
		if !strings.HasPrefix(line, "SIZE-DISKLESS-3AZ ") {
			continue
		}
		fields := strings.SplitN(strings.TrimPrefix(line, "SIZE-DISKLESS-3AZ "), " ", 4)
		if len(fields) != 4 || fields[1] != "=>" || !strings.HasPrefix(fields[3], ":: ") {
			t.Fatalf("malformed fixture: %q", line)
		}
		var sz struct {
			BrokersPerZone []int `json:"brokersPerZone"`
			DisklessTiming struct {
				CommitIntervalMs *int `json:"commitIntervalMs"`
				UploadMs         *int `json:"uploadMs"`
			} `json:"disklessTiming"`
		}
		if err := json.Unmarshal([]byte(strings.TrimPrefix(fields[3], ":: ")), &sz); err != nil {
			t.Fatalf("%s: fixture json: %v", fields[2], err)
		}
		cluster, size, _, err := shapeToParams("diskless-3az", &ClusterShape{
			Brokers:        sz.BrokersPerZone[0],
			DisklessTiming: &DisklessTimingShape{CommitIntervalMs: sz.DisklessTiming.CommitIntervalMs, UploadMs: sz.DisklessTiming.UploadMs},
		})
		if err != nil {
			t.Errorf("%s: shapeToParams: %v", fields[2], err)
			continue
		}
		if cluster != "diskless-3az" || size != fields[2] {
			t.Errorf("shape for %s: cluster %q size %q", fields[2], cluster, size)
		}
		checked++
	}
	if checked != 2 {
		t.Fatalf("replayed %d SIZE-DISKLESS-3AZ fixtures, want 2", checked)
	}
}
