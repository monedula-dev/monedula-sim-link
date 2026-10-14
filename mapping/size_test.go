package mapping

import (
	"encoding/json"
	"strings"
	"testing"
)

func i64p(v int64) *int64 { return &v }

// The `want` strings below were round-tripped through the REAL simulator codec
// (src/sim/freePlayTopologies.ts decodeFreePlaySize → encodeFreePlaySize) and
// came back verbatim with the expected topics/shape — see the SIZE lines in
// testdata/ts-fixtures.txt. That makes them canonical fixed points of the
// simulator's clamp, which is exactly the guarantee buildSize must uphold.
func TestBuildSize(t *testing.T) {
	intp := func(v int) *int { return &v }

	cases := []struct {
		name     string
		brokers  int
		primary  *topicShape
		extras   []topicShape
		diskless disklessTail
		want     string
	}{
		{
			name:    "base only, real orders primary",
			brokers: 3,
			primary: &topicShape{simName: "orders", partitions: 12, rf: 3},
			want:    "3x12x3",
		},
		{
			name:    "phantom primary with extra topics",
			brokers: 10,
			extras: []topicShape{
				{simName: "payments", partitions: 6, rf: 3},
				{simName: "audit", partitions: 2, rf: 2},
			},
			want: "10x1x1xpayments~6~3~1~d~~~~!audit~2~2~1~d~~~~",
		},
		{
			name:    "underscore and dash survive",
			brokers: 10,
			extras:  []topicShape{{simName: "a-b_c", partitions: 3, rf: 2}},
			want:    "10x1x1xa-b_c~3~2~1~d~~~~",
		},
		{
			name:    "x in a topic name is slug-escaped to *",
			brokers: 4,
			extras:  []topicShape{{simName: "tx-events", partitions: 2, rf: 2}},
			want:    "4x1x1xt*-events~2~2~1~d~~~~",
		},
		{
			name:    "extra topic carries real config (minISR, compact, retention)",
			brokers: 10,
			primary: &topicShape{simName: "orders", partitions: 12, rf: 3},
			extras: []topicShape{
				{simName: "payments", partitions: 6, rf: 3, minISR: 2, cleanupPolicy: "compact", retentionMs: i64p(604800000), retentionBytes: i64p(1073741824)},
			},
			want: "10x12x3xpayments~6~3~2~c~604800000~1073741824~~",
		},
		{
			name:    "extra topic compact,delete, no retention",
			brokers: 6,
			primary: &topicShape{simName: "orders", partitions: 4, rf: 3},
			extras:  []topicShape{{simName: "audit", partitions: 3, rf: 2, minISR: 1, cleanupPolicy: "compact,delete"}},
			want:    "6x4x3xaudit~3~2~1~cd~~~~",
		},
		{
			name:    "extra topic retentionBytes only",
			brokers: 3,
			extras:  []topicShape{{simName: "metrics", partitions: 2, rf: 1, minISR: 1, cleanupPolicy: "delete", retentionBytes: i64p(500000)}},
			want:    "3x1x1xmetrics~2~1~1~d~~500000~~",
		},
		{
			name:    "primary topic config, no extras",
			brokers: 5,
			primary: &topicShape{simName: "orders", partitions: 4, rf: 4, minISR: 1, cleanupPolicy: "compact", retentionMs: i64p(3600000), retentionBytes: i64p(2000000)},
			want:    "5x4x4xxxxxxxxxxx1~c~3600000~2000000~",
		},
		{
			name:    "primary config + extras together",
			brokers: 6,
			primary: &topicShape{simName: "orders", partitions: 3, rf: 3, cleanupPolicy: "compact,delete"},
			extras:  []topicShape{{simName: "events", partitions: 2, rf: 2, minISR: 1, cleanupPolicy: "delete", retentionBytes: i64p(100)}},
			want:    "6x3x3xevents~2~2~1~d~~100~~xxxxxxxxxx~cd~~~",
		},
		{
			// SIZE ts-fixtures.txt: extra topic with disklessEnable.
			name:    "extra topic disklessEnable",
			brokers: 10,
			extras:  []topicShape{{simName: "wal", partitions: 4, rf: 3, minISR: 1, cleanupPolicy: "delete", disklessEnable: true}},
			want:    "10x1x1xwal~4~3~1~d~~~~~1",
		},
		{
			// SIZE ts-fixtures.txt: two extras, one diskless one classic.
			name:    "diskless and classic extras together",
			brokers: 10,
			extras: []topicShape{
				{simName: "wal", partitions: 4, rf: 3, minISR: 1, cleanupPolicy: "delete", disklessEnable: true},
				{simName: "classic", partitions: 2, rf: 2, minISR: 1, cleanupPolicy: "delete"},
			},
			want: "10x1x1xwal~4~3~1~d~~~~~1!classic~2~2~1~d~~~~",
		},
		{
			// SIZE ts-fixtures.txt: primary diskless flag only, no config, no extras.
			name:     "primary diskless flag only",
			brokers:  5,
			primary:  &topicShape{simName: "orders", partitions: 4, rf: 3},
			diskless: disklessTail{primary: true},
			want:     "5x4x3xxxxxxxxxxxxxxxxxxd",
		},
		{
			// SIZE ts-fixtures.txt: primary diskless flag + full timing.
			name:     "primary diskless flag with timing",
			brokers:  5,
			primary:  &topicShape{simName: "orders", partitions: 4, rf: 3},
			diskless: disklessTail{primary: true, commitMs: intp(250), uploadMs: intp(600)},
			want:     "5x4x3xxxxxxxxxxxxxxxxxxd250.600",
		},
		{
			// SIZE ts-fixtures.txt: diskless timing only (no primary flag), an
			// extra topic carries diskless instead. Only uploadMs is set.
			name:     "diskless timing only, upload side",
			brokers:  5,
			primary:  &topicShape{simName: "orders", partitions: 4, rf: 3},
			extras:   []topicShape{{simName: "wal", partitions: 4, rf: 3, minISR: 1, cleanupPolicy: "delete", disklessEnable: true}},
			diskless: disklessTail{uploadMs: intp(900)},
			want:     "5x4x3xwal~4~3~1~d~~~~~1xxxxxxxxxxxxxxxxx.900",
		},
		// Canonical tail index 10 (SIZE lines dated 2026-10-14 in ts-fixtures.txt):
		// a field equal to the simulator's default is left out, and a primary with
		// nothing left to say has no tail 10 at all.
		{
			name:    "primary at every default has no tail",
			brokers: 5,
			primary: &topicShape{simName: "orders", partitions: 4, rf: 3, minISR: 2, cleanupPolicy: "delete"},
			want:    "5x4x3",
		},
		{
			name:    "primary min.ISR 1 at RF 1 is the default",
			brokers: 3,
			primary: &topicShape{simName: "orders", partitions: 1, rf: 1, minISR: 1, cleanupPolicy: "delete"},
			want:    "3x1x1",
		},
		{
			name:    "primary min.ISR 1 at RF 2 is the default",
			brokers: 3,
			primary: &topicShape{simName: "orders", partitions: 1, rf: 2, minISR: 1, cleanupPolicy: "delete"},
			want:    "3x1x2",
		},
		{
			name:    "primary min.ISR 2 at RF 2 is not the default",
			brokers: 3,
			primary: &topicShape{simName: "orders", partitions: 1, rf: 2, minISR: 2, cleanupPolicy: "delete"},
			want:    "3x1x2xxxxxxxxxxx2~~~~",
		},
		{
			name:    "primary min.ISR 1 at RF 3 is not the default, delete is dropped",
			brokers: 5,
			primary: &topicShape{simName: "orders", partitions: 4, rf: 3, minISR: 1, cleanupPolicy: "delete"},
			want:    "5x4x3xxxxxxxxxxx1~~~~",
		},
		{
			name:    "primary default min.ISR dropped, compact kept",
			brokers: 5,
			primary: &topicShape{simName: "orders", partitions: 4, rf: 3, minISR: 2, cleanupPolicy: "compact"},
			want:    "5x4x3xxxxxxxxxxx~c~~~",
		},
		{
			name:    "primary default min.ISR at RF 4 dropped, compact,delete kept",
			brokers: 5,
			primary: &topicShape{simName: "orders", partitions: 4, rf: 4, minISR: 3, cleanupPolicy: "compact,delete"},
			want:    "5x4x4xxxxxxxxxxx~cd~~~",
		},
		{
			name:    "primary retention only",
			brokers: 5,
			primary: &topicShape{simName: "orders", partitions: 4, rf: 3, minISR: 2, cleanupPolicy: "delete", retentionMs: i64p(3600000)},
			want:    "5x4x3xxxxxxxxxxx~~3600000~~",
		},
		{
			name:    "primary retention bytes only",
			brokers: 5,
			primary: &topicShape{simName: "orders", partitions: 4, rf: 3, retentionBytes: i64p(2000000)},
			want:    "5x4x3xxxxxxxxxxx~~~2000000~",
		},
		{
			name:    "primary defaults beside retention carry only the retention",
			brokers: 5,
			primary: &topicShape{simName: "orders", partitions: 4, rf: 3, minISR: 2, cleanupPolicy: "delete", retentionMs: i64p(3600000), retentionBytes: i64p(2000000)},
			want:    "5x4x3xxxxxxxxxxx~~3600000~2000000~",
		},
		{
			name:    "primary at every default beside an extra topic leaves the length-1 tail",
			brokers: 6,
			primary: &topicShape{simName: "orders", partitions: 3, rf: 3, minISR: 2, cleanupPolicy: "delete"},
			extras:  []topicShape{{simName: "events", partitions: 2, rf: 2, minISR: 1, cleanupPolicy: "delete", retentionBytes: i64p(100)}},
			want:    "6x3x3xevents~2~2~1~d~~100~~",
		},
		{
			name:    "min.ISR above RF is clamped to RF before the default check",
			brokers: 5,
			primary: &topicShape{simName: "orders", partitions: 4, rf: 3, minISR: 9, cleanupPolicy: "delete"},
			want:    "5x4x3xxxxxxxxxxx3~~~~",
		},
	}
	for _, c := range cases {
		if got := buildSize(c.brokers, c.primary, c.extras, c.diskless); got != c.want {
			t.Errorf("%s: got %q want %q", c.name, got, c.want)
		}
	}
}

func TestSimTopicName(t *testing.T) {
	cases := map[string]string{
		"orders":      "orders",
		"my.topic":    "my-topic",
		"weird name!": "weird-name-",
		"__consumer":  "__consumer",
		"señal":       "se-al", // regexp replaces per RUNE: ñ becomes one '-'
		"":            "topic",
	}
	for in, want := range cases {
		if got := simTopicName(in); got != want {
			t.Errorf("simTopicName(%q) = %q, want %q", in, got, want)
		}
	}
	long := simTopicName("a-very-long-topic-name-that-exceeds-the-forty-character-cap")
	if len(long) != 40 {
		t.Errorf("long name not truncated to 40: %q (%d)", long, len(long))
	}
}

func TestAssignSimTopicNamesDedup(t *testing.T) {
	got := assignSimTopicNames([]string{"my.topic", "my-topic", "orders.v1"}, "orders")
	if got["my.topic"] != "my-topic" {
		t.Errorf("first sanitize: %v", got)
	}
	if got["my-topic"] != "my-topic-2" {
		t.Errorf("collision suffix: %v", got)
	}
	if got["orders.v1"] != "orders-v1" {
		t.Errorf("dot replace: %v", got)
	}
}

// tsSize is the JSON the SIZE fixture lines carry: the simulator's decoded
// FreePlaySize (src/sim/freePlayTopologies.ts), restricted to the fields this
// mapping can set.
type tsSize struct {
	BrokersPerZone    []int `json:"brokersPerZone"`
	Partitions        int   `json:"partitions"`
	ReplicationFactor int   `json:"replicationFactor"`
	ExtraTopics       []struct {
		Name              string `json:"name"`
		Partitions        int    `json:"partitions"`
		ReplicationFactor int    `json:"replicationFactor"`
		MinInSyncReplicas int    `json:"minInSyncReplicas"`
		CleanupPolicy     string `json:"cleanupPolicy"`
		RetentionMs       *int64 `json:"retentionMs"`
		RetentionBytes    *int64 `json:"retentionBytes"`
		DisklessEnable    bool   `json:"disklessEnable"`
	} `json:"extraTopics"`
	PrimaryMinIsr         *int   `json:"primaryMinIsr"`
	PrimaryCleanup        string `json:"primaryCleanup"`
	PrimaryRetentionMs    *int64 `json:"primaryRetentionMs"`
	PrimaryRetentionBytes *int64 `json:"primaryRetentionBytes"`
	PrimaryDiskless       bool   `json:"primaryDiskless"`
	DisklessTiming        *struct {
		CommitIntervalMs *int `json:"commitIntervalMs"`
		UploadMs         *int `json:"uploadMs"`
	} `json:"disklessTiming"`
}

// TestBuildSizeIsFixedPointOfTypeScript replays every SIZE fixture: take the
// simulator's decoded size, hand buildSize the same shape with the primary
// topic's min.ISR and cleanup spelled out even where they equal the defaults
// (what Map passes when a cluster simply runs on them), and the slug must come
// back byte-identical to the simulator's own re-encoding. A field left in that
// the simulator drops on decode (`1~d~...` at RF 3 and cleanup delete) fails here.
func TestBuildSizeIsFixedPointOfTypeScript(t *testing.T) {
	tsFixtures(t, "SIZE", func(fields []string) {
		if len(fields) != 4 || fields[1] != "=>" || !strings.HasPrefix(fields[3], ":: ") {
			t.Fatalf("malformed SIZE fixture: %v", fields)
		}
		canonical := fields[2]
		var sz tsSize
		if err := json.Unmarshal([]byte(strings.TrimPrefix(fields[3], ":: ")), &sz); err != nil {
			t.Fatalf("%s: fixture json: %v", canonical, err)
		}
		if len(sz.BrokersPerZone) != 1 {
			t.Fatalf("%s: single-dc fixtures only, got zones %v", canonical, sz.BrokersPerZone)
		}
		// The primary: an unset field is the simulator's default, which Map spells
		// out (min.ISR from DescribeConfigs, cleanup "delete").
		primary := &topicShape{
			simName: PrimaryTopicName, partitions: sz.Partitions, rf: sz.ReplicationFactor,
			minISR:        defaultPrimaryMinISR(sz.ReplicationFactor),
			cleanupPolicy: "delete",
			retentionMs:   sz.PrimaryRetentionMs, retentionBytes: sz.PrimaryRetentionBytes,
		}
		if sz.PrimaryMinIsr != nil {
			primary.minISR = *sz.PrimaryMinIsr
		}
		if sz.PrimaryCleanup != "" {
			primary.cleanupPolicy = sz.PrimaryCleanup
		}
		var extras []topicShape
		for _, e := range sz.ExtraTopics {
			extras = append(extras, topicShape{
				simName: e.Name, partitions: e.Partitions, rf: e.ReplicationFactor,
				minISR: e.MinInSyncReplicas, cleanupPolicy: e.CleanupPolicy,
				retentionMs: e.RetentionMs, retentionBytes: e.RetentionBytes, disklessEnable: e.DisklessEnable,
			})
		}
		diskless := disklessTail{primary: sz.PrimaryDiskless}
		if sz.DisklessTiming != nil {
			diskless.commitMs, diskless.uploadMs = sz.DisklessTiming.CommitIntervalMs, sz.DisklessTiming.UploadMs
		}
		if got := buildSize(sz.BrokersPerZone[0], primary, extras, diskless); got != canonical {
			t.Errorf("buildSize = %q, the simulator re-encodes the same size as %q", got, canonical)
		}
	})
}
