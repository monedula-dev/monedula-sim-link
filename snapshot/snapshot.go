// Package snapshot defines the boundary type between the (future) read-only
// Kafka client and the action-log mapping. A ClusterSnapshot is a plain,
// point-in-time view of a cluster's brokers and topics — no Kafka library
// types leak past this package, so the mapping and codec stay testable with
// hand-built fixtures.
package snapshot

// Broker is a single cluster node.
type Broker struct {
	ID     string
	Rack   string // "" when the broker has no rack assigned
	Online bool
}

// Partition is one topic partition's placement and offset bounds.
type Partition struct {
	ID             int
	Leader         string   // broker id of the current leader ("" if none)
	Replicas       []string // assigned replica broker ids, in preference order
	ISR            []string // in-sync replica broker ids
	EarliestOffset int64
	LatestOffset   int64
}

// TopicConfig is the subset of a topic's DescribeConfigs result the mapping
// understands. A nil pointer field means the config could not be read (a
// DescribeConfigs error for that topic) or the broker returned no value; the
// mapping then falls back to the simulator's own default for that knob.
type TopicConfig struct {
	CleanupPolicy     string // "delete", "compact" or "compact,delete"; "" = unread
	MinInSyncReplicas *int
	RetentionMs       *int64
	RetentionBytes    *int64
}

// Topic is a topic and its partitions.
type Topic struct {
	Name       string
	Partitions []Partition
	Config     TopicConfig
}

// GroupMember is one member of a consumer group, as returned by DescribeGroups.
type GroupMember struct {
	ID       string // the group-assigned member id
	ClientID string
}

// Group is a consumer group and what OffsetFetch/DescribeGroups reveal about
// it: state, members, and its committed offsets restricted to the selected
// topics (never the whole cluster — OffsetFetch is scoped to the snapshot's
// topic selection).
type Group struct {
	ID      string
	State   string // e.g. "Stable", "Empty", "Dead"
	Members []GroupMember
	// Offsets is topic -> partition -> committed offset. Only entries with a
	// valid (>= 0) committed offset are present; a partition never committed
	// is simply absent.
	Offsets map[string]map[int]int64
}

// ClusterSnapshot is the whole read-only view fed into the mapping.
type ClusterSnapshot struct {
	Brokers []Broker
	Topics  []Topic
	Groups  []Group
}
