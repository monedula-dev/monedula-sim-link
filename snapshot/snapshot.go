// Package snapshot defines the boundary type between the (future) read-only
// Kafka client and the action-log mapping. A ClusterSnapshot is a plain,
// point-in-time view of a cluster's brokers and topics — no Kafka library
// types leak past this package, so the mapping and codec stay testable with
// hand-built fixtures.
package snapshot

import "time"

// Broker is a single cluster node.
type Broker struct {
	ID     string
	Rack   string // "" when the broker has no rack assigned
	Online bool
}

// ProducerWrite is one active producer's footprint on one partition, as
// DescribeProducers (KIP-664) reports it. Kafka lists idempotent and
// transactional producers alike; a producer that uses neither does not appear
// at all.
type ProducerWrite struct {
	ProducerID int64
	// LastTimestamp is the timestamp, in ms since the Unix epoch, of the
	// producer's last write on the partition; negative when the broker did not
	// report one.
	LastTimestamp int64
	// CurrentTxnStartOffset is the first offset of the producer's open
	// transaction on the partition, or -1 when it has none (an idempotent
	// producer, or a transactional one between transactions).
	CurrentTxnStartOffset int64
}

// Partition is one topic partition's placement and offset bounds.
type Partition struct {
	ID             int
	Leader         string   // broker id of the current leader ("" if none)
	Replicas       []string // assigned replica broker ids, in preference order
	ISR            []string // in-sync replica broker ids
	EarliestOffset int64
	LatestOffset   int64
	// Producers is what DescribeProducers reported for the partition, sorted by
	// producer id. Empty unless the snapshot was taken with FetchOptions.Producers
	// and the partition answered.
	Producers []ProducerWrite
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

// ProducersRead records how the opt-in DescribeProducers read went, so the
// mapping can say what is missing instead of silently showing fewer producers.
// Every failure here is non-fatal: the snapshot is still complete without it.
type ProducersRead struct {
	// At is the time the read was taken (the reader's clock); the mapping
	// measures its activity window back from it.
	At time.Time
	// Unsupported is true when the cluster cannot answer DescribeProducers:
	// UNSUPPORTED_VERSION, or a broker too old to know the API (before Kafka 2.8).
	Unsupported bool
	// ClusterDenied is true when a broker answered CLUSTER_AUTHORIZATION_FAILED.
	ClusterDenied bool
	// DeniedTopics lists, sorted, the topics with a partition that answered
	// TOPIC_AUTHORIZATION_FAILED (the request needs READ on the topic).
	DeniedTopics []string
	// Errors describes every other failure, one entry each: a request that did
	// not complete, or partitions that answered another error code.
	Errors []string
}

// ClusterSnapshot is the whole read-only view fed into the mapping.
type ClusterSnapshot struct {
	Brokers []Broker
	Topics  []Topic
	Groups  []Group
	// Producers is nil unless the snapshot was taken with FetchOptions.Producers.
	Producers *ProducersRead
}
