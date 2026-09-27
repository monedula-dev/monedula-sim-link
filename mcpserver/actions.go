package mcpserver

import (
	"fmt"

	"github.com/monedula-dev/monedula-sim-link/actionlog"
)

// ActionInput is one sandbox action as a flat JSON object: a "kind"
// discriminator plus the fields that kind uses (the rest stay unset). The same
// shape is echoed back by the tools after the URL self-decode, so a caller can
// diff intent against what the playground will load.
type ActionInput struct {
	Kind string `json:"kind" jsonschema:"one of: produce, kill_broker, restart_broker, add_broker, remove_broker, add_producer, remove_producer, add_group, add_consumer, add_share_group, config_change, reassign_partition, change_replication_factor, set_replica_speed, tier_offload"`
	At   int    `json:"at" jsonschema:"simulation time in milliseconds at which the action fires; actions are replayed sorted by this value (>= 0). Space actions ~1000ms apart so the playground animates them one at a time"`

	BrokerID string `json:"brokerId,omitempty" jsonschema:"broker id, e.g. broker-2 (kill_broker, restart_broker, add_broker, remove_broker, set_replica_speed). The default free-play cluster names brokers broker-1..broker-N"`
	Graceful bool   `json:"graceful,omitempty" jsonschema:"kill_broker only: true for a graceful shutdown, false/omitted for a hard kill"`
	Rack     string `json:"rack,omitempty" jsonschema:"add_broker only: optional rack to pin placement"`

	ProducerID string  `json:"producerId,omitempty" jsonschema:"producer id, e.g. p1 (produce, add_producer). The default free-play cluster starts with producer p1"`
	Topic      string  `json:"topic,omitempty" jsonschema:"topic name (produce, add_share_group, reassign_partition, change_replication_factor, tier_offload; optional per-topic pin for add_producer). The free-play primary topic is 'orders'"`
	Key        *string `json:"key,omitempty" jsonschema:"produce only: optional record key; controls partition routing. Any text (reversibly escaped in the URL)"`
	Value      *string `json:"value,omitempty" jsonschema:"produce: optional record value (any text; safely escaped). config_change: the new value, e.g. 'all' or '2' or 'compact,delete'"`
	Tombstone  bool    `json:"tombstone,omitempty" jsonschema:"produce only: true produces a null-value tombstone (mutually exclusive with value)"`

	GroupID string   `json:"groupId,omitempty" jsonschema:"consumer/share group id, e.g. analytics (add_group, add_consumer, add_share_group)"`
	Topics  []string `json:"topics,omitempty" jsonschema:"add_group only: subscribed topics (at least one; the first is the primary)"`
	// AutoOffsetReset is add_group's optional explicit auto.offset.reset.
	AutoOffsetReset string   `json:"autoOffsetReset,omitempty" jsonschema:"add_group only: optional explicit auto.offset.reset for the new group - earliest, latest or none. earliest starts it at the log start, which is what kafka-consumer-groups --reset-offsets --to-earliest lowers to; omitted, a new group starts at the high watermark"`
	Members         []string `json:"members,omitempty" jsonschema:"add_share_group only: optional explicit member ids"`
	MemberID        string   `json:"memberId,omitempty" jsonschema:"add_consumer only: id of the member joining the group, e.g. c2"`

	Path  string `json:"path,omitempty" jsonschema:"config_change only: whitelisted config path, e.g. producers.p1.config.acks or topics.orders.retentionMs (see §6 of docs/playground-url-api.md); non-whitelisted paths are rejected"`
	Clear bool   `json:"clear,omitempty" jsonschema:"config_change only: true clears the knob back to unset (allowed only for the 'or cleared' paths: producer maxInFlight/produceIntervalMs/quotaProducerByteRate/bufferMemory/maxBlockMs/batchSize and topic retentionMs/retentionBytes/localRetentionMs); mutually exclusive with value"`

	Partition               *int     `json:"partition,omitempty" jsonschema:"partition number >= 0 (reassign_partition, tier_offload)"`
	Replicas                []string `json:"replicas,omitempty" jsonschema:"reassign_partition only: the full target replica list as broker ids, e.g. [broker-1, broker-3]"`
	TargetReplicationFactor *int     `json:"targetReplicationFactor,omitempty" jsonschema:"change_replication_factor only: the new replication factor"`
	ToOffset                *int     `json:"toOffset,omitempty" jsonschema:"tier_offload only: offload the local log up to this offset (exclusive) to remote storage"`
	Slow                    *bool    `json:"slow,omitempty" jsonschema:"set_replica_speed only: true throttles the broker's follower replication, false restores full speed"`
}

// toEntry converts one ActionInput to a codec entry. i is the caller's index
// into the actions array, used to make every error name the offending action.
func toEntry(i int, a ActionInput) (actionlog.Entry, error) {
	fail := func(format string, args ...any) (actionlog.Entry, error) {
		return actionlog.Entry{}, fmt.Errorf("actions[%d] (kind %q): %s", i, a.Kind, fmt.Sprintf(format, args...))
	}
	if a.At < 0 {
		return fail("field \"at\" must be >= 0, got %d", a.At)
	}
	need := func(field, val string) error {
		if val == "" {
			_, err := fail("missing required field %q", field)
			return err
		}
		return nil
	}

	var action actionlog.Action
	switch a.Kind {
	case "kill_broker":
		if err := need("brokerId", a.BrokerID); err != nil {
			return actionlog.Entry{}, err
		}
		action = actionlog.KillBroker{BrokerID: a.BrokerID, Graceful: a.Graceful}
	case "restart_broker":
		if err := need("brokerId", a.BrokerID); err != nil {
			return actionlog.Entry{}, err
		}
		action = actionlog.RestartBroker{BrokerID: a.BrokerID}
	case "add_broker":
		if err := need("brokerId", a.BrokerID); err != nil {
			return actionlog.Entry{}, err
		}
		action = actionlog.AddBroker{BrokerID: a.BrokerID, Rack: a.Rack}
	case "remove_broker":
		if err := need("brokerId", a.BrokerID); err != nil {
			return actionlog.Entry{}, err
		}
		action = actionlog.RemoveBroker{BrokerID: a.BrokerID}
	case "add_producer":
		if err := need("producerId", a.ProducerID); err != nil {
			return actionlog.Entry{}, err
		}
		action = actionlog.AddProducer{ProducerID: a.ProducerID, Topic: a.Topic}
	case "remove_producer":
		if err := need("producerId", a.ProducerID); err != nil {
			return actionlog.Entry{}, err
		}
		action = actionlog.RemoveProducer{ProducerID: a.ProducerID}
	case "produce":
		if err := need("producerId", a.ProducerID); err != nil {
			return actionlog.Entry{}, err
		}
		if err := need("topic", a.Topic); err != nil {
			return actionlog.Entry{}, err
		}
		if a.Tombstone && a.Value != nil {
			return fail("fields \"value\" and \"tombstone\" are mutually exclusive")
		}
		rec := actionlog.ProduceRecord{ProducerID: a.ProducerID, Topic: a.Topic, Key: a.Key}
		if a.Tombstone {
			rec.Value = &actionlog.RecordValue{Tombstone: true}
		} else if a.Value != nil {
			rec.Value = &actionlog.RecordValue{Value: *a.Value}
		}
		action = rec
	case "add_group":
		if err := need("groupId", a.GroupID); err != nil {
			return actionlog.Entry{}, err
		}
		if len(a.Topics) == 0 {
			return fail("field \"topics\" needs at least one topic")
		}
		action = actionlog.AddGroup{GroupID: a.GroupID, Topics: a.Topics, AutoOffsetReset: a.AutoOffsetReset}
	case "add_consumer":
		if err := need("groupId", a.GroupID); err != nil {
			return actionlog.Entry{}, err
		}
		if err := need("memberId", a.MemberID); err != nil {
			return actionlog.Entry{}, err
		}
		action = actionlog.AddConsumer{GroupID: a.GroupID, MemberID: a.MemberID}
	case "add_share_group":
		if err := need("groupId", a.GroupID); err != nil {
			return actionlog.Entry{}, err
		}
		if err := need("topic", a.Topic); err != nil {
			return actionlog.Entry{}, err
		}
		action = actionlog.AddShareGroup{GroupID: a.GroupID, Topic: a.Topic, Members: a.Members}
	case "config_change":
		if err := need("path", a.Path); err != nil {
			return actionlog.Entry{}, err
		}
		if a.Clear && a.Value != nil {
			return fail("fields \"value\" and \"clear\" are mutually exclusive")
		}
		if !a.Clear && a.Value == nil {
			return fail("set field \"value\" (or \"clear\": true for a clearable path)")
		}
		cc := actionlog.ConfigChange{Path: a.Path, Clear: a.Clear}
		if a.Value != nil {
			cc.Value = *a.Value
		}
		action = cc
	case "reassign_partition":
		if err := need("topic", a.Topic); err != nil {
			return actionlog.Entry{}, err
		}
		if a.Partition == nil {
			return fail("missing required field \"partition\"")
		}
		if len(a.Replicas) == 0 {
			return fail("field \"replicas\" needs at least one broker id")
		}
		action = actionlog.ReassignPartition{Topic: a.Topic, Partition: *a.Partition, Replicas: a.Replicas}
	case "change_replication_factor":
		if err := need("topic", a.Topic); err != nil {
			return actionlog.Entry{}, err
		}
		if a.TargetReplicationFactor == nil {
			return fail("missing required field \"targetReplicationFactor\"")
		}
		action = actionlog.ChangeReplicationFactor{Topic: a.Topic, TargetRF: *a.TargetReplicationFactor}
	case "set_replica_speed":
		if err := need("brokerId", a.BrokerID); err != nil {
			return actionlog.Entry{}, err
		}
		if a.Slow == nil {
			return fail("missing required field \"slow\" (true = throttle, false = full speed)")
		}
		action = actionlog.SetReplicaSpeed{BrokerID: a.BrokerID, Slow: *a.Slow}
	case "tier_offload":
		if err := need("topic", a.Topic); err != nil {
			return actionlog.Entry{}, err
		}
		if a.Partition == nil {
			return fail("missing required field \"partition\"")
		}
		if a.ToOffset == nil {
			return fail("missing required field \"toOffset\"")
		}
		action = actionlog.TierOffload{Topic: a.Topic, Partition: *a.Partition, ToOffset: *a.ToOffset}
	case "":
		return fail("missing required field \"kind\"")
	default:
		return fail("unknown kind; expected one of produce, kill_broker, restart_broker, add_broker, remove_broker, add_producer, remove_producer, add_group, add_consumer, add_share_group, config_change, reassign_partition, change_replication_factor, set_replica_speed, tier_offload")
	}

	e := actionlog.Entry{At: a.At, Action: action}
	// Surface field-level codec validation (reserved separators, whitelist)
	// with the action index attached.
	if _, err := actionlog.EncodeEntry(e); err != nil {
		return fail("%v", err)
	}
	return e, nil
}

// toEntries converts a whole actions array, failing on the first bad action.
func toEntries(actions []ActionInput) ([]actionlog.Entry, error) {
	if len(actions) > actionlog.MaxURLActions {
		return nil, fmt.Errorf("actions has %d entries, exceeds MAX_URL_ACTIONS (%d)", len(actions), actionlog.MaxURLActions)
	}
	entries := make([]actionlog.Entry, len(actions))
	for i, a := range actions {
		e, err := toEntry(i, a)
		if err != nil {
			return nil, err
		}
		entries[i] = e
	}
	return entries, nil
}

// fromEntry converts a decoded codec entry back to the flat JSON echo shape.
func fromEntry(e actionlog.Entry) ActionInput {
	out := ActionInput{At: e.At}
	switch a := e.Action.(type) {
	case actionlog.KillBroker:
		out.Kind = "kill_broker"
		out.BrokerID = a.BrokerID
		out.Graceful = a.Graceful
	case actionlog.RestartBroker:
		out.Kind = "restart_broker"
		out.BrokerID = a.BrokerID
	case actionlog.AddBroker:
		out.Kind = "add_broker"
		out.BrokerID = a.BrokerID
		out.Rack = a.Rack
	case actionlog.RemoveBroker:
		out.Kind = "remove_broker"
		out.BrokerID = a.BrokerID
	case actionlog.AddProducer:
		out.Kind = "add_producer"
		out.ProducerID = a.ProducerID
		out.Topic = a.Topic
	case actionlog.RemoveProducer:
		out.Kind = "remove_producer"
		out.ProducerID = a.ProducerID
	case actionlog.ProduceRecord:
		out.Kind = "produce"
		out.ProducerID = a.ProducerID
		out.Topic = a.Topic
		out.Key = a.Key
		if a.Value != nil {
			if a.Value.Tombstone {
				out.Tombstone = true
			} else {
				v := a.Value.Value
				out.Value = &v
			}
		}
	case actionlog.AddGroup:
		out.Kind = "add_group"
		out.GroupID = a.GroupID
		out.Topics = a.Topics
		out.AutoOffsetReset = a.AutoOffsetReset
	case actionlog.AddConsumer:
		out.Kind = "add_consumer"
		out.GroupID = a.GroupID
		out.MemberID = a.MemberID
	case actionlog.AddShareGroup:
		out.Kind = "add_share_group"
		out.GroupID = a.GroupID
		out.Topic = a.Topic
		out.Members = a.Members
	case actionlog.ConfigChange:
		out.Kind = "config_change"
		out.Path = a.Path
		out.Clear = a.Clear
		if !a.Clear {
			v := a.Value
			out.Value = &v
		}
	case actionlog.ReassignPartition:
		out.Kind = "reassign_partition"
		out.Topic = a.Topic
		p := a.Partition
		out.Partition = &p
		out.Replicas = a.Replicas
	case actionlog.ChangeReplicationFactor:
		out.Kind = "change_replication_factor"
		out.Topic = a.Topic
		rf := a.TargetRF
		out.TargetReplicationFactor = &rf
	case actionlog.SetReplicaSpeed:
		out.Kind = "set_replica_speed"
		out.BrokerID = a.BrokerID
		s := a.Slow
		out.Slow = &s
	case actionlog.TierOffload:
		out.Kind = "tier_offload"
		out.Topic = a.Topic
		p := a.Partition
		out.Partition = &p
		off := a.ToOffset
		out.ToOffset = &off
	}
	return out
}

func fromEntries(entries []actionlog.Entry) []ActionInput {
	out := make([]ActionInput, len(entries))
	for i, e := range entries {
		out[i] = fromEntry(e)
	}
	return out
}
