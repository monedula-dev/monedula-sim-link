package actionlog

import (
	"fmt"
	"strings"
)

// decodeAction reconstructs the typed Action for one entry. It mirrors the
// per-kind decoders in actionLog.ts but returns an error (rather than nil/drop)
// so malformed logs surface instead of silently losing data.
func decodeAction(kind, payload string) (Action, error) {
	switch kind {
	case "K":
		return decodeKillBroker(payload)
	case "R":
		return RestartBroker{BrokerID: payload}, nil
	case "AB":
		return decodeAddBroker(payload)
	case "RB":
		return RemoveBroker{BrokerID: payload}, nil
	case "AP":
		return decodeAddProducer(payload)
	case "RP":
		return RemoveProducer{ProducerID: payload}, nil
	case "AC":
		return decodeAddConsumer(payload)
	case "P":
		return decodeProduceRecord(payload)
	case "AG":
		return decodeAddGroup(payload)
	case "ASG":
		return decodeAddShareGroup(payload)
	case "C":
		return decodeConfigChange(payload)
	case "RA":
		return decodeReassign(payload)
	case "CRF":
		return decodeChangeReplicationFactor(payload)
	case "SR":
		return decodeSetReplicaSpeed(payload)
	case "TO":
		return decodeTierOffload(payload)
	case "EL":
		return decodeElectPreferredLeaders(payload)
	default:
		return nil, fmt.Errorf("unsupported kind %q", kind)
	}
}

func decodeKillBroker(payload string) (Action, error) {
	parts := strings.Split(payload, ":")
	if len(parts) == 1 {
		if parts[0] == "" {
			return nil, fmt.Errorf("kill_broker: empty broker id")
		}
		return KillBroker{BrokerID: parts[0]}, nil
	}
	if len(parts) == 2 && parts[0] != "" && parts[1] == "g" {
		return KillBroker{BrokerID: parts[0], Graceful: true}, nil
	}
	return nil, fmt.Errorf("kill_broker: malformed payload %q", payload)
}

func decodeAddBroker(payload string) (Action, error) {
	parts := strings.Split(payload, ":")
	if len(parts) == 1 {
		if parts[0] == "" {
			return nil, fmt.Errorf("add_broker: empty broker id")
		}
		return AddBroker{BrokerID: parts[0]}, nil
	}
	if len(parts) == 2 && parts[0] != "" && parts[1] != "" {
		return AddBroker{BrokerID: parts[0], Rack: parts[1]}, nil
	}
	return nil, fmt.Errorf("add_broker: malformed payload %q", payload)
}

func decodeAddProducer(payload string) (Action, error) {
	parts := strings.Split(payload, ":")
	if len(parts) == 1 {
		if parts[0] == "" {
			return nil, fmt.Errorf("add_producer: empty producer id")
		}
		return AddProducer{ProducerID: parts[0]}, nil
	}
	if len(parts) == 2 {
		if parts[0] == "" || parts[1] == "" {
			return nil, fmt.Errorf("add_producer: empty field in %q", payload)
		}
		return AddProducer{ProducerID: parts[0], Topic: parts[1]}, nil
	}
	return nil, fmt.Errorf("add_producer: malformed payload %q", payload)
}

func decodeProduceRecord(payload string) (Action, error) {
	parts := strings.Split(payload, ":")
	if len(parts) < 2 {
		return nil, fmt.Errorf("produce_record: too few fields in %q", payload)
	}
	producerID, topic := parts[0], parts[1]
	if producerID == "" || topic == "" {
		return nil, fmt.Errorf("produce_record: empty producer/topic in %q", payload)
	}
	// Value-less legacy form: P:<producer>:<topic>[:<key>]. The key is reversibly
	// escaped (escapeProduceKey); a legacy raw key without markers decodes unchanged.
	if len(parts) <= 3 {
		rec := ProduceRecord{ProducerID: producerID, Topic: topic}
		if len(parts) == 3 {
			key := unescapeProduceKey(parts[2])
			rec.Key = &key
		}
		return rec, nil
	}
	// Value form: an empty key segment ⇒ keyless; the value is rejoined from the
	// tail so it may itself contain ':'.
	valueField := strings.Join(parts[3:], ":")
	rec := ProduceRecord{ProducerID: producerID, Topic: topic}
	if parts[2] != "" {
		key := unescapeProduceKey(parts[2])
		rec.Key = &key
	}
	if valueField == "t" {
		rec.Value = &RecordValue{Tombstone: true}
		return rec, nil
	}
	if strings.HasPrefix(valueField, "v=") {
		rec.Value = &RecordValue{Value: unescapeProduceValue(valueField[2:])}
		return rec, nil
	}
	return nil, fmt.Errorf("produce_record: malformed value segment %q", valueField)
}

func decodeAddConsumer(payload string) (Action, error) {
	parts := strings.Split(payload, ":")
	if len(parts) != 2 {
		return nil, fmt.Errorf("add_consumer: expected 2 fields in %q", payload)
	}
	if parts[0] == "" || parts[1] == "" {
		return nil, fmt.Errorf("add_consumer: empty field in %q", payload)
	}
	return AddConsumer{GroupID: parts[0], MemberID: parts[1]}, nil
}

func decodeAddGroup(payload string) (Action, error) {
	parts := strings.Split(payload, ":")
	if len(parts) != 2 && len(parts) != 3 {
		return nil, fmt.Errorf("add_group: expected 2 or 3 fields in %q", payload)
	}
	groupID, topicField := parts[0], parts[1]
	reset := ""
	if len(parts) == 3 {
		for value, code := range offsetResetCodes {
			if code == parts[2] {
				reset = value
			}
		}
		if reset == "" {
			return nil, fmt.Errorf("add_group: unknown auto.offset.reset code %q in %q (want e, l or n)", parts[2], payload)
		}
	}
	if groupID == "" || topicField == "" {
		return nil, fmt.Errorf("add_group: empty field in %q", payload)
	}
	var topics []string
	for _, t := range strings.Split(topicField, "~") {
		if t != "" {
			topics = append(topics, t)
		}
	}
	if len(topics) == 0 {
		return nil, fmt.Errorf("add_group: no topics in %q", payload)
	}
	return AddGroup{GroupID: groupID, Topics: topics, AutoOffsetReset: reset}, nil
}

func decodeAddShareGroup(payload string) (Action, error) {
	parts := strings.Split(payload, ":")
	if len(parts) != 2 && len(parts) != 3 {
		return nil, fmt.Errorf("add_share_group: expected 2 or 3 fields in %q", payload)
	}
	groupID, topic := parts[0], parts[1]
	if groupID == "" || topic == "" {
		return nil, fmt.Errorf("add_share_group: empty field in %q", payload)
	}
	if len(parts) == 2 {
		return AddShareGroup{GroupID: groupID, Topic: topic}, nil
	}
	members := strings.Split(parts[2], ".")
	for _, m := range members {
		if m == "" {
			return nil, fmt.Errorf("add_share_group: empty member in %q", payload)
		}
	}
	return AddShareGroup{GroupID: groupID, Topic: topic, Members: members}, nil
}

func decodeConfigChange(payload string) (Action, error) {
	sep := strings.LastIndex(payload, ":")
	if sep < 0 {
		return nil, fmt.Errorf("config_change: missing value separator in %q", payload)
	}
	path := payload[:sep]
	rule := lookupConfigRule(path)
	if rule == nil {
		return nil, fmt.Errorf("config_change path %q not on the accepted whitelist", path)
	}
	raw := payload[sep+1:]
	if raw == "" {
		// The explicit cleared marker (§6) — or, for keyValue, the literal empty
		// string, which that path accepts as a value.
		if rule.clearable {
			return ConfigChange{Path: path, Clear: true}, nil
		}
		if rule.accept("") {
			return ConfigChange{Path: path, Value: ""}, nil
		}
		return nil, fmt.Errorf("config_change path %q does not accept an empty value segment", path)
	}
	// Reverse the one-way comma escape applied on encode.
	value := strings.ReplaceAll(raw, "~", ",")
	if !rule.accept(value) {
		return nil, fmt.Errorf("config_change value %q not accepted for path %q (expected %s)", value, path, rule.expect)
	}
	return ConfigChange{Path: path, Value: value}, nil
}

func decodeReassign(payload string) (Action, error) {
	parts := strings.Split(payload, ":")
	if len(parts) != 3 {
		return nil, fmt.Errorf("reassign_partition: expected 3 fields in %q", payload)
	}
	topic := parts[0]
	if topic == "" {
		return nil, fmt.Errorf("reassign_partition: empty topic in %q", payload)
	}
	partition, ok := canonicalInt(parts[1])
	if !ok {
		return nil, fmt.Errorf("reassign_partition: non-canonical partition %q", parts[1])
	}
	replicas := strings.Split(parts[2], ".")
	for _, b := range replicas {
		if b == "" {
			return nil, fmt.Errorf("reassign_partition: empty replica in %q", payload)
		}
	}
	return ReassignPartition{Topic: topic, Partition: partition, Replicas: replicas}, nil
}

func decodeChangeReplicationFactor(payload string) (Action, error) {
	parts := strings.Split(payload, ":")
	if len(parts) != 2 {
		return nil, fmt.Errorf("change_replication_factor: expected 2 fields in %q", payload)
	}
	if parts[0] == "" {
		return nil, fmt.Errorf("change_replication_factor: empty topic in %q", payload)
	}
	targetRF, ok := canonicalInt(parts[1])
	if !ok {
		return nil, fmt.Errorf("change_replication_factor: non-canonical targetRf %q", parts[1])
	}
	return ChangeReplicationFactor{Topic: parts[0], TargetRF: targetRF}, nil
}

func decodeSetReplicaSpeed(payload string) (Action, error) {
	parts := strings.Split(payload, ":")
	if len(parts) != 2 {
		return nil, fmt.Errorf("set_replica_speed: expected 2 fields in %q", payload)
	}
	if parts[0] == "" || (parts[1] != "0" && parts[1] != "1") {
		return nil, fmt.Errorf("set_replica_speed: malformed payload %q", payload)
	}
	return SetReplicaSpeed{BrokerID: parts[0], Slow: parts[1] == "1"}, nil
}

func decodeElectPreferredLeaders(payload string) (Action, error) {
	// payload = "<topic>" (scoped) or "*" (all topic partitions). Non-empty is
	// guaranteed by decodeEntry; "*" is the reserved all-partitions sentinel.
	if payload == "*" {
		return ElectPreferredLeaders{}, nil
	}
	return ElectPreferredLeaders{Topic: payload}, nil
}

func decodeTierOffload(payload string) (Action, error) {
	parts := strings.Split(payload, ":")
	if len(parts) != 3 {
		return nil, fmt.Errorf("tier_offload: expected 3 fields in %q", payload)
	}
	if parts[0] == "" {
		return nil, fmt.Errorf("tier_offload: empty topic in %q", payload)
	}
	partition, ok := canonicalInt(parts[1])
	if !ok {
		return nil, fmt.Errorf("tier_offload: non-canonical partition %q", parts[1])
	}
	toOffset, ok := canonicalInt(parts[2])
	if !ok || toOffset < 0 {
		return nil, fmt.Errorf("tier_offload: invalid toOffset %q", parts[2])
	}
	return TierOffload{Topic: parts[0], Partition: partition, ToOffset: toOffset}, nil
}
