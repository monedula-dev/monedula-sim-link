package actionlog

import (
	"fmt"
	"strconv"
	"strings"
)

// Action is one sandbox operation. Concrete types below implement it. The
// interface is intentionally unexported-method-only so the closed set of
// encodable kinds lives entirely in this package (mirroring the TypeScript
// union in src/kafka-simulator/actionLog.ts).
type Action interface {
	// kind returns the uppercase kind code that prefixes the entry (K, AB, …).
	kind() string
	// payload returns the entry body between "<KIND>:" and "@<at>", validating
	// every raw field against the separator rules (§3/§7 of the URL API spec).
	payload() (string, error)
	isAction()
}

// reservedSeparators are the characters that structure the action log: entry
// (","), field (":"), time ("@"), sub-list / value-escape ("~"), list-item
// (".") and network-partition group ("|"). A raw (unescaped) field carrying any
// of them would corrupt the log on decode, so generators must keep them out.
const reservedSeparators = ",:@~.|"

// reservedSeparatorsDotOK is the same set minus ".", used for genuinely dotted
// fields (config paths).
const reservedSeparatorsDotOK = ",:@~|"

// validateRawField rejects an empty field or one containing any reserved
// separator. Used for broker / producer / group / member ids, topic names and
// produce keys — everything emitted raw.
func validateRawField(name, value string) error {
	if value == "" {
		return fmt.Errorf("%s must not be empty", name)
	}
	if i := strings.IndexAny(value, reservedSeparators); i >= 0 {
		return fmt.Errorf("%s %q contains reserved separator %q", name, value, string(value[i]))
	}
	return nil
}

// validateConfigPath is validateRawField for a dotted config path: dots are
// allowed, the remaining separators are not.
func validateConfigPath(value string) error {
	if value == "" {
		return fmt.Errorf("config path must not be empty")
	}
	if i := strings.IndexAny(value, reservedSeparatorsDotOK); i >= 0 {
		return fmt.Errorf("config path %q contains reserved separator %q", value, string(value[i]))
	}
	return nil
}

// --- kill_broker (K) ------------------------------------------------------

// KillBroker fails a broker. Graceful appends ":g"; a hard kill has no suffix.
type KillBroker struct {
	BrokerID string
	Graceful bool
}

func (KillBroker) isAction()    {}
func (KillBroker) kind() string { return "K" }
func (a KillBroker) payload() (string, error) {
	if err := validateRawField("broker id", a.BrokerID); err != nil {
		return "", err
	}
	if a.Graceful {
		return a.BrokerID + ":g", nil
	}
	return a.BrokerID, nil
}

// --- restart_broker (R) ---------------------------------------------------

type RestartBroker struct {
	BrokerID string
}

func (RestartBroker) isAction()    {}
func (RestartBroker) kind() string { return "R" }
func (a RestartBroker) payload() (string, error) {
	if err := validateRawField("broker id", a.BrokerID); err != nil {
		return "", err
	}
	return a.BrokerID, nil
}

// --- add_broker (AB) ------------------------------------------------------

// AddBroker adds a broker. A non-empty Rack pins placement.
type AddBroker struct {
	BrokerID string
	Rack     string
}

func (AddBroker) isAction()    {}
func (AddBroker) kind() string { return "AB" }
func (a AddBroker) payload() (string, error) {
	if err := validateRawField("broker id", a.BrokerID); err != nil {
		return "", err
	}
	if a.Rack != "" {
		if err := validateRawField("rack", a.Rack); err != nil {
			return "", err
		}
		return a.BrokerID + ":" + a.Rack, nil
	}
	return a.BrokerID, nil
}

// --- remove_broker (RB) ---------------------------------------------------

type RemoveBroker struct {
	BrokerID string
}

func (RemoveBroker) isAction()    {}
func (RemoveBroker) kind() string { return "RB" }
func (a RemoveBroker) payload() (string, error) {
	if err := validateRawField("broker id", a.BrokerID); err != nil {
		return "", err
	}
	return a.BrokerID, nil
}

// --- add_producer (AP) ----------------------------------------------------

// AddProducer adds a producer. A non-empty Topic pins per-topic routing.
type AddProducer struct {
	ProducerID string
	Topic      string
}

func (AddProducer) isAction()    {}
func (AddProducer) kind() string { return "AP" }
func (a AddProducer) payload() (string, error) {
	if err := validateRawField("producer id", a.ProducerID); err != nil {
		return "", err
	}
	if a.Topic != "" {
		if err := validateRawField("topic", a.Topic); err != nil {
			return "", err
		}
		return a.ProducerID + ":" + a.Topic, nil
	}
	return a.ProducerID, nil
}

// --- remove_producer (RP) ---------------------------------------------------

// RemoveProducer removes a producer (RP:<producerId>). Free play starts with a
// template producer "p1"; a generator removes it so its auto-produce loop does
// not add records beyond the synthetic ones the log carries.
type RemoveProducer struct {
	ProducerID string
}

func (RemoveProducer) isAction()    {}
func (RemoveProducer) kind() string { return "RP" }
func (a RemoveProducer) payload() (string, error) {
	if err := validateRawField("producer id", a.ProducerID); err != nil {
		return "", err
	}
	return a.ProducerID, nil
}

// --- produce_record (P) ---------------------------------------------------

// RecordValue is the optional value segment of a produce_record: a tombstone
// (value=null → "t") or a string value ("v=<escaped>").
type RecordValue struct {
	Tombstone bool
	Value     string // used when !Tombstone; safely escaped (may contain , ~ :)
}

// ProduceRecord produces one record. Key nil ⇒ keyless. Value nil ⇒ the legacy
// value-less form; otherwise a tombstone or a string value is appended.
type ProduceRecord struct {
	ProducerID string
	Topic      string
	Key        *string
	Value      *RecordValue
}

func (ProduceRecord) isAction()    {}
func (ProduceRecord) kind() string { return "P" }
func (a ProduceRecord) payload() (string, error) {
	if err := validateRawField("producer id", a.ProducerID); err != nil {
		return "", err
	}
	if err := validateRawField("topic", a.Topic); err != nil {
		return "", err
	}
	// The key is reversibly escaped (escapeProduceKey), not validated/rejected:
	// unlike the value it is a FIXED field, so ',' / ':' / '~' in it are escaped
	// rather than corrupting the entry. A key without those chars is byte-identical.
	// Value-less form: P:<producer>:<topic>[:<key>].
	if a.Value == nil {
		if a.Key != nil {
			return a.ProducerID + ":" + a.Topic + ":" + escapeProduceKey(*a.Key), nil
		}
		return a.ProducerID + ":" + a.Topic, nil
	}
	// Value form: the key segment is always emitted (empty when keyless) so the
	// decoder can locate the value after it.
	keySeg := ""
	if a.Key != nil {
		keySeg = escapeProduceKey(*a.Key)
	}
	valSeg := "t"
	if !a.Value.Tombstone {
		valSeg = "v=" + escapeProduceValue(a.Value.Value)
	}
	return a.ProducerID + ":" + a.Topic + ":" + keySeg + ":" + valSeg, nil
}

// --- add_consumer (AC) -----------------------------------------------------

// AddConsumer adds a named member to an existing consumer group.
type AddConsumer struct {
	GroupID  string
	MemberID string
}

func (AddConsumer) isAction()    {}
func (AddConsumer) kind() string { return "AC" }
func (a AddConsumer) payload() (string, error) {
	if err := validateRawField("group id", a.GroupID); err != nil {
		return "", err
	}
	if err := validateRawField("member id", a.MemberID); err != nil {
		return "", err
	}
	return a.GroupID + ":" + a.MemberID, nil
}

// --- add_group (AG) -------------------------------------------------------

// AddGroup adds a consumer group. Topics must hold ≥1 topic; ≥2 topics are
// "~"-joined and topics[0] becomes the primary topic. AutoOffsetReset, when
// set, is the group's explicit auto.offset.reset ("earliest", "latest" or
// "none"), encoded as a trailing ":e" / ":l" / ":n" - the form the terminal's
// `kafka-consumer-groups --reset-offsets` lowers to. Empty keeps the plain
// two-field form every older link uses.
type AddGroup struct {
	GroupID         string
	Topics          []string
	AutoOffsetReset string
}

// offsetResetCodes maps an auto.offset.reset value to its one-letter code.
var offsetResetCodes = map[string]string{"earliest": "e", "latest": "l", "none": "n"}

func (AddGroup) isAction()    {}
func (AddGroup) kind() string { return "AG" }
func (a AddGroup) payload() (string, error) {
	if err := validateRawField("group id", a.GroupID); err != nil {
		return "", err
	}
	if len(a.Topics) == 0 {
		return "", fmt.Errorf("add_group %q needs at least one topic", a.GroupID)
	}
	for _, t := range a.Topics {
		if err := validateRawField("topic", t); err != nil {
			return "", err
		}
	}
	out := a.GroupID + ":" + strings.Join(a.Topics, "~")
	if a.AutoOffsetReset != "" {
		code, ok := offsetResetCodes[a.AutoOffsetReset]
		if !ok {
			return "", fmt.Errorf("add_group %q: autoOffsetReset %q is not earliest, latest or none", a.GroupID, a.AutoOffsetReset)
		}
		out += ":" + code
	}
	return out, nil
}

// --- add_share_group (ASG) ------------------------------------------------

// AddShareGroup adds a KIP-932 share group on a single topic with an optional
// "."-joined member list.
type AddShareGroup struct {
	GroupID string
	Topic   string
	Members []string
}

func (AddShareGroup) isAction()    {}
func (AddShareGroup) kind() string { return "ASG" }
func (a AddShareGroup) payload() (string, error) {
	if err := validateRawField("group id", a.GroupID); err != nil {
		return "", err
	}
	if err := validateRawField("topic", a.Topic); err != nil {
		return "", err
	}
	if len(a.Members) == 0 {
		return a.GroupID + ":" + a.Topic, nil
	}
	for _, m := range a.Members {
		if err := validateRawField("member id", m); err != nil {
			return "", err
		}
	}
	return a.GroupID + ":" + a.Topic + ":" + strings.Join(a.Members, "."), nil
}

// --- config_change (C) ----------------------------------------------------

// ConfigChange sets one whitelisted config path (§6). Value is the canonical,
// unescaped value (e.g. "all", "2", "true", "compact,delete"); the encoder
// applies the ","→"~" escape. Clear=true emits the explicit cleared marker (an
// empty value segment, §6 "or cleared" rows) and ignores Value.
type ConfigChange struct {
	Path  string
	Value string
	Clear bool
}

func (ConfigChange) isAction()    {}
func (ConfigChange) kind() string { return "C" }
func (a ConfigChange) payload() (string, error) {
	if err := validateConfigPath(a.Path); err != nil {
		return "", err
	}
	rule := lookupConfigRule(a.Path)
	if rule == nil {
		return "", fmt.Errorf("config_change path %q is not on the accepted whitelist (§6 of docs/playground-url-api.md)", a.Path)
	}
	if a.Clear {
		if !rule.clearable {
			return "", fmt.Errorf("config_change path %q does not accept a cleared (empty) value", a.Path)
		}
		return a.Path + ":", nil
	}
	if !rule.accept(a.Value) {
		return "", fmt.Errorf("config_change value %q not accepted for path %q (expected %s)", a.Value, a.Path, rule.expect)
	}
	return a.Path + ":" + strings.ReplaceAll(a.Value, ",", "~"), nil
}

// --- reassign_partition (RA) ----------------------------------------------

// ReassignPartition sets a partition's replica assignment ("."-joined brokers).
type ReassignPartition struct {
	Topic     string
	Partition int
	Replicas  []string
}

func (ReassignPartition) isAction()    {}
func (ReassignPartition) kind() string { return "RA" }
func (a ReassignPartition) payload() (string, error) {
	if err := validateRawField("topic", a.Topic); err != nil {
		return "", err
	}
	if a.Partition < 0 {
		return "", fmt.Errorf("reassign_partition partition %d must be ≥ 0", a.Partition)
	}
	if len(a.Replicas) == 0 {
		return "", fmt.Errorf("reassign_partition %s/%d needs at least one replica", a.Topic, a.Partition)
	}
	for _, b := range a.Replicas {
		if err := validateRawField("replica broker id", b); err != nil {
			return "", err
		}
	}
	return a.Topic + ":" + strconv.Itoa(a.Partition) + ":" + strings.Join(a.Replicas, "."), nil
}

// --- change_replication_factor (CRF) --------------------------------------

type ChangeReplicationFactor struct {
	Topic    string
	TargetRF int
}

func (ChangeReplicationFactor) isAction()    {}
func (ChangeReplicationFactor) kind() string { return "CRF" }
func (a ChangeReplicationFactor) payload() (string, error) {
	if err := validateRawField("topic", a.Topic); err != nil {
		return "", err
	}
	if a.TargetRF < 0 {
		return "", fmt.Errorf("change_replication_factor targetRf %d must be ≥ 0", a.TargetRF)
	}
	return a.Topic + ":" + strconv.Itoa(a.TargetRF), nil
}

// --- set_replica_speed (SR) -----------------------------------------------

// SetReplicaSpeed throttles a broker's follower replication (Slow=true ⇒ "1").
type SetReplicaSpeed struct {
	BrokerID string
	Slow     bool
}

func (SetReplicaSpeed) isAction()    {}
func (SetReplicaSpeed) kind() string { return "SR" }
func (a SetReplicaSpeed) payload() (string, error) {
	if err := validateRawField("broker id", a.BrokerID); err != nil {
		return "", err
	}
	if a.Slow {
		return a.BrokerID + ":1", nil
	}
	return a.BrokerID + ":0", nil
}

// --- tier_offload (TO) ----------------------------------------------------

// TierOffload offloads a partition's local log up to ToOffset to remote storage.
type TierOffload struct {
	Topic     string
	Partition int
	ToOffset  int
}

func (TierOffload) isAction()    {}
func (TierOffload) kind() string { return "TO" }
func (a TierOffload) payload() (string, error) {
	if err := validateRawField("topic", a.Topic); err != nil {
		return "", err
	}
	if a.Partition < 0 {
		return "", fmt.Errorf("tier_offload partition %d must be ≥ 0", a.Partition)
	}
	if a.ToOffset < 0 {
		return "", fmt.Errorf("tier_offload toOffset %d must be ≥ 0", a.ToOffset)
	}
	return a.Topic + ":" + strconv.Itoa(a.Partition) + ":" + strconv.Itoa(a.ToOffset), nil
}

// --- elect_preferred_leaders (EL) -----------------------------------------

// ElectPreferredLeaders triggers a preferred-leader election. An empty Topic
// elects for ALL topic partitions (encoded as the "*" sentinel — a non-empty
// payload that can never be a real topic name); a non-empty Topic scopes it to
// that one topic.
type ElectPreferredLeaders struct {
	Topic string
}

func (ElectPreferredLeaders) isAction()    {}
func (ElectPreferredLeaders) kind() string { return "EL" }
func (a ElectPreferredLeaders) payload() (string, error) {
	if a.Topic == "" {
		return "*", nil
	}
	if err := validateRawField("topic", a.Topic); err != nil {
		return "", err
	}
	return a.Topic, nil
}

// escapeProduceValue reversibly escapes a produce value: "~"→"~t" then ","→"~c"
// (unescape reverses "~c" before "~t"). Mirrors escapeProduceValue in
// actionLog.ts.
func escapeProduceValue(v string) string {
	return strings.ReplaceAll(strings.ReplaceAll(v, "~", "~t"), ",", "~c")
}

func unescapeProduceValue(v string) string {
	return strings.ReplaceAll(strings.ReplaceAll(v, "~c", ","), "~t", "~")
}

// escapeProduceKey reversibly escapes a produce KEY: "~"→"~t", ","→"~c",
// ":"→"~s" (unescape reverses "~c"/"~s" before "~t", so a decoded "~"/":"/","
// can't re-form a marker). The key is a FIXED field, so unlike the value it also
// escapes ":". Mirrors escapeProduceKey in actionLog.ts.
func escapeProduceKey(v string) string {
	return strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(v, "~", "~t"), ",", "~c"), ":", "~s")
}

func unescapeProduceKey(v string) string {
	return strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(v, "~c", ","), "~s", ":"), "~t", "~")
}
