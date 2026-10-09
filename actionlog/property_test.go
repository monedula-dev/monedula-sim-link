package actionlog

import "testing"

// Compact mirror of the TypeScript property round-trip
// (src/kafka-simulator/__tests__/actionLog.property.test.ts) over the SAME
// alphabets, so the Go and TS codecs cannot drift. A hand-rolled, FIXED-SEED
// mulberry32 PRNG (no dependency, deterministic — no flakiness) generates random
// sequences over every kind the Go codec supports, and roundTrip asserts
// decode(encode(x)) == x (canonical, at-sorted) plus encode/decode stability.
//
// Escapable-field set is identical to TS for the kinds the Go codec supports:
// the produce_record VALUE and the add_group settings free-text values
// (region, rack, principal) are reversibly escaped, so the HOSTILE alphabet is
// fed ONLY there; every raw field (ids, topics, produce KEYS) draws from the
// SAFE alphabet.

// mulberry32 is the same PRNG the TS test uses, on uint32 (which wraps like the
// JS `| 0` / `>>> 0` masking). next() returns a float64 in [0, 1).
type mulberry32 struct{ a uint32 }

func newMulberry32(seed uint32) *mulberry32 { return &mulberry32{a: seed} }

func (m *mulberry32) next() float64 {
	m.a += 0x6d2b79f5
	t := m.a
	t = (t ^ (t >> 15)) * (t | 1)
	t ^= t + (t^(t>>7))*(t|61)
	return float64(t^(t>>14)) / 4294967296.0
}

func (m *mulberry32) intn(min, max int) int        { return min + int(m.next()*float64(max-min+1)) }
func (m *mulberry32) boolean() bool                { return m.next() < 0.5 }
func (m *mulberry32) choice(opts ...string) string { return opts[m.intn(0, len(opts)-1)] }

// SAFE — no separator characters; used for every raw field. HOSTILE — every
// character the spec warns about, used ONLY in produce values (the one field the
// codec reversibly escapes). Both mirror the TS alphabets. Built as []rune so
// the astral 😀 stays one unit and the string is always well-formed UTF-8.
const safeAlphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"

var hostileRunes = []rune(safeAlphabet + ",:@~.|%+&# " + "éxçÑ" + "√≈" + "😀")

func (m *mulberry32) safeID() string {
	n := m.intn(1, 8)
	b := []rune(safeAlphabet)
	out := make([]rune, n)
	for i := range out {
		out[i] = b[m.intn(0, len(b)-1)]
	}
	return string(out)
}

func (m *mulberry32) hostileValue() string {
	n := m.intn(0, 16)
	out := make([]rune, n)
	for i := range out {
		out[i] = hostileRunes[m.intn(0, len(hostileRunes)-1)]
	}
	return string(out)
}

// genConfig mirrors the §6 whitelist gens: each returns a ConfigChange whose
// Value/Clear is exactly what Decode reconstructs. clearableInt returns Clear or
// a valid ≥1 integer string.
func (m *mulberry32) clearable() (value string, clear bool) {
	if m.boolean() {
		return "", true
	}
	return itoa(m.intn(1, 100000)), false
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

func (m *mulberry32) genConfig() ConfigChange {
	gens := []func() ConfigChange{
		func() ConfigChange {
			return ConfigChange{Path: "producers." + m.safeID() + ".topic", Value: m.safeID()}
		},
		func() ConfigChange {
			return ConfigChange{Path: "producers." + m.safeID() + ".config.acks", Value: m.choice("0", "1", "all")}
		},
		func() ConfigChange {
			return ConfigChange{Path: "producers." + m.safeID() + ".config.enableIdempotence", Value: m.choice("true", "false")}
		},
		func() ConfigChange {
			v, c := m.clearable()
			return ConfigChange{Path: "producers." + m.safeID() + ".config.maxInFlight", Value: v, Clear: c}
		},
		func() ConfigChange {
			return ConfigChange{Path: "producers." + m.safeID() + ".config.retries", Value: itoa(m.intn(0, 10))}
		},
		func() ConfigChange {
			return ConfigChange{Path: "producers." + m.safeID() + ".config.lingerMs", Value: itoa(m.intn(0, 5000))}
		},
		func() ConfigChange {
			v, c := m.clearable()
			return ConfigChange{Path: "producers." + m.safeID() + ".config.produceIntervalMs", Value: v, Clear: c}
		},
		func() ConfigChange {
			return ConfigChange{Path: "producers." + m.safeID() + ".config.partitioner", Value: m.choice("default", "roundrobin", "uniform-sticky")}
		},
		func() ConfigChange {
			return ConfigChange{Path: "producers." + m.safeID() + ".config.compressionType", Value: m.choice("none", "gzip", "snappy", "lz4", "zstd")}
		},
		func() ConfigChange {
			return ConfigChange{Path: "producers." + m.safeID() + ".config.keyStrategy", Value: m.choice("current", "empty", "fixed")}
		},
		func() ConfigChange {
			// keyValue accepts any string without ~ : @ (may be empty).
			return ConfigChange{Path: "producers." + m.safeID() + ".config.keyValue", Value: m.maybeEmptySafe()}
		},
		func() ConfigChange {
			return ConfigChange{Path: "topics." + m.safeID() + ".minInSyncReplicas", Value: itoa(m.intn(0, 5))}
		},
		func() ConfigChange {
			// Partition COUNT (#560) — positive integer (≥ 1), always set (NOT clearable).
			return ConfigChange{Path: "topics." + m.safeID() + ".partitions", Value: itoa(m.intn(1, 64))}
		},
		func() ConfigChange {
			return ConfigChange{Path: "topics." + m.safeID() + ".cleanupPolicy", Value: m.choice("delete", "compact", "compact,delete")}
		},
		func() ConfigChange {
			return ConfigChange{Path: "topics." + m.safeID() + ".remoteStorageEnable", Value: m.choice("true", "false")}
		},
		func() ConfigChange {
			v, c := m.clearable()
			return ConfigChange{Path: "topics." + m.safeID() + ".retentionMs", Value: v, Clear: c}
		},
		func() ConfigChange {
			return ConfigChange{Path: "topic." + m.safeID() + ".unclean.leader.election.enable", Value: m.choice("true", "false")}
		},
		func() ConfigChange { return ConfigChange{Path: "consumer.pollMs", Value: itoa(m.intn(0, 5000))} },
		func() ConfigChange {
			return ConfigChange{Path: "group." + m.safeID() + ".consumer.pollMs", Value: itoa(m.intn(0, 5000))}
		},
		func() ConfigChange {
			return ConfigChange{Path: "group." + m.safeID() + ".consumer.autoOffsetReset", Value: m.choice("earliest", "latest", "none")}
		},
		func() ConfigChange {
			return ConfigChange{Path: "group." + m.safeID() + ".partition.assignment.strategy", Value: m.choice("range", "roundrobin", "sticky", "cooperative-sticky")}
		},
		func() ConfigChange {
			return ConfigChange{Path: "group." + m.safeID() + ".topics", Value: m.safeID() + "." + m.safeID()}
		},
		func() ConfigChange {
			return ConfigChange{Path: "shareGroup." + m.safeID() + ".ackMode", Value: m.choice("implicit", "explicit")}
		},
		func() ConfigChange {
			return ConfigChange{Path: "shareGroup." + m.safeID() + ".deliveryLimit", Value: itoa(m.intn(1, 20))}
		},
		func() ConfigChange {
			return ConfigChange{Path: "member." + m.safeID() + ".groupInstanceId", Value: m.choice("true", "false")}
		},
	}
	return gens[m.intn(0, len(gens)-1)]()
}

// maybeEmptySafe returns a possibly-empty SAFE string (for keyValue).
func (m *mulberry32) maybeEmptySafe() string {
	if m.boolean() {
		return ""
	}
	return m.safeID()
}

// genAction returns one canonical Action across every kind the Go codec supports
// (the subset of the TS union — broker/producer/produce/group/config/reassign/
// RF/replica-speed/tier). Non-supported kinds are simply not generated here.
func (m *mulberry32) genAction() Action {
	gens := []func() Action{
		func() Action { return KillBroker{BrokerID: m.safeID(), Graceful: m.boolean()} },
		func() Action { return RestartBroker{BrokerID: m.safeID()} },
		func() Action {
			if m.boolean() {
				return AddBroker{BrokerID: m.safeID()}
			}
			return AddBroker{BrokerID: m.safeID(), Rack: m.safeID()}
		},
		func() Action { return RemoveBroker{BrokerID: m.safeID()} },
		func() Action {
			if m.boolean() {
				return AddProducer{ProducerID: m.safeID()}
			}
			return AddProducer{ProducerID: m.safeID(), Topic: m.safeID()}
		},
		func() Action { return RemoveProducer{ProducerID: m.safeID()} },
		func() Action { return m.genProduce() },
		func() Action { return AddConsumer{GroupID: m.safeID(), MemberID: m.safeID()} },
		func() Action {
			n := m.intn(1, 3)
			topics := make([]string, n)
			for i := range topics {
				topics[i] = m.safeID()
			}
			return AddGroup{GroupID: m.safeID(), Topics: topics, AutoOffsetReset: []string{"", "earliest", "latest", "none"}[m.intn(0, 3)], Settings: m.genGroupSettings()}
		},
		func() Action {
			sg := AddShareGroup{GroupID: m.safeID(), Topic: m.safeID()}
			if !m.boolean() {
				n := m.intn(1, 3)
				sg.Members = make([]string, n)
				for i := range sg.Members {
					sg.Members[i] = m.safeID()
				}
			}
			return sg
		},
		func() Action { return m.genConfig() },
		func() Action {
			n := m.intn(1, 3)
			rep := make([]string, n)
			for i := range rep {
				rep[i] = m.safeID()
			}
			return ReassignPartition{Topic: m.safeID(), Partition: m.intn(0, 12), Replicas: rep}
		},
		func() Action { return ChangeReplicationFactor{Topic: m.safeID(), TargetRF: m.intn(1, 5)} },
		func() Action { return SetReplicaSpeed{BrokerID: m.safeID(), Slow: m.boolean()} },
		func() Action {
			return TierOffload{Topic: m.safeID(), Partition: m.intn(0, 12), ToOffset: m.intn(0, 100000)}
		},
		func() Action {
			// elect_preferred_leaders: topicless (all-topic-partitions, "*") or scoped.
			if m.boolean() {
				return ElectPreferredLeaders{}
			}
			return ElectPreferredLeaders{Topic: m.safeID()}
		},
	}
	return gens[m.intn(0, len(gens)-1)]()
}

// genGroupSettings mirrors the TS add_group generator: a random subset of the
// settings (often none, so the pre-settings form stays covered), the three
// free-text ones fully HOSTILE (they are escaped, and may be empty), pollMs any
// integer like consumer.pollMs, the two poll caps >= 1.
func (m *mulberry32) genGroupSettings() GroupSettings {
	var s GroupSettings
	str := func(v string) *string { return &v }
	num := func(n int) *int { return &n }
	if m.boolean() {
		s.Region = str(m.hostileValue())
	}
	if m.boolean() {
		s.Assignor = str(m.choice("range", "roundrobin", "sticky", "cooperative-sticky"))
	}
	if m.boolean() {
		s.IsolationLevel = str(m.choice("read_committed", "read_uncommitted"))
	}
	if m.boolean() {
		b := m.boolean()
		s.AutoCommit = &b
	}
	if m.boolean() {
		s.PollMs = num(m.intn(-5000, 5000))
	}
	if m.boolean() {
		s.MaxPollRecords = num(m.intn(1, 500))
	}
	if m.boolean() {
		s.MaxPollIntervalMs = num(m.intn(1, 300000))
	}
	if m.boolean() {
		s.Rack = str(m.hostileValue())
	}
	if m.boolean() {
		s.Principal = str(m.hostileValue())
	}
	return s
}

// genProduce builds the canonical produce_record the decoder yields; the VALUE
// (the one escapable field) draws from the HOSTILE alphabet, KEY stays SAFE.
func (m *mulberry32) genProduce() Action {
	rec := ProduceRecord{ProducerID: m.safeID(), Topic: m.safeID()}
	if m.boolean() {
		k := m.safeID() // raw ⇒ SAFE, non-empty
		rec.Key = &k
	}
	switch m.intn(0, 2) {
	case 0:
		// legacy value-less form
	case 1:
		rec.Value = &RecordValue{Tombstone: true}
	default:
		rec.Value = &RecordValue{Value: m.hostileValue()}
	}
	return rec
}

// genSequence builds a sequence with STRICTLY INCREASING At so the decoder's
// ascending sort is a no-op and equality is order-exact.
func (m *mulberry32) genSequence(length int) []Entry {
	out := make([]Entry, length)
	at := m.intn(0, 1000)
	for i := range out {
		at += m.intn(1, 50)
		out[i] = Entry{At: at, Action: m.genAction()}
	}
	return out
}

// TestPropertyRoundTripSeeded mirrors the TS seeded property test: a few hundred
// random sequences over every supported kind must round-trip exactly
// (decode(encode)==x) and be encode/decode stable (roundTrip also calls Verify).
// Distinct from TestPropertyRoundTrip (math/rand, narrower): this uses the same
// hand-rolled mulberry32 + hostile alphabet as the TS test to catch drift.
func TestPropertyRoundTripSeeded(t *testing.T) {
	m := newMulberry32(0x9e3779b9) // fixed seed — fully reproducible
	for i := 0; i < 500; i++ {
		roundTrip(t, m.genSequence(m.intn(1, 12)))
	}
}

// TestPropertyGroupSettingsSeeded focuses on the add_group settings field:
// every entry carries a random settings subset (HOSTILE free text), a random
// reset code and 1-3 topics, some of them dotted like the playground's
// multi-region topics.
func TestPropertyGroupSettingsSeeded(t *testing.T) {
	m := newMulberry32(0x5e771265)
	for i := 0; i < 300; i++ {
		n := m.intn(1, 6)
		entries := make([]Entry, n)
		at := m.intn(0, 1000)
		for j := range entries {
			at += m.intn(1, 50)
			topics := make([]string, m.intn(1, 3))
			for k := range topics {
				topics[k] = m.safeID()
				if m.boolean() {
					topics[k] = m.choice("west", "east") + "." + topics[k]
				}
			}
			entries[j] = Entry{At: at, Action: AddGroup{
				GroupID:         m.safeID(),
				Topics:          topics,
				AutoOffsetReset: m.choice("", "earliest", "latest", "none"),
				Settings:        m.genGroupSettings(),
			}}
		}
		roundTrip(t, entries)
	}
}

// TestPropertyHostileProduceValues focuses the HOSTILE alphabet on produce
// values (comma, colon, at, tilde, dot, pipe, percent, plus, ampersand, hash,
// space, unicode) — the field the codec reversibly escapes.
func TestPropertyHostileProduceValuesSeeded(t *testing.T) {
	m := newMulberry32(0xdeadbeef)
	for i := 0; i < 300; i++ {
		n := m.intn(1, 10)
		entries := make([]Entry, n)
		at := m.intn(0, 1000)
		for j := range entries {
			at += m.intn(1, 50)
			rec := ProduceRecord{ProducerID: m.safeID(), Topic: m.safeID(), Value: &RecordValue{Value: m.hostileValue()}}
			if m.boolean() {
				k := m.safeID()
				rec.Key = &k
			}
			entries[j] = Entry{At: at, Action: rec}
		}
		roundTrip(t, entries)
	}
}
