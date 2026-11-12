package actionlog

import (
	"math/rand"
	"reflect"
	"strings"
	"testing"
)

func strptr(s string) *string { return &s }

// roundTrip encodes entries, decodes them back and asserts the decode equals
// the sort-normalized input (decode(encode(x)) == x, §deliverable 3).
func roundTrip(t *testing.T, entries []Entry) {
	t.Helper()
	enc, err := Encode(entries)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	dec, err := Decode(enc)
	if err != nil {
		t.Fatalf("Decode(%q): %v", enc, err)
	}
	want := sortedEntries(entries)
	if len(want) == 0 {
		want = nil // Decode of an empty log yields nil, not an empty slice
	}
	if !reflect.DeepEqual(dec, want) {
		t.Fatalf("round-trip mismatch\n  enc:  %s\n  want: %#v\n  got:  %#v", enc, want, dec)
	}
	if err := Verify(entries); err != nil {
		t.Fatalf("Verify: %v", err)
	}
}

func TestRoundTripEachKind(t *testing.T) {
	cases := map[string][]Entry{
		"add_broker plain":     {{At: 100, Action: AddBroker{BrokerID: "1"}}},
		"add_broker rack":      {{At: 100, Action: AddBroker{BrokerID: "1", Rack: "rack-a"}}},
		"remove_broker":        {{At: 100, Action: RemoveBroker{BrokerID: "2"}}},
		"kill_broker hard":     {{At: 100, Action: KillBroker{BrokerID: "broker-2"}}},
		"kill_broker grace":    {{At: 100, Action: KillBroker{BrokerID: "broker-2", Graceful: true}}},
		"restart_broker":       {{At: 100, Action: RestartBroker{BrokerID: "broker-2"}}},
		"add_producer":         {{At: 100, Action: AddProducer{ProducerID: "p1"}}},
		"add_producer topic":   {{At: 100, Action: AddProducer{ProducerID: "p1", Topic: "orders"}}},
		"produce keyless":      {{At: 100, Action: ProduceRecord{ProducerID: "p1", Topic: "orders"}}},
		"produce keyed":        {{At: 100, Action: ProduceRecord{ProducerID: "p1", Topic: "orders", Key: strptr("O1")}}},
		"produce tombstone":    {{At: 100, Action: ProduceRecord{ProducerID: "p1", Topic: "orders", Key: strptr("O1"), Value: &RecordValue{Tombstone: true}}}},
		"produce tombstone kl": {{At: 100, Action: ProduceRecord{ProducerID: "p1", Topic: "orders", Value: &RecordValue{Tombstone: true}}}},
		"produce value":        {{At: 100, Action: ProduceRecord{ProducerID: "p1", Topic: "orders", Key: strptr("O1"), Value: &RecordValue{Value: "hello"}}}},
		"produce value kl":     {{At: 100, Action: ProduceRecord{ProducerID: "p1", Topic: "orders", Value: &RecordValue{Value: "hello"}}}},
		"produce value tricky": {{At: 100, Action: ProduceRecord{ProducerID: "p1", Topic: "orders", Key: strptr("O2"), Value: &RecordValue{Value: "qty:5,paid~x"}}}},
		"add_consumer":         {{At: 100, Action: AddConsumer{GroupID: "analytics", MemberID: "c1"}}},
		"add_group single":     {{At: 100, Action: AddGroup{GroupID: "analytics", Topics: []string{"orders"}}}},
		"add_group multi":      {{At: 100, Action: AddGroup{GroupID: "analytics", Topics: []string{"orders", "payments"}}}},
		"add_group earliest":   {{At: 100, Action: AddGroup{GroupID: "g1", Topics: []string{"orders"}, AutoOffsetReset: "earliest"}}},
		"add_group multi none": {{At: 100, Action: AddGroup{GroupID: "g1", Topics: []string{"a", "b"}, AutoOffsetReset: "none"}}},
		"remove_group":         {{At: 100, Action: RemoveGroup{GroupID: "c1"}}},
		"add_share_group":      {{At: 100, Action: AddShareGroup{GroupID: "sg", Topic: "orders"}}},
		"add_share_group mem":  {{At: 100, Action: AddShareGroup{GroupID: "sg", Topic: "orders", Members: []string{"m1", "m2"}}}},
		"config acks":          {{At: 100, Action: ConfigChange{Path: "producers.p1.config.acks", Value: "all"}}},
		"config minISR":        {{At: 100, Action: ConfigChange{Path: "topics.orders.minInSyncReplicas", Value: "2"}}},
		"config cleanup":       {{At: 100, Action: ConfigChange{Path: "topics.orders.cleanupPolicy", Value: "compact,delete"}}},
		"config consumer":      {{At: 100, Action: ConfigChange{Path: "consumer.autoOffsetReset", Value: "earliest"}}},
		"reassign":             {{At: 100, Action: ReassignPartition{Topic: "orders", Partition: 0, Replicas: []string{"1", "2", "3"}}}},
		"change_rf":            {{At: 100, Action: ChangeReplicationFactor{Topic: "orders", TargetRF: 3}}},
		"set_replica_speed":    {{At: 100, Action: SetReplicaSpeed{BrokerID: "1", Slow: true}}},
		"set_replica_speed 0":  {{At: 100, Action: SetReplicaSpeed{BrokerID: "1", Slow: false}}},
		"tier_offload":         {{At: 100, Action: TierOffload{Topic: "orders", Partition: 0, ToOffset: 5}}},
		"elect all":            {{At: 100, Action: ElectPreferredLeaders{}}},
		"elect topic":          {{At: 100, Action: ElectPreferredLeaders{Topic: "orders"}}},
		"kill_sr":              {{At: 100, Action: KillSR{InstanceID: "sr-dc-1-1"}}},
		"restart_sr":           {{At: 100, Action: RestartSR{InstanceID: "sr-dc-a-2"}}},
		"sr_failover":          {{At: 100, Action: SRFailover{DeploymentID: "sr"}}},
		"restart_producer":     {{At: 100, Action: RestartProducer{ProducerID: "p1"}}},
		"restart_producer new": {{At: 100, Action: RestartProducer{ProducerID: "p1", NewSchema: true}}},
		"set_sr_eligibility 1": {{At: 100, Action: SetSREligibility{InstanceID: "sr-dc-b-1", Eligible: true}}},
		"set_sr_eligibility 0": {{At: 100, Action: SetSREligibility{InstanceID: "sr-dc-b-1"}}},
	}
	for name, entries := range cases {
		t.Run(name, func(t *testing.T) { roundTrip(t, entries) })
	}
}

// TestEscapedValueEntry is the §8.4 example: a value with both ':' and ','.
// TestAddGroupResetWire pins the auto.offset.reset suffix to the playground's
// own form (actionLog.ts): `AG:<id>:<topics>:e|l|n`, absent when unset so
// older links stay byte-identical, and an unknown code refused rather than
// silently defaulted.
func TestAddGroupResetWire(t *testing.T) {
	got, err := Encode([]Entry{{At: 5, Action: AddGroup{GroupID: "g", Topics: []string{"orders"}, AutoOffsetReset: "earliest"}}})
	if err != nil || got != "AG:g:orders:e@5" {
		t.Fatalf("Encode = %q, %v; want AG:g:orders:e@5", got, err)
	}
	if got, _ := Encode([]Entry{{At: 5, Action: AddGroup{GroupID: "g", Topics: []string{"orders"}}}}); got != "AG:g:orders@5" {
		t.Fatalf("unset reset = %q; want AG:g:orders@5", got)
	}
	if _, err := Decode("AG:g:orders:x@5"); err == nil {
		t.Fatal("Decode accepted an unknown reset code")
	}
	if _, err := Encode([]Entry{{At: 5, Action: AddGroup{GroupID: "g", Topics: []string{"orders"}, AutoOffsetReset: "oldest"}}}); err == nil {
		t.Fatal("Encode accepted an unknown auto.offset.reset")
	}
}

func TestEscapedValueEntry(t *testing.T) {
	e := Entry{At: 200, Action: ProduceRecord{
		ProducerID: "p1", Topic: "orders", Key: strptr("O2"),
		Value: &RecordValue{Value: "qty:5,paid"},
	}}
	got, err := EncodeEntry(e)
	if err != nil {
		t.Fatalf("EncodeEntry: %v", err)
	}
	const want = "P:p1:orders:O2:v=qty:5~cpaid@200"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
	roundTrip(t, []Entry{e})
}

// TestEscapedKeyEntry is the §8.7 example: a produce KEY with both ':' and ','.
func TestEscapedKeyEntry(t *testing.T) {
	e := Entry{At: 100, Action: ProduceRecord{
		ProducerID: "p1", Topic: "orders", Key: strptr("user:42,v2"),
	}}
	got, err := EncodeEntry(e)
	if err != nil {
		t.Fatalf("EncodeEntry: %v", err)
	}
	const want = "P:p1:orders:user~s42~cv2@100"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
	roundTrip(t, []Entry{e})
}

// TestHostileKeyRoundTrips proves every separator survives in a produce key, in
// both the value-less and valued forms — the Go parity for the TS key escape.
func TestHostileKeyRoundTrips(t *testing.T) {
	for _, key := range []string{"a,b", "a:b", "a~b", "a~c,b:c", ":,~", "x&y#z%q+w", " spaced "} {
		roundTrip(t, []Entry{{At: 1, Action: ProduceRecord{ProducerID: "p1", Topic: "orders", Key: strptr(key)}}})
		roundTrip(t, []Entry{{At: 2, Action: ProduceRecord{
			ProducerID: "p1", Topic: "orders", Key: strptr(key), Value: &RecordValue{Value: "v:1,2~3"},
		}}})
	}
}

// TestPlainKeyByteStable asserts a key without separators is byte-identical to
// the pre-escape grammar, so every legacy Go/playground link is unchanged.
func TestPlainKeyByteStable(t *testing.T) {
	got, err := EncodeEntry(Entry{At: 4200, Action: ProduceRecord{ProducerID: "p1", Topic: "orders", Key: strptr("O42")}})
	if err != nil {
		t.Fatalf("EncodeEntry: %v", err)
	}
	const want = "P:p1:orders:O42@4200"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestEncodeSortsByAt(t *testing.T) {
	entries := []Entry{
		{At: 300, Action: RestartBroker{BrokerID: "1"}},
		{At: 100, Action: KillBroker{BrokerID: "1"}},
		{At: 200, Action: AddBroker{BrokerID: "2"}},
	}
	got, err := Encode(entries)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	const want = "K:1@100,AB:2@200,R:1@300"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestEmptyLog(t *testing.T) {
	got, err := Encode(nil)
	if err != nil || got != "" {
		t.Fatalf("Encode(nil) = %q, %v; want \"\", nil", got, err)
	}
	dec, err := Decode("")
	if err != nil || dec != nil {
		t.Fatalf("Decode(\"\") = %#v, %v; want nil, nil", dec, err)
	}
}

func TestMaxURLActionsCap(t *testing.T) {
	entries := make([]Entry, MaxURLActions+1)
	for i := range entries {
		entries[i] = Entry{At: i, Action: AddBroker{BrokerID: "b" + string(rune('0'+i%10))}}
	}
	if _, err := Encode(entries); err == nil {
		t.Fatalf("expected error over MAX_URL_ACTIONS, got nil")
	}
}

func TestValidationRejectsSeparators(t *testing.T) {
	bad := []Entry{
		{At: 1, Action: AddBroker{BrokerID: "a:b"}},
		{At: 1, Action: AddBroker{BrokerID: "a.b"}},
		{At: 1, Action: AddBroker{BrokerID: "a,b"}},
		{At: 1, Action: AddBroker{BrokerID: "a@b"}},
		{At: 1, Action: AddBroker{BrokerID: "a~b"}},
		{At: 1, Action: AddBroker{BrokerID: "a|b"}},
		{At: 1, Action: AddBroker{BrokerID: ""}},
		{At: 1, Action: AddGroup{GroupID: "g", Topics: []string{"a~b"}}},
	}
	for i, e := range bad {
		if _, err := Encode([]Entry{e}); err == nil {
			t.Errorf("case %d: expected validation error, got nil", i)
		}
	}
}

func TestConfigWhitelistRejectsUnknown(t *testing.T) {
	bad := []Entry{
		{At: 1, Action: ConfigChange{Path: "broker.rack", Value: "a"}},                                // not whitelisted
		{At: 1, Action: ConfigChange{Path: "producers.p1.config.acks", Value: "2"}},                   // wrong value
		{At: 1, Action: ConfigChange{Path: "producers.p1.config.maxInFlight", Value: "0"}},            // < 1
		{At: 1, Action: ConfigChange{Path: "topics.orders.minInSyncReplicas", Value: "x"}},            // not int
		{At: 1, Action: ConfigChange{Path: "producers.p1.config.acks", Clear: true}},                  // not clearable
		{At: 1, Action: ConfigChange{Path: "topics.orders.retentionMs", Value: "0"}},                  // < 1
		{At: 1, Action: ConfigChange{Path: "producers.p1.topic", Value: ""}},                          // empty topic
		{At: 1, Action: ConfigChange{Path: "group.g1.topics", Value: "orders."}},                      // dangling list item
		{At: 1, Action: ConfigChange{Path: "producers.p1.config.keyValue", Value: "a~b"}},             // unencodable "~"
		{At: 1, Action: ConfigChange{Path: "shareGroup.sg1.deliveryLimit", Value: "0"}},               // < 1
		{At: 1, Action: ConfigChange{Path: "group.g1.partition.assignment.strategy", Value: "eager"}}, // wrong enum
	}
	for i, e := range bad {
		if _, err := Encode([]Entry{e}); err == nil {
			t.Errorf("case %d (%v): expected whitelist rejection, got nil", i, e.Action)
		}
	}
}

// TestDecodeRejectsMalformed proves the self-check decoder FAILS on the same
// inputs the tolerant playground decoder would silently drop.
func TestDecodeRejectsMalformed(t *testing.T) {
	bad := []string{
		"AB:1",                // no @at
		"AB:1@08500",          // non-canonical at
		"AB:1@1e3",            // non-canonical at
		"ZZ:1@100",            // unknown kind
		"K@100",               // no kind separator
		"K:@100",              // empty payload
		"SR:1:2@100",          // bad speed code
		"RA:orders:0@100",     // wrong arity
		"C:bad.path:x@100",    // non-whitelisted config
		"KSR:@100",            // empty sr instance id
		"RPR::n@100",          // empty producer id
		"SRE:sr-dc-1-1@100",   // missing eligibility flag
		"SRE:sr-dc-1-1:2@100", // bad eligibility flag
		"SRE::1@100",          // empty sr instance id
	}
	for _, raw := range bad {
		if _, err := Decode(raw); err == nil {
			t.Errorf("Decode(%q): expected error, got nil", raw)
		}
	}
}

// TestPropertyRoundTrip generates random valid logs and asserts they round-trip.
func TestPropertyRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for iter := 0; iter < 500; iter++ {
		n := rng.Intn(8)
		entries := make([]Entry, n)
		for i := range entries {
			entries[i] = Entry{At: rng.Intn(100000), Action: randomAction(rng)}
		}
		roundTrip(t, entries)
	}
}

func randomAction(rng *rand.Rand) Action {
	id := func(prefix string) string { return prefix + string(rune('a'+rng.Intn(6))) }
	switch rng.Intn(13) {
	case 0:
		return AddBroker{BrokerID: id("b"), Rack: pick(rng, "", id("rack"))}
	case 1:
		return KillBroker{BrokerID: id("b"), Graceful: rng.Intn(2) == 0}
	case 2:
		return AddProducer{ProducerID: id("p"), Topic: pick(rng, "", id("t"))}
	case 3:
		return AddGroup{GroupID: id("g"), Topics: randTopics(rng)}
	case 4:
		return AddShareGroup{GroupID: id("g"), Topic: id("t"), Members: randMembers(rng)}
	case 5:
		return ReassignPartition{Topic: id("t"), Partition: rng.Intn(5), Replicas: []string{id("b"), id("b")}}
	case 6:
		return ChangeReplicationFactor{Topic: id("t"), TargetRF: rng.Intn(4) + 1}
	case 7:
		return SetReplicaSpeed{BrokerID: id("b"), Slow: rng.Intn(2) == 0}
	case 8:
		return TierOffload{Topic: id("t"), Partition: rng.Intn(3), ToOffset: rng.Intn(100)}
	case 9:
		return ConfigChange{Path: "producers." + id("p") + ".config.acks", Value: pick(rng, "0", "1", "all")}
	case 10:
		return AddConsumer{GroupID: id("g"), MemberID: id("c")}
	case 11:
		return pickAction(rng,
			KillSR{InstanceID: id("sr")}, RestartSR{InstanceID: id("sr")}, SRFailover{DeploymentID: id("sr")},
			RestartProducer{ProducerID: id("p"), NewSchema: rng.Intn(2) == 0},
			SetSREligibility{InstanceID: id("sr"), Eligible: rng.Intn(2) == 0})
	default:
		return produceRandom(rng, id)
	}
}

func produceRandom(rng *rand.Rand, id func(string) string) Action {
	rec := ProduceRecord{ProducerID: id("p"), Topic: id("t")}
	if rng.Intn(2) == 0 {
		rec.Key = strptr(id("k"))
	}
	switch rng.Intn(3) {
	case 0:
		// value-less
	case 1:
		rec.Value = &RecordValue{Tombstone: true}
	default:
		rec.Value = &RecordValue{Value: randValue(rng)}
	}
	return rec
}

func randValue(rng *rand.Rand) string {
	alphabet := []rune("abc:,~=")
	n := rng.Intn(10)
	var sb strings.Builder
	for i := 0; i < n; i++ {
		sb.WriteRune(alphabet[rng.Intn(len(alphabet))])
	}
	return sb.String()
}

func randTopics(rng *rand.Rand) []string {
	n := rng.Intn(3) + 1
	out := make([]string, n)
	for i := range out {
		out[i] = "t" + string(rune('a'+rng.Intn(6))) + string(rune('0'+i))
	}
	return out
}

func randMembers(rng *rand.Rand) []string {
	n := rng.Intn(3)
	if n == 0 {
		return nil
	}
	out := make([]string, n)
	for i := range out {
		out[i] = "m" + string(rune('0'+i))
	}
	return out
}

func pick(rng *rand.Rand, opts ...string) string { return opts[rng.Intn(len(opts))] }

func pickAction(rng *rand.Rand, opts ...Action) Action { return opts[rng.Intn(len(opts))] }

// TestSRWire pins the Schema Registry kinds (v2.2 preview) to the playground's
// own encoder output (actionLog.ts encodeOne): KSR/RSR/SFO carry the bare id,
// RPR appends ":n" only for a new schema, SRE ends in "1" or "0".
func TestSRWire(t *testing.T) {
	cases := []struct {
		entry Entry
		want  string
	}{
		{Entry{At: 0, Action: KillSR{InstanceID: "sr-dc-1-1"}}, "KSR:sr-dc-1-1@0"},
		{Entry{At: 1000, Action: RestartSR{InstanceID: "sr-dc-1-1"}}, "RSR:sr-dc-1-1@1000"},
		{Entry{At: 2000, Action: SRFailover{DeploymentID: "sr"}}, "SFO:sr@2000"},
		{Entry{At: 3000, Action: RestartProducer{ProducerID: "p1"}}, "RPR:p1@3000"},
		{Entry{At: 4000, Action: RestartProducer{ProducerID: "p1", NewSchema: true}}, "RPR:p1:n@4000"},
		{Entry{At: 5000, Action: SetSREligibility{InstanceID: "sr-dc-c-1", Eligible: true}}, "SRE:sr-dc-c-1:1@5000"},
		{Entry{At: 6000, Action: SetSREligibility{InstanceID: "sr-dc-c-1"}}, "SRE:sr-dc-c-1:0@6000"},
	}
	for _, c := range cases {
		got, err := EncodeEntry(c.entry)
		if err != nil {
			t.Fatalf("EncodeEntry(%#v): %v", c.entry.Action, err)
		}
		if got != c.want {
			t.Errorf("EncodeEntry(%#v) = %q, want %q", c.entry.Action, got, c.want)
		}
		back, err := Decode(c.want)
		if err != nil {
			t.Fatalf("Decode(%q): %v", c.want, err)
		}
		if len(back) != 1 || back[0] != c.entry {
			t.Errorf("Decode(%q) = %#v, want %#v", c.want, back, c.entry)
		}
	}
	for _, a := range []Action{KillSR{InstanceID: "a:b"}, RestartSR{}, SRFailover{DeploymentID: "s,r"}, RestartProducer{ProducerID: "p@1"}, SetSREligibility{InstanceID: "sr.1"}} {
		if _, err := EncodeEntry(Entry{At: 0, Action: a}); err == nil {
			t.Errorf("EncodeEntry(%#v): expected a validation error", a)
		}
	}
}

// TestRestartProducerDecodesLikePlayground: the playground reads the RPR
// flag only when it is "n" and ignores the rest, so these load (and preview)
// as restarts rather than drops; they re-encode to the canonical form.
func TestRestartProducerDecodesLikePlayground(t *testing.T) {
	for raw, want := range map[string]RestartProducer{
		"RPR:p1:x@100":   {ProducerID: "p1"},
		"RPR:p1:@100":    {ProducerID: "p1"},
		"RPR:p1:n:z@100": {ProducerID: "p1", NewSchema: true},
	} {
		got, err := Decode(raw)
		if err != nil {
			t.Fatalf("Decode(%q): %v", raw, err)
		}
		if len(got) != 1 || got[0].Action != want {
			t.Errorf("Decode(%q) = %#v, want %#v", raw, got, want)
		}
	}
}
