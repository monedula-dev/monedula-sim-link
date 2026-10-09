package actionlog

import (
	"reflect"
	"testing"
)

// TestConfigWhitelistFullSection6 round-trips one representative entry for
// every §6 row the whitelist grew to (PR #497 on the TS side), proving parity
// with docs/playground-url-api.md.
func TestConfigWhitelistFullSection6(t *testing.T) {
	cases := map[string]ConfigChange{
		"producer topic":      {Path: "producers.p1.topic", Value: "payments"},
		"acks":                {Path: "producers.p1.config.acks", Value: "all"},
		"idempotence":         {Path: "producers.p1.config.enableIdempotence", Value: "true"},
		"maxInFlight":         {Path: "producers.p1.config.maxInFlight", Value: "5"},
		"retries":             {Path: "producers.p1.config.retries", Value: "0"},
		"retryBackoffMs":      {Path: "producers.p1.config.retryBackoffMs", Value: "100"},
		"lingerMs":            {Path: "producers.p1.config.lingerMs", Value: "0"},
		"produceIntervalMs":   {Path: "producers.p1.config.produceIntervalMs", Value: "500"},
		"partitioner":         {Path: "producers.p1.config.partitioner", Value: "uniform-sticky"},
		"compressionType":     {Path: "producers.p1.config.compressionType", Value: "zstd"},
		"quota":               {Path: "producers.p1.config.quotaProducerByteRate", Value: "1024"},
		"bufferMemory":        {Path: "producers.p1.config.bufferMemory", Value: "32768"},
		"maxBlockMs":          {Path: "producers.p1.config.maxBlockMs", Value: "60000"},
		"batchSize":           {Path: "producers.p1.config.batchSize", Value: "16384"},
		"keyStrategy":         {Path: "producers.p1.config.keyStrategy", Value: "fixed"},
		"keyValue":            {Path: "producers.p1.config.keyValue", Value: "order-42"},
		"keyValue empty":      {Path: "producers.p1.config.keyValue", Value: ""},
		"keyValue with comma": {Path: "producers.p1.config.keyValue", Value: "a,b"},
		"minISR":              {Path: "topics.orders.minInSyncReplicas", Value: "2"},
		"partitions":          {Path: "topics.orders.partitions", Value: "6"},
		"cleanupPolicy":       {Path: "topics.orders.cleanupPolicy", Value: "compact,delete"},
		"remoteStorage":       {Path: "topics.orders.remoteStorageEnable", Value: "true"},
		"disklessEnable":      {Path: "topics.orders.disklessEnable", Value: "true"},
		"retentionMs":         {Path: "topics.orders.retentionMs", Value: "60000"},
		"retentionBytes":      {Path: "topics.orders.retentionBytes", Value: "1048576"},
		"localRetentionMs":    {Path: "topics.orders.localRetentionMs", Value: "700"},
		"unclean election":    {Path: "topic.orders.unclean.leader.election.enable", Value: "false"},
		"pollMs":              {Path: "consumer.pollMs", Value: "800"},
		"group pollMs":        {Path: "group.g2.consumer.pollMs", Value: "800"},
		"autoOffsetReset":     {Path: "consumer.autoOffsetReset", Value: "latest"},
		"group offset reset":  {Path: "group.g2.consumer.autoOffsetReset", Value: "none"},
		"isolationLevel":      {Path: "consumer.isolationLevel", Value: "read_committed"},
		"group isolation":     {Path: "group.g2.consumer.isolationLevel", Value: "read_uncommitted"},
		"assignment strategy": {Path: "group.g2.partition.assignment.strategy", Value: "cooperative-sticky"},
		"group topics":        {Path: "group.g2.topics", Value: "orders.payments"},
		"share ackMode":       {Path: "shareGroup.sg1.ackMode", Value: "explicit"},
		"share acquireMode":   {Path: "shareGroup.sg1.acquireMode", Value: "batch_optimized"},
		"share renewAck":      {Path: "shareGroup.sg1.renewAckEnable", Value: "true"},
		"share deliveryLimit": {Path: "shareGroup.sg1.deliveryLimit", Value: "5"},
		"share maxPoll":       {Path: "shareGroup.sg1.maxPollRecords", Value: "100"},
		"member instance id":  {Path: "member.c1.groupInstanceId", Value: "true"},
	}
	for name, cc := range cases {
		t.Run(name, func(t *testing.T) { roundTrip(t, []Entry{{At: 100, Action: cc}}) })
	}
}

// TestConfigClearedMarker covers the §6/§8.6 explicit cleared marker: an empty
// value segment on the "or cleared" rows, e.g. "C:topics.orders.retentionMs:@4000".
func TestConfigClearedMarker(t *testing.T) {
	clearable := []string{
		"producers.p1.config.maxInFlight",
		"producers.p1.config.produceIntervalMs",
		"producers.p1.config.quotaProducerByteRate",
		"producers.p1.config.bufferMemory",
		"producers.p1.config.maxBlockMs",
		"producers.p1.config.batchSize",
		"topics.orders.retentionMs",
		"topics.orders.retentionBytes",
		"topics.orders.localRetentionMs",
	}
	for _, path := range clearable {
		e := Entry{At: 4000, Action: ConfigChange{Path: path, Clear: true}}
		enc, err := EncodeEntry(e)
		if err != nil {
			t.Fatalf("EncodeEntry(clear %s): %v", path, err)
		}
		want := "C:" + path + ":@4000"
		if enc != want {
			t.Errorf("cleared %s: got %q want %q", path, enc, want)
		}
		roundTrip(t, []Entry{e})
	}
	// The doc's §8.6 example decodes to the cleared marker.
	dec, err := Decode("C:topics.orders.retentionMs:@4000")
	if err != nil {
		t.Fatalf("Decode cleared example: %v", err)
	}
	want := []Entry{{At: 4000, Action: ConfigChange{Path: "topics.orders.retentionMs", Clear: true}}}
	if !reflect.DeepEqual(dec, want) {
		t.Fatalf("cleared decode: got %#v want %#v", dec, want)
	}
	// The pre-fix literal "undefined" value still decodes to a drop (§6).
	if _, err := Decode("C:topics.orders.retentionMs:undefined@4000"); err == nil {
		t.Error(`literal "undefined" value: expected decode error, got nil`)
	}
	// An empty value segment on a non-clearable path is a drop.
	if _, err := Decode("C:producers.p1.config.acks:@4000"); err == nil {
		t.Error("empty segment on non-clearable path: expected decode error, got nil")
	}
	// ... except keyValue, where it is the literal empty string, not a clear.
	dec, err = Decode("C:producers.p1.config.keyValue:@4000")
	if err != nil {
		t.Fatalf("Decode empty keyValue: %v", err)
	}
	want = []Entry{{At: 4000, Action: ConfigChange{Path: "producers.p1.config.keyValue", Value: ""}}}
	if !reflect.DeepEqual(dec, want) {
		t.Fatalf("empty keyValue decode: got %#v want %#v", dec, want)
	}
}

// TestConfigPartitionsResize covers the #560 topics.<t>.partitions whitelist row:
// the kafka-topics --alter --partitions N resize round-trips as a positive int,
// and non-positive / empty / malformed counts are dropped (a count is ALWAYS set,
// so it is NOT clearable).
func TestConfigPartitionsResize(t *testing.T) {
	roundTrip(t, []Entry{{At: 3200, Action: ConfigChange{Path: "topics.payments.partitions", Value: "12"}}})
	for _, raw := range []string{
		"C:topics.orders.partitions:@3200", // empty (not clearable)
		"C:topics.orders.partitions:0@3200",
		"C:topics.orders.partitions:x@3200",
	} {
		if _, err := Decode(raw); err == nil {
			t.Errorf("Decode(%q): expected error, got nil", raw)
		}
	}
}

// TestConfigGroupPollMs covers the per-group consumer poll interval path
// group.<id>.consumer.pollMs, which the §6 table lists alongside the global
// consumer.pollMs (same shape as the autoOffsetReset / isolationLevel rows):
// it takes the same integer rule as the global path, round-trips through
// encode/decode, and drops a non-integer on both the encode and decode side.
func TestConfigGroupPollMs(t *testing.T) {
	const wire = "C:group.g2.consumer.pollMs:800@3000"
	entry := Entry{At: 3000, Action: ConfigChange{Path: "group.g2.consumer.pollMs", Value: "800"}}

	enc, err := EncodeEntry(entry)
	if err != nil {
		t.Fatalf("EncodeEntry(group pollMs): %v", err)
	}
	if enc != wire {
		t.Fatalf("group pollMs encode: got %q want %q", enc, wire)
	}
	dec, err := Decode(wire)
	if err != nil {
		t.Fatalf("Decode(%q): %v", wire, err)
	}
	if !reflect.DeepEqual(dec, []Entry{entry}) {
		t.Fatalf("group pollMs decode: got %#v want %#v", dec, []Entry{entry})
	}
	roundTrip(t, []Entry{entry})

	// The per-group path rejects exactly what the global path rejects.
	for _, bad := range []string{"fast", "1.5", "08", ""} {
		for _, path := range []string{"consumer.pollMs", "group.g2.consumer.pollMs"} {
			if _, err := EncodeEntry(Entry{At: 3000, Action: ConfigChange{Path: path, Value: bad}}); err == nil {
				t.Errorf("EncodeEntry(%s=%q): expected error, got nil", path, bad)
			}
			if _, err := Decode("C:" + path + ":" + bad + "@3000"); err == nil {
				t.Errorf("Decode(%s=%q): expected error, got nil", path, bad)
			}
		}
	}

	// pollMs is not a clearable knob on either path.
	if _, err := EncodeEntry(Entry{At: 3000, Action: ConfigChange{Path: "group.g2.consumer.pollMs", Clear: true}}); err == nil {
		t.Error("cleared group pollMs: expected error, got nil")
	}
}

// TestSection86Example decodes the doc's §8.6 worked example verbatim.
func TestSection86Example(t *testing.T) {
	raw := "C:producers.p1.topic:payments@1000,C:group.g2.partition.assignment.strategy:range@2000,C:topics.orders.retentionMs:60000@3000,C:topics.orders.retentionMs:@4000"
	dec, err := Decode(raw)
	if err != nil {
		t.Fatalf("Decode(§8.6): %v", err)
	}
	want := []Entry{
		{At: 1000, Action: ConfigChange{Path: "producers.p1.topic", Value: "payments"}},
		{At: 2000, Action: ConfigChange{Path: "group.g2.partition.assignment.strategy", Value: "range"}},
		{At: 3000, Action: ConfigChange{Path: "topics.orders.retentionMs", Value: "60000"}},
		{At: 4000, Action: ConfigChange{Path: "topics.orders.retentionMs", Clear: true}},
	}
	if !reflect.DeepEqual(dec, want) {
		t.Fatalf("§8.6 decode: got %#v want %#v", dec, want)
	}
	reEnc, err := Encode(dec)
	if err != nil {
		t.Fatalf("re-encode: %v", err)
	}
	if reEnc != raw {
		t.Fatalf("§8.6 re-encode: got %q want %q", reEnc, raw)
	}
}

// TestDecodeTolerant mirrors the playground: malformed entries drop with a
// reason, the rest survive sorted.
func TestDecodeTolerant(t *testing.T) {
	raw := "K:broker-2@8500,ZZ:1@100,AG:analytics:orders@1000,C:bad.path:x@50,AB:1"
	entries, dropped := DecodeTolerant(raw)
	if len(entries) != 2 {
		t.Fatalf("kept %d entries, want 2: %#v", len(entries), entries)
	}
	if entries[0].At != 1000 || entries[1].At != 8500 {
		t.Errorf("entries not sorted by at: %#v", entries)
	}
	if len(dropped) != 3 {
		t.Fatalf("dropped %d, want 3: %#v", len(dropped), dropped)
	}
	if dropped[0].Raw != "ZZ:1@100" || dropped[1].Raw != "C:bad.path:x@50" || dropped[2].Raw != "AB:1" {
		t.Errorf("dropped raws wrong: %#v", dropped)
	}
	for _, d := range dropped {
		if d.Reason == "" {
			t.Errorf("dropped %q has empty reason", d.Raw)
		}
	}
	if e, d := DecodeTolerant(""); e != nil || d != nil {
		t.Errorf("empty log: got %#v / %#v", e, d)
	}
}
