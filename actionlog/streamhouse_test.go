package actionlog

import (
	"strings"
	"testing"
)

// The examples of docs/playground-url-api.md §2 (website PR #977) for the
// Streamhouse kinds (v3.0, next), verbatim. Each is also what the playground's
// own encodeActions writes back after decodeActions (dumped from website main
// 27b30dca), so each must decode here and re-encode byte for byte.
var streamhouseDocExamples = []string{
	`IO:{"op":"set_properties"~c"table":"orders"~c"updates":{"write.target-file-size-bytes":"134217728"}~c"removals":[]}@5000`,
	`IO:{"op":"append"~c"input":{"table":"orders"~c"dataFiles":[{"specId":0~c"partition":{}~c"recordCount":100~c"sizeBytes":4096~c"stats":{}}]}}@6000`,
	`KCW:connect-1@8000`,
	`RCW:connect-1@20000`,
	`RCN:orders-sink@25000`,
	`KCW:connect-1@8000,RCW:connect-1@20000,RCN:orders-sink@25000`,
}

func TestStreamhouseDocExamples(t *testing.T) {
	for _, raw := range streamhouseDocExamples {
		entries, err := Decode(raw)
		if err != nil {
			t.Errorf("Decode(%q): %v", raw, err)
			continue
		}
		got, err := Encode(entries)
		if err != nil || got != raw {
			t.Errorf("re-encode of %q = %q, %v", raw, got, err)
		}
	}
}

// TestStreamhouseWire pins the encoder to the playground's output for typed
// entries.
func TestStreamhouseWire(t *testing.T) {
	cases := []struct {
		entry Entry
		want  string
	}{
		{Entry{At: 8000, Action: KillConnectWorker{WorkerID: "connect-1"}}, "KCW:connect-1@8000"},
		{Entry{At: 20000, Action: RestartConnectWorker{WorkerID: "connect-1"}}, "RCW:connect-1@20000"},
		{Entry{At: 25000, Action: RestartConnector{Name: "orders-sink"}}, "RCN:orders-sink@25000"},
		// Ids are escaped like a produce value: "," -> "~c", "~" -> "~t"; ":" and "." stay.
		{Entry{At: 100, Action: KillConnectWorker{WorkerID: "a,b~c"}}, "KCW:a~cb~tc@100"},
		{Entry{At: 100, Action: RestartConnector{Name: "orders-sink:x"}}, "RCN:orders-sink:x@100"},
		{Entry{At: 100, Action: IcebergOp{Request: `{"op":"catalog_down"}`}}, `IO:{"op":"catalog_down"}@100`},
		{Entry{At: 5000, Action: IcebergOp{Request: `{"op":"set_properties","table":"orders","updates":{"write.target-file-size-bytes":"134217728"},"removals":[]}`}},
			`IO:{"op":"set_properties"~c"table":"orders"~c"updates":{"write.target-file-size-bytes":"134217728"}~c"removals":[]}@5000`},
	}
	for _, c := range cases {
		got, err := EncodeEntry(c.entry)
		if err != nil || got != c.want {
			t.Errorf("EncodeEntry(%#v) = %q, %v; want %q", c.entry, got, err, c.want)
		}
	}
}

// TestIcebergOpCanonical: the playground decodes an IO payload with JSON.parse
// and writes it back with JSON.stringify, so whitespace goes, numbers take
// JavaScript's form and a "~" inside a string is escaped. These pairs are the
// playground's own decode / encode output.
func TestIcebergOpCanonical(t *testing.T) {
	cases := []struct{ in, want string }{
		{`IO:{ "op" : "catalog_down" }@100`, `IO:{"op":"catalog_down"}@100`},
		{`IO:{"op":"rollback"~c"table":"orders"~c"snapshotId":1.0}@100`, `IO:{"op":"rollback"~c"table":"orders"~c"snapshotId":1}@100`},
		{`IO:{"op":"rollback"~c"table":"o~tré"~c"snapshotId":1e3}@100`, `IO:{"op":"rollback"~c"table":"o~tré"~c"snapshotId":1000}@100`},
		// A repeated key keeps its first position and its last value.
		{`IO:{"op":"rollback"~c"table":"a"~c"snapshotId":1~c"table":"b"}@1`, `IO:{"op":"rollback"~c"table":"b"~c"snapshotId":1}@1`},
	}
	for _, c := range cases {
		entries, err := Decode(c.in)
		if err != nil {
			t.Errorf("Decode(%q): %v", c.in, err)
			continue
		}
		got, err := Encode(entries)
		if err != nil || got != c.want {
			t.Errorf("Decode+Encode(%q) = %q, %v; want %q", c.in, got, err, c.want)
		}
	}
}

func TestJSNumber(t *testing.T) {
	for f, want := range map[float64]string{
		0: "0", 1: "1", -1: "-1", 1000: "1000", 134217728: "134217728", 0.5: "0.5", 123.456: "123.456",
		1e20: "100000000000000000000", 1e21: "1e+21", 1.5e21: "1.5e+21", 0.000001: "0.000001", 1e-7: "1e-7",
		-2.5e-8: "-2.5e-8",
	} {
		if got := jsNumber(f); got != want {
			t.Errorf("jsNumber(%v) = %q, want %q", f, got, want)
		}
	}
}

// TestIcebergOpShapes accepts one request of every operation, each as the
// playground's parseIcebergOpRequest takes it, and round-trips it.
func TestIcebergOpShapes(t *testing.T) {
	ok := []string{
		`{"op":"create_table","input":{"name":"events","fields":[{"name":"id","type":"long","required":true}],"spec":[{"sourceName":"id","transform":"bucket[4]","name":"id_bucket"}],"formatVersion":3,"properties":{"write.format.default":"parquet"}}}`,
		`{"op":"append","input":{"table":"orders","dataFiles":[{"specId":0,"partition":{"ts_day":null},"recordCount":2,"sizeBytes":512.5,"stats":{"1":{"nullCount":0,"min":"a","max":"b"}},"rows":[{"1":"a","2":7}],"source":{"topic":"orders","partition":0,"fromOffset":0,"toOffset":2,"schemaId":1}}],"branch":"audit","summary":{"k":"v"}}}`,
		`{"op":"delete","input":{"table":"orders","where":{"fieldId":1,"op":"eq","values":["a"]}}}`,
		`{"op":"update","input":{"table":"orders","where":{"fieldId":1,"op":"in","values":["a","b"]},"set":{"2":null},"branch":"b"}}`,
		`{"op":"merge","input":{"table":"orders","keyFieldId":1,"rows":[{"1":"a"}]}}`,
		`{"op":"equality_delete","input":{"table":"orders","equalityFieldIds":[1],"keys":[["a"]],"upsertRows":[{"1":"a"}]}}`,
		`{"op":"rewrite_data_files","input":{"table":"orders","strategy":"sort","sort_order":"id ASC","options":{"min-input-files":2,"partial-progress.enabled":true},"where":{"fieldId":1,"op":"between","values":[1,5]}}}`,
		`{"op":"rewrite_position_delete_files","input":{"table":"orders","options":{"rewrite-all":true}}}`,
		`{"op":"rewrite_manifests","table":"orders"}`,
		`{"op":"create_tag","table":"orders","name":"v1","snapshotId":3,"options":{"maxRefAgeMs":1000}}`,
		`{"op":"create_branch","table":"orders","name":"audit","snapshotId":3,"options":{"minSnapshotsToKeep":2}}`,
		`{"op":"replace_ref","table":"orders","name":"audit","snapshotId":4}`,
		`{"op":"drop_ref","table":"orders","name":"audit"}`,
		`{"op":"fast_forward","table":"orders","branch":"main","to":"audit"}`,
		`{"op":"rollback","table":"orders","snapshotId":2}`,
		`{"op":"set_current","table":"orders","snapshotId":2}`,
		`{"op":"cherrypick","table":"orders","snapshotId":5}`,
		`{"op":"expire_snapshots","table":"orders","options":{"olderThanMs":10000,"retainLast":1}}`,
		`{"op":"expire_snapshots","table":"orders","options":{}}`,
		`{"op":"remove_orphan_files","table":"orders","olderThanMs":60000}`,
		`{"op":"set_properties","table":"orders","updates":{},"removals":["commit.retry.num-retries"]}`,
		`{"op":"upgrade_format","table":"orders","to":3}`,
		`{"op":"update_schema","table":"orders","changes":[{"op":"add","name":"amount","type":"double","required":false},{"op":"drop","name":"x"},{"op":"rename","name":"a","to":"b"},{"op":"move","name":"b","first":true},{"op":"move","name":"c","after":"b"},{"op":"promote","name":"n","to":"long"}]}`,
		`{"op":"update_spec","table":"orders","changes":[{"op":"add","sourceName":"ts","transform":"hour","name":"ts_hour"},{"op":"remove","name":"ts_day"},{"op":"rename","name":"a","to":"b"}]}`,
		`{"op":"replace_sort_order","table":"orders","fields":[{"sourceName":"id","transform":"identity","direction":"asc","nullOrder":"nulls-last"}]}`,
		`{"op":"catalog_down"}`,
		`{"op":"catalog_up"}`,
	}
	seen := map[string]bool{}
	for _, req := range ok {
		canon, err := CanonicalIcebergOp([]byte(req))
		if err != nil {
			t.Errorf("CanonicalIcebergOp(%s): %v", req, err)
			continue
		}
		if canon != req {
			t.Errorf("CanonicalIcebergOp(%s) = %s, want it unchanged", req, canon)
		}
		roundTrip(t, []Entry{{At: 100, Action: IcebergOp{Request: req}}})
		v, _ := parseOrderedJSON([]byte(req))
		op, _ := v.get("op")
		seen[op.str] = true
	}
	for _, name := range IcebergOpNames {
		if !seen[name] {
			t.Errorf("no accepted request covers op %q", name)
		}
	}
}

// TestIcebergOpRejects: everything the playground's decoder skips (JSON that
// does not parse, an unknown op, an unknown or missing key, a field of the
// wrong type) is an error here, on encode and on decode.
func TestIcebergOpRejects(t *testing.T) {
	bad := map[string]string{
		`not json`:                           "invalid JSON",
		``:                                   "empty JSON",
		`[]`:                                 "JSON object",
		`{"table":"orders"}`:                 `string "op"`,
		`{"op":"nope"}`:                      `unknown op "nope"`,
		`{"op":"catalog_up","x":1}`:          "shape",
		`{"op":"rollback","table":"orders"}`: "shape",
		`{"op":"rollback","table":"orders","snapshotId":1.5}`:                                                 "shape",
		`{"op":"rollback","table":"orders","snapshotId":"1"}`:                                                 "shape",
		`{"op":"upgrade_format","table":"orders","to":4}`:                                                     "shape",
		`{"op":"rewrite_data_files","input":{"table":"o","branch":"b"}}`:                                      "shape",
		`{"op":"delete","input":{"table":"o","where":{"fieldId":1,"op":"lt","values":[]}}}`:                   "shape",
		`{"op":"merge","input":{"table":"o","keyFieldId":1,"rows":[{"a":1}]}}`:                                "shape",
		`{"op":"update_schema","table":"o","changes":[{"op":"add","name":"a","type":"int","required":true}]}`: "shape",
		`{"op":"catalog_down"} {}`:                                                                            "after the JSON value",
		`{"op":"rollback","table":"o","snapshotId":1e999}`:                                                    "out of range",
	}
	for req, needle := range bad {
		_, err := EncodeEntry(Entry{At: 1, Action: IcebergOp{Request: req}})
		if err == nil || !strings.Contains(err.Error(), needle) {
			t.Errorf("encode %q: err %v, want it to mention %q", req, err, needle)
		}
		if req == "" {
			continue // an empty payload is refused before the kind is looked at
		}
		if _, err := Decode("IO:" + escapeProduceValue(req) + "@1"); err == nil {
			t.Errorf("Decode IO:%s: expected an error", req)
		}
	}
}

func TestStreamhouseRejectsEmpty(t *testing.T) {
	for _, a := range []Action{KillConnectWorker{}, RestartConnectWorker{}, RestartConnector{}} {
		if _, err := EncodeEntry(Entry{At: 1, Action: a}); err == nil {
			t.Errorf("%T with an empty id: expected an error", a)
		}
	}
	for _, raw := range []string{"KCW:@100", "RCW:@100", "RCN:@100", "IO:@100"} {
		if _, err := Decode(raw); err == nil {
			t.Errorf("Decode(%q): expected an error (the playground drops it)", raw)
		}
	}
}
