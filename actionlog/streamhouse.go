package actionlog

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
)

// --- Streamhouse (v3.0, next) ----------------------------------------------
//
// Simulator v3.0 adds the Streamhouse free-play cluster (cluster=streamhouse):
// a classic Kafka cluster with Kafka Connect and Apache Iceberg's sink
// connector writing the `orders` topic into an Iceberg table. Its action-log
// codes are at release stage `next`: the playground decodes them only in a
// build that ships the `streamhouse` feature and skips them (like an unknown
// kind) everywhere else, including today's public playground.
//
//	IO:<op request as JSON>   one Iceberg table operation
//	KCW:<workerId>            kill a Kafka Connect worker (connect-1, ...)
//	RCW:<workerId>            restart a Kafka Connect worker
//	RCN:<connector name>      restart a sink connector (the preset's is orders-sink)
//
// Every payload is escaped like a produce value ("~" -> "~t", "," -> "~c"; ":"
// and "@" need no escape, since the kind ends at the first ":" and the time
// starts at the last "@"). Mirrors encodeOne / decodeOne in actionLog.ts and
// docs/playground-url-api.md §2 (website PR #977).

// KillConnectWorker kills one Kafka Connect worker (KCW). Its tasks move to the
// surviving workers after the rebalance delay.
type KillConnectWorker struct {
	WorkerID string
}

func (KillConnectWorker) isAction()    {}
func (KillConnectWorker) kind() string { return "KCW" }
func (a KillConnectWorker) payload() (string, error) {
	return escapedID("connect worker id", a.WorkerID)
}

// RestartConnectWorker starts a Kafka Connect worker again (RCW); it rejoins the
// Connect group.
type RestartConnectWorker struct {
	WorkerID string
}

func (RestartConnectWorker) isAction()    {}
func (RestartConnectWorker) kind() string { return "RCW" }
func (a RestartConnectWorker) payload() (string, error) {
	return escapedID("connect worker id", a.WorkerID)
}

// RestartConnector restarts a sink connector (RCN), for example after it
// failed. The Streamhouse preset's connector is orders-sink.
type RestartConnector struct {
	Name string
}

func (RestartConnector) isAction()    {}
func (RestartConnector) kind() string { return "RCN" }
func (a RestartConnector) payload() (string, error) {
	return escapedID("connector name", a.Name)
}

// escapedID escapes a free-text id like a produce value. An empty id is an
// error: the playground drops an entry with an empty payload.
func escapedID(name, v string) (string, error) {
	if v == "" {
		return "", fmt.Errorf("%s must not be empty", name)
	}
	return escapeProduceValue(v), nil
}

// IcebergOp is one Iceberg table operation (IO). Request is the op request as
// JSON: an object whose "op" names the operation (IcebergOpNames) and whose
// other fields are that operation's input, e.g.
//
//	{"op":"set_properties","table":"orders","updates":{"write.target-file-size-bytes":"134217728"},"removals":[]}
//
// The encoder checks the request against the playground's own shape check
// (parseIcebergOpRequest in src/sim/icebergEngine.ts: exactly the keys of the
// operation's type, each of its JSON type) and writes it the way the
// playground's JSON.stringify does: no whitespace, keys in the request's
// order, numbers and strings in JavaScript's form. A request the playground
// would skip is an error here.
type IcebergOp struct {
	Request string
}

func (IcebergOp) isAction()    {}
func (IcebergOp) kind() string { return "IO" }
func (a IcebergOp) payload() (string, error) {
	canon, err := CanonicalIcebergOp([]byte(a.Request))
	if err != nil {
		return "", err
	}
	return escapeProduceValue(canon), nil
}

// CanonicalIcebergOp validates an Iceberg op request and returns it in the
// form the playground writes back (JSON.stringify of the parsed request).
func CanonicalIcebergOp(raw []byte) (string, error) {
	v, err := parseOrderedJSON(raw)
	if err != nil {
		return "", fmt.Errorf("iceberg op: %w", err)
	}
	if err := checkIcebergOp(v); err != nil {
		return "", fmt.Errorf("iceberg op: %w", err)
	}
	var b strings.Builder
	v.write(&b)
	return b.String(), nil
}

func decodeIcebergOp(payload string) (Action, error) {
	canon, err := CanonicalIcebergOp([]byte(unescapeProduceValue(payload)))
	if err != nil {
		return nil, err
	}
	return IcebergOp{Request: canon}, nil
}

// IcebergOpNames lists the operations an IO entry may carry (OP_NAMES in
// src/sim/icebergEngine.ts), in the playground's order.
var IcebergOpNames = []string{
	"create_table", "append", "delete", "update", "merge", "equality_delete",
	"rewrite_data_files", "rewrite_position_delete_files", "rewrite_manifests",
	"create_tag", "create_branch", "replace_ref", "drop_ref", "fast_forward",
	"rollback", "set_current", "cherrypick", "expire_snapshots",
	"remove_orphan_files", "set_properties", "upgrade_format", "update_schema",
	"update_spec", "replace_sort_order", "catalog_down", "catalog_up",
}

// --- ordered JSON ------------------------------------------------------------
//
// A request is kept as an ordered tree so it re-encodes with its keys in the
// caller's order, as JSON.stringify(JSON.parse(text)) does in the playground.

type jsonKind int

const (
	jsonNull jsonKind = iota
	jsonBool
	jsonNumber
	jsonString
	jsonArray
	jsonObject
)

type jsonValue struct {
	kind jsonKind
	b    bool
	num  float64
	str  string
	arr  []*jsonValue
	keys []string
	vals map[string]*jsonValue
}

func (v *jsonValue) get(k string) (*jsonValue, bool) {
	x, ok := v.vals[k]
	return x, ok
}

// parseOrderedJSON parses one JSON value, keeping object key order. A repeated
// key keeps its first position and its last value, like JSON.parse. A number
// outside float64 range is an error (JSON.parse makes it Infinity, which the
// playground's shape check refuses).
func parseOrderedJSON(raw []byte) (*jsonValue, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	v, err := parseJSONValue(dec)
	if err != nil {
		return nil, err
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("unexpected data after the JSON value")
	}
	return v, nil
}

func parseJSONValue(dec *json.Decoder) (*jsonValue, error) {
	tok, err := dec.Token()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("empty JSON")
		}
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}
	switch t := tok.(type) {
	case nil:
		return &jsonValue{kind: jsonNull}, nil
	case bool:
		return &jsonValue{kind: jsonBool, b: t}, nil
	case json.Number:
		f, err := strconv.ParseFloat(string(t), 64)
		if err != nil || math.IsInf(f, 0) {
			return nil, fmt.Errorf("number %s is out of range", t)
		}
		return &jsonValue{kind: jsonNumber, num: f}, nil
	case string:
		return &jsonValue{kind: jsonString, str: t}, nil
	case json.Delim:
		switch t {
		case '[':
			v := &jsonValue{kind: jsonArray}
			for dec.More() {
				item, err := parseJSONValue(dec)
				if err != nil {
					return nil, err
				}
				v.arr = append(v.arr, item)
			}
			if _, err := dec.Token(); err != nil {
				return nil, fmt.Errorf("invalid JSON: %w", err)
			}
			return v, nil
		case '{':
			v := &jsonValue{kind: jsonObject, vals: map[string]*jsonValue{}}
			for dec.More() {
				kt, err := dec.Token()
				if err != nil {
					return nil, fmt.Errorf("invalid JSON: %w", err)
				}
				k, ok := kt.(string)
				if !ok {
					return nil, fmt.Errorf("invalid JSON: object key %v", kt)
				}
				item, err := parseJSONValue(dec)
				if err != nil {
					return nil, err
				}
				if _, dup := v.vals[k]; !dup {
					v.keys = append(v.keys, k)
				}
				v.vals[k] = item
			}
			if _, err := dec.Token(); err != nil {
				return nil, fmt.Errorf("invalid JSON: %w", err)
			}
			return v, nil
		}
	}
	return nil, fmt.Errorf("invalid JSON token %v", tok)
}

// write renders the value like JavaScript's JSON.stringify.
func (v *jsonValue) write(b *strings.Builder) {
	switch v.kind {
	case jsonNull:
		b.WriteString("null")
	case jsonBool:
		b.WriteString(strconv.FormatBool(v.b))
	case jsonNumber:
		b.WriteString(jsNumber(v.num))
	case jsonString:
		jsQuote(b, v.str)
	case jsonArray:
		b.WriteByte('[')
		for i, item := range v.arr {
			if i > 0 {
				b.WriteByte(',')
			}
			item.write(b)
		}
		b.WriteByte(']')
	case jsonObject:
		b.WriteByte('{')
		for i, k := range v.keys {
			if i > 0 {
				b.WriteByte(',')
			}
			jsQuote(b, k)
			b.WriteByte(':')
			v.vals[k].write(b)
		}
		b.WriteByte('}')
	}
}

// jsNumber formats a finite float64 like ECMAScript's Number::toString: the
// shortest digits that round-trip, plain notation for 1e-7 < |f| < 1e21 and
// exponent notation ("1e+21", "1.5e-7") outside it.
func jsNumber(f float64) string {
	if f == 0 {
		return "0"
	}
	sign := ""
	if f < 0 {
		sign, f = "-", -f
	}
	mant, expStr, _ := strings.Cut(strconv.FormatFloat(f, 'e', -1, 64), "e")
	exp, _ := strconv.Atoi(expStr)
	digits := strings.Replace(mant, ".", "", 1)
	k, n := len(digits), exp+1
	switch {
	case k <= n && n <= 21:
		return sign + digits + strings.Repeat("0", n-k)
	case 0 < n && n <= 21:
		return sign + digits[:n] + "." + digits[n:]
	case -6 < n && n <= 0:
		return sign + "0." + strings.Repeat("0", -n) + digits
	}
	e := n - 1
	es := "+"
	if e < 0 {
		es, e = "-", -e
	}
	m := digits[:1]
	if k > 1 {
		m += "." + digits[1:]
	}
	return sign + m + "e" + es + strconv.Itoa(e)
}

// jsQuote writes s as a JSON string like JSON.stringify: only '"', '\\' and
// control characters are escaped (the short forms where JSON has them,
// otherwise \u00xx in lower-case hex); everything else, non-ASCII included,
// is written as is.
func jsQuote(b *strings.Builder, s string) {
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '\b':
			b.WriteString(`\b`)
		case '\f':
			b.WriteString(`\f`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		default:
			if r < 0x20 {
				fmt.Fprintf(b, `\u%04x`, r)
			} else {
				b.WriteRune(r)
			}
		}
	}
	b.WriteByte('"')
}

// --- the op request shape (parseIcebergOpRequest) ------------------------------
//
// A port of the shape check in src/sim/icebergEngine.ts: every object has
// exactly the keys of its type (no unknown key, every required one), every
// field its JSON type, every literal one of its values. Maps keep free keys
// (table properties, partition tuples, summaries, compaction options); rows
// and stats are keyed by field id. Values the type allows are left to the
// simulator's Iceberg module, which refuses bad ones when the action runs.

type jsonCheck func(*jsonValue) bool

func isStr(v *jsonValue) bool  { return v.kind == jsonString }
func isNum(v *jsonValue) bool  { return v.kind == jsonNumber }
func isBool(v *jsonValue) bool { return v.kind == jsonBool }
func isInt(v *jsonValue) bool  { return v.kind == jsonNumber && v.num == math.Trunc(v.num) }
func anyJSON(*jsonValue) bool  { return true }

func oneOfStr(values ...string) jsonCheck {
	return func(v *jsonValue) bool {
		if v.kind != jsonString {
			return false
		}
		for _, s := range values {
			if v.str == s {
				return true
			}
		}
		return false
	}
}

func oneOfNum(values ...float64) jsonCheck {
	return func(v *jsonValue) bool {
		if v.kind != jsonNumber {
			return false
		}
		for _, n := range values {
			if v.num == n {
				return true
			}
		}
		return false
	}
}

func oneOfBool(want bool) jsonCheck {
	return func(v *jsonValue) bool { return v.kind == jsonBool && v.b == want }
}

func arrayOf(item jsonCheck) jsonCheck {
	return func(v *jsonValue) bool {
		if v.kind != jsonArray {
			return false
		}
		for _, x := range v.arr {
			if !item(x) {
				return false
			}
		}
		return true
	}
}

func mapOf(value jsonCheck, key func(string) bool) jsonCheck {
	return func(v *jsonValue) bool {
		if v.kind != jsonObject {
			return false
		}
		for _, k := range v.keys {
			if (key != nil && !key(k)) || !value(v.vals[k]) {
				return false
			}
		}
		return true
	}
}

type fields map[string]jsonCheck

func shapeOK(v *jsonValue, required, optional fields) bool {
	if v.kind != jsonObject {
		return false
	}
	for _, k := range v.keys {
		_, r := required[k]
		_, o := optional[k]
		if !r && !o {
			return false
		}
	}
	for k, check := range required {
		x, ok := v.get(k)
		if !ok || !check(x) {
			return false
		}
	}
	for k, check := range optional {
		if x, ok := v.get(k); ok && !check(x) {
			return false
		}
	}
	return true
}

func exactly(required, optional fields) jsonCheck {
	return func(v *jsonValue) bool { return shapeOK(v, required, optional) }
}

func isFieldIDKey(k string) bool {
	if k == "0" {
		return true
	}
	if k == "" || k[0] < '1' || k[0] > '9' {
		return false
	}
	for i := 1; i < len(k); i++ {
		if k[i] < '0' || k[i] > '9' {
			return false
		}
	}
	return true
}

func isValue(v *jsonValue) bool { return v.kind == jsonNull || isStr(v) || isNum(v) }
func isBound(v *jsonValue) bool { return isStr(v) || isNum(v) }

func isSchemaChange(v *jsonValue) bool {
	if v.kind != jsonObject {
		return false
	}
	op, _ := v.get("op")
	if op == nil || op.kind != jsonString {
		return false
	}
	switch op.str {
	case "add":
		return shapeOK(v, fields{"op": anyJSON, "name": isStr, "type": isFieldType}, fields{"required": oneOfBool(false)})
	case "drop":
		return shapeOK(v, fields{"op": anyJSON, "name": isStr}, nil)
	case "rename":
		return shapeOK(v, fields{"op": anyJSON, "name": isStr, "to": isStr}, nil)
	case "move":
		return shapeOK(v, fields{"op": anyJSON, "name": isStr}, fields{"first": oneOfBool(true), "after": isStr, "before": isStr})
	case "promote":
		return shapeOK(v, fields{"op": anyJSON, "name": isStr, "to": oneOfStr("long")}, nil)
	}
	return false
}

func isSpecChange(v *jsonValue) bool {
	if v.kind != jsonObject {
		return false
	}
	op, _ := v.get("op")
	if op == nil || op.kind != jsonString {
		return false
	}
	switch op.str {
	case "add":
		return shapeOK(v, fields{"op": anyJSON, "sourceName": isStr, "transform": isStr}, fields{"name": isStr})
	case "remove":
		return shapeOK(v, fields{"op": anyJSON, "name": isStr}, nil)
	case "rename":
		return shapeOK(v, fields{"op": anyJSON, "name": isStr, "to": isStr}, nil)
	}
	return false
}

var (
	isRow           = mapOf(isValue, isFieldIDKey)
	isStrMap        = mapOf(isStr, nil)
	isFieldType     = oneOfStr("int", "long", "string", "timestamp", "double")
	isFormatVersion = oneOfNum(2, 3)
	isPredicate     = exactly(fields{"fieldId": isInt, "op": oneOfStr("eq", "in", "between"), "values": arrayOf(isValue)}, nil)
	isStats         = mapOf(exactly(fields{"nullCount": isInt}, fields{"min": isBound, "max": isBound}), isFieldIDKey)
	isSource        = exactly(fields{"topic": isStr, "partition": isInt, "fromOffset": isInt, "toOffset": isInt}, fields{"schemaId": isInt})
	isDataFile      = exactly(
		fields{"specId": isInt, "partition": mapOf(isValue, nil), "recordCount": isInt, "sizeBytes": isNum, "stats": isStats},
		fields{"rows": arrayOf(isRow), "source": isSource},
	)
	isRewriteOptions = mapOf(func(v *jsonValue) bool { return isNum(v) || isBool(v) }, nil)
	isTableField     = exactly(fields{"name": isStr, "type": isFieldType, "required": isBool}, nil)
	isSpecItem       = exactly(fields{"sourceName": isStr, "transform": isStr, "name": isStr}, nil)
	isSortField      = exactly(
		fields{"sourceName": isStr, "transform": isStr, "direction": oneOfStr("asc", "desc")},
		fields{"nullOrder": oneOfStr("nulls-first", "nulls-last")},
	)
	isTagOptions    = exactly(nil, fields{"maxRefAgeMs": isInt})
	isBranchOptions = exactly(nil, fields{"maxRefAgeMs": isInt, "maxSnapshotAgeMs": isInt, "minSnapshotsToKeep": isInt})
	isExpireOptions = exactly(nil, fields{"olderThanMs": isInt, "retainLast": isInt})
)

// beginInput is each row-level or compaction operation's input (BEGIN_INPUT).
var beginInput = map[string]jsonCheck{
	"append": exactly(fields{"table": isStr, "dataFiles": arrayOf(isDataFile)}, fields{"branch": isStr, "summary": isStrMap}),
	"delete": exactly(fields{"table": isStr, "where": isPredicate}, fields{"branch": isStr}),
	"update": exactly(fields{"table": isStr, "where": isPredicate, "set": isRow}, fields{"branch": isStr}),
	"merge":  exactly(fields{"table": isStr, "keyFieldId": isInt, "rows": arrayOf(isRow)}, fields{"branch": isStr}),
	"equality_delete": exactly(
		fields{"table": isStr, "equalityFieldIds": arrayOf(isInt), "keys": arrayOf(arrayOf(isValue))},
		fields{"branch": isStr, "upsertRows": arrayOf(isRow)},
	),
	// Compactions run on main: no branch.
	"rewrite_data_files": exactly(
		fields{"table": isStr},
		fields{"strategy": oneOfStr("binpack", "sort"), "sort_order": isStr, "options": isRewriteOptions, "where": isPredicate},
	),
	"rewrite_position_delete_files": exactly(fields{"table": isStr}, fields{"options": isRewriteOptions}),
}

// opShape is each op request's shape, "op" included (OP_SHAPE).
var opShape = map[string]jsonCheck{
	"append":                        exactly(fields{"op": anyJSON, "input": beginInput["append"]}, nil),
	"delete":                        exactly(fields{"op": anyJSON, "input": beginInput["delete"]}, nil),
	"update":                        exactly(fields{"op": anyJSON, "input": beginInput["update"]}, nil),
	"merge":                         exactly(fields{"op": anyJSON, "input": beginInput["merge"]}, nil),
	"equality_delete":               exactly(fields{"op": anyJSON, "input": beginInput["equality_delete"]}, nil),
	"rewrite_data_files":            exactly(fields{"op": anyJSON, "input": beginInput["rewrite_data_files"]}, nil),
	"rewrite_position_delete_files": exactly(fields{"op": anyJSON, "input": beginInput["rewrite_position_delete_files"]}, nil),
	"create_table": exactly(fields{"op": anyJSON, "input": exactly(
		fields{"name": isStr, "fields": arrayOf(isTableField)},
		fields{"spec": arrayOf(isSpecItem), "formatVersion": isFormatVersion, "properties": isStrMap},
	)}, nil),
	"rewrite_manifests":   exactly(fields{"op": anyJSON, "table": isStr}, nil),
	"create_tag":          exactly(fields{"op": anyJSON, "table": isStr, "name": isStr, "snapshotId": isInt}, fields{"options": isTagOptions}),
	"create_branch":       exactly(fields{"op": anyJSON, "table": isStr, "name": isStr, "snapshotId": isInt}, fields{"options": isBranchOptions}),
	"replace_ref":         exactly(fields{"op": anyJSON, "table": isStr, "name": isStr, "snapshotId": isInt}, nil),
	"drop_ref":            exactly(fields{"op": anyJSON, "table": isStr, "name": isStr}, nil),
	"fast_forward":        exactly(fields{"op": anyJSON, "table": isStr, "branch": isStr, "to": isStr}, nil),
	"rollback":            exactly(fields{"op": anyJSON, "table": isStr, "snapshotId": isInt}, nil),
	"set_current":         exactly(fields{"op": anyJSON, "table": isStr, "snapshotId": isInt}, nil),
	"cherrypick":          exactly(fields{"op": anyJSON, "table": isStr, "snapshotId": isInt}, nil),
	"expire_snapshots":    exactly(fields{"op": anyJSON, "table": isStr, "options": isExpireOptions}, nil),
	"remove_orphan_files": exactly(fields{"op": anyJSON, "table": isStr, "olderThanMs": isInt}, nil),
	"set_properties":      exactly(fields{"op": anyJSON, "table": isStr, "updates": isStrMap, "removals": arrayOf(isStr)}, nil),
	"upgrade_format":      exactly(fields{"op": anyJSON, "table": isStr, "to": isFormatVersion}, nil),
	"update_schema":       exactly(fields{"op": anyJSON, "table": isStr, "changes": arrayOf(isSchemaChange)}, nil),
	"update_spec":         exactly(fields{"op": anyJSON, "table": isStr, "changes": arrayOf(isSpecChange)}, nil),
	"replace_sort_order":  exactly(fields{"op": anyJSON, "table": isStr, "fields": arrayOf(isSortField)}, nil),
	"catalog_down":        exactly(fields{"op": anyJSON}, nil),
	"catalog_up":          exactly(fields{"op": anyJSON}, nil),
}

func checkIcebergOp(v *jsonValue) error {
	if v.kind != jsonObject {
		return fmt.Errorf("the request must be a JSON object")
	}
	op, ok := v.get("op")
	if !ok || op.kind != jsonString {
		return fmt.Errorf(`the request needs a string "op"`)
	}
	check, ok := opShape[op.str]
	if !ok {
		return fmt.Errorf("unknown op %q; expected one of %s", op.str, strings.Join(IcebergOpNames, ", "))
	}
	if !check(v) {
		return fmt.Errorf("the %s request does not have the shape the playground accepts (a missing or unknown key, or a field of the wrong type); see docs/mapping.md", op.str)
	}
	return nil
}
