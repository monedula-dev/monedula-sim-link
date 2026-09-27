package simurl_test

import (
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/monedula-dev/monedula-sim-link/actionlog"
	"github.com/monedula-dev/monedula-sim-link/simurl"
)

func strptr(s string) *string { return &s }

// §8.2 — free-play: add a group, produce three keyed records, kill and restart
// a broker. Asserts the plain action string AND the full percent-encoded URL
// byte-for-byte against the spec.
func TestExample82(t *testing.T) {
	entries := []actionlog.Entry{
		{At: 1000, Action: actionlog.AddGroup{GroupID: "analytics", Topics: []string{"orders"}}},
		{At: 2000, Action: actionlog.ProduceRecord{ProducerID: "p1", Topic: "orders", Key: strptr("O1")}},
		{At: 3000, Action: actionlog.ProduceRecord{ProducerID: "p1", Topic: "orders", Key: strptr("O2")}},
		{At: 4000, Action: actionlog.ProduceRecord{ProducerID: "p1", Topic: "orders", Key: strptr("O3")}},
		{At: 8500, Action: actionlog.KillBroker{BrokerID: "broker-2"}},
		{At: 22000, Action: actionlog.RestartBroker{BrokerID: "broker-2"}},
	}
	const wantActions = "AG:analytics:orders@1000,P:p1:orders:O1@2000,P:p1:orders:O2@3000,P:p1:orders:O3@4000,K:broker-2@8500,R:broker-2@22000"
	gotActions, err := actionlog.Encode(entries)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if gotActions != wantActions {
		t.Fatalf("actions\n got: %s\nwant: %s", gotActions, wantActions)
	}

	const wantURL = "https://monedula.dev/kafka-simulator/playground?scenario=free&actions=AG%3Aanalytics%3Aorders%401000%2CP%3Ap1%3Aorders%3AO1%402000%2CP%3Ap1%3Aorders%3AO2%403000%2CP%3Ap1%3Aorders%3AO3%404000%2CK%3Abroker-2%408500%2CR%3Abroker-2%4022000"
	gotURL, err := simurl.Build(simurl.Params{Scenario: "free", Actions: entries})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if gotURL != wantURL {
		t.Fatalf("url\n got: %s\nwant: %s", gotURL, wantURL)
	}
}

// §8.3 — a session with config changes. Byte-for-byte string and URL.
func TestExample83(t *testing.T) {
	entries := []actionlog.Entry{
		{At: 1000, Action: actionlog.ConfigChange{Path: "producers.p1.config.acks", Value: "all"}},
		{At: 2000, Action: actionlog.ConfigChange{Path: "topics.orders.minInSyncReplicas", Value: "2"}},
		{At: 3000, Action: actionlog.ConfigChange{Path: "producers.p1.config.enableIdempotence", Value: "true"}},
		{At: 4000, Action: actionlog.ProduceRecord{ProducerID: "p1", Topic: "orders", Key: strptr("O1")}},
	}
	const wantActions = "C:producers.p1.config.acks:all@1000,C:topics.orders.minInSyncReplicas:2@2000,C:producers.p1.config.enableIdempotence:true@3000,P:p1:orders:O1@4000"
	gotActions, err := actionlog.Encode(entries)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if gotActions != wantActions {
		t.Fatalf("actions\n got: %s\nwant: %s", gotActions, wantActions)
	}

	const wantURL = "https://monedula.dev/kafka-simulator/playground?scenario=free&actions=C%3Aproducers.p1.config.acks%3Aall%401000%2CC%3Atopics.orders.minInSyncReplicas%3A2%402000%2CC%3Aproducers.p1.config.enableIdempotence%3Atrue%403000%2CP%3Ap1%3Aorders%3AO1%404000"
	gotURL, err := simurl.Build(simurl.Params{Scenario: "free", Actions: entries})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if gotURL != wantURL {
		t.Fatalf("url\n got: %s\nwant: %s", gotURL, wantURL)
	}
}

// §8.4 — a produce value with a comma and a colon round-trips through the URL.
func TestExample84(t *testing.T) {
	entries := []actionlog.Entry{
		{At: 200, Action: actionlog.ProduceRecord{
			ProducerID: "p1", Topic: "orders", Key: strptr("O2"),
			Value: &actionlog.RecordValue{Value: "qty:5,paid"},
		}},
	}
	got, err := actionlog.Encode(entries)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	const want = "P:p1:orders:O2:v=qty:5~cpaid@200"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
	decoded, err := simurl.DecodeActionsFromURL(got)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !reflect.DeepEqual(decoded, entries) {
		t.Fatalf("round-trip mismatch: %#v", decoded)
	}
}

// §8.5 — a long session (150 produces) exceeds the 1500-char threshold, so
// EncodeActionsForURL emits the "~1" compressed form, which decodes back to the
// exact 150-action log.
func TestExample85Compression(t *testing.T) {
	entries := make([]actionlog.Entry, 150)
	for i := range entries {
		entries[i] = actionlog.Entry{
			At: (i + 1) * 100,
			Action: actionlog.ProduceRecord{
				ProducerID: "p1", Topic: "orders",
				Key:   strptr("O" + strconv.Itoa(i)),
				Value: &actionlog.RecordValue{Value: "value-number-" + strconv.Itoa(i)},
			},
		}
	}
	plain, err := actionlog.Encode(entries)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(plain) <= simurl.CompressionThreshold {
		t.Fatalf("expected plain length > %d, got %d", simurl.CompressionThreshold, len(plain))
	}

	urlVal, err := simurl.EncodeActionsForURL(entries)
	if err != nil {
		t.Fatalf("EncodeActionsForURL: %v", err)
	}
	if !strings.HasPrefix(urlVal, "~1") {
		t.Fatalf("expected ~1 compressed prefix, got %.10q…", urlVal)
	}
	if len(urlVal) >= len(plain) {
		t.Fatalf("compressed form (%d) not shorter than plain (%d)", len(urlVal), len(plain))
	}

	decoded, err := simurl.DecodeActionsFromURL(urlVal)
	if err != nil {
		t.Fatalf("DecodeActionsFromURL: %v", err)
	}
	if !reflect.DeepEqual(decoded, entries) {
		t.Fatalf("compressed round-trip mismatch")
	}
}

// §4 — the "~1" prefix covers two layouts and this tool must decode BOTH. The
// playground EMITS lz-string; this is a real lz-string "~1" value captured from
// the playground codec (frozen — it stands in for a shared link in the wild).
// PlainActions / DecodeActionsFromURL must inflate it, so validate_actions can
// preview a playground-generated URL. The plain form uses only Go-supported kinds.
func TestDecodesPlaygroundLZStringForm(t *testing.T) {
	const lzValue = "~1IIBQXADgjGD2BOATApvAzgAQAwBpzTiVTTAHkoMotcBpMAI3lgGtUBaAJjAHMMPqcAJWCEU6MFgZNW8NlAB0jFuwDMGFQIDCYAC6wIASwDGaeQjGn4yHcgB2Og7FsBZEgDZq1DABZqQA"
	const wantPlain = "AP:p1:orders@0,P:p1:orders:O1@100,K:broker-2:g@200,RA:orders:0:broker-1.broker-3@300,C:topics.orders.retentionMs:60000@400"

	plain, compressed, err := simurl.PlainActions(lzValue)
	if err != nil {
		t.Fatalf("PlainActions(lz): %v", err)
	}
	if !compressed {
		t.Fatalf("expected compressed=true for a ~1 value")
	}
	if plain != wantPlain {
		t.Fatalf("lz-string inflate mismatch:\n got: %s\nwant: %s", plain, wantPlain)
	}

	// The whole value decodes to the same entries the plain string decodes to —
	// proving lz-string and plain are interchangeable to the decoder.
	viaLZ, err := simurl.DecodeActionsFromURL(lzValue)
	if err != nil {
		t.Fatalf("DecodeActionsFromURL(lz): %v", err)
	}
	viaPlain, err := actionlog.Decode(wantPlain)
	if err != nil {
		t.Fatalf("Decode(plain): %v", err)
	}
	if !reflect.DeepEqual(viaLZ, viaPlain) {
		t.Fatalf("lz-string decode differs from plain decode")
	}
}

// A short log stays plain (no compression prefix).
func TestShortLogStaysPlain(t *testing.T) {
	entries := []actionlog.Entry{{At: 100, Action: actionlog.AddBroker{BrokerID: "1"}}}
	got, err := simurl.EncodeActionsForURL(entries)
	if err != nil {
		t.Fatalf("EncodeActionsForURL: %v", err)
	}
	if got != "AB:1@100" {
		t.Fatalf("got %q", got)
	}
}

func TestBuildOmitsEmptyActions(t *testing.T) {
	got, err := simurl.Build(simurl.Params{})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	const want = "https://monedula.dev/kafka-simulator/playground?scenario=free"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestBuildClusterAndSizePassthrough(t *testing.T) {
	got, err := simurl.Build(simurl.Params{
		Scenario: "free",
		Cluster:  "active-active",
		Size:     "b3xp3xr3",
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	const want = "https://monedula.dev/kafka-simulator/playground?scenario=free&cluster=active-active&size=b3xp3xr3"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestBuildCustomBaseURL(t *testing.T) {
	got, err := simurl.Build(simurl.Params{
		BaseURL:  "http://localhost:4321/kafka-simulator/playground",
		Scenario: "free",
		Actions:  []actionlog.Entry{{At: 1, Action: actionlog.AddBroker{BrokerID: "1"}}},
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if !strings.HasPrefix(got, "http://localhost:4321/kafka-simulator/playground?scenario=free&actions=AB%3A1%401") {
		t.Fatalf("unexpected url: %s", got)
	}
}
