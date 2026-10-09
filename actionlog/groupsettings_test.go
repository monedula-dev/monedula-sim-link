package actionlog

import (
	"reflect"
	"strings"
	"testing"
)

func intptr(n int) *int    { return &n }
func boolptr(b bool) *bool { return &b }

// Real add_group entries carrying the settings field (§2.4), written by the
// WEBSITE's own codec at monedula-website main a5c85587 (PR #595 merged):
//   - "unit full", "unit pinned", "unit negative pollMs": wires asserted
//     verbatim in src/kafka-simulator/__tests__/actionLog.test.ts.
//   - "unit hostile": encodeActions() of that test's `hostile` action.
//   - "free-play + group": encodeActions() of the action
//     Playground.freePlayAddGroupRegion.test.tsx asserts the canvas "+ group"
//     dispatches on active-active (at 1200).
//   - "doc 8.10": the add_group of the URL-format reference's §8.10 example
//     (asserted in url-api-doc-examples.test.ts).
//   - "free-play reset": the add_group from the actions param PlaygroundStore
//     wrote after `kafka-consumer-groups --reset-offsets --to-earliest
//     --execute` on a configured west group of free-play active-active,
//     driven as resetOffsetsKeepsGroup.test.ts drives it.
//   - "09.2.3 reset", "12.0.4 reset": encodeActions() of the actions the same
//     terminal command (--to-latest) dispatches in those scenarios (at 9000).
var websiteGroupSettingsFixtures = []struct {
	name string
	wire string
	want Entry
}{
	{
		"unit full",
		"AG:g1:west.orders~east.orders:e:region=west~assignor=range~isolationLevel=read_committed~autoCommit=false~pollMs=0~maxPollRecords=3~maxPollIntervalMs=60000~rack=west-1a~principal=User:CN=west~_OU=apps@5",
		Entry{At: 5, Action: AddGroup{GroupID: "g1", Topics: []string{"west.orders", "east.orders"}, AutoOffsetReset: "earliest", Settings: GroupSettings{
			Region: strptr("west"), Assignor: strptr("range"), IsolationLevel: strptr("read_committed"), AutoCommit: boolptr(false),
			PollMs: intptr(0), MaxPollRecords: intptr(3), MaxPollIntervalMs: intptr(60000), Rack: strptr("west-1a"), Principal: strptr("User:CN=west,OU=apps"),
		}}},
	},
	{
		"unit pinned",
		"AG:g:orders::region=west@5",
		Entry{At: 5, Action: AddGroup{GroupID: "g", Topics: []string{"orders"}, Settings: GroupSettings{Region: strptr("west")}}},
	},
	{
		"unit hostile",
		"AG:g:orders::rack=-r~principal=~--a~_b~-_:c=d@e@5",
		Entry{At: 5, Action: AddGroup{GroupID: "g", Topics: []string{"orders"}, Settings: GroupSettings{Rack: strptr("-r"), Principal: strptr("~-a,b~_:c=d@e")}}},
	},
	{
		"unit negative pollMs",
		"AG:g:orders:e:pollMs=-5@5",
		Entry{At: 5, Action: AddGroup{GroupID: "g", Topics: []string{"orders"}, AutoOffsetReset: "earliest", Settings: GroupSettings{PollMs: intptr(-5)}}},
	},
	{
		"free-play + group",
		"AG:g3:west.orders::region=west@1200",
		Entry{At: 1200, Action: AddGroup{GroupID: "g3", Topics: []string{"west.orders"}, Settings: GroupSettings{Region: strptr("west")}}},
	},
	{
		"doc 8.10",
		"AG:g1:west.orders~east.orders:e:region=west~assignor=range~principal=User:CN=west~_OU=apps@9000",
		Entry{At: 9000, Action: AddGroup{GroupID: "g1", Topics: []string{"west.orders", "east.orders"}, AutoOffsetReset: "earliest", Settings: GroupSettings{
			Region: strptr("west"), Assignor: strptr("range"), Principal: strptr("User:CN=west,OU=apps"),
		}}},
	},
	{
		"free-play reset",
		"AG:g1:west.orders~east.orders:e:region=west~assignor=range~isolationLevel=read_committed~autoCommit=true~pollMs=500~maxPollRecords=3~maxPollIntervalMs=60000~rack=west-1a~principal=User:CN=west~_OU=apps@6000",
		Entry{At: 6000, Action: AddGroup{GroupID: "g1", Topics: []string{"west.orders", "east.orders"}, AutoOffsetReset: "earliest", Settings: GroupSettings{
			Region: strptr("west"), Assignor: strptr("range"), IsolationLevel: strptr("read_committed"), AutoCommit: boolptr(true),
			PollMs: intptr(500), MaxPollRecords: intptr(3), MaxPollIntervalMs: intptr(60000), Rack: strptr("west-1a"), Principal: strptr("User:CN=west,OU=apps"),
		}}},
	},
	{
		"09.2.3 reset",
		"AG:cg-west:west.orders~east.orders:l:region=west@9000",
		Entry{At: 9000, Action: AddGroup{GroupID: "cg-west", Topics: []string{"west.orders", "east.orders"}, AutoOffsetReset: "latest", Settings: GroupSettings{Region: strptr("west")}}},
	},
	{
		"12.0.4 reset",
		"AG:cg-east:east.events~west.events:l:region=east@9000",
		Entry{At: 9000, Action: AddGroup{GroupID: "cg-east", Topics: []string{"east.events", "west.events"}, AutoOffsetReset: "latest", Settings: GroupSettings{Region: strptr("east")}}},
	},
}

// TestGroupSettingsWebsiteFixtures decodes every website-written entry to the
// action the website decodes it to, and re-encodes it byte-identically.
func TestGroupSettingsWebsiteFixtures(t *testing.T) {
	for _, f := range websiteGroupSettingsFixtures {
		t.Run(f.name, func(t *testing.T) {
			dec, err := Decode(f.wire)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if want := []Entry{f.want}; !reflect.DeepEqual(dec, want) {
				t.Fatalf("decode mismatch\n  got:  %#v\n  want: %#v", dec, want)
			}
			reEnc, err := Encode(dec)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			if reEnc != f.wire {
				t.Fatalf("re-encode not byte-identical\n  got:  %s\n  want: %s", reEnc, f.wire)
			}
			roundTrip(t, []Entry{f.want})
		})
	}
}

// TestGroupSettingsResetLog decodes the whole actions value PlaygroundStore
// wrote for the free-play active-active reset (see the fixtures above). This
// codec does not model kill_consumer (KC) or remove_group (RG), so those two
// fail; everything else, the configured add_group included, survives.
func TestGroupSettingsResetLog(t *testing.T) {
	const log = "C:group.g1.consumer.pollMs:500@0,P:pw:west.orders:w-1@0,P:pe:east.orders:e-1@0,KC:w1@6000,RG:g1@6000," +
		"AG:g1:west.orders~east.orders:e:region=west~assignor=range~isolationLevel=read_committed~autoCommit=true~pollMs=500~maxPollRecords=3~maxPollIntervalMs=60000~rack=west-1a~principal=User:CN=west~_OU=apps@6000," +
		"AC:g1:w1@6000"
	kept, dropped := DecodeTolerant(log)
	if len(dropped) != 2 || !strings.HasPrefix(dropped[0].Raw, "KC:") || !strings.HasPrefix(dropped[1].Raw, "RG:") {
		t.Fatalf("dropped = %+v, want only the KC and RG entries", dropped)
	}
	var ag *AddGroup
	for _, e := range kept {
		if a, ok := e.Action.(AddGroup); ok {
			ag = &a
		}
	}
	var want AddGroup
	for _, f := range websiteGroupSettingsFixtures {
		if f.name == "free-play reset" {
			want = f.want.Action.(AddGroup)
		}
	}
	if ag == nil || !reflect.DeepEqual(*ag, want) {
		t.Fatalf("add_group = %#v, want %#v", ag, want)
	}
}

// Accept / reject parity with the website decoder. Every verdict and every
// canonical re-encoding below is what decodeActions / encodeActions returned
// for the same input at monedula-website main a5c85587; the first twelve
// rejects are the drop list of actionLog.test.ts.
var groupSettingsRejected = []string{
	"AG:g:orders::@5",                  // empty settings field
	"AG:g:orders:@5",                   // empty reset field with no settings after it
	"AG:g:orders:x:region=w@5",         // unknown reset code
	"AG:g:orders::region=w~region=e@5", // a repeated setting
	"AG:g:orders::bogus=1@5",           // an unknown setting
	"AG:g:orders::region@5",            // no "="
	"AG:g:orders::assignor=greedy@5",   // not an assignor
	"AG:g:orders::pollMs=08@5",         // not a canonical integer
	"AG:g:orders::maxPollRecords=0@5",  // below the floor
	"AG:g:orders::autoCommit=yes@5",
	"AG:g:orders:constructor@5", // a prototype key is not a reset code
	"AG:g:orders:toString:region=w@5",
	"AG:g:orders::=x@5",         // empty name
	"AG:g:orders::region=w~@5",  // a trailing boundary leaves an empty pair
	"AG:g:orders::~region=w@5",  // a leading boundary too
	"AG:g:orders::region=w~x@5", // a bare "~" in a value is a boundary
	"AG:g:orders::region=w~~@5",
	"AG:g:orders::region=a~-~region=b@5", // "~-" is a literal "~", then a repeat
	"AG:g:orders::maxPollIntervalMs=0@5",
	"AG:g:orders::maxPollIntervalMs=-1@5",
	"AG:g:orders::pollMs=-0@5",
	"AG:g:orders::pollMs=+5@5",
	"AG:g:orders::pollMs=1e3@5",
	"AG:g:orders::pollMs=1.5@5",
	"AG:g:orders::pollMs=@5",
	"AG:g:orders::autoCommit=@5",
	"AG:g:orders::autoCommit=TRUE@5",
	"AG:g:orders::isolationLevel=READ_COMMITTED@5",
	"AG:g:orders::assignor=@5",
	"AG:g:orders::Region=w@5", // names are case-sensitive
	"AG:g:orders:e:@5",
	"AG:g:orders:::region=w@5", // the settings field is the tail, so it starts with ":"
	"AG::orders::region=w@5",
	"AG:g:::region=w@5",
	"AG:g:~::region=w@5",
}

var groupSettingsAccepted = []struct{ in, canonical string }{
	{"AG:g:orders::region=@5", "AG:g:orders::region=@5"}, // an empty free-text value is a value
	{"AG:g:orders::region=w:@5", "AG:g:orders::region=w:@5"},
	{"AG:g:orders::principal=a=b@5", "AG:g:orders::principal=a=b@5"},
	{"AG:g:orders::principal=@x@5", "AG:g:orders::principal=@x@5"},
	{"AG:g:orders::principal=User:a:b@5", "AG:g:orders::principal=User:a:b@5"},
	{"AG:g:orders::region=~-~_@5", "AG:g:orders::region=~-~_@5"},
	{"AG:g:orders::rack=r~region=w@5", "AG:g:orders::region=w~rack=r@5"}, // any order in, canonical order out
	{"AG:g:orders:n:maxPollRecords=1@5", "AG:g:orders:n:maxPollRecords=1@5"},
	{"AG:g:a~~b::region=w@5", "AG:g:a~b::region=w@5"},
	{"AG:g:orders::pollMs=-2147483648@5", "AG:g:orders::pollMs=-2147483648@5"},
	{"AG:g:orders::pollMs=9007199254740991@5", "AG:g:orders::pollMs=9007199254740991@5"},
	{
		"AG:g:orders::region=w~assignor=cooperative-sticky~isolationLevel=read_uncommitted~autoCommit=true~pollMs=-1~maxPollRecords=500~maxPollIntervalMs=1~rack=~_~principal=~-@5",
		"AG:g:orders::region=w~assignor=cooperative-sticky~isolationLevel=read_uncommitted~autoCommit=true~pollMs=-1~maxPollRecords=500~maxPollIntervalMs=1~rack=~_~principal=~-@5",
	},
	{"AG:g:west.orders~east.orders::region=west@5", "AG:g:west.orders~east.orders::region=west@5"},
	// Links without the field decode as before.
	{"AG:g:orders@5", "AG:g:orders@5"},
	{"AG:g:orders:l@5", "AG:g:orders:l@5"},
	{"AG:g:orders~payments@5", "AG:g:orders~payments@5"},
}

func TestGroupSettingsRejectedLikeWebsite(t *testing.T) {
	for _, raw := range groupSettingsRejected {
		if dec, err := Decode(raw); err == nil {
			t.Errorf("Decode(%q) = %#v, want an error", raw, dec)
		}
		if kept, dropped := DecodeTolerant(raw); len(kept) != 0 || len(dropped) != 1 {
			t.Errorf("DecodeTolerant(%q) kept %d dropped %d, want 0/1", raw, len(kept), len(dropped))
		}
	}
}

func TestGroupSettingsAcceptedLikeWebsite(t *testing.T) {
	for _, c := range groupSettingsAccepted {
		dec, err := Decode(c.in)
		if err != nil {
			t.Errorf("Decode(%q): %v", c.in, err)
			continue
		}
		got, err := Encode(dec)
		if err != nil || got != c.canonical {
			t.Errorf("Encode(Decode(%q)) = %q, %v; want %q", c.in, got, err, c.canonical)
		}
	}
}

// TestGroupSettingsOldLinksUnchanged: an add_group with no settings decodes to
// a zero GroupSettings and keeps the exact pre-settings wire forms.
func TestGroupSettingsOldLinksUnchanged(t *testing.T) {
	for _, raw := range []string{"AG:analytics:orders@1000", "AG:g:orders~payments@300", "AG:g:orders:e@5", "AG:g1:a~b:n@7"} {
		dec, err := Decode(raw)
		if err != nil {
			t.Fatalf("Decode(%q): %v", raw, err)
		}
		if ag := dec[0].Action.(AddGroup); !ag.Settings.IsZero() {
			t.Errorf("Decode(%q) settings = %#v, want none", raw, ag.Settings)
		}
		if got, _ := Encode(dec); got != raw {
			t.Errorf("Encode(Decode(%q)) = %q", raw, got)
		}
	}
}

// TestGroupSettingsEncoderRefuses: the encoder refuses a value the website's
// decoder would drop the whole add_group for.
func TestGroupSettingsEncoderRefuses(t *testing.T) {
	bad := []GroupSettings{
		{Assignor: strptr("greedy")},
		{Assignor: strptr("")},
		{IsolationLevel: strptr("READ_COMMITTED")},
		{MaxPollRecords: intptr(0)},
		{MaxPollIntervalMs: intptr(-1)},
	}
	for _, s := range bad {
		if got, err := Encode([]Entry{{At: 1, Action: AddGroup{GroupID: "g", Topics: []string{"orders"}, Settings: s}}}); err == nil {
			t.Errorf("Encode accepted %#v as %q", s, got)
		}
	}
	// The domain edges it accepts.
	for _, s := range []GroupSettings{
		{PollMs: intptr(-5)}, {PollMs: intptr(0)}, {MaxPollRecords: intptr(1)}, {MaxPollIntervalMs: intptr(1)},
		{Region: strptr("")}, {Principal: strptr("CN=a,OU=b~c:d@e")}, {AutoCommit: boolptr(false)},
	} {
		roundTrip(t, []Entry{{At: 1, Action: AddGroup{GroupID: "g", Topics: []string{"orders"}, Settings: s}}})
	}
}
