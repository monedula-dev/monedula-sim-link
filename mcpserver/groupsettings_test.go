package mcpserver

import (
	"net/url"
	"reflect"
	"strings"
	"testing"
)

// The add_group settings field (§2.4 of the URL API spec) through the MCP
// tools. The links are the website's own (see the fixture notes in
// actionlog/groupsettings_test.go): validate_actions used to report each of
// these add_group entries as dropped, losing the group.

func sp(s string) *string { return &s }
func ip(n int) *int       { return &n }
func bp(b bool) *bool     { return &b }

// TestValidateActionsResetKeepsSettings: the doc's §8.10 reset of a configured,
// region-pinned group. The add_group survives with its settings and re-encodes
// byte-identically; only remove_group, which this codec does not model, drops.
func TestValidateActionsResetKeepsSettings(t *testing.T) {
	const ag = "AG:g1:west.orders~east.orders:e:region=west~assignor=range~principal=User:CN=west~_OU=apps@9000"
	s := newSession(t)
	var out ValidateActionsOutput
	structured(t, callTool(t, s, "validate_actions", map[string]any{"actions": "RG:g1@9000," + ag + ",AC:g1:c1@9000"}), &out)

	if out.DroppedCount != 1 || !strings.HasPrefix(out.Dropped[0].Entry, "RG:") {
		t.Fatalf("dropped = %+v, want only the RG entry", out.Dropped)
	}
	if out.KeptCount != 2 || out.Entries[0].Kind != "add_group" {
		t.Fatalf("entries = %+v, want the add_group then the add_consumer", out.Entries)
	}
	want := &GroupSettings{Region: sp("west"), Assignor: sp("range"), Principal: sp("User:CN=west,OU=apps")}
	if got := out.Entries[0].Settings; !reflect.DeepEqual(got, want) {
		t.Errorf("settings = %+v, want %+v", got, want)
	}
	if got := out.Entries[0]; got.AutoOffsetReset != "earliest" || !reflect.DeepEqual(got.Topics, []string{"west.orders", "east.orders"}) {
		t.Errorf("add_group = %+v", got)
	}
	if want := ag + ",AC:g1:c1@9000"; out.CanonicalEncoding != want {
		t.Errorf("canonical:\n  got  %q\n  want %q", out.CanonicalEncoding, want)
	}
}

// TestValidateActionsActiveActiveGroup: the canvas "+ group" on free-play
// active-active pins the group to its topic's home region, as a full URL.
func TestValidateActionsActiveActiveGroup(t *testing.T) {
	const ag = "AG:g3:west.orders::region=west@1200"
	full := "https://monedula.dev/kafka-simulator/playground?scenario=free&cluster=active-active&actions=" + url.QueryEscape(ag)
	s := newSession(t)
	var out ValidateActionsOutput
	structured(t, callTool(t, s, "validate_actions", map[string]any{"actions": full}), &out)
	if out.DroppedCount != 0 || out.KeptCount != 1 {
		t.Fatalf("kept %d dropped %d, want 1/0: %+v", out.KeptCount, out.DroppedCount, out.Dropped)
	}
	if got := out.Entries[0].Settings; got == nil || got.Region == nil || *got.Region != "west" {
		t.Errorf("settings = %+v, want region west", got)
	}
	if out.CanonicalEncoding != ag {
		t.Errorf("canonical = %q, want %q", out.CanonicalEncoding, ag)
	}
}

// TestValidateActionsSettingsEcho: every setting reaches the JSON echo,
// including the false and zero values an omitempty field would lose.
func TestValidateActionsSettingsEcho(t *testing.T) {
	const ag = "AG:g1:west.orders~east.orders:e:region=west~assignor=range~isolationLevel=read_committed~autoCommit=false~pollMs=0~maxPollRecords=3~maxPollIntervalMs=60000~rack=west-1a~principal=User:CN=west~_OU=apps@5"
	s := newSession(t)
	var out ValidateActionsOutput
	structured(t, callTool(t, s, "validate_actions", map[string]any{"actions": ag}), &out)
	want := &GroupSettings{
		Region: sp("west"), Assignor: sp("range"), IsolationLevel: sp("read_committed"), AutoCommit: bp(false),
		PollMs: ip(0), MaxPollRecords: ip(3), MaxPollIntervalMs: ip(60000), Rack: sp("west-1a"), Principal: sp("User:CN=west,OU=apps"),
	}
	if out.KeptCount != 1 || !reflect.DeepEqual(out.Entries[0].Settings, want) {
		t.Fatalf("entries = %+v, want settings %+v", out.Entries, want)
	}
	// A group without settings echoes no settings object.
	var plain ValidateActionsOutput
	structured(t, callTool(t, s, "validate_actions", map[string]any{"actions": "AG:g:orders:e@5"}), &plain)
	if plain.Entries[0].Settings != nil {
		t.Errorf("settings = %+v, want none", plain.Entries[0].Settings)
	}
}

// TestValidateActionsWholeResetLink: the actions value PlaygroundStore wrote
// for a terminal reset on free-play active-active. Its produces target the
// dotted region topics, which this codec's generator refuses to write, so the
// preview reports that in a note instead of failing; the configured add_group
// still survives with every setting.
func TestValidateActionsWholeResetLink(t *testing.T) {
	const log = "C:group.g1.consumer.pollMs:500@0,P:pw:west.orders:w-1@0,P:pe:east.orders:e-1@0,KC:w1@6000,RG:g1@6000," +
		"AG:g1:west.orders~east.orders:e:region=west~assignor=range~isolationLevel=read_committed~autoCommit=true~pollMs=500~maxPollRecords=3~maxPollIntervalMs=60000~rack=west-1a~principal=User:CN=west~_OU=apps@6000," +
		"AC:g1:w1@6000"
	s := newSession(t)
	var out ValidateActionsOutput
	structured(t, callTool(t, s, "validate_actions", map[string]any{"actions": url.QueryEscape(log)}), &out)
	for _, d := range out.Dropped {
		if strings.HasPrefix(d.Entry, "AG:") {
			t.Fatalf("add_group dropped: %+v", d)
		}
	}
	if out.KeptCount != 5 || out.DroppedCount != 2 {
		t.Fatalf("kept %d dropped %d, want 5/2 (KC and RG): %+v", out.KeptCount, out.DroppedCount, out.Dropped)
	}
	var ag *ActionInput
	for i := range out.Entries {
		if out.Entries[i].Kind == "add_group" {
			ag = &out.Entries[i]
		}
	}
	if ag == nil || ag.Settings == nil || ag.Settings.Region == nil || *ag.Settings.Region != "west" ||
		ag.Settings.PollMs == nil || *ag.Settings.PollMs != 500 || ag.Settings.AutoCommit == nil || !*ag.Settings.AutoCommit {
		t.Fatalf("add_group = %+v", ag)
	}
	if out.CanonicalEncoding != "" {
		t.Errorf("canonical = %q, want none (dotted produce topics)", out.CanonicalEncoding)
	}
	if !strings.Contains(strings.Join(out.Notes, "\n"), "no canonicalEncoding") {
		t.Errorf("notes = %q, want the re-encode note", out.Notes)
	}
}

// TestBuildURLRejectsGroupSettings: settings are echo only; the generator does
// not write the field, and says so rather than dropping it silently.
func TestBuildURLRejectsGroupSettings(t *testing.T) {
	s := newSession(t)
	res := callTool(t, s, "build_playground_url", map[string]any{
		"actions": []map[string]any{
			{"kind": "add_group", "at": 100, "groupId": "g", "topics": []string{"orders"}, "settings": map[string]any{"region": "west"}},
		},
	})
	wantToolError(t, res, "actions[0]", "settings")
}
