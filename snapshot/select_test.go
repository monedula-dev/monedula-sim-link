package snapshot

import (
	"regexp"
	"strings"
	"testing"
)

var selectFixture = []TopicInfo{
	{Name: "orders"},
	{Name: "payments"},
	{Name: "audit-log"},
	{Name: "__consumer_offsets", Internal: true},
	{Name: "_schemas", Internal: true},
}

func names(t *testing.T, sel Selection) []string {
	t.Helper()
	got, err := sel.Apply(selectFixture)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	return got
}

func TestSelectionDefaultExcludesInternal(t *testing.T) {
	got := names(t, Selection{})
	want := "audit-log,orders,payments"
	if strings.Join(got, ",") != want {
		t.Fatalf("got %v want %s", got, want)
	}
}

func TestSelectionExactIncludesInternal(t *testing.T) {
	got := names(t, Selection{Exact: []string{"_schemas", "orders"}})
	if strings.Join(got, ",") != "_schemas,orders" {
		t.Fatalf("got %v", got)
	}
}

func TestSelectionRegexSkipsInternal(t *testing.T) {
	got := names(t, Selection{Regex: regexp.MustCompile("s$")})
	// orders, payments end in s; _schemas does too but is internal.
	if strings.Join(got, ",") != "orders,payments" {
		t.Fatalf("got %v", got)
	}
}

func TestSelectionExactPlusRegex(t *testing.T) {
	got := names(t, Selection{Exact: []string{"audit-log"}, Regex: regexp.MustCompile("^orders$")})
	if strings.Join(got, ",") != "audit-log,orders" {
		t.Fatalf("got %v", got)
	}
}

func TestSelectionMissingExactFails(t *testing.T) {
	_, err := Selection{Exact: []string{"orders", "nope", "also-nope"}}.Apply(selectFixture)
	if err == nil || !strings.Contains(err.Error(), "also-nope, nope") {
		t.Fatalf("want missing-topics error, got %v", err)
	}
}

func TestSelectionNoMatchFails(t *testing.T) {
	_, err := Selection{Regex: regexp.MustCompile("^zzz")}.Apply(selectFixture)
	if err == nil || !strings.Contains(err.Error(), "matched nothing") {
		t.Fatalf("want no-match error, got %v", err)
	}
	// The message must describe the whole internal-topic rule (broker-flagged
	// or leading underscore), not just the underscore convention.
	for _, want := range []string{"flags as internal", "starts with an underscore"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("no-match error should mention %q, got %v", want, err)
		}
	}
	_, err = Selection{}.Apply([]TopicInfo{{Name: "_only", Internal: true}})
	if err == nil {
		t.Fatal("want error when only internal topics exist")
	}
}
