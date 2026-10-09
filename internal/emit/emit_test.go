package emit

import (
	"strings"
	"testing"

	"github.com/monedula-dev/monedula-sim-link/actionlog"
	"github.com/monedula-dev/monedula-sim-link/mapping"
)

// TestBuildEmitsCompressedForm proves the reconciled behavior: a log past the
// compression threshold now ships in the deflate "~1" form WITHOUT Force — the
// playground decodes both the raw-DEFLATE layout this tool emits and its own
// lz-string layout (docs/playground-url-api.md §4) — with no warning, and
// Result.Compressed reports the chosen form.
func TestBuildEmitsCompressedForm(t *testing.T) {
	res := &mapping.Result{Size: "3x3x3"}
	key := "k"
	for i := 0; i < 400; i++ {
		res.Entries = append(res.Entries, actionlog.Entry{
			At:     500 + i*250,
			Action: actionlog.ProduceRecord{ProducerID: "kp1", Topic: "orders", Key: &key},
		})
	}
	out, err := BuildFromMapping(res, BuildOptions{})
	if err != nil || out.URL == "" {
		t.Fatalf("compressed form must now emit without Force: %v", err)
	}
	if !out.Compressed {
		t.Fatalf("emit should report the compressed form: %+v", out)
	}
	for _, w := range out.Warnings {
		if strings.Contains(w, "compressed form") {
			t.Fatalf("no compressed-form warning expected: %v", out.Warnings)
		}
	}
}

// TestBuildRoundTrips confirms a small mapped log builds a URL whose actions
// decode back to the identical entries.
func TestBuildRoundTrips(t *testing.T) {
	key := "O1"
	res := &mapping.Result{
		Size:        "3x3x3",
		BrokerNames: map[string]string{"1": "broker-1"},
		TopicNames:  map[string]string{"orders": "orders"},
		Entries: []actionlog.Entry{
			{At: 500, Action: actionlog.AddProducer{ProducerID: "kp1", Topic: "orders"}},
			{At: 750, Action: actionlog.ProduceRecord{ProducerID: "kp1", Topic: "orders", Key: &key}},
		},
	}
	out, err := BuildFromMapping(res, BuildOptions{})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if out.ActionCount != 2 || len(out.DecodedActions) != 2 || out.Compressed {
		t.Fatalf("meta wrong: %+v", out)
	}
	plain, _ := actionlog.Encode(res.Entries)
	echo, _ := actionlog.Encode(out.DecodedActions)
	if echo != plain {
		t.Fatalf("decoded echo %q != %q", echo, plain)
	}
}
