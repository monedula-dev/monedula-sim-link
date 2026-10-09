package mcpserver

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/monedula-dev/monedula-sim-link/simurl"
)

// newSession connects the real server to an in-process client over the
// SDK's in-memory transport pair — the full MCP wire path, no network.
func newSession(t *testing.T) *mcp.ClientSession {
	t.Helper()
	ctx := context.Background()
	srv := New(Options{})
	st, ct := mcp.NewInMemoryTransports()
	if _, err := srv.Connect(ctx, st, nil); err != nil {
		t.Fatalf("server connect: %v", err)
	}
	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "0.0.0"}, nil)
	session, err := client.Connect(ctx, ct, nil)
	if err != nil {
		t.Fatalf("client connect: %v", err)
	}
	t.Cleanup(func() { session.Close() })
	return session
}

func callTool(t *testing.T, s *mcp.ClientSession, name string, args map[string]any) *mcp.CallToolResult {
	t.Helper()
	res, err := s.CallTool(context.Background(), &mcp.CallToolParams{Name: name, Arguments: args})
	if err != nil {
		t.Fatalf("CallTool(%s): %v", name, err)
	}
	return res
}

// structured unmarshals the tool's structured output into out.
func structured(t *testing.T, res *mcp.CallToolResult, out any) {
	t.Helper()
	if res.IsError {
		t.Fatalf("tool returned error: %s", errText(res))
	}
	raw, err := json.Marshal(res.StructuredContent)
	if err != nil {
		t.Fatalf("marshal structured content: %v", err)
	}
	if err := json.Unmarshal(raw, out); err != nil {
		t.Fatalf("unmarshal structured content: %v", err)
	}
}

func errText(res *mcp.CallToolResult) string {
	var parts []string
	for _, c := range res.Content {
		if tc, ok := c.(*mcp.TextContent); ok {
			parts = append(parts, tc.Text)
		}
	}
	return strings.Join(parts, "\n")
}

// wantToolError asserts an IsError result whose message contains every needle
// — the "which field / which path" actionability contract.
func wantToolError(t *testing.T, res *mcp.CallToolResult, needles ...string) {
	t.Helper()
	if !res.IsError {
		t.Fatalf("expected tool error, got success: %+v", res.StructuredContent)
	}
	text := errText(res)
	for _, n := range needles {
		if !strings.Contains(text, n) {
			t.Errorf("error %q does not name %q", text, n)
		}
	}
}

func actionsParamOf(t *testing.T, rawURL string) string {
	t.Helper()
	u, err := url.Parse(rawURL)
	if err != nil {
		t.Fatalf("generated URL does not parse: %v", err)
	}
	return u.Query().Get("actions")
}

func TestListTools(t *testing.T) {
	s := newSession(t)
	res, err := s.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	got := map[string]bool{}
	for _, tool := range res.Tools {
		got[tool.Name] = true
		if tool.Description == "" {
			t.Errorf("tool %s has no description", tool.Name)
		}
		if tool.InputSchema == nil {
			t.Errorf("tool %s has no input schema", tool.Name)
		}
	}
	for _, want := range []string{"build_playground_url", "open_scenario", "validate_actions"} {
		if !got[want] {
			t.Errorf("tool %s not registered (got %v)", want, got)
		}
	}
}

// TestBuildURLSection82 rebuilds the doc's §8.2 worked example from structured
// action objects and asserts the URL carries the exact documented action log.
func TestBuildURLSection82(t *testing.T) {
	s := newSession(t)
	res := callTool(t, s, "build_playground_url", map[string]any{
		"actions": []map[string]any{
			{"kind": "add_group", "at": 1000, "groupId": "analytics", "topics": []string{"orders"}},
			{"kind": "produce", "at": 2000, "producerId": "p1", "topic": "orders", "key": "O1"},
			{"kind": "produce", "at": 3000, "producerId": "p1", "topic": "orders", "key": "O2"},
			{"kind": "produce", "at": 4000, "producerId": "p1", "topic": "orders", "key": "O3"},
			{"kind": "kill_broker", "at": 8500, "brokerId": "broker-2"},
			{"kind": "restart_broker", "at": 22000, "brokerId": "broker-2"},
		},
	})
	var out BuildPlaygroundURLOutput
	structured(t, res, &out)

	const wantPlain = "AG:analytics:orders@1000,P:p1:orders:O1@2000,P:p1:orders:O2@3000,P:p1:orders:O3@4000,K:broker-2@8500,R:broker-2@22000"
	if got := actionsParamOf(t, out.URL); got != wantPlain {
		t.Errorf("URL actions param:\n  got  %q\n  want %q", got, wantPlain)
	}
	if !strings.HasPrefix(out.URL, simurl.DefaultBaseURL+"?scenario=free") {
		t.Errorf("URL %q does not open free-play on the default base", out.URL)
	}
	if out.Compressed || out.ActionCount != 6 || out.PlainLength != len(wantPlain) {
		t.Errorf("meta wrong: %+v", out)
	}
	if len(out.DecodedActions) != 6 {
		t.Fatalf("echo has %d actions, want 6", len(out.DecodedActions))
	}
	echo := out.DecodedActions[0]
	if echo.Kind != "add_group" || echo.GroupID != "analytics" || echo.At != 1000 {
		t.Errorf("echo[0] wrong: %+v", echo)
	}
	if k := out.DecodedActions[1]; k.Kind != "produce" || k.Key == nil || *k.Key != "O1" {
		t.Errorf("echo[1] wrong: %+v", k)
	}
}

func TestBuildURLValueTombstoneAndConfig(t *testing.T) {
	s := newSession(t)
	res := callTool(t, s, "build_playground_url", map[string]any{
		"actions": []map[string]any{
			{"kind": "config_change", "at": 1000, "path": "producers.p1.config.acks", "value": "all"},
			{"kind": "produce", "at": 2000, "producerId": "p1", "topic": "orders", "key": "O2", "value": "qty:5,paid"},
			{"kind": "produce", "at": 3000, "producerId": "p1", "topic": "orders", "key": "O2", "tombstone": true},
			{"kind": "config_change", "at": 4000, "path": "topics.orders.retentionMs", "clear": true},
		},
	})
	var out BuildPlaygroundURLOutput
	structured(t, res, &out)
	const want = "C:producers.p1.config.acks:all@1000,P:p1:orders:O2:v=qty:5~cpaid@2000,P:p1:orders:O2:t@3000,C:topics.orders.retentionMs:@4000"
	if got := actionsParamOf(t, out.URL); got != want {
		t.Errorf("actions param:\n  got  %q\n  want %q", got, want)
	}
	if v := out.DecodedActions[1].Value; v == nil || *v != "qty:5,paid" {
		t.Errorf("escaped value did not round-trip: %+v", out.DecodedActions[1])
	}
	if !out.DecodedActions[2].Tombstone {
		t.Errorf("tombstone did not round-trip: %+v", out.DecodedActions[2])
	}
	if !out.DecodedActions[3].Clear {
		t.Errorf("cleared config did not round-trip: %+v", out.DecodedActions[3])
	}
}

// TestBuildURLEscapedKey: a produce key carrying separators (once rejected) is
// now reversibly escaped and round-trips through the echo (§8.7 parity).
func TestBuildURLEscapedKey(t *testing.T) {
	s := newSession(t)
	res := callTool(t, s, "build_playground_url", map[string]any{
		"actions": []map[string]any{
			{"kind": "produce", "at": 100, "producerId": "p1", "topic": "orders", "key": "user:42,v2"},
		},
	})
	var out BuildPlaygroundURLOutput
	structured(t, res, &out)
	const want = "P:p1:orders:user~s42~cv2@100"
	if got := actionsParamOf(t, out.URL); got != want {
		t.Errorf("actions param:\n  got  %q\n  want %q", got, want)
	}
	if k := out.DecodedActions[0].Key; k == nil || *k != "user:42,v2" {
		t.Errorf("escaped key did not round-trip: %+v", out.DecodedActions[0])
	}
}

func TestBuildURLCompressesLongLogs(t *testing.T) {
	s := newSession(t)
	actions := make([]map[string]any, 150)
	for i := range actions {
		actions[i] = map[string]any{
			"kind": "produce", "at": (i + 1) * 100,
			"producerId": "p1", "topic": "orders", "key": fmt.Sprintf("order-%03d", i),
		}
	}
	res := callTool(t, s, "build_playground_url", map[string]any{"actions": actions})
	var out BuildPlaygroundURLOutput
	structured(t, res, &out)
	if !out.Compressed {
		t.Fatalf("150-action log should compress (plain length %d)", out.PlainLength)
	}
	if !strings.HasPrefix(actionsParamOf(t, out.URL), "~1") {
		t.Errorf("actions param lacks the ~1 prefix")
	}
	if len(out.DecodedActions) != 150 {
		t.Errorf("echo has %d actions, want 150 — compression lost data", len(out.DecodedActions))
	}
}

func TestBuildURLShape(t *testing.T) {
	s := newSession(t)
	// The §14.3 worked example: 6 brokers, orders 4/3, plus payments and audit.
	minISR2, minISR1 := 2, 1
	res := callTool(t, s, "build_playground_url", map[string]any{
		"shape": map[string]any{
			"brokers": 6,
			"topics": []map[string]any{
				{"name": "orders", "partitions": 4, "replicationFactor": 3},
				{"name": "payments", "partitions": 6, "replicationFactor": 3, "minInSyncReplicas": minISR2, "cleanupPolicy": "delete"},
				{"name": "audit", "partitions": 2, "replicationFactor": 2, "minInSyncReplicas": minISR1, "cleanupPolicy": "compact"},
			},
		},
	})
	var out BuildPlaygroundURLOutput
	structured(t, res, &out)
	const wantSize = "6x4x3xpayments~6~3~2~d~~~~!audit~2~2~1~c~~~~"
	if out.Size != wantSize {
		t.Errorf("size slug:\n  got  %q\n  want %q", out.Size, wantSize)
	}
	u, _ := url.Parse(out.URL)
	if got := u.Query().Get("size"); got != wantSize {
		t.Errorf("URL size param: got %q want %q", got, wantSize)
	}
	if u.Query().Get("cluster") != "" {
		t.Errorf("single-dc must omit the cluster param, got %q", u.Query().Get("cluster"))
	}
}

func TestBuildURLShapeErrors(t *testing.T) {
	s := newSession(t)
	cases := []struct {
		name    string
		args    map[string]any
		needles []string
	}{
		{
			"broker cap",
			map[string]any{"shape": map[string]any{"brokers": 12}},
			[]string{"12", "cap", "10"},
		},
		{
			"rf exceeds brokers",
			map[string]any{"shape": map[string]any{"brokers": 6, "topics": []map[string]any{{"name": "orders", "partitions": 3, "replicationFactor": 10}}}},
			[]string{"replicationFactor", "10", "6"},
		},
		{
			"unknown topology",
			map[string]any{"cluster": "mega-dc"},
			[]string{"mega-dc", "single-dc"},
		},
		{
			"mrc shape unsupported",
			map[string]any{"cluster": "stretched-2-5", "shape": map[string]any{"brokersPerZone": []int{3, 3}}},
			[]string{"stretched-2-5", "MRC"},
		},
		{
			"zone count mismatch",
			map[string]any{"cluster": "active-passive", "shape": map[string]any{"brokers": 4}},
			[]string{"brokersPerZone", "2"},
		},
		{
			"bad topic name",
			map[string]any{"shape": map[string]any{"topics": []map[string]any{{"name": "or ders", "partitions": 1, "replicationFactor": 1}}}},
			[]string{"topics[0].name", "or ders"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wantToolError(t, callTool(t, s, "build_playground_url", tc.args), tc.needles...)
		})
	}
}

func TestBuildURLActionErrors(t *testing.T) {
	s := newSession(t)
	cases := []struct {
		name    string
		actions []map[string]any
		needles []string
	}{
		{
			"missing topic",
			[]map[string]any{
				{"kind": "produce", "at": 1000, "producerId": "p1", "topic": "orders"},
				{"kind": "produce", "at": 2000, "producerId": "p1"},
			},
			[]string{"actions[1]", "produce", "topic"},
		},
		{
			"unknown kind",
			[]map[string]any{{"kind": "explode_broker", "at": 1000}},
			[]string{"actions[0]", "explode_broker"},
		},
		{
			"non-whitelisted config path",
			[]map[string]any{{"kind": "config_change", "at": 1000, "path": "broker.rack", "value": "a"}},
			[]string{"actions[0]", "broker.rack", "whitelist"},
		},
		{
			"bad config value",
			[]map[string]any{{"kind": "config_change", "at": 1000, "path": "producers.p1.config.acks", "value": "2"}},
			[]string{"actions[0]", "producers.p1.config.acks", `"2"`, "all"},
		},
		{
			"clear on non-clearable",
			[]map[string]any{{"kind": "config_change", "at": 1000, "path": "producers.p1.config.acks", "clear": true}},
			[]string{"actions[0]", "producers.p1.config.acks", "cleared"},
		},
		{
			"value and tombstone",
			[]map[string]any{{"kind": "produce", "at": 1000, "producerId": "p1", "topic": "orders", "value": "v", "tombstone": true}},
			[]string{"actions[0]", "mutually exclusive"},
		},
		{
			"negative at",
			[]map[string]any{{"kind": "kill_broker", "at": -5, "brokerId": "broker-1"}},
			[]string{"actions[0]", "at"},
		},
		{
			"reassign missing partition",
			[]map[string]any{{"kind": "reassign_partition", "at": 1000, "topic": "orders", "replicas": []string{"broker-1"}}},
			[]string{"actions[0]", "partition"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := callTool(t, s, "build_playground_url", map[string]any{"actions": tc.actions})
			wantToolError(t, res, tc.needles...)
		})
	}
}

func TestBuildURLMaxActionsCap(t *testing.T) {
	s := newSession(t)
	actions := make([]map[string]any, 2001)
	for i := range actions {
		actions[i] = map[string]any{"kind": "produce", "at": i, "producerId": "p1", "topic": "orders"}
	}
	res := callTool(t, s, "build_playground_url", map[string]any{"actions": actions})
	wantToolError(t, res, "2001", "MAX_URL_ACTIONS", "2000")
}

func TestOpenScenario(t *testing.T) {
	s := newSession(t)
	res := callTool(t, s, "open_scenario", map[string]any{"scenarioId": "00.1.3"})
	var out OpenScenarioOutput
	structured(t, res, &out)
	if want := simurl.DefaultBaseURL + "?scenario=00.1.3"; out.URL != want {
		t.Errorf("URL: got %q want %q", out.URL, want)
	}

	for _, bad := range []string{"", "0.1", "free", "00.1.3.4", "abc"} {
		res := callTool(t, s, "open_scenario", map[string]any{"scenarioId": bad})
		if !res.IsError {
			t.Errorf("scenarioId %q: expected error", bad)
		} else if !strings.Contains(errText(res), "scenarioId") {
			t.Errorf("scenarioId %q: error %q does not name the field", bad, errText(res))
		}
	}
}

func TestValidateActions(t *testing.T) {
	s := newSession(t)
	res := callTool(t, s, "validate_actions", map[string]any{
		"actions": "K:broker-2@8500,NOPE,AG:analytics:orders@1000,C:bad.path:x@50",
	})
	var out ValidateActionsOutput
	structured(t, res, &out)
	if out.KeptCount != 2 || len(out.Entries) != 2 {
		t.Fatalf("kept %d, want 2: %+v", out.KeptCount, out)
	}
	if out.Entries[0].Kind != "add_group" || out.Entries[1].Kind != "kill_broker" {
		t.Errorf("entries not sorted by at: %+v", out.Entries)
	}
	if out.DroppedCount != 2 || len(out.Dropped) != 2 {
		t.Fatalf("dropped %d, want 2: %+v", out.DroppedCount, out.Dropped)
	}
	if out.Dropped[0].Entry != "NOPE" || out.Dropped[0].Reason == "" {
		t.Errorf("dropped[0] wrong: %+v", out.Dropped[0])
	}
	if !strings.Contains(out.Dropped[1].Reason, "bad.path") {
		t.Errorf("dropped[1] reason %q does not name the config path", out.Dropped[1].Reason)
	}
	if out.Compressed || out.CompressionThreshold != 1500 || out.MaxURLActions != 2000 {
		t.Errorf("meta wrong: %+v", out)
	}
	if want := "AG:analytics:orders@1000,K:broker-2@8500"; out.CanonicalEncoding != want {
		t.Errorf("canonical: got %q want %q", out.CanonicalEncoding, want)
	}
}

func TestValidateActionsFromFullURL(t *testing.T) {
	s := newSession(t)
	// §8.2's full, percent-encoded URL.
	full := "https://monedula.dev/kafka-simulator/playground?scenario=free&actions=AG%3Aanalytics%3Aorders%401000%2CP%3Ap1%3Aorders%3AO1%402000%2CP%3Ap1%3Aorders%3AO2%403000%2CP%3Ap1%3Aorders%3AO3%404000%2CK%3Abroker-2%408500%2CR%3Abroker-2%4022000"
	res := callTool(t, s, "validate_actions", map[string]any{"actions": full})
	var out ValidateActionsOutput
	structured(t, res, &out)
	if out.KeptCount != 6 || out.DroppedCount != 0 {
		t.Fatalf("kept %d dropped %d, want 6/0: %+v", out.KeptCount, out.DroppedCount, out.Dropped)
	}

	// A compressed value round-trips through the same tool.
	build := callTool(t, s, "build_playground_url", map[string]any{"actions": manyProduces(150)})
	var built BuildPlaygroundURLOutput
	structured(t, build, &built)
	res = callTool(t, s, "validate_actions", map[string]any{"actions": built.URL})
	structured(t, res, &out)
	if !out.Compressed || out.KeptCount != 150 || out.DroppedCount != 0 {
		t.Errorf("compressed URL validate: %+v", out)
	}
}

func manyProduces(n int) []map[string]any {
	actions := make([]map[string]any, n)
	for i := range actions {
		actions[i] = map[string]any{
			"kind": "produce", "at": (i + 1) * 100,
			"producerId": "p1", "topic": "orders", "key": fmt.Sprintf("order-%03d", i),
		}
	}
	return actions
}

func TestValidateActionsErrors(t *testing.T) {
	s := newSession(t)
	wantToolError(t, callTool(t, s, "validate_actions", map[string]any{"actions": ""}), "actions")
	wantToolError(t, callTool(t, s, "validate_actions", map[string]any{"actions": "~1$$$not-base64$$$"}), "~1")
	wantToolError(t, callTool(t, s, "validate_actions", map[string]any{"actions": "https://monedula.dev/kafka-simulator/playground?scenario=free"}), "no actions")
}

// TestBaseURLOverride proves the --base-url flag path reaches every tool.
func TestBaseURLOverride(t *testing.T) {
	ctx := context.Background()
	srv := New(Options{BaseURL: "http://localhost:4321/kafka-simulator/playground"})
	st, ct := mcp.NewInMemoryTransports()
	if _, err := srv.Connect(ctx, st, nil); err != nil {
		t.Fatalf("server connect: %v", err)
	}
	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "0.0.0"}, nil)
	session, err := client.Connect(ctx, ct, nil)
	if err != nil {
		t.Fatalf("client connect: %v", err)
	}
	defer session.Close()

	res, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "open_scenario", Arguments: map[string]any{"scenarioId": "00.1.3"}})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	var out OpenScenarioOutput
	structured(t, res, &out)
	if want := "http://localhost:4321/kafka-simulator/playground?scenario=00.1.3"; out.URL != want {
		t.Errorf("URL: got %q want %q", out.URL, want)
	}
}
