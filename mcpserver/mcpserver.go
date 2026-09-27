// Package mcpserver exposes the monedula-sim-link codec as a Model Context Protocol
// server over stdio, so LLM clients (Claude Code, Claude Desktop, any MCP
// client) can generate monedula.dev kafka-simulator playground URLs — curated
// scenario links and bespoke free-play sessions built from a declarative
// description.
//
// Every tool goes through the fail-fast codec (actionlog + simurl): action
// logs are validated field by field, config_change paths are checked against
// the §6 whitelist, the MAX_URL_ACTIONS cap and the §12 size clamps are
// enforced, and no URL is returned unless its actions value self-decodes back
// to the same log.
package mcpserver

import (
	"context"
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/monedula-dev/monedula-sim-link/actionlog"
	"github.com/monedula-dev/monedula-sim-link/simurl"
)

// ServerVersion is reported to MCP clients; the CLI sets it to its build
// version before serving.
var ServerVersion = "dev"

// Options configures New.
type Options struct {
	// BaseURL overrides the playground endpoint in every generated link
	// (default simurl.DefaultBaseURL).
	BaseURL string
}

// New assembles the MCP server with all tools registered. Run it over stdio
// with srv.Run(ctx, &mcp.StdioTransport{}), or connect it to an in-memory
// transport in tests.
func New(opts Options) *mcp.Server {
	base := opts.BaseURL
	if base == "" {
		base = simurl.DefaultBaseURL
	}

	srv := mcp.NewServer(&mcp.Implementation{
		Name:    "monedula-sim-link",
		Title:   "monedula-sim-link — Kafka simulator playground link builder",
		Version: ServerVersion,
	}, nil)

	mcp.AddTool(srv, &mcp.Tool{
		Name: "build_playground_url",
		Description: `Build a monedula.dev kafka-simulator free-play playground URL from a declarative description: an optional cluster topology + shape (brokers, topics) and an ordered list of timed actions (produce records, kill/restart brokers, add producers/consumer groups/share groups, whitelisted config changes, partition reassignment, replication-factor changes, tiered-storage offload, replica throttling).

The link opens a live, animated sandbox that replays the actions on the described cluster. Every input is validated against the playground's URL grammar before a URL is returned (the playground itself silently DROPS anything malformed, so validation errors here name the offending field instead). The response echoes the action list decoded back out of the generated URL — confirm it matches your intent.

Defaults when omitted: single-dc topology, 3 brokers (broker-1..broker-3), topic "orders" with 3 partitions / RF 3, one producer p1 and one consumer c1. Reference those ids in actions, or create your own with add_broker/add_producer/add_group first. Space actions ~1000ms apart.`,
	}, buildPlaygroundURLHandler(base))

	mcp.AddTool(srv, &mcp.Tool{
		Name:        "open_scenario",
		Description: `Build the URL that opens one of the playground's curated, scripted walkthrough scenarios, paused on its first step. Input is the scenario id in dotted form, e.g. "00.1.3" (track 00, module 1, scenario 3); tracks range 00 (Kafka anatomy) through 13 (ops from the terminal). The id is validated for format only — an id the deployed release does not include falls back gracefully in the playground.`,
	}, openScenarioHandler(base))

	mcp.AddTool(srv, &mcp.Tool{
		Name:        "validate_actions",
		Description: `Preview what the kafka-simulator playground would ACTUALLY load from a raw actions string — the playground's decoder silently drops anything it cannot parse, so use this to check a hand-written or third-party actions value (or a full playground URL; the actions parameter is extracted automatically). Returns the surviving decoded actions, each dropped entry with the reason, whether the value used the plain or the "~1" compressed form, and the length/count against the codec caps (1500-char compression threshold, 2000-action MAX_URL_ACTIONS).`,
	}, validateActionsHandler())

	mcp.AddTool(srv, &mcp.Tool{
		Name: "cluster_to_url",
		Description: `Connect READ-ONLY to a real Kafka cluster and build the playground URL that visualizes it — brokers, topics, partition placement, offline brokers, ISR gaps and a sample of data volume. The live-cluster counterpart to build_playground_url's declarative shape; it mirrors the "monedula-sim-link connect" CLI.

The connection is strictly read-only by construction: the ONLY Kafka requests issued are ApiVersions (the client handshake), Metadata (sent with AllowAutoTopicCreation=false, so a topic lookup can never create it) and ListOffsets. No records are produced, no topic or config is created or altered, no consumer group is joined, no offset is committed.

Inputs mirror the CLI: bootstrap brokers (required), an exact topic list and/or a regex, TLS (caCert/clientCert/clientKey are FILE PATHS resolved on the MCP SERVER's host), SASL (prefer the MONEDULA_SIM_LINK_PASSWORD env var over passing the password), a timeout, and the record-volume cap. If the selection exceeds the simulator's caps the call fails with the violations listed unless clamp is set; a selection matching nothing is an actionable error, never a silent empty link.

Returns the URL, the size param, the real→simulator broker/topic name tables, any warnings/approximations, and the actions decoded back OUT of the URL so you can confirm the mapping is lossless.`,
	}, clusterToURLHandler(base))

	return srv
}

// ---------------------------------------------------------------------------
// build_playground_url

// BuildPlaygroundURLInput describes the free-play session to encode.
type BuildPlaygroundURLInput struct {
	Cluster string        `json:"cluster,omitempty" jsonschema:"free-play topology: single-dc (default), active-passive, active-active, stretched-2-5, stretched-3, or diskless-3az (v2.0+, KIP-1163 draft)"`
	Shape   *ClusterShape `json:"shape,omitempty" jsonschema:"starting cluster shape (brokers, topics). Omit for the topology default. Not supported for stretched-2-5"`
	Actions []ActionInput `json:"actions,omitempty" jsonschema:"ordered list of timed actions to replay on the starting cluster (may be empty to just open the cluster)"`
}

// BuildPlaygroundURLOutput is the generated link plus the lossless-ness echo.
type BuildPlaygroundURLOutput struct {
	URL            string        `json:"url" jsonschema:"the full playground URL (actions percent-encoded; compressed automatically past the plain threshold)"`
	Cluster        string        `json:"cluster,omitempty" jsonschema:"the cluster query param the URL carries, if any"`
	Size           string        `json:"size,omitempty" jsonschema:"the size query param the URL carries, if any"`
	ActionCount    int           `json:"actionCount" jsonschema:"number of encoded actions"`
	PlainLength    int           `json:"plainLength" jsonschema:"length of the plain action-log string (compressed form kicks in past 1500)"`
	Compressed     bool          `json:"compressed" jsonschema:"true when the URL carries the ~1 compressed actions form"`
	DecodedActions []ActionInput `json:"decodedActions,omitempty" jsonschema:"the actions decoded back OUT of the generated URL — compare against your input to confirm losslessness"`
	Notes          []string      `json:"notes,omitempty" jsonschema:"non-fatal adjustments or hints"`
}

func buildPlaygroundURLHandler(base string) mcp.ToolHandlerFor[BuildPlaygroundURLInput, BuildPlaygroundURLOutput] {
	return func(_ context.Context, _ *mcp.CallToolRequest, in BuildPlaygroundURLInput) (*mcp.CallToolResult, BuildPlaygroundURLOutput, error) {
		var zero BuildPlaygroundURLOutput

		clusterParam, sizeParam, notes, err := shapeToParams(in.Cluster, in.Shape)
		if err != nil {
			return nil, zero, err
		}
		entries, err := toEntries(in.Actions)
		if err != nil {
			return nil, zero, err
		}

		// Build self-checks: the emitted actions value must decode back to the
		// identical log, or Build errors instead of returning a URL.
		full, err := simurl.Build(simurl.Params{
			BaseURL:  base,
			Scenario: "free",
			Cluster:  clusterParam,
			Size:     sizeParam,
			Actions:  entries,
		})
		if err != nil {
			return nil, zero, err
		}

		actionsVal, err := simurl.EncodeActionsForURL(entries)
		if err != nil {
			return nil, zero, err
		}
		plain, compressed, err := simurl.PlainActions(actionsVal)
		if err != nil {
			return nil, zero, err
		}
		decoded, err := simurl.DecodeActionsFromURL(actionsVal)
		if err != nil {
			return nil, zero, fmt.Errorf("self-check decode failed: %w", err)
		}
		if compressed {
			notes = append(notes, "actions exceeded the 1500-char plain threshold and were compressed (~1 raw-DEFLATE form per §4); the playground decodes this alongside its own lz-string form. The plain form is still the most portable, so trimming the log under 1500 chars is preferred where practical")
		}

		return nil, BuildPlaygroundURLOutput{
			URL:            full,
			Cluster:        clusterParam,
			Size:           sizeParam,
			ActionCount:    len(entries),
			PlainLength:    len(plain),
			Compressed:     compressed,
			DecodedActions: fromEntries(decoded),
			Notes:          notes,
		}, nil
	}
}

// ---------------------------------------------------------------------------
// open_scenario

// OpenScenarioInput selects a curated scenario.
type OpenScenarioInput struct {
	ScenarioID string `json:"scenarioId" jsonschema:"scenario id in dotted form, e.g. 00.1.3"`
}

// OpenScenarioOutput is the scripted-walkthrough link.
type OpenScenarioOutput struct {
	URL        string `json:"url" jsonschema:"URL that opens the scenario paused on its scripted walkthrough"`
	ScenarioID string `json:"scenarioId"`
	Note       string `json:"note,omitempty"`
}

var scenarioIDRe = regexp.MustCompile(`^\d{2}\.\d+\.\d+$`)

func openScenarioHandler(base string) mcp.ToolHandlerFor[OpenScenarioInput, OpenScenarioOutput] {
	return func(_ context.Context, _ *mcp.CallToolRequest, in OpenScenarioInput) (*mcp.CallToolResult, OpenScenarioOutput, error) {
		var zero OpenScenarioOutput
		id := in.ScenarioID
		if id == "" {
			return nil, zero, fmt.Errorf("scenarioId is required (dotted form, e.g. \"00.1.3\")")
		}
		if !scenarioIDRe.MatchString(id) {
			return nil, zero, fmt.Errorf("scenarioId %q: expected the dotted form \"NN.M.S\" (two-digit track, module, scenario — e.g. \"00.1.3\"); use build_playground_url for a free-play session", id)
		}
		return nil, OpenScenarioOutput{
			URL:        base + "?scenario=" + url.QueryEscape(id),
			ScenarioID: id,
			Note:       "Opens paused on the scripted walkthrough; the id's existence is not verified against the deployed catalog.",
		}, nil
	}
}

// ---------------------------------------------------------------------------
// validate_actions

// ValidateActionsInput carries the raw string to check.
type ValidateActionsInput struct {
	Actions string `json:"actions" jsonschema:"the raw actions value — plain grammar (K:broker-2@8500,...), the ~1 compressed form, a percent-encoded value, or a full playground URL (the actions query param is extracted)"`
}

// DroppedEntry is one entry the playground would silently discard.
type DroppedEntry struct {
	Entry  string `json:"entry" jsonschema:"the raw entry text"`
	Reason string `json:"reason" jsonschema:"why the decoder rejects it"`
}

// ValidateActionsOutput reports what the playground would actually load.
type ValidateActionsOutput struct {
	Entries              []ActionInput  `json:"entries,omitempty" jsonschema:"the decoded actions the playground would load, sorted by at"`
	KeptCount            int            `json:"keptCount" jsonschema:"actions that survive (after the MAX_URL_ACTIONS truncation)"`
	DroppedCount         int            `json:"droppedCount" jsonschema:"entries the playground would silently drop"`
	Dropped              []DroppedEntry `json:"dropped,omitempty"`
	TruncatedCount       int            `json:"truncatedCount" jsonschema:"decodable actions beyond the 2000-action cap the playground would discard (earliest 2000 win)"`
	Compressed           bool           `json:"compressed" jsonschema:"true when the value used the ~1 compressed form"`
	PlainLength          int            `json:"plainLength" jsonschema:"length of the plain action-log string"`
	CompressionThreshold int            `json:"compressionThreshold" jsonschema:"plain lengths past this are emitted compressed (1500)"`
	MaxURLActions        int            `json:"maxUrlActions" jsonschema:"the action-count cap (2000)"`
	CanonicalEncoding    string         `json:"canonicalEncoding,omitempty" jsonschema:"the surviving actions re-encoded canonically (sorted, plain form)"`
	Notes                []string       `json:"notes,omitempty"`
}

func validateActionsHandler() mcp.ToolHandlerFor[ValidateActionsInput, ValidateActionsOutput] {
	return func(_ context.Context, _ *mcp.CallToolRequest, in ValidateActionsInput) (*mcp.CallToolResult, ValidateActionsOutput, error) {
		var zero ValidateActionsOutput
		raw := strings.TrimSpace(in.Actions)
		if raw == "" {
			return nil, zero, fmt.Errorf("actions is required: pass the raw actions value or a full playground URL")
		}
		var notes []string

		// A full URL: pull out the actions query parameter.
		if strings.Contains(raw, "://") {
			u, err := url.Parse(raw)
			if err != nil {
				return nil, zero, fmt.Errorf("actions looks like a URL but does not parse: %v", err)
			}
			raw = u.Query().Get("actions")
			if raw == "" {
				return nil, zero, fmt.Errorf("actions URL carries no actions query parameter")
			}
			notes = append(notes, "extracted the actions query parameter from the URL")
		} else if strings.Contains(raw, "%") {
			// A stand-alone percent-encoded value (the recommended wire form).
			if dec, err := url.QueryUnescape(raw); err == nil {
				raw = dec
				notes = append(notes, "percent-decoded the actions value")
			}
		}

		plain, compressed, err := simurl.PlainActions(raw)
		if err != nil {
			return nil, zero, fmt.Errorf("actions value has the ~1 compressed prefix but is neither valid raw-DEFLATE nor lz-string base64url data (§4): %v", err)
		}

		entries, droppedRaw := actionlog.DecodeTolerant(plain)
		truncated := 0
		if len(entries) > actionlog.MaxURLActions {
			truncated = len(entries) - actionlog.MaxURLActions
			entries = entries[:actionlog.MaxURLActions]
			notes = append(notes, fmt.Sprintf("the playground keeps only the earliest %d actions after sorting", actionlog.MaxURLActions))
		}

		dropped := make([]DroppedEntry, len(droppedRaw))
		for i, d := range droppedRaw {
			dropped[i] = DroppedEntry{Entry: d.Raw, Reason: d.Reason}
		}
		canonical := ""
		if len(entries) > 0 {
			canonical, err = actionlog.Encode(entries)
			if err != nil {
				return nil, zero, fmt.Errorf("re-encoding the surviving actions failed: %v", err)
			}
		}

		return nil, ValidateActionsOutput{
			Entries:              fromEntries(entries),
			KeptCount:            len(entries),
			DroppedCount:         len(dropped),
			Dropped:              dropped,
			TruncatedCount:       truncated,
			Compressed:           compressed,
			PlainLength:          len(plain),
			CompressionThreshold: simurl.CompressionThreshold,
			MaxURLActions:        actionlog.MaxURLActions,
			CanonicalEncoding:    canonical,
			Notes:                notes,
		}, nil
	}
}
