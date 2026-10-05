package mcpserver

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/monedula-dev/monedula-sim-link/internal/emit"
)

// ---------------------------------------------------------------------------
// cluster_to_url

// ClusterToURLInput mirrors the `monedula-sim-link connect` CLI surface: where to
// connect (bootstrap brokers, TLS, SASL), which topics to include, and the
// mapping caps. It is the live-cluster counterpart to build_playground_url's
// declarative shape.
type ClusterToURLInput struct {
	Brokers     []string `json:"brokers" jsonschema:"Kafka bootstrap brokers as host:port; at least one, required. The connection is STRICTLY READ-ONLY (ApiVersions, Metadata, ListOffsets, ListGroups, DescribeGroups, OffsetFetch and DescribeConfigs requests only - no produce, no topic/config mutation, no consumer group joined)"`
	Topics      []string `json:"topics,omitempty" jsonschema:"exact topic names to include. Fails if any is missing. Also the only way to include internal ('_'-prefixed) topics. Omit (with no topicsRegex) to include every non-internal topic"`
	TopicsRegex string   `json:"topicsRegex,omitempty" jsonschema:"additionally include non-internal topics whose name matches this Go (RE2) regexp"`

	TLS        bool   `json:"tls,omitempty" jsonschema:"connect over TLS (implied when any of caCert/clientCert/clientKey is set)"`
	CACert     string `json:"caCert,omitempty" jsonschema:"PEM CA certificate FILE PATH to trust, appended to the system pool. The path is resolved on the MCP server's host, not the client's"`
	ClientCert string `json:"clientCert,omitempty" jsonschema:"PEM client certificate FILE PATH for mTLS (needs clientKey). Resolved on the MCP server's host"`
	ClientKey  string `json:"clientKey,omitempty" jsonschema:"PEM client key FILE PATH for mTLS (needs clientCert). Resolved on the MCP server's host"`

	SASLMechanism string `json:"saslMechanism,omitempty" jsonschema:"SASL mechanism: plain, scram-sha-256 or scram-sha-512 (requires username)"`
	Username      string `json:"username,omitempty" jsonschema:"SASL username"`
	Password      string `json:"password,omitempty" jsonschema:"SASL password. Prefer setting the MONEDULA_SIM_LINK_PASSWORD environment variable on the MCP server's host over passing the secret here; the env var is the fallback when this is empty"`

	TimeoutSeconds         int  `json:"timeoutSeconds,omitempty" jsonschema:"overall snapshot timeout in seconds (default 15)"`
	MaxRecordsPerPartition int  `json:"maxRecordsPerPartition,omitempty" jsonschema:"cap on synthetic trailing records produced per partition to represent data volume (default 8)"`
	Clamp                  bool `json:"clamp,omitempty" jsonschema:"when the selection exceeds the simulator's free-play caps (10 brokers / 6 extra topics / 24 partitions per topic / 4 consumer groups), clamp deterministically (lowest ids / alphabetical) with warnings instead of failing"`
}

// NameMapping is one real-name → simulator-name entry in the echo tables.
type NameMapping struct {
	Real      string `json:"real" jsonschema:"the real cluster broker id / topic name"`
	Simulator string `json:"simulator" jsonschema:"the id / name it is rendered as in the simulator link"`
}

// ClusterToURLOutput is the generated link plus the losslessness echo and the
// real→simulator name tables.
type ClusterToURLOutput struct {
	URL            string        `json:"url" jsonschema:"the full playground URL visualizing the snapshotted cluster"`
	Cluster        string        `json:"cluster,omitempty" jsonschema:"the cluster query param the URL carries, if any (empty for the single-dc default this mapping targets)"`
	Size           string        `json:"size,omitempty" jsonschema:"the size query param the URL carries: broker count, and each topic's real partition count and RF"`
	ActionCount    int           `json:"actionCount" jsonschema:"number of encoded actions"`
	PlainLength    int           `json:"plainLength" jsonschema:"length of the plain action-log string (compressed form kicks in past 1500)"`
	Compressed     bool          `json:"compressed" jsonschema:"true when the URL carries the ~1 compressed actions form"`
	DecodedActions []ActionInput `json:"decodedActions,omitempty" jsonschema:"the actions decoded back OUT of the generated URL — the losslessness proof for the snapshot mapping"`
	BrokerNames    []NameMapping `json:"brokerNames,omitempty" jsonschema:"real broker id → simulator broker id table"`
	TopicNames     []NameMapping `json:"topicNames,omitempty" jsonschema:"real topic name → simulator topic name table (topics not representable verbatim are renamed)"`
	GroupNames     []NameMapping `json:"groupNames,omitempty" jsonschema:"real consumer-group id → simulator group id table (a group with no committed offset on a rendered topic, or dropped by the group cap, is absent)"`
	Warnings       []string      `json:"warnings,omitempty" jsonschema:"snapshot and mapping warnings: unreadable offsets, approximations, and any clamps applied"`
	Notes          []string      `json:"notes,omitempty" jsonschema:"non-fatal hints"`
}

func clusterToURLHandler(base string) mcp.ToolHandlerFor[ClusterToURLInput, ClusterToURLOutput] {
	return func(ctx context.Context, _ *mcp.CallToolRequest, in ClusterToURLInput) (*mcp.CallToolResult, ClusterToURLOutput, error) {
		var zero ClusterToURLOutput

		brokers := trimNonEmpty(in.Brokers)
		if len(brokers) == 0 {
			return nil, zero, fmt.Errorf("brokers is required: pass at least one bootstrap host:port")
		}

		snap, warnings, err := emit.Snapshot(ctx, emit.ConnectParams{
			Brokers:       brokers,
			Topics:        in.Topics,
			TopicsRegex:   in.TopicsRegex,
			TLSEnable:     in.TLS,
			CACert:        in.CACert,
			ClientCert:    in.ClientCert,
			ClientKey:     in.ClientKey,
			SASLMechanism: in.SASLMechanism,
			Username:      in.Username,
			Password:      in.Password,
			Timeout:       time.Duration(in.TimeoutSeconds) * time.Second,
		})
		if err != nil {
			return nil, zero, err
		}

		res, err := emit.Build(snap, emit.BuildOptions{
			BaseURL:                base,
			MaxRecordsPerPartition: in.MaxRecordsPerPartition,
			Clamp:                  in.Clamp,
		})
		if err != nil {
			return nil, zero, err
		}

		return nil, ClusterToURLOutput{
			URL:            res.URL,
			Cluster:        res.Cluster,
			Size:           res.Size,
			ActionCount:    res.ActionCount,
			PlainLength:    res.PlainLength,
			Compressed:     res.Compressed,
			DecodedActions: fromEntries(res.DecodedActions),
			BrokerNames:    sortedNameMappings(res.BrokerNames),
			TopicNames:     sortedNameMappings(res.TopicNames),
			GroupNames:     sortedNameMappings(res.GroupNames),
			Warnings:       append(append([]string(nil), warnings...), res.Warnings...),
			Notes:          res.Notes,
		}, nil
	}
}

func trimNonEmpty(in []string) []string {
	var out []string
	for _, s := range in {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// sortedNameMappings flattens a real→sim map into a table sorted by real name,
// so the tool output is deterministic.
func sortedNameMappings(m map[string]string) []NameMapping {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]NameMapping, len(keys))
	for i, k := range keys {
		out[i] = NameMapping{Real: k, Simulator: m[k]}
	}
	return out
}
