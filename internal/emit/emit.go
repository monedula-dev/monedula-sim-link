// Package emit is the snapshot → mapping → self-validation → URL pipeline
// shared by the CLI (cmd/monedula-sim-link) and the MCP cluster_to_url tool, so both
// produce byte-identical links from the same read-only snapshot instead of
// duplicating the connect, mapping and validation logic.
//
// The pipeline is strictly READ-ONLY on the cluster side: Snapshot builds the
// snapshot.Client (read requests only, listed in snapshot/kafka.go) and never writes.
// Build then performs the pre-print self-validation the playground cannot do
// (it silently drops malformed entries), so a link is never returned unless its
// actions value self-decodes back to the identical log.
package emit

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/monedula-dev/monedula-sim-link/actionlog"
	"github.com/monedula-dev/monedula-sim-link/mapping"
	"github.com/monedula-dev/monedula-sim-link/simurl"
	"github.com/monedula-dev/monedula-sim-link/snapshot"
)

// PasswordEnvVar is the environment fallback for the SASL password, keeping
// secrets out of shell history, process lists and tool arguments.
const PasswordEnvVar = "MONEDULA_SIM_LINK_PASSWORD"

// DefaultTimeout is the snapshot timeout applied when a caller passes none.
const DefaultTimeout = 15 * time.Second

// MaxURLLength is the sanity ceiling on the emitted link. Browsers and chat
// clients commonly start truncating past ~2k characters, and a truncated
// actions param decodes to a silently shorter log.
const MaxURLLength = 2048

// ConnectParams is the already-parsed connect surface shared by the CLI flags
// and the MCP tool inputs (host lists split, timeout as a duration).
type ConnectParams struct {
	Brokers       []string
	Topics        []string
	TopicsRegex   string
	TLSEnable     bool
	CACert        string
	ClientCert    string
	ClientKey     string
	SASLMechanism string
	Username      string
	Password      string
	Timeout       time.Duration
}

// Snapshot connects READ-ONLY and takes a point-in-time snapshot, returning it
// with any non-fatal warnings (partitions whose offsets could not be listed,
// topics with metadata errors). It applies the PasswordEnvVar fallback when no
// SASL password is set. An empty selection (a filter matching nothing) is a
// returned error, never a silent empty snapshot.
func Snapshot(ctx context.Context, p ConnectParams) (snapshot.ClusterSnapshot, []string, error) {
	sel, err := buildSelection(p.Topics, p.TopicsRegex)
	if err != nil {
		return snapshot.ClusterSnapshot{}, nil, err
	}
	client, err := snapshot.Connect(buildConnectConfig(p))
	if err != nil {
		return snapshot.ClusterSnapshot{}, nil, err
	}
	defer client.Close()

	timeout := p.Timeout
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return client.Fetch(cctx, sel)
}

func buildSelection(topics []string, regexStr string) (snapshot.Selection, error) {
	sel := snapshot.Selection{}
	for _, t := range topics {
		if t = strings.TrimSpace(t); t != "" {
			sel.Exact = append(sel.Exact, t)
		}
	}
	if regexStr != "" {
		re, err := regexp.Compile(regexStr)
		if err != nil {
			return sel, fmt.Errorf("invalid topics regexp %q: %w", regexStr, err)
		}
		sel.Regex = re
	}
	return sel, nil
}

func buildConnectConfig(p ConnectParams) snapshot.ConnectConfig {
	cfg := snapshot.ConnectConfig{}
	for _, b := range p.Brokers {
		if b = strings.TrimSpace(b); b != "" {
			cfg.Brokers = append(cfg.Brokers, b)
		}
	}
	if p.TLSEnable || p.CACert != "" || p.ClientCert != "" || p.ClientKey != "" {
		cfg.TLS = &snapshot.TLSConfig{CACert: p.CACert, ClientCert: p.ClientCert, ClientKey: p.ClientKey}
	}
	if p.SASLMechanism != "" || p.Username != "" {
		password := p.Password
		if password == "" {
			password = os.Getenv(PasswordEnvVar)
		}
		cfg.SASL = &snapshot.SASLConfig{Mechanism: p.SASLMechanism, Username: p.Username, Password: password}
	}
	return cfg
}

// BuildOptions tunes the mapping → validation → URL step.
type BuildOptions struct {
	// BaseURL overrides the playground endpoint (default simurl.DefaultBaseURL).
	BaseURL string
	// MaxRecordsPerPartition caps synthetic trailing records per partition
	// (0 = mapping.DefaultMaxRecordsPerPartition).
	MaxRecordsPerPartition int
	// Clamp trims over-cap brokers, partitions and --topics lists
	// deterministically instead of failing (mapping.Options.Clamp).
	Clamp bool
	// PinnedTopics are the exact topic names the caller listed; an over-cap
	// selection keeps them before ranking the rest.
	PinnedTopics []string
	// Force downgrades the compressed-form and over-length checks from hard
	// errors to warnings. It never overrides the lossless round-trip check.
	Force bool
}

// Result is the fully-built, self-validated pipeline output. Warnings collects
// every mapping approximation/clamp plus any Force-downgraded validation
// warning; a caller surfaces them alongside the URL.
type Result struct {
	URL            string
	Cluster        string // free-play topology param; "" = single-dc default
	Size           string // free-play size slug (always non-empty)
	Entries        []actionlog.Entry
	DecodedActions []actionlog.Entry // Entries decoded back OUT of the URL (losslessness proof)
	BrokerNames    map[string]string // real broker id → simulator broker id
	TopicNames     map[string]string // real topic name → simulator topic name
	GroupNames     map[string]string // real consumer-group id → simulator group id
	Warnings       []string
	Notes          []string
	PlainLength    int
	Compressed     bool
	ActionCount    int
}

// Build maps a snapshot to a simulator state and returns the self-validated
// playground URL. A hard failure (a broker, partition or --topics cap breach
// without Clamp; a lossy round-trip; or — without Force — the compressed form or an over-length URL)
// is returned as an error.
func Build(snap snapshot.ClusterSnapshot, opts BuildOptions) (*Result, error) {
	mres, err := mapping.Map(snap, mapping.Options{
		MaxRecordsPerPartition: opts.MaxRecordsPerPartition,
		Clamp:                  opts.Clamp,
		PinnedTopics:           opts.PinnedTopics,
	})
	if err != nil {
		return nil, fmt.Errorf("mapping: %w", err)
	}
	return BuildFromMapping(mres, opts)
}

// BuildFromMapping runs the validation + URL tail on an already-mapped result.
// It is the seam Build goes through and the entry point tests use to exercise
// the compressed-form / over-length branches without a live cluster.
//
// The self-validation the playground cannot do for us (it drops malformed
// entries silently):
//
//  1. the action log round-trips encode→decode→encode losslessly (never
//     overridable — a lossy log must not ship),
//  2. the final URL stays under MaxURLLength; Force downgrades this to a
//     warning.
//
// The compressed "~1" form is no longer a hard failure: the playground now
// decodes both the raw-DEFLATE layout this tool emits and its own lz-string
// layout (docs/playground-url-api.md §4). The plain form is still preferred and
// stays used under the threshold; Result.Compressed reports which form shipped.
func BuildFromMapping(mres *mapping.Result, opts BuildOptions) (*Result, error) {
	out := &Result{
		Cluster:     mres.Cluster,
		Size:        mres.Size,
		Entries:     mres.Entries,
		BrokerNames: mres.BrokerNames,
		TopicNames:  mres.TopicNames,
		GroupNames:  mres.GroupNames,
		Warnings:    append([]string(nil), mres.Warnings...),
	}

	if err := actionlog.Verify(mres.Entries); err != nil {
		return nil, fmt.Errorf("validation: action log does not round-trip: %w", err)
	}
	plain, err := actionlog.Encode(mres.Entries)
	if err != nil {
		return nil, fmt.Errorf("validation: %w", err)
	}
	value, err := simurl.EncodeActionsForURL(mres.Entries)
	if err != nil {
		return nil, fmt.Errorf("validation: %w", err)
	}
	// Explicit re-decode of the URL-ready value back to the plain log.
	decoded, err := simurl.DecodeActionsFromURL(value)
	if err != nil {
		return nil, fmt.Errorf("validation: actions value does not decode: %w", err)
	}
	reEnc, err := actionlog.Encode(decoded)
	if err != nil || reEnc != plain {
		return nil, fmt.Errorf("validation: actions value decodes to a different log")
	}
	_, compressed, err := simurl.PlainActions(value)
	if err != nil {
		return nil, fmt.Errorf("validation: %w", err)
	}

	url, err := simurl.Build(simurl.Params{
		BaseURL:  opts.BaseURL,
		Scenario: "free",
		Cluster:  mres.Cluster,
		Size:     mres.Size,
		Actions:  mres.Entries,
	})
	if err != nil {
		return nil, fmt.Errorf("validation: %w", err)
	}
	if len(url) > MaxURLLength {
		msg := fmt.Sprintf("URL is %d chars (> %d): long links get truncated by browsers/chat clients, and a truncated actions param silently loses actions — reduce the max records per partition or narrow the topic selection", len(url), MaxURLLength)
		if !opts.Force {
			return nil, fmt.Errorf("validation: %s, or force to emit anyway", msg)
		}
		out.Warnings = append(out.Warnings, msg+" (force given, emitting anyway)")
	}

	out.URL = url
	out.DecodedActions = decoded
	out.PlainLength = len(plain)
	out.Compressed = compressed
	out.ActionCount = len(mres.Entries)
	return out, nil
}
