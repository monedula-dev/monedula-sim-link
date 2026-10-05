// Command monedula-sim-link connects READ-ONLY to a Kafka cluster and prints a
// monedula.dev kafka-simulator playground URL that visualizes it.
//
// `connect` is the real path: snapshot a cluster over franz-go (read requests
// only - see snapshot/kafka.go), map it to a free-play simulator state (see
// docs/mapping.md) and print a self-validated URL. `demo` feeds a built-in
// fixture through the same pipeline, no cluster needed.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime/debug"
	"sort"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/monedula-dev/monedula-sim-link/actionlog"
	"github.com/monedula-dev/monedula-sim-link/internal/emit"
	"github.com/monedula-dev/monedula-sim-link/mapping"
	"github.com/monedula-dev/monedula-sim-link/mcpserver"
	"github.com/monedula-dev/monedula-sim-link/simurl"
	"github.com/monedula-dev/monedula-sim-link/snapshot"
)

// passwordEnvVar is the environment fallback for --password, keeping secrets
// out of shell history and process lists.
const passwordEnvVar = emit.PasswordEnvVar

type config struct {
	brokers                string
	topics                 string
	topicsRegex            string
	tlsEnable              bool
	caCert                 string
	clientCert             string
	clientKey              string
	saslMechanism          string
	username               string
	password               string
	timeout                time.Duration
	clamp                  bool
	maxRecordsPerPartition int
	baseURL                string
	dryRun                 bool
	force                  bool
}

func registerConnectFlags(fs *flag.FlagSet) *config {
	c := &config{}
	fs.StringVar(&c.brokers, "brokers", "", "comma-separated Kafka bootstrap brokers (host:port); required")
	fs.StringVar(&c.topics, "topics", "", "comma-separated exact topic names to include (also the only way to include internal topics: flagged internal by the broker, or with a leading '_')")
	fs.StringVar(&c.topicsRegex, "topics-regex", "", "additionally include non-internal topics matching this Go regexp")
	fs.BoolVar(&c.tlsEnable, "tls", false, "connect over TLS")
	fs.StringVar(&c.caCert, "ca-cert", "", "PEM CA certificate file to trust (implies --tls)")
	fs.StringVar(&c.clientCert, "client-cert", "", "PEM client certificate for mTLS (implies --tls, needs --client-key)")
	fs.StringVar(&c.clientKey, "client-key", "", "PEM client key for mTLS")
	fs.StringVar(&c.saslMechanism, "sasl-mechanism", "", "SASL mechanism: plain, scram-sha-256 or scram-sha-512")
	fs.StringVar(&c.username, "username", "", "SASL username")
	fs.StringVar(&c.password, "password", "", "SASL password (prefer the "+passwordEnvVar+" env var)")
	fs.DurationVar(&c.timeout, "timeout", 15*time.Second, "overall snapshot timeout")
	registerMappingFlags(fs, c)
	return c
}

// registerMappingFlags holds the flags shared by connect and demo (everything
// downstream of the snapshot).
func registerMappingFlags(fs *flag.FlagSet, c *config) {
	fs.BoolVar(&c.clamp, "clamp", false, "when the selection exceeds simulator caps, clamp deterministically (lowest ids / alphabetical) instead of failing")
	fs.IntVar(&c.maxRecordsPerPartition, "max-records-per-partition", mapping.DefaultMaxRecordsPerPartition, "cap on synthetic trailing records produced per partition")
	fs.StringVar(&c.baseURL, "base-url", simurl.DefaultBaseURL, "playground base URL")
	fs.BoolVar(&c.dryRun, "dry-run", false, "also print the decoded action list, name mappings and size param")
	fs.BoolVar(&c.force, "force", false, "emit the URL even when it exceeds the length limit (over-length links risk truncation)")
}

func usage(w io.Writer) {
	fmt.Fprint(w, `monedula-sim-link — generate a kafka-simulator playground URL from a Kafka cluster

Usage:
  monedula-sim-link connect --brokers host:port[,host:port…] [flags]
  monedula-sim-link demo [flags]

Commands:
  connect  Snapshot a real cluster (STRICTLY READ-ONLY: ApiVersions, Metadata,
           ListOffsets, ListGroups, DescribeGroups, OffsetFetch and
           DescribeConfigs requests only) and print a playground URL.
  demo     Feed a built-in cluster fixture through the same mapping (no
           cluster needed).
  mcp      Serve the Model Context Protocol over stdio, exposing the playground
           link-builder tools (build_playground_url, open_scenario,
           validate_actions, cluster_to_url) to LLM clients. Flags: --base-url.
  version  Print the monedula-sim-link version.
  help     Show this help.

Run 'monedula-sim-link connect -h' or 'monedula-sim-link demo -h' for the flag list.
`)
}

// version is stamped by release builds (-ldflags "-X main.version=v0.1.0").
var version = "dev"

// buildVersion is the stamped version or, for a `go install ...@v0.1.0`
// build, the module version the Go toolchain recorded; "dev" otherwise.
func buildVersion() string {
	if version != "dev" {
		return version
	}
	if bi, ok := debug.ReadBuildInfo(); ok && bi.Main.Version != "" && bi.Main.Version != "(devel)" {
		return bi.Main.Version
	}
	return version
}

func main() {
	mcpserver.ServerVersion = buildVersion()
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) < 1 {
		usage(stderr)
		return 2
	}
	switch args[0] {
	case "connect":
		return runConnect(args[1:], stdout, stderr)
	case "demo":
		return runDemo(args[1:], stdout, stderr)
	case "mcp":
		return runMCP(args[1:])
	case "version", "--version":
		fmt.Fprintln(stdout, "monedula-sim-link", buildVersion())
		return 0
	case "help", "-h", "--help":
		usage(stdout)
		return 0
	default:
		fmt.Fprintf(stderr, "unknown command %q\n\n", args[0])
		usage(stderr)
		return 2
	}
}

func runConnect(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("connect", flag.ContinueOnError)
	fs.SetOutput(stderr)
	c := registerConnectFlags(fs)
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if c.brokers == "" {
		fmt.Fprintln(stderr, "connect: --brokers is required")
		return 2
	}

	snap, warnings, err := emit.Snapshot(context.Background(), connectParams(c))
	for _, w := range warnings {
		fmt.Fprintf(stderr, "warning: %s\n", w)
	}
	if err != nil {
		fmt.Fprintf(stderr, "connect: %v\n", err)
		return 1
	}

	return emitURL(snap, c, stdout, stderr)
}

// connectParams turns the parsed CLI flags into the shared connect surface,
// splitting the comma-separated --brokers / --topics lists (trimming happens in
// package emit).
func connectParams(c *config) emit.ConnectParams {
	return emit.ConnectParams{
		Brokers:       strings.Split(c.brokers, ","),
		Topics:        strings.Split(c.topics, ","),
		TopicsRegex:   c.topicsRegex,
		TLSEnable:     c.tlsEnable,
		CACert:        c.caCert,
		ClientCert:    c.clientCert,
		ClientKey:     c.clientKey,
		SASLMechanism: c.saslMechanism,
		Username:      c.username,
		Password:      c.password,
		Timeout:       c.timeout,
	}
}

// emitURL runs the shared mapping → validation → URL tail (package emit) and
// prints the result: warnings and, under --dry-run, the size param + name
// tables + decoded action list to stderr/stdout, then the URL to stdout.
func emitURL(snap snapshot.ClusterSnapshot, c *config, stdout, stderr io.Writer) int {
	res, err := emit.Build(snap, emit.BuildOptions{
		BaseURL:                c.baseURL,
		MaxRecordsPerPartition: c.maxRecordsPerPartition,
		Clamp:                  c.clamp,
		Force:                  c.force,
	})
	if err != nil {
		fmt.Fprintf(stderr, "%v\n", err)
		return 1
	}
	for _, w := range res.Warnings {
		fmt.Fprintf(stderr, "warning: %s\n", w)
	}
	if c.dryRun {
		printDryRun(res, stdout, stderr)
	}
	fmt.Fprintln(stdout, res.URL)
	return 0
}

func printDryRun(res *emit.Result, stdout, stderr io.Writer) {
	fmt.Fprintf(stdout, "size: %s\n", res.Size)
	if len(res.BrokerNames) > 0 {
		fmt.Fprintf(stdout, "brokers:\n")
		for _, line := range sortedMapLines(res.BrokerNames) {
			fmt.Fprintf(stdout, "  %s\n", line)
		}
	}
	if len(res.TopicNames) > 0 {
		fmt.Fprintf(stdout, "topics:\n")
		for _, line := range sortedMapLines(res.TopicNames) {
			fmt.Fprintf(stdout, "  %s\n", line)
		}
	}
	if len(res.GroupNames) > 0 {
		fmt.Fprintf(stdout, "groups:\n")
		for _, line := range sortedMapLines(res.GroupNames) {
			fmt.Fprintf(stdout, "  %s\n", line)
		}
	}
	fmt.Fprintf(stdout, "actions (%d entries):\n", len(res.Entries))
	for _, e := range res.Entries {
		enc, err := actionlog.EncodeEntry(e)
		if err != nil {
			fmt.Fprintf(stderr, "encode entry failed: %v\n", err)
			continue
		}
		fmt.Fprintf(stdout, "  %s\n", enc)
	}
	fmt.Fprintln(stdout)
}

func sortedMapLines(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]string, len(keys))
	for i, k := range keys {
		out[i] = fmt.Sprintf("%s -> %s", k, m[k])
	}
	return out
}

// runMCP serves MCP over stdio until the client disconnects. Logs go to
// stderr; stdout carries only the protocol stream.
func runMCP(args []string) int {
	fs := flag.NewFlagSet("mcp", flag.ContinueOnError)
	baseURL := fs.String("base-url", simurl.DefaultBaseURL, "playground base URL used in every generated link")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	srv := mcpserver.New(mcpserver.Options{BaseURL: *baseURL})
	if err := srv.Run(context.Background(), &mcp.StdioTransport{}); err != nil {
		fmt.Fprintf(os.Stderr, "mcp server: %v\n", err)
		return 1
	}
	return 0
}

// demoFixture is a hand-built single-DC cluster exercising every mapping
// feature: an offline broker (kill), a partition off default placement
// (reassign), an ISR gap (slow replica) and non-empty offset ranges
// (produces).
func demoFixture() snapshot.ClusterSnapshot {
	return snapshot.ClusterSnapshot{
		Brokers: []snapshot.Broker{
			{ID: "1", Online: true},
			{ID: "2", Online: true},
			{ID: "3", Online: true},
			{ID: "4", Online: false},
		},
		Topics: []snapshot.Topic{
			{
				Name: "orders",
				Partitions: []snapshot.Partition{
					{ID: 0, Leader: "1", Replicas: []string{"1", "2", "3"}, ISR: []string{"1", "2", "3"}, EarliestOffset: 0, LatestOffset: 5},
					{ID: 1, Leader: "2", Replicas: []string{"2", "3", "4"}, ISR: []string{"2", "3"}, EarliestOffset: 2, LatestOffset: 4},
					{ID: 2, Leader: "3", Replicas: []string{"3", "1", "2"}, ISR: []string{"3", "1"}, EarliestOffset: 0, LatestOffset: 0},
				},
			},
			{
				Name: "payments",
				Partitions: []snapshot.Partition{
					{ID: 0, Leader: "2", Replicas: []string{"2", "1"}, ISR: []string{"2", "1"}, EarliestOffset: 0, LatestOffset: 3},
				},
			},
		},
	}
}

func runDemo(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("demo", flag.ContinueOnError)
	fs.SetOutput(stderr)
	c := &config{}
	registerMappingFlags(fs, c)
	if err := fs.Parse(args); err != nil {
		return 2
	}
	return emitURL(demoFixture(), c, stdout, stderr)
}
