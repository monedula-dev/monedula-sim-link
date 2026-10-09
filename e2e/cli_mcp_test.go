//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/monedula-dev/monedula-sim-link/mcpserver"
)

var (
	binOnce sync.Once
	binPath string
	binErr  error
)

// binary builds the real monedula-sim-link executable once per run.
func binary(t *testing.T) string {
	t.Helper()
	binOnce.Do(func() {
		dir, err := os.MkdirTemp("", "monedula-sim-link-e2e")
		if err != nil {
			binErr = err
			return
		}
		binPath = filepath.Join(dir, "monedula-sim-link")
		if runtime.GOOS == "windows" {
			binPath += ".exe"
		}
		out, err := exec.Command("go", "build", "-o", binPath, "../cmd/monedula-sim-link").CombinedOutput()
		if err != nil {
			binErr = errors.New("go build: " + err.Error() + "\n" + string(out))
		}
	})
	if binErr != nil {
		t.Fatal(binErr)
	}
	return binPath
}

// removeBinary deletes the built executable's directory; TestMain calls it.
func removeBinary() {
	if binPath != "" {
		os.RemoveAll(filepath.Dir(binPath))
	}
}

func runCLI(t *testing.T, args ...string) (stdout, stderr string, code int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary(t), args...)
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	err := cmd.Run()
	var exit *exec.ExitError
	switch {
	case err == nil:
	case errors.As(err, &exit):
		code = exit.ExitCode()
	default:
		t.Fatalf("run %v: %v", args, err)
	}
	return out.String(), errb.String(), code
}

// TestCLIConnect runs `monedula-sim-link connect` as a user would. The URL it
// prints must be the one the library builds from the same cluster: the
// mapping is deterministic, so two snapshots of an unchanged cluster agree.
func TestCLIConnect(t *testing.T) {
	stdout, stderr, code := runCLI(t, "connect", "--brokers", strings.Join(brokers(), ","))
	if code != 0 {
		t.Fatalf("exit %d\nstderr: %s", code, stderr)
	}
	// Mapping approximations are reported on stderr, never mixed into stdout.
	if !strings.Contains(stderr, `topic "audit.log" is not representable verbatim; renamed to "audit-log"`) {
		t.Errorf("stderr lacks the audit.log rename warning:\n%s", stderr)
	}
	got := strings.TrimSpace(stdout)
	if want := buildURL(t).URL; got != want {
		t.Fatalf("CLI URL differs from the library's:\ncli  %s\nlib  %s", got, want)
	}
}

func TestCLIConnectMissingTopic(t *testing.T) {
	_, stderr, code := runCLI(t, "connect", "--brokers", strings.Join(brokers(), ","), "--topics", missingTopic)
	if code != 1 || !strings.Contains(stderr, missingTopic) {
		t.Fatalf("exit %d, stderr %q: want exit 1 naming %s", code, stderr, missingTopic)
	}
}

// TestMCPClusterToURL starts `monedula-sim-link mcp` as a subprocess, talks to
// it over stdio like any MCP client would, and
// calls cluster_to_url. It must return the same link as the CLI.
func TestMCPClusterToURL(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	client := mcp.NewClient(&mcp.Implementation{Name: "e2e", Version: "0.0.0"}, nil)
	session, err := client.Connect(ctx, &mcp.CommandTransport{Command: exec.Command(binary(t), "mcp")}, nil)
	if err != nil {
		t.Fatalf("connect to mcp server: %v", err)
	}
	defer session.Close()

	res, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name:      "cluster_to_url",
		Arguments: map[string]any{"brokers": brokers()},
	})
	if err != nil {
		t.Fatalf("cluster_to_url: %v", err)
	}
	if res.IsError {
		t.Fatalf("cluster_to_url returned an error: %+v", res.Content)
	}
	var out mcpserver.ClusterToURLOutput
	raw, _ := json.Marshal(res.StructuredContent)
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("structured output: %v", err)
	}

	stdout, stderr, code := runCLI(t, "connect", "--brokers", strings.Join(brokers(), ","))
	if code != 0 {
		t.Fatalf("cli exit %d: %s", code, stderr)
	}
	if cli := strings.TrimSpace(stdout); out.URL != cli {
		t.Fatalf("MCP URL differs from the CLI's:\nmcp  %s\ncli  %s", out.URL, cli)
	}
	if len(out.DecodedActions) != out.ActionCount || out.ActionCount == 0 {
		t.Errorf("decodedActions %d vs actionCount %d", len(out.DecodedActions), out.ActionCount)
	}
}
