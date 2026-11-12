package mcpserver

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/monedula-dev/monedula-sim-link/internal/emit"
)

// newSessionWith is newSession with server options.
func newSessionWith(t *testing.T, opts Options) *mcp.ClientSession {
	t.Helper()
	ctx := context.Background()
	st, ct := mcp.NewInMemoryTransports()
	if _, err := New(opts).Connect(ctx, st, nil); err != nil {
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

// TestClusterToURLSchemaHasNoFilePaths: the model driving the client must not
// be able to name a file on the server host.
func TestClusterToURLSchemaHasNoFilePaths(t *testing.T) {
	s := newSession(t)
	res, err := s.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	for _, tool := range res.Tools {
		if tool.Name != "cluster_to_url" {
			continue
		}
		raw, err := json.Marshal(tool.InputSchema)
		if err != nil {
			t.Fatal(err)
		}
		for _, banned := range []string{`"caCert"`, `"clientCert"`, `"clientKey"`} {
			if strings.Contains(string(raw), banned) {
				t.Errorf("cluster_to_url input schema still has %s", banned)
			}
		}
		return
	}
	t.Fatal("cluster_to_url not registered")
}

// TestClusterToURLRejectsFilePathArguments: an old client (or an injected
// call) passing caCert gets an error, not a silently ignored argument.
func TestClusterToURLRejectsFilePathArguments(t *testing.T) {
	s := newSession(t)
	for _, arg := range []string{"caCert", "clientCert", "clientKey"} {
		res, err := s.CallTool(context.Background(), &mcp.CallToolParams{
			Name:      "cluster_to_url",
			Arguments: map[string]any{"brokers": []string{"127.0.0.1:1"}, "timeoutSeconds": 1, arg: "/etc/passwd"},
		})
		var msg string
		switch {
		case err != nil:
			msg = err.Error()
		case res.IsError:
			msg = errText(res)
		default:
			t.Errorf("%s was accepted", arg)
			continue
		}
		// The schema must reject the argument by name, before any connection.
		if !strings.Contains(msg, arg) {
			t.Errorf("%s: error %q does not name the rejected argument", arg, msg)
		}
	}
}

// TestClusterToURLAllowedBrokersRefusesOthers: with an allow-list, a call
// naming another broker fails before any connection is made.
func TestClusterToURLAllowedBrokersRefusesOthers(t *testing.T) {
	s := newSessionWith(t, Options{Security: Security{AllowedBrokers: []string{"kafka-1.internal:9092"}}})
	res := callTool(t, s, "cluster_to_url", map[string]any{"brokers": []string{"evil.example:9092"}})
	wantToolError(t, res, "--allowed-brokers")
}

// TestClusterToURLAllowedBrokersConnects: allow-listed brokers still work
// end to end.
func TestClusterToURLAllowedBrokersConnects(t *testing.T) {
	brokers := standardFake(t)
	s := newSessionWith(t, Options{Security: Security{AllowedBrokers: brokers}})
	var out ClusterToURLOutput
	structured(t, callTool(t, s, "cluster_to_url", map[string]any{"brokers": brokers}), &out)
	if out.URL == "" {
		t.Fatal("no URL")
	}
}

// TestClusterToURLEnvPasswordNotSentToUnlistedBroker: the env password must
// never go to a broker the call picked unless the operator listed it.
func TestClusterToURLEnvPasswordNotSentToUnlistedBroker(t *testing.T) {
	t.Setenv(emit.PasswordEnvVar, "s3cret")
	s := newSession(t)
	res := callTool(t, s, "cluster_to_url", map[string]any{
		"brokers":       []string{"127.0.0.1:1"},
		"saslMechanism": "plain",
		"username":      "u",
	})
	wantToolError(t, res, emit.PasswordEnvVar, "--allowed-brokers")
	if strings.Contains(errText(res), "s3cret") {
		t.Fatal("error leaks the password")
	}
}

func TestApplyConnectPolicy(t *testing.T) {
	t.Setenv(emit.PasswordEnvVar, "s3cret")
	listed := Security{
		CACert: "/srv/ca.pem", ClientCert: "/srv/c.pem", ClientKey: "/srv/c.key",
		AllowedBrokers: []string{"Kafka-1.internal:9092", "[::1]:9093"},
	}

	t.Run("listed brokers get TLS files and the env password", func(t *testing.T) {
		p := emit.ConnectParams{Brokers: []string{" kafka-1.INTERNAL:9092", "[::1]:9093"}, SASLMechanism: "plain", Username: "u"}
		if err := listed.applyConnectPolicy(&p, ""); err != nil {
			t.Fatal(err)
		}
		if !p.TLSEnable || p.CACert != "/srv/ca.pem" || p.ClientCert != "/srv/c.pem" || p.ClientKey != "/srv/c.key" {
			t.Fatalf("TLS not applied: %+v", p)
		}
		if p.Password != "s3cret" || !p.NoPasswordEnv {
			t.Fatalf("password = %q, NoPasswordEnv = %v", p.Password, p.NoPasswordEnv)
		}
	})

	t.Run("an explicit password goes through without the list", func(t *testing.T) {
		p := emit.ConnectParams{Brokers: []string{"any:9092"}, SASLMechanism: "scram-sha-512", Username: "u"}
		if err := (Security{}).applyConnectPolicy(&p, "given"); err != nil {
			t.Fatal(err)
		}
		if p.Password != "given" || !p.NoPasswordEnv {
			t.Fatalf("password = %q", p.Password)
		}
	})

	t.Run("no SASL needs no list", func(t *testing.T) {
		p := emit.ConnectParams{Brokers: []string{"any:9092"}}
		if err := (Security{CACert: "/srv/ca.pem"}).applyConnectPolicy(&p, ""); err != nil {
			t.Fatal(err)
		}
		if p.Password != "" || !p.TLSEnable || p.CACert != "/srv/ca.pem" || p.ClientCert != "" {
			t.Fatalf("%+v", p)
		}
	})

	t.Run("unlisted broker is refused when a list is set", func(t *testing.T) {
		p := emit.ConnectParams{Brokers: []string{"kafka-1.internal:9092", "evil:9092"}}
		if err := listed.applyConnectPolicy(&p, ""); err == nil {
			t.Fatal("mixed list accepted")
		}
	})
}

func TestSecurityValidate(t *testing.T) {
	for _, tc := range []struct {
		name string
		sec  Security
		want string
	}{
		{"empty", Security{}, ""},
		{"ca only", Security{CACert: "ca.pem"}, ""},
		{"cert without key", Security{ClientCert: "c.pem", AllowedBrokers: []string{"a:1"}}, "together"},
		{"cert without list", Security{ClientCert: "c.pem", ClientKey: "c.key"}, "--allowed-brokers"},
		{"cert with list", Security{ClientCert: "c.pem", ClientKey: "c.key", AllowedBrokers: []string{"a:1"}}, ""},
		{"bad broker", Security{AllowedBrokers: []string{"no-port"}}, "host:port"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.sec.Validate()
			if tc.want == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
		})
	}
}
