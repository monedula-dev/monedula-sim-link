package mcpserver

import (
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/monedula-dev/monedula-sim-link/internal/emit"
)

// The cluster_to_url arguments are chosen by the model driving the MCP client,
// and a model can be steered by text it reads (prompt injection, including
// topic names from a cluster). So nothing in a tool call may point the server
// at its own files or send its own secrets to a host the call picked:
//
//   - TLS file paths (CA, client certificate, client key) are server flags,
//     never tool arguments.
//   - Server-held credentials, the client certificate and the PasswordEnvVar
//     fallback, are used only when every bootstrap broker of the call is on
//     the AllowedBrokers list given at startup.
//   - A non-empty AllowedBrokers list limits every cluster_to_url call to
//     those brokers, with or without credentials.

// Security is the server-side connection policy for cluster_to_url, set by the
// operator on the `monedula-sim-link mcp` command line.
type Security struct {
	// CACert, ClientCert and ClientKey are PEM file paths on the server host.
	// Any of them turns TLS on for every cluster_to_url call.
	CACert     string
	ClientCert string
	ClientKey  string
	// AllowedBrokers lists the bootstrap brokers (host:port) cluster_to_url may
	// connect to. Empty means any broker, but then the client certificate and
	// the PasswordEnvVar fallback are never used.
	AllowedBrokers []string
}

// tlsFromFlags reports whether the operator configured any TLS file.
func (s Security) tlsFromFlags() bool {
	return s.CACert != "" || s.ClientCert != "" || s.ClientKey != ""
}

// normalizeBroker lower-cases the host and trims the address so the allow-list
// comparison ignores case and surrounding spaces. IPv6 literals keep their
// brackets through net.SplitHostPort/JoinHostPort.
func normalizeBroker(b string) string {
	b = strings.TrimSpace(b)
	host, port, err := net.SplitHostPort(b)
	if err != nil {
		return strings.ToLower(b)
	}
	return net.JoinHostPort(strings.ToLower(host), port)
}

// brokersAllowed reports whether every broker is on the allow-list. An empty
// allow-list allows nothing here: callers decide what an empty list means.
func (s Security) brokersAllowed(brokers []string) bool {
	if len(s.AllowedBrokers) == 0 {
		return false
	}
	allowed := make(map[string]bool, len(s.AllowedBrokers))
	for _, a := range s.AllowedBrokers {
		allowed[normalizeBroker(a)] = true
	}
	for _, b := range brokers {
		if !allowed[normalizeBroker(b)] {
			return false
		}
	}
	return true
}

// Validate checks the policy at startup: a client certificate needs its key,
// and server-held client credentials need an allow-list to be sent to.
func (s Security) Validate() error {
	if (s.ClientCert == "") != (s.ClientKey == "") {
		return fmt.Errorf("--client-cert and --client-key must be given together")
	}
	if s.ClientCert != "" && len(s.AllowedBrokers) == 0 {
		return fmt.Errorf("--client-cert needs --allowed-brokers: the client certificate is only presented to brokers on that list")
	}
	for _, b := range s.AllowedBrokers {
		if _, _, err := net.SplitHostPort(strings.TrimSpace(b)); err != nil {
			return fmt.Errorf("--allowed-brokers: %q is not host:port", b)
		}
	}
	return nil
}

// applyConnectPolicy fills the TLS files and the password into p and refuses
// a call the policy does not allow. password is the tool argument; the
// PasswordEnvVar fallback is applied here, never inside emit.
func (s Security) applyConnectPolicy(p *emit.ConnectParams, password string) error {
	listed := s.brokersAllowed(p.Brokers)
	if len(s.AllowedBrokers) > 0 && !listed {
		return fmt.Errorf("brokers %v are not all on the server's --allowed-brokers list %v; the MCP server only connects to those", p.Brokers, s.AllowedBrokers)
	}

	if s.tlsFromFlags() {
		p.TLSEnable = true
		p.CACert = s.CACert
		if listed {
			p.ClientCert = s.ClientCert
			p.ClientKey = s.ClientKey
		}
	}

	p.NoPasswordEnv = true
	p.Password = password
	if password == "" && (p.SASLMechanism != "" || p.Username != "") {
		if env := os.Getenv(emit.PasswordEnvVar); env != "" {
			if !listed {
				return fmt.Errorf("the %s password is only sent to brokers on the MCP server's --allowed-brokers list; start the server with --allowed-brokers naming these bootstrap brokers", emit.PasswordEnvVar)
			}
			p.Password = env
		}
	}
	return nil
}
