// Package simurl assembles a kafka-simulator playground URL from an action log.
// It owns the URL-level concerns of docs/playground-url-api.md: the plain vs
// compressed representation choice (§4) and always percent-encoding the
// actions parameter value (§7.4).
package simurl

import (
	"bytes"
	"compress/flate"
	"encoding/base64"
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/monedula-dev/monedula-sim-link/actionlog"
)

// DefaultBaseURL is the public playground endpoint. Callers can override it
// (e.g. a local dev server) via Params.BaseURL.
const DefaultBaseURL = "https://monedula.dev/kafka-simulator/playground"

// CompressionThreshold is the plain-string length past which the compressed
// form is attempted (COMPRESSION_THRESHOLD in actionLog.ts).
const CompressionThreshold = 1500

const compressedPrefix = "~1"

// EncodeActionsForURL chooses the actions-parameter representation: the plain
// log by default, or the compressed form (§4) once the plain string exceeds
// CompressionThreshold and the compressed form is strictly shorter. Returns ""
// for an empty log (the caller then omits the actions parameter).
//
// The compressed form is "~1" + base64url(raw DEFLATE(plain)) — one of the two
// layouts docs/playground-url-api.md §4 accepts under the "~1" prefix (the other
// is lz-string, which the playground itself emits; DecodeActionsFromURL decodes
// both). The plain form is still the preferred, most portable target; keep logs
// under the threshold when you can (§9).
func EncodeActionsForURL(entries []actionlog.Entry) (string, error) {
	plain, err := actionlog.Encode(entries)
	if err != nil {
		return "", err
	}
	if plain == "" {
		return "", nil
	}
	if len(plain) <= CompressionThreshold {
		return plain, nil
	}
	comp, err := compress(plain)
	if err != nil {
		return "", err
	}
	candidate := compressedPrefix + comp
	// Never let the "compressed" form be longer than plain (incompressible
	// fallback): plain wins unless compressed is strictly shorter.
	if len(candidate) < len(plain) {
		return candidate, nil
	}
	return plain, nil
}

// PlainActions expands an actions parameter value to its plain action-log
// string, routing the "~1" compressed prefix; compressed reports which form
// the value used. It does not parse the log — pair it with actionlog.Decode
// or actionlog.DecodeTolerant.
func PlainActions(value string) (plain string, compressed bool, err error) {
	if value == "" {
		return "", false, nil
	}
	if strings.HasPrefix(value, compressedPrefix) {
		p, err := decompress(value[len(compressedPrefix):])
		if err != nil {
			return "", true, fmt.Errorf("compressed actions value does not inflate: %w", err)
		}
		return p, true, nil
	}
	return value, false, nil
}

// DecodeActionsFromURL is the inverse of EncodeActionsForURL: it routes the
// "~1" compressed prefix vs the plain form and decodes back to entries. Used by
// the self-check that verifies a link before it is printed.
func DecodeActionsFromURL(value string) ([]actionlog.Entry, error) {
	plain, _, err := PlainActions(value)
	if err != nil {
		return nil, err
	}
	return actionlog.Decode(plain)
}

// Params configures Build. Scenario defaults to "free"; Cluster and Size are
// opaque free-play passthrough strings, emitted only when non-empty.
type Params struct {
	BaseURL  string
	Scenario string
	Cluster  string
	Size     string
	Actions  []actionlog.Entry
}

// Build assembles the full playground URL. The actions value is always
// percent-encoded (§7.4). It also self-checks that the emitted actions value
// decodes back to the same log, so a caller never ships a link the playground
// would silently mangle.
func Build(p Params) (string, error) {
	base := p.BaseURL
	if base == "" {
		base = DefaultBaseURL
	}
	scenario := p.Scenario
	if scenario == "" {
		scenario = "free"
	}

	actionsVal, err := EncodeActionsForURL(p.Actions)
	if err != nil {
		return "", err
	}
	if actionsVal != "" {
		roundTrip, err := DecodeActionsFromURL(actionsVal)
		if err != nil {
			return "", fmt.Errorf("self-check decode failed: %w", err)
		}
		reEnc, err := actionlog.Encode(roundTrip)
		if err != nil {
			return "", fmt.Errorf("self-check re-encode failed: %w", err)
		}
		plain, _ := actionlog.Encode(p.Actions)
		if reEnc != plain {
			return "", fmt.Errorf("self-check mismatch: the action log does not round-trip through the URL codec")
		}
	}

	var b strings.Builder
	b.WriteString(base)
	b.WriteString("?scenario=")
	b.WriteString(url.QueryEscape(scenario))
	if actionsVal != "" {
		b.WriteString("&actions=")
		b.WriteString(url.QueryEscape(actionsVal))
	}
	if p.Cluster != "" {
		b.WriteString("&cluster=")
		b.WriteString(url.QueryEscape(p.Cluster))
	}
	if p.Size != "" {
		b.WriteString("&size=")
		b.WriteString(url.QueryEscape(p.Size))
	}
	return b.String(), nil
}

func compress(s string) (string, error) {
	var buf bytes.Buffer
	w, err := flate.NewWriter(&buf, flate.BestCompression)
	if err != nil {
		return "", err
	}
	if _, err := w.Write([]byte(s)); err != nil {
		return "", err
	}
	if err := w.Close(); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf.Bytes()), nil
}

// decompress inflates a "~1" payload, accepting BOTH accepted layouts (§4):
// raw DEFLATE + base64url (this tool's output) and lz-string base64url (what the
// playground emits). Raw DEFLATE is tried first — it self-validates, so an
// lz-string blob almost never inflates as valid DEFLATE; when it does, the
// result is required to look like a plain action log (starts with an uppercase
// kind code) before it is preferred, otherwise the lz-string reading wins. This
// mirrors the playground decoder's deterministic routing.
func decompress(b64 string) (string, error) {
	deflated, deflateErr := inflateRawB64(b64)
	if deflateErr == nil && looksLikePlainLog(deflated) {
		return deflated, nil
	}
	if lz, lzErr := inflateLZB64(b64); lzErr == nil && lz != "" {
		return lz, nil
	}
	if deflateErr == nil {
		// DEFLATE decoded but wasn't log-shaped and lz-string didn't apply;
		// return the DEFLATE result rather than inventing an error.
		return deflated, nil
	}
	return "", deflateErr
}

// inflateRawB64 decodes base64url then inflates raw DEFLATE (RFC 1951).
func inflateRawB64(b64 string) (string, error) {
	data, err := base64.RawURLEncoding.DecodeString(b64)
	if err != nil {
		return "", err
	}
	r := flate.NewReader(bytes.NewReader(data))
	defer r.Close()
	out, err := io.ReadAll(r)
	if err != nil {
		return "", err
	}
	return string(out), nil
}

// inflateLZB64 maps a base64url value to the lz-string base64 alphabet and
// decompresses it (the layout the playground emits).
func inflateLZB64(b64url string) (string, error) {
	lzAlphabet := strings.NewReplacer("-", "+", "_", "/").Replace(b64url)
	return decompressLZBase64(lzAlphabet)
}

// looksLikePlainLog reports whether s starts with an uppercase kind code, which
// every plain action-log entry does — used to disambiguate the two "~1" layouts.
func looksLikePlainLog(s string) bool {
	return len(s) > 0 && s[0] >= 'A' && s[0] <= 'Z'
}
