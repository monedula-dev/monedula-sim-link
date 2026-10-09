// Package actionlog encodes and decodes the kafka-simulator playground action
// log — the plain grammar of docs/playground-url-api.md §2. It implements the
// subset a cluster visualizer needs (broker lifecycle, producers, produces,
// consumer / share groups, config changes, reassignment, replication factor,
// replica speed and tiered offload) and is a faithful Go port of the
// TypeScript codec in src/kafka-simulator/actionLog.ts.
//
// The playground decoder is TOLERANT: any entry it cannot parse is dropped
// SILENTLY. To make generated links safe, this package instead FAILS FAST — the
// encoder validates every raw field and refuses non-whitelisted config paths,
// and the decoder returns an error (rather than dropping) on any malformed
// entry. Verify round-trips a log through encode→decode→encode so a generator
// can prove its output before shipping a URL.
package actionlog

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// MaxURLActions caps the number of actions a URL may carry (MAX_URL_ACTIONS in
// actionLog.ts). The playground keeps only the earliest 2000 after sorting and
// silently drops the tail; the encoder refuses to exceed the cap instead.
const MaxURLActions = 2000

// Entry is one timed action: the action plus its simulation time in ms.
type Entry struct {
	At     int
	Action Action
}

// Encode renders entries to the plain action-log string. Entries are sorted by
// At ascending (stable) and comma-joined. Returns "" for an empty log. Errors
// if any field is invalid or the count exceeds MaxURLActions.
func Encode(entries []Entry) (string, error) {
	if len(entries) == 0 {
		return "", nil
	}
	if len(entries) > MaxURLActions {
		return "", fmt.Errorf("action log has %d entries, exceeds MAX_URL_ACTIONS (%d)", len(entries), MaxURLActions)
	}
	sorted := sortedEntries(entries)
	parts := make([]string, len(sorted))
	for i, e := range sorted {
		s, err := encodeEntry(e)
		if err != nil {
			return "", fmt.Errorf("entry %d: %w", i, err)
		}
		parts[i] = s
	}
	return strings.Join(parts, ","), nil
}

// EncodeEntry renders a single entry to its "<KIND>:<payload>@<at>" form. Handy
// for logging / debugging one action; Encode is the log-level entry point.
func EncodeEntry(e Entry) (string, error) {
	return encodeEntry(e)
}

func encodeEntry(e Entry) (string, error) {
	if e.Action == nil {
		return "", fmt.Errorf("nil action")
	}
	pl, err := e.Action.payload()
	if err != nil {
		return "", err
	}
	return e.Action.kind() + ":" + pl + "@" + strconv.Itoa(e.At), nil
}

func sortedEntries(entries []Entry) []Entry {
	out := make([]Entry, len(entries))
	copy(out, entries)
	sort.SliceStable(out, func(i, j int) bool { return out[i].At < out[j].At })
	return out
}

// Decode parses a plain action-log string back into entries, sorted by At
// ascending. Unlike the tolerant playground decoder it returns an error on the
// FIRST malformed entry rather than dropping it, so callers can detect data
// loss. An empty string decodes to a nil slice.
func Decode(raw string) ([]Entry, error) {
	if raw == "" {
		return nil, nil
	}
	fields := strings.Split(raw, ",")
	out := make([]Entry, 0, len(fields))
	for _, entry := range fields {
		e, err := decodeEntry(entry)
		if err != nil {
			return nil, fmt.Errorf("entry %q: %w", entry, err)
		}
		out = append(out, e)
	}
	return sortedEntries(out), nil
}

func decodeEntry(entry string) (Entry, error) {
	atIdx := strings.LastIndex(entry, "@")
	if atIdx < 0 {
		return Entry{}, fmt.Errorf("missing @<at>")
	}
	head := entry[:atIdx]
	atRaw := entry[atIdx+1:]
	at, ok := canonicalInt(atRaw)
	if !ok {
		return Entry{}, fmt.Errorf("non-canonical at %q", atRaw)
	}
	firstColon := strings.Index(head, ":")
	if firstColon < 0 {
		return Entry{}, fmt.Errorf("missing kind separator")
	}
	kind := head[:firstColon]
	payload := head[firstColon+1:]
	if payload == "" {
		return Entry{}, fmt.Errorf("empty payload")
	}
	action, err := decodeAction(kind, payload)
	if err != nil {
		return Entry{}, err
	}
	return Entry{At: at, Action: action}, nil
}

// Dropped records one entry DecodeTolerant could not parse and the reason —
// exactly the entries the playground's tolerant decoder would discard.
type Dropped struct {
	Raw    string
	Reason string
}

// DecodeTolerant mirrors the PLAYGROUND's decoder: every malformed entry is
// dropped (recorded with its reason) instead of failing the whole log, and the
// surviving entries are returned sorted by At ascending. Use it to preview
// what an arbitrary actions string would actually load; use Decode when data
// loss must be an error.
func DecodeTolerant(raw string) ([]Entry, []Dropped) {
	if raw == "" {
		return nil, nil
	}
	var out []Entry
	var dropped []Dropped
	for _, entry := range strings.Split(raw, ",") {
		e, err := decodeEntry(entry)
		if err != nil {
			dropped = append(dropped, Dropped{Raw: entry, Reason: err.Error()})
			continue
		}
		out = append(out, e)
	}
	return sortedEntries(out), dropped
}

// Verify proves a log round-trips: encode → decode → encode must be stable. A
// generator should call this before shipping a URL, because the playground
// silently drops entries it cannot parse. Returns nil when the log is safe.
func Verify(entries []Entry) error {
	enc, err := Encode(entries)
	if err != nil {
		return err
	}
	decoded, err := Decode(enc)
	if err != nil {
		return fmt.Errorf("re-decode failed: %w", err)
	}
	reEnc, err := Encode(decoded)
	if err != nil {
		return fmt.Errorf("re-encode failed: %w", err)
	}
	if reEnc != enc {
		return fmt.Errorf("round-trip mismatch:\n  encoded: %s\n  redecoded+encoded: %s", enc, reEnc)
	}
	return nil
}
