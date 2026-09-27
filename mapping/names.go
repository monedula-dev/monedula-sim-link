package mapping

import (
	"fmt"
	"regexp"
	"strconv"
)

// PrimaryTopicName is the simulator's fixed free-play primary topic. The size
// param's base segments (partitions, RF) describe it; it cannot be renamed
// from the URL. A real topic named exactly "orders" maps onto it; otherwise a
// minimal 1-partition placeholder occupies the slot (see mapping.md).
const PrimaryTopicName = "orders"

// simNameDisallowed matches every character that cannot appear in a simulator
// topic name monedula-sim-link emits. The simulator's own sanitizer keeps
// [A-Za-z0-9._-] (sanitizeTopicName in src/sim/freePlayTopologies.ts), but the
// action-log grammar reserves "." as a list separator, so dots must go too:
// action entries (produce_record, reassign_partition) reference topics raw.
var simNameDisallowed = regexp.MustCompile(`[^A-Za-z0-9_-]`)

// maxSimTopicNameLen mirrors the simulator sanitizer's 40-char cap.
const maxSimTopicNameLen = 40

// simTopicName maps one real topic name to a simulator-safe name: every
// disallowed character becomes "-" (keeping names readable — "my.topic" →
// "my-topic"), truncated to 40 chars. The result is a fixed point of the
// simulator's own sanitizer, so the name in the size slug is exactly the topic
// name the simulator creates and the action log can reference.
func simTopicName(real string) string {
	name := simNameDisallowed.ReplaceAllString(real, "-")
	if len(name) > maxSimTopicNameLen {
		name = name[:maxSimTopicNameLen]
	}
	if name == "" {
		name = "topic"
	}
	return name
}

// assignSimTopicNames maps real topic names (in the given order) to unique
// simulator names, deduping against `reserved` (the primary topic) and each
// other with a deterministic "-2", "-3", … suffix. Collisions are only
// possible when sanitization merges distinct real names.
func assignSimTopicNames(reals []string, reserved ...string) map[string]string {
	return assignUniqueNames(reals, simTopicName, maxSimTopicNameLen, reserved...)
}

// assignUniqueNames maps `reals` (in order) to unique names via `sanitize`,
// deduping against `reserved` and each other with a deterministic "-2", "-3",
// … suffix truncated to fit `maxLen`. Shared by topic, group and member id
// naming so every real→simulator id table follows the same collision rule.
func assignUniqueNames(reals []string, sanitize func(string) string, maxLen int, reserved ...string) map[string]string {
	taken := make(map[string]struct{}, len(reals)+len(reserved))
	for _, r := range reserved {
		taken[r] = struct{}{}
	}
	out := make(map[string]string, len(reals))
	for _, real := range reals {
		name := sanitize(real)
		if _, clash := taken[name]; clash {
			for i := 2; ; i++ {
				suffix := "-" + strconv.Itoa(i)
				base := name
				if len(base)+len(suffix) > maxLen {
					base = base[:maxLen-len(suffix)]
				}
				candidate := base + suffix
				if _, c := taken[candidate]; !c {
					name = candidate
					break
				}
			}
		}
		taken[name] = struct{}{}
		out[real] = name
	}
	return out
}

// assignUniqueNamesIndexed is assignUniqueNames for callers whose `reals` list
// may itself contain duplicates (e.g. two group members sharing one real
// client id) — a map keyed by the real string would collapse such duplicates
// onto one entry, silently losing one member. Returns names aligned by index
// instead.
func assignUniqueNamesIndexed(reals []string, sanitize func(string) string, maxLen int) []string {
	taken := make(map[string]struct{}, len(reals))
	out := make([]string, len(reals))
	for i, real := range reals {
		name := sanitize(real)
		if _, clash := taken[name]; clash {
			for n := 2; ; n++ {
				suffix := "-" + strconv.Itoa(n)
				base := name
				if len(base)+len(suffix) > maxLen {
					base = base[:maxLen-len(suffix)]
				}
				candidate := base + suffix
				if _, c := taken[candidate]; !c {
					name = candidate
					break
				}
			}
		}
		taken[name] = struct{}{}
		out[i] = name
	}
	return out
}

// idDisallowed matches every character not safe in an action-log raw field
// (the reserved separators , : @ ~ . |) — the constraint for group and member
// ids, which (unlike topic names) never need to fit the simulator's own name
// sanitizer or appear in the `size` slug.
var idDisallowed = regexp.MustCompile(`[,:@~.|]`)

// maxSimIDLen is a generous, arbitrary legibility cap for group/member ids
// (real client ids can be long, e.g. a UUID-suffixed rdkafka client id).
const maxSimIDLen = 40

// simGroupID and simMemberID sanitize a real consumer-group / member id into
// an action-log-safe one: every reserved separator becomes "-", truncated to
// maxSimIDLen. Unlike topic names, there is no simulator-side name sanitizer
// or `size`-slug alphabet to also satisfy — only the raw-field separator rule.
func simGroupID(real string) string  { return sanitizeID(real, "group") }
func simMemberID(real string) string { return sanitizeID(real, "member") }

func sanitizeID(real, fallback string) string {
	name := idDisallowed.ReplaceAllString(real, "-")
	if len(name) > maxSimIDLen {
		name = name[:maxSimIDLen]
	}
	if name == "" {
		name = fallback
	}
	return name
}

// simBrokerID formats the simulator broker id for the i-th (0-based) broker in
// the deterministic broker order: broker-1, broker-2, …
func simBrokerID(i int) string {
	return fmt.Sprintf("broker-%d", i+1)
}
