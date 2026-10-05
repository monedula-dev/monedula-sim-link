package snapshot

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// TopicInfo is the minimal per-topic metadata topic selection needs.
type TopicInfo struct {
	Name string
	// Internal marks internal/system topics: flagged internal by the broker
	// (e.g. __consumer_offsets) or carrying the conventional leading underscore
	// (e.g. _schemas). Excluded unless explicitly listed by exact name.
	Internal bool
}

// Selection filters cluster topics down to what the simulator should render.
// Zero value = every non-internal topic.
type Selection struct {
	// Exact topic names (--topics). An exact name matches even an internal
	// topic — listing one is the explicit opt-in.
	Exact []string
	// Regex (--topics-regex) matches additional NON-internal topics.
	Regex *regexp.Regexp
}

// IsEmpty reports whether the selection has no constraints (match all
// non-internal topics).
func (s Selection) IsEmpty() bool {
	return len(s.Exact) == 0 && s.Regex == nil
}

// Apply returns the selected topic names, sorted ascending. It fails when any
// exact name is absent from the cluster, and when the overall selection
// matches nothing — a wrong filter should never produce an empty simulator
// link silently.
func (s Selection) Apply(topics []TopicInfo) ([]string, error) {
	byName := make(map[string]TopicInfo, len(topics))
	for _, t := range topics {
		byName[t.Name] = t
	}
	var missing []string
	picked := map[string]bool{}
	for _, name := range s.Exact {
		if _, ok := byName[name]; ok {
			picked[name] = true
		} else {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return nil, fmt.Errorf("topics not found in the cluster: %s", strings.Join(missing, ", "))
	}
	for _, t := range topics {
		if t.Internal || picked[t.Name] {
			continue
		}
		if s.IsEmpty() || (s.Regex != nil && s.Regex.MatchString(t.Name)) {
			picked[t.Name] = true
		}
	}
	if len(picked) == 0 {
		if s.IsEmpty() {
			return nil, fmt.Errorf("the cluster has no non-internal topics to visualize")
		}
		return nil, fmt.Errorf("topic selection matched nothing (internal topics, meaning topics the broker flags as internal or whose name starts with an underscore, are excluded unless listed exactly via --topics)")
	}
	out := make([]string, 0, len(picked))
	for name := range picked {
		out = append(out, name)
	}
	sort.Strings(out)
	return out, nil
}
