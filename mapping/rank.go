package mapping

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/monedula-dev/monedula-sim-link/snapshot"
)

// When a selection holds more topics, or more consumer groups read the
// rendered topics, than free play draws, the mapping keeps the most eventful
// ones instead of failing: what someone opening the link most likely wants to
// see. The ranking is deterministic, so the same snapshot always keeps the
// same topics and groups.

// maxListedNames bounds how many dropped names one warning spells out; a
// cluster with hundreds of topics would otherwise flood stderr.
const maxListedNames = 10

// topicRank is the evidence one topic is ranked by.
type topicRank struct {
	name      string
	pinned    bool  // named exactly by the caller (--topics)
	unhealthy int   // partitions with no live leader, or an ISR smaller than the replica set
	lag       int64 // committed-offset lag, summed over every group and partition
	groups    int   // consumer groups with a committed offset on the topic
	records   int64 // retained records: latest minus earliest offset, summed
}

// moreEventful orders topics for keeping: pinned first, then partitions in
// trouble, then consumer lag, then how many groups read the topic, then
// retained records, then name.
func (a topicRank) moreEventful(b topicRank) bool {
	switch {
	case a.pinned != b.pinned:
		return a.pinned
	case a.unhealthy != b.unhealthy:
		return a.unhealthy > b.unhealthy
	case a.lag != b.lag:
		return a.lag > b.lag
	case a.groups != b.groups:
		return a.groups > b.groups
	case a.records != b.records:
		return a.records > b.records
	}
	return a.name < b.name
}

// reason names the strongest piece of evidence, for the keep warning. When
// every kept topic is pinned, being pinned explains nothing, so withPinned is
// false and the evidence that ranked them is named instead.
func (r topicRank) reason(withPinned bool) string {
	switch {
	case r.pinned && withPinned:
		return "listed in --topics"
	case r.unhealthy > 0:
		return plural(r.unhealthy, "partition") + " under-replicated or offline"
	case r.lag > 0:
		return "lag " + strconv.FormatInt(r.lag, 10)
	case r.groups > 0:
		return "read by " + plural(r.groups, "group")
	case r.records > 0:
		return plural64(r.records, "record")
	}
	return "idle"
}

// rankTopic gathers one topic's evidence. online is the broker liveness map;
// a leader that is absent or not online counts as offline.
func rankTopic(t snapshot.Topic, groups []snapshot.Group, online map[string]bool, pinned bool) topicRank {
	r := topicRank{name: t.Name, pinned: pinned}
	latest := make(map[int]int64, len(t.Partitions))
	for _, p := range t.Partitions {
		latest[p.ID] = p.LatestOffset
		if p.Leader == "" || !online[p.Leader] || len(p.ISR) < len(p.Replicas) {
			r.unhealthy++
		}
		if n := p.LatestOffset - p.EarliestOffset; n > 0 {
			r.records += n
		}
	}
	for _, g := range groups {
		offsets, ok := g.Offsets[t.Name]
		if !ok || len(offsets) == 0 {
			continue
		}
		r.groups++
		for pid, committed := range offsets {
			if end, ok := latest[pid]; ok && end > committed {
				r.lag += end - committed
			}
		}
	}
	return r
}

// pickTopics trims the extra topics to MaxExtraTopics. Pinned topics are kept
// first and the free slots go to the most eventful of the rest, with a warning
// that says what was kept and why. More pinned topics than the cap is a cap
// breach routed through overCap (an error unless --clamp); under --clamp the
// most eventful pinned topics are kept. The result is sorted by name, the
// emission order docs/mapping.md documents.
func pickTopics(extras []snapshot.Topic, groups []snapshot.Group, online map[string]bool, pinned map[string]bool, overCap, warnf func(string, ...any)) []snapshot.Topic {
	ranks := make([]topicRank, len(extras))
	byName := make(map[string]snapshot.Topic, len(extras))
	nPinned := 0
	for i, t := range extras {
		ranks[i] = rankTopic(t, groups, online, pinned[t.Name])
		byName[t.Name] = t
		if ranks[i].pinned {
			nPinned++
		}
	}
	sort.Slice(ranks, func(i, j int) bool { return ranks[i].moreEventful(ranks[j]) })
	kept, dropped := ranks[:MaxExtraTopics], ranks[MaxExtraTopics:]

	if nPinned > MaxExtraTopics {
		overCap("--topics lists %d topics besides %q; free play renders at most %d extra - keeping the most eventful: %s; dropping %s",
			nPinned, PrimaryTopicName, MaxExtraTopics, keptList(kept, false), droppedList(dropped))
	} else {
		warnf("selection has %d topics besides %q; free play renders at most %d extra, so it keeps the most eventful: %s; dropping %s (name the topics you want with --topics)",
			len(extras), PrimaryTopicName, MaxExtraTopics, keptList(kept, true), droppedList(dropped))
	}

	out := make([]snapshot.Topic, len(kept))
	for i, r := range kept {
		out[i] = byName[r.name]
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// groupCandidate is a consumer group with a committed offset on at least one
// rendered topic, and its lag across the rendered partitions.
type groupCandidate struct {
	group  snapshot.Group
	topics []string // rendered real topic names it has offsets on, sorted
	lag    int64
}

// pickGroups trims the candidates to MaxGroups, keeping the groups with the
// most lag, then the most members, then by id, with a warning. The result is
// sorted by group id.
func pickGroups(cands []groupCandidate, warnf func(string, ...any)) []groupCandidate {
	ranked := append([]groupCandidate(nil), cands...)
	sort.Slice(ranked, func(i, j int) bool {
		a, b := ranked[i], ranked[j]
		if a.lag != b.lag {
			return a.lag > b.lag
		}
		if len(a.group.Members) != len(b.group.Members) {
			return len(a.group.Members) > len(b.group.Members)
		}
		return a.group.ID < b.group.ID
	})
	kept, dropped := ranked[:MaxGroups], ranked[MaxGroups:]
	keptNames := make([]string, len(kept))
	for i, c := range kept {
		switch {
		case c.lag > 0:
			keptNames[i] = fmt.Sprintf("%s (lag %d)", c.group.ID, c.lag)
		case len(c.group.Members) > 0:
			keptNames[i] = fmt.Sprintf("%s (%s)", c.group.ID, plural(len(c.group.Members), "member"))
		default:
			keptNames[i] = c.group.ID
		}
	}
	droppedNames := make([]string, len(dropped))
	for i, c := range dropped {
		droppedNames[i] = c.group.ID
	}
	warnf("%d consumer groups have committed offsets on the rendered topics; free play renders at most %d, so it keeps the ones with the most lag: %s; dropping %s",
		len(cands), MaxGroups, strings.Join(keptNames, ", "), listNames(droppedNames))

	sort.Slice(kept, func(i, j int) bool { return kept[i].group.ID < kept[j].group.ID })
	return kept
}

func keptList(ranks []topicRank, withPinned bool) string {
	parts := make([]string, len(ranks))
	for i, r := range ranks {
		parts[i] = fmt.Sprintf("%s (%s)", r.name, r.reason(withPinned))
	}
	return strings.Join(parts, ", ")
}

func droppedList(ranks []topicRank) string {
	names := make([]string, len(ranks))
	for i, r := range ranks {
		names[i] = r.name
	}
	return listNames(names)
}

// listNames joins names, spelling out at most maxListedNames of them.
func listNames(names []string) string {
	if len(names) <= maxListedNames {
		return strings.Join(names, ", ")
	}
	return fmt.Sprintf("%s and %d more", strings.Join(names[:maxListedNames], ", "), len(names)-maxListedNames)
}

func plural(n int, noun string) string { return plural64(int64(n), noun) }

func plural64(n int64, noun string) string {
	if n == 1 {
		return "1 " + noun
	}
	return strconv.FormatInt(n, 10) + " " + noun + "s"
}
