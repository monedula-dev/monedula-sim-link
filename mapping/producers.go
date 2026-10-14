package mapping

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/monedula-dev/monedula-sim-link/snapshot"
)

// Producers are opt-in (--producers): DescribeProducers needs READ on each topic,
// so a default run never reads them (snapshot.FetchOptions.Producers). When the
// snapshot carries them, the producers that wrote to the rendered topics lately
// are added to the link as persistent free-play producers; docs/mapping.md has
// the whole design.

const (
	// MaxProducers mirrors MAX_FREE_PLAY_PRODUCERS (src/sim/freePlayTopologies.ts),
	// which is also the engine's MAX_PRODUCERS (src/sim/producerPool.ts): an
	// add_producer past 4 live producers is a silent no-op, so more would not
	// even render.
	MaxProducers = 4

	// DefaultProducersWindow is the --producers-window default: a producer is
	// active when it wrote to a partition within this long before the snapshot.
	DefaultProducersWindow = 15 * time.Minute
)

// producerSimID names a producer in the simulator: "pid-" and the Kafka
// producer id (a non-negative int64, so at most 19 digits and 23 characters in
// all, well inside the 40 the other generated ids stay under). The id is a raw
// action-log field and sits in a config path (`producers.<id>.config...`): the
// digits and the dash keep it clear of every reserved separator and of the
// template producer `p1` and the transient `kpN`, which are gone before it is
// added.
func producerSimID(id int64) string { return "pid-" + strconv.FormatInt(id, 10) }

// producerPlan is one producer chosen for the link.
type producerPlan struct {
	id         int64
	simID      string
	topic      string // simulator name of the topic it is pinned to
	partitions int    // active partitions it wrote to, across the rendered topics
	lastWrite  int64  // its newest LastTimestamp among them, ms since the epoch
	openTxn    []string
}

// planProducers picks the producers the link adds, and warns about everything
// that limits what it can show. Active means a LastTimestamp within `window`
// before the snapshot time on a rendered partition. They are ranked by how many
// partitions they wrote to, then by the most recent write, then by producer id;
// the top MaxProducers are kept. Each is pinned to the rendered topic it wrote
// to on the most partitions (ties go to the earlier topic in emission order:
// the primary, then alphabetical).
func planProducers(rep *snapshot.ProducersRead, mapped []mappedTopic, window time.Duration, warnf func(string, ...any)) []producerPlan {
	if window <= 0 {
		window = DefaultProducersWindow
	}
	problems, unavailable := warnProducersRead(rep, mapped, warnf)
	if unavailable {
		return nil
	}

	nowMs, windowMs := rep.At.UnixMilli(), window.Milliseconds()
	type agg struct {
		plan     producerPlan
		perTopic []int // active partitions per rendered topic, by index in mapped
	}
	byID := map[int64]*agg{}
	for ti := range mapped {
		mt := &mapped[ti]
		for _, p := range mt.parts {
			for _, w := range p.Producers {
				// An unknown timestamp cannot be shown to be inside the window.
				if w.LastTimestamp < 0 || nowMs-w.LastTimestamp > windowMs {
					continue
				}
				a := byID[w.ProducerID]
				if a == nil {
					a = &agg{plan: producerPlan{id: w.ProducerID, simID: producerSimID(w.ProducerID)}, perTopic: make([]int, len(mapped))}
					byID[w.ProducerID] = a
				}
				a.plan.partitions++
				a.perTopic[ti]++
				if w.LastTimestamp > a.plan.lastWrite {
					a.plan.lastWrite = w.LastTimestamp
				}
				if w.CurrentTxnStartOffset >= 0 {
					a.plan.openTxn = append(a.plan.openTxn, mt.real+"/"+strconv.Itoa(p.ID))
				}
			}
		}
	}

	ranked := make([]producerPlan, 0, len(byID))
	for _, a := range byID {
		best := 0
		for ti, n := range a.perTopic {
			if n > a.perTopic[best] {
				best = ti
			}
		}
		a.plan.topic = mapped[best].sim
		ranked = append(ranked, a.plan)
	}
	sort.Slice(ranked, func(i, j int) bool {
		a, b := ranked[i], ranked[j]
		switch {
		case a.partitions != b.partitions:
			return a.partitions > b.partitions
		case a.lastWrite != b.lastWrite:
			return a.lastWrite > b.lastWrite
		}
		return a.id < b.id
	})

	if len(ranked) == 0 {
		if !problems {
			warnf("--producers: no producer wrote to the rendered topics in the last %s (DescribeProducers lists none, or only older ones); the link has no producers", windowString(window))
		}
		return nil
	}
	if len(ranked) > MaxProducers {
		kept, dropped := ranked[:MaxProducers], ranked[MaxProducers:]
		keptNames := make([]string, len(kept))
		for i, p := range kept {
			keptNames[i] = fmt.Sprintf("%s (%s)", p.simID, plural(p.partitions, "partition"))
		}
		droppedNames := make([]string, len(dropped))
		for i, p := range dropped {
			droppedNames[i] = p.simID
		}
		warnf("%d producers wrote to the rendered topics in the last %s; free play renders at most %d, so it keeps the ones that wrote to the most partitions: %s; dropping %s",
			len(ranked), windowString(window), MaxProducers, strings.Join(keptNames, ", "), listNames(droppedNames))
		ranked = kept
	}

	// The URL grammar has no transactional producer (no transactional.id in the
	// config whitelist or in the size slug, and begin_transaction does nothing
	// for a producer without one), so a producer with an open transaction is
	// shown as the idempotent producer it is underneath.
	for _, p := range ranked {
		if len(p.openTxn) == 0 {
			continue
		}
		at := p.openTxn[0]
		if len(p.openTxn) > 1 {
			at += fmt.Sprintf(" and %d more", len(p.openTxn)-1)
		}
		warnf("producer %d (shown as %s) has an open transaction on %s; the playground cannot express a transactional producer (the URL grammar has no transactional.id), so it is shown as an idempotent one",
			p.id, p.simID, at)
	}
	return ranked
}

// warnProducersRead turns the outcome of the DescribeProducers read into
// warnings. problems reports that something limited what was read (so an empty
// result is not reported as "no producers"), unavailable that nothing usable was
// read at all.
func warnProducersRead(rep *snapshot.ProducersRead, mapped []mappedTopic, warnf func(string, ...any)) (problems, unavailable bool) {
	if rep.Unsupported {
		warnf("--producers: this cluster does not support DescribeProducers (KIP-664, Kafka 2.8 or newer; UNSUPPORTED_VERSION or the broker is too old); the link has no producers")
		return true, true
	}
	if rep.ClusterDenied {
		warnf("--producers: DescribeProducers was refused with CLUSTER_AUTHORIZATION_FAILED; the link has no producers")
		return true, true
	}
	if len(rep.DeniedTopics) > 0 {
		rendered := map[string]bool{}
		for _, mt := range mapped {
			rendered[mt.real] = true
		}
		var denied []string
		for _, name := range rep.DeniedTopics {
			if rendered[name] {
				denied = append(denied, name)
			}
		}
		if len(denied) > 0 {
			problems = true
			tail := "producers writing only to those topics are left out"
			if len(denied) == len(mapped) {
				tail, unavailable = "the link has no producers", true
			}
			warnf("--producers: DescribeProducers needs READ on a topic and was refused with TOPIC_AUTHORIZATION_FAILED for %s; %s", listNames(denied), tail)
		}
	}
	for _, e := range rep.Errors {
		problems = true
		warnf("--producers: DescribeProducers failed: %s; the producers of those partitions are left out", e)
	}
	return problems, unavailable
}

// windowString renders the activity window the way a person writes it: 15m, 1h,
// 90s, not Go's 15m0s.
func windowString(d time.Duration) string {
	s := d.String()
	if strings.HasSuffix(s, "m0s") {
		s = strings.TrimSuffix(s, "0s")
	}
	if strings.HasSuffix(s, "h0m") {
		s = strings.TrimSuffix(s, "0m")
	}
	return s
}
