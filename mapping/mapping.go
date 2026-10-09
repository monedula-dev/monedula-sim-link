// Package mapping turns a snapshot.ClusterSnapshot into a simulator state: the
// free-play `size` query param describing the cluster shape plus a validated
// action log reproducing placement, failures and data volume. The full design
// — what is represented, every approximation, and what is knowingly left out —
// lives in docs/mapping.md; the comments here cover only local invariants.
package mapping

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/monedula-dev/monedula-sim-link/actionlog"
	"github.com/monedula-dev/monedula-sim-link/snapshot"
)

// Options tunes the mapping.
type Options struct {
	// MaxRecordsPerPartition caps the synthetic trailing records produced per
	// partition. Zero uses DefaultMaxRecordsPerPartition.
	MaxRecordsPerPartition int
	// Clamp selects the policy for the broker and partition caps, and for a
	// --topics list longer than the extra-topic cap: true trims
	// deterministically with a warning; false fails with a message listing
	// every violation. A wider selection (every topic, or a regex) and the
	// consumer-group cap never fail: the most eventful topics and groups are
	// kept, with a warning (rank.go).
	Clamp bool
	// PinnedTopics are the topic names the caller listed exactly (--topics).
	// An over-cap selection keeps them before ranking the rest.
	PinnedTopics []string
}

// DefaultMaxRecordsPerPartition is the --max-records-per-partition default.
const DefaultMaxRecordsPerPartition = 8

// Result is the mapped simulator state.
type Result struct {
	Entries []actionlog.Entry
	// Size is the free-play size slug (always non-empty).
	Size string
	// Cluster is the free-play topology param; "" means the single-dc default,
	// the only topology this mapping targets.
	Cluster string
	// Warnings lists every approximation or clamp applied, for stderr.
	Warnings []string
	// TopicNames maps real topic name → simulator topic name.
	TopicNames map[string]string
	// BrokerNames maps real broker id → simulator broker id.
	BrokerNames map[string]string
	// GroupNames maps real consumer-group id → simulator group id, for every
	// group the mapping actually emitted (a group skipped for having no
	// committed offset on a rendered topic, or dropped by MaxGroups, is absent).
	GroupNames map[string]string
}

// stepMs spaces successive actions on the simulation timeline: enough for the
// playground to animate them distinctly, small enough to keep the log short.
const stepMs = 250

// startMs is the first action's time.
const startMs = 500

// produceSettleMs is the gap between a topic's last produce and its producer's
// removal, covering the free-play produce flush delay so every record acks
// before the producer disappears.
const produceSettleMs = 2000

// templateProducerID is the producer the single-DC free-play template starts
// with (TEMPLATES['single-dc'] in src/sim/freePlayTopologies.ts). The mapping
// removes it first so the playground's auto-produce loop cannot add records
// beyond the snapshot-derived ones.
const templateProducerID = "p1"

// mappedTopic is one selected topic after clamping and renaming.
type mappedTopic struct {
	real       string
	sim        string
	partitions int // sim partition count (post-clamp)
	rf         int
	parts      []snapshot.Partition // kept partitions, id ascending, id < partitions
	config     snapshot.TopicConfig
}

// Map converts a snapshot into a simulator state. The returned action log is
// already Verify-ed (encode→decode→encode stable); callers still run their own
// URL-level checks.
func Map(snap snapshot.ClusterSnapshot, opts Options) (*Result, error) {
	maxRecords := opts.MaxRecordsPerPartition
	if maxRecords <= 0 {
		maxRecords = DefaultMaxRecordsPerPartition
	}
	res := &Result{
		TopicNames:  map[string]string{},
		BrokerNames: map[string]string{},
		GroupNames:  map[string]string{},
	}
	var violations []string
	warnf := func(format string, args ...any) {
		res.Warnings = append(res.Warnings, fmt.Sprintf(format, args...))
	}
	// overCap routes a cap breach: a warning under --clamp, a hard error line
	// otherwise.
	overCap := func(format string, args ...any) {
		msg := fmt.Sprintf(format, args...)
		if opts.Clamp {
			res.Warnings = append(res.Warnings, msg)
		} else {
			violations = append(violations, msg)
		}
	}

	// --- Brokers: universe, deterministic order, cap, sim ids ---------------
	// The universe is metadata brokers plus any id referenced by a replica /
	// ISR / leader list but absent from metadata — those are dead-but-assigned
	// nodes, represented as killed brokers.
	online := map[string]bool{}
	var realBrokers []string
	seen := map[string]bool{}
	for _, b := range snap.Brokers {
		if b.ID == "" || seen[b.ID] {
			continue
		}
		seen[b.ID] = true
		online[b.ID] = b.Online
		realBrokers = append(realBrokers, b.ID)
	}
	for _, t := range snap.Topics {
		for _, p := range t.Partitions {
			for _, id := range referencedBrokers(p) {
				if id == "" || seen[id] {
					continue
				}
				seen[id] = true
				online[id] = false
				realBrokers = append(realBrokers, id)
				warnf("broker %s appears in replica lists but not in metadata; representing it as a killed broker", id)
			}
		}
	}
	if len(realBrokers) == 0 {
		return nil, fmt.Errorf("snapshot has no brokers")
	}
	sortBrokerIDs(realBrokers)
	if len(realBrokers) > MaxBrokers {
		overCap("cluster has %d brokers; single-DC free play renders at most %d — keeping the %d lowest ids, dropping %s",
			len(realBrokers), MaxBrokers, MaxBrokers, strings.Join(realBrokers[MaxBrokers:], ", "))
		realBrokers = realBrokers[:MaxBrokers]
	}
	simBrokers := make([]string, len(realBrokers))
	for i, id := range realBrokers {
		simBrokers[i] = simBrokerID(i)
		res.BrokerNames[id] = simBrokers[i]
	}

	// --- Topics: order, caps, sim names, shapes -----------------------------
	// Emission order: the "orders" primary first (it maps onto the simulator's
	// fixed primary topic), then the rest alphabetically by real name.
	topics := make([]snapshot.Topic, len(snap.Topics))
	copy(topics, snap.Topics)
	sort.Slice(topics, func(i, j int) bool { return topics[i].Name < topics[j].Name })
	var primarySnap *snapshot.Topic
	var extraSnaps []snapshot.Topic
	for i := range topics {
		if topics[i].Name == PrimaryTopicName && primarySnap == nil {
			primarySnap = &topics[i]
		} else {
			extraSnaps = append(extraSnaps, topics[i])
		}
	}
	if len(extraSnaps) > MaxExtraTopics {
		pinned := map[string]bool{}
		for _, name := range opts.PinnedTopics {
			pinned[strings.TrimSpace(name)] = true
		}
		extraSnaps = pickTopics(extraSnaps, snap.Groups, online, pinned, overCap, warnf)
	}

	extraReals := make([]string, len(extraSnaps))
	for i, t := range extraSnaps {
		extraReals[i] = t.Name
	}
	simNames := assignSimTopicNames(extraReals, PrimaryTopicName)
	for real, sim := range simNames {
		if sim != real {
			warnf("topic %q is not representable verbatim; renamed to %q in the simulator", real, sim)
		}
	}

	var ordered []snapshot.Topic
	if primarySnap != nil {
		ordered = append(ordered, *primarySnap)
	}
	ordered = append(ordered, extraSnaps...)

	var mapped []mappedTopic
	for _, t := range ordered {
		sim := t.Name
		if t.Name != PrimaryTopicName {
			sim = simNames[t.Name]
		}
		mt, err := shapeTopic(t, sim, len(simBrokers), overCap, warnf)
		if err != nil {
			return nil, err
		}
		mapped = append(mapped, mt)
		res.TopicNames[t.Name] = sim
	}
	if len(violations) > 0 {
		return nil, fmt.Errorf("selection exceeds what the simulator can render (pass --clamp to trim deterministically instead):\n  - %s",
			strings.Join(violations, "\n  - "))
	}

	// --- Size slug -----------------------------------------------------------
	var primaryShape *topicShape
	var extraShapes []topicShape
	for i := range mapped {
		mt := &mapped[i]
		shape := topicShape{
			simName: mt.sim, partitions: mt.partitions, rf: mt.rf,
			cleanupPolicy:  mt.config.CleanupPolicy,
			retentionMs:    mt.config.RetentionMs,
			retentionBytes: mt.config.RetentionBytes,
		}
		if mt.config.MinInSyncReplicas != nil {
			minISR := *mt.config.MinInSyncReplicas
			if minISR < 1 {
				minISR = 1
			} else if minISR > mt.rf {
				minISR = mt.rf
			}
			shape.minISR = minISR
		}
		if mt.real == PrimaryTopicName {
			primaryShape = &shape
		} else {
			extraShapes = append(extraShapes, shape)
		}
	}
	if primaryShape == nil {
		warnf("no selected topic is named %q; the simulator's fixed primary topic stays as a minimal 1-partition placeholder", PrimaryTopicName)
	}
	// KIP-1163 draft `diskless.enable` (v2.0+) has no
	// DescribeConfigs equivalent on a real cluster today, so this mapping
	// never sets a diskless flag or timing override from a live snapshot -
	// buildSize's diskless tail stays empty and every slug it emits is
	// byte-identical to before the feature existed.
	res.Size = buildSize(len(simBrokers), primaryShape, extraShapes, disklessTail{})

	// --- Action log ----------------------------------------------------------
	at := startMs
	emit := func(a actionlog.Action) {
		res.Entries = append(res.Entries, actionlog.Entry{At: at, Action: a})
		at += stepMs
	}

	// 1. Remove the template producer before anything else.
	emit(actionlog.RemoveProducer{ProducerID: templateProducerID})

	// 2. Reassign every partition whose real replica set differs from the
	// simulator's default placement (set comparison: replica ORDER is preference
	// only and the simulator elects its own leader either way; docs/mapping.md).
	for i := range mapped {
		mt := &mapped[i]
		defaults := defaultPlacement(simBrokers, mt.partitions, mt.rf)
		for _, p := range mt.parts {
			simReplicas, droppedAny := mapReplicas(p.Replicas, res.BrokerNames)
			if droppedAny {
				warnf("%s/%d: some replicas sit on brokers dropped by the broker cap; the reassignment keeps only rendered brokers", mt.real, p.ID)
			}
			if len(simReplicas) == 0 {
				if len(p.Replicas) > 0 {
					warnf("%s/%d: every replica broker was dropped by the broker cap; leaving the partition on default placement", mt.real, p.ID)
				}
				continue
			}
			if !sameSet(simReplicas, defaults[p.ID]) {
				emit(actionlog.ReassignPartition{Topic: mt.sim, Partition: p.ID, Replicas: simReplicas})
			}
		}
	}

	// 3. Kill offline brokers (metadata-offline or replica-referenced ghosts).
	killed := map[string]bool{}
	for i, real := range realBrokers {
		if !online[real] {
			killed[real] = true
			emit(actionlog.KillBroker{BrokerID: simBrokers[i]})
		}
	}

	// 4. Approximate under-replication: an alive broker assigned to a partition
	// but missing from its ISR gets its follower replication slowed, so the
	// simulator shows it lagging out of the ISR too (approximation: broker-wide,
	// not per-partition; docs/mapping.md).
	lagging := map[string]bool{}
	for i := range mapped {
		mt := &mapped[i]
		for _, p := range mt.parts {
			inISR := map[string]bool{}
			for _, id := range p.ISR {
				inISR[id] = true
			}
			for _, id := range p.Replicas {
				if _, kept := res.BrokerNames[id]; !kept {
					continue
				}
				if !inISR[id] && !killed[id] && !lagging[id] {
					lagging[id] = true
					warnf("broker %s is out of the ISR of %s/%d; approximating by slowing ALL of its follower replication", id, mt.real, p.ID)
				}
			}
		}
	}
	for i, real := range realBrokers {
		if lagging[real] {
			emit(actionlog.SetReplicaSpeed{BrokerID: simBrokers[i], Slow: true})
		}
	}

	// 5. Data volume: per topic, one transient producer emitting up to
	// maxRecords keyed records per partition — keys brute-forced through the
	// simulator's partitioner so each record lands on its intended partition.
	producerSeq := 0
	for i := range mapped {
		mt := &mapped[i]
		need := make([]int, mt.partitions)
		total := 0
		for _, p := range mt.parts {
			n := p.LatestOffset - p.EarliestOffset
			if n <= 0 {
				continue
			}
			if n > int64(maxRecords) {
				n = int64(maxRecords)
			}
			if !hasLiveReplica(p, res.BrokerNames, online) {
				warnf("%s/%d holds records but no rendered replica broker is alive; skipping its produces (they could never acknowledge)", mt.real, p.ID)
				continue
			}
			need[p.ID] = int(n)
			total += int(n)
		}
		if total == 0 {
			continue
		}
		keys, err := keysForPartitions(mt.partitions, need)
		if err != nil {
			return nil, fmt.Errorf("topic %s: %w", mt.real, err)
		}
		producerSeq++
		producerID := "kp" + strconv.Itoa(producerSeq)
		emit(actionlog.AddProducer{ProducerID: producerID, Topic: mt.sim})
		for pid := 0; pid < mt.partitions; pid++ {
			for _, key := range keys[pid] {
				k := key
				emit(actionlog.ProduceRecord{ProducerID: producerID, Topic: mt.sim, Key: &k})
			}
		}
		// Remove the producer once its records have settled, keeping at most one
		// synthetic producer alive at a time (free play caps producers at 4).
		at += produceSettleMs - stepMs
		emit(actionlog.RemoveProducer{ProducerID: producerID})
	}

	// 6. Consumer groups: map each real group that has a committed offset on at
	// least one rendered topic. add_group creates the group empty; add_consumer
	// then joins its (capped) real members. A group with measurable aggregate
	// lag across its rendered partitions has its auto.offset.reset switched to
	// earliest right after creation (before its first poll), so the simulator
	// shows it draining the topic's synthetic backlog instead of starting
	// caught up — real committed offsets are not carried exactly; see
	// docs/mapping.md. When more groups qualify than MaxGroups, the ones with
	// the most lag are kept (pickGroups).
	mappedByReal := make(map[string]*mappedTopic, len(mapped))
	for i := range mapped {
		mappedByReal[mapped[i].real] = &mapped[i]
	}
	groups := make([]snapshot.Group, len(snap.Groups))
	copy(groups, snap.Groups)
	sort.Slice(groups, func(i, j int) bool { return groups[i].ID < groups[j].ID })
	var cands []groupCandidate
	var skipped []string
	for _, g := range groups {
		c := groupCandidate{group: g}
		for topic := range g.Offsets {
			if _, ok := res.TopicNames[topic]; ok {
				c.topics = append(c.topics, topic)
			}
		}
		if len(c.topics) == 0 {
			skipped = append(skipped, g.ID)
			continue
		}
		sort.Strings(c.topics)
		for _, topic := range c.topics {
			for _, p := range mappedByReal[topic].parts {
				committed, ok := g.Offsets[topic][p.ID]
				if !ok {
					continue
				}
				if d := p.LatestOffset - committed; d > 0 {
					c.lag += d
				}
			}
		}
		cands = append(cands, c)
	}
	if len(skipped) > 0 {
		warnf("skipping %s with no committed offsets on a rendered topic: %s", plural(len(skipped), "consumer group"), listNames(skipped))
	}
	if len(cands) > MaxGroups {
		cands = pickGroups(cands, warnf)
	}

	groupReals := make([]string, len(cands))
	for i, c := range cands {
		groupReals[i] = c.group.ID
	}
	groupNames := assignUniqueNames(groupReals, simGroupID, maxSimIDLen)

	for _, c := range cands {
		g, lag := c.group, c.lag
		simGroup := groupNames[g.ID]
		res.GroupNames[g.ID] = simGroup
		groupTopicsSim := make([]string, len(c.topics))
		for i, topic := range c.topics {
			groupTopicsSim[i] = res.TopicNames[topic]
		}
		emit(actionlog.AddGroup{GroupID: simGroup, Topics: groupTopicsSim})

		if lag > 0 {
			emit(actionlog.ConfigChange{Path: "group." + simGroup + ".consumer.autoOffsetReset", Value: "earliest"})
			warnf("group %s has ~%d records of committed-offset lag across its rendered partitions; approximating with auto.offset.reset=earliest so it visibly drains the topic's synthetic backlog (exact offsets are not carried)", g.ID, lag)
		}

		members := g.Members
		if len(members) > MaxGroupMembers {
			warnf("group %s has %d members; free play renders at most %d — keeping the first %d", g.ID, len(members), MaxGroupMembers, MaxGroupMembers)
			members = members[:MaxGroupMembers]
		}
		memberReals := make([]string, len(members))
		for i, m := range members {
			real := m.ClientID
			if real == "" {
				real = m.ID
			}
			if real == "" {
				real = "m" + strconv.Itoa(i+1)
			}
			memberReals[i] = real
		}
		// Indexed (not map-based) dedup: two real members can legitimately share
		// the same ClientID, and a map keyed by that string would collapse them.
		memberNames := assignUniqueNamesIndexed(memberReals, simMemberID, maxSimIDLen)
		for _, sim := range memberNames {
			emit(actionlog.AddConsumer{GroupID: simGroup, MemberID: sim})
		}
	}

	if len(res.Entries) > actionlog.MaxURLActions {
		return nil, fmt.Errorf("mapping needs %d actions but a URL carries at most %d — reduce --max-records-per-partition or narrow --topics",
			len(res.Entries), actionlog.MaxURLActions)
	}
	if err := actionlog.Verify(res.Entries); err != nil {
		return nil, fmt.Errorf("mapping produced a non-round-tripping log: %w", err)
	}
	return res, nil
}

// shapeTopic clamps one topic to the simulator's per-topic caps and collects
// its kept partitions (ids ascending, deduped).
func shapeTopic(t snapshot.Topic, sim string, brokerCount int, overCap, warnf func(string, ...any)) (mappedTopic, error) {
	parts := make([]snapshot.Partition, len(t.Partitions))
	copy(parts, t.Partitions)
	sort.Slice(parts, func(i, j int) bool { return parts[i].ID < parts[j].ID })
	nParts := 0
	rf := 1
	kept := parts[:0]
	for _, p := range parts {
		if p.ID < 0 || (len(kept) > 0 && p.ID == kept[len(kept)-1].ID) {
			continue
		}
		kept = append(kept, p)
		if p.ID+1 > nParts {
			nParts = p.ID + 1
		}
		if len(p.Replicas) > rf {
			rf = len(p.Replicas)
		}
	}
	if nParts == 0 {
		return mappedTopic{}, fmt.Errorf("topic %s has no partitions", t.Name)
	}
	if nParts > MaxPartitionsPerTopic {
		overCap("topic %s has %d partitions; free play renders at most %d per topic — keeping partitions 0..%d",
			t.Name, nParts, MaxPartitionsPerTopic, MaxPartitionsPerTopic-1)
		nParts = MaxPartitionsPerTopic
		trimmed := kept[:0]
		for _, p := range kept {
			if p.ID < MaxPartitionsPerTopic {
				trimmed = append(trimmed, p)
			}
		}
		kept = trimmed
	}
	if rf > brokerCount {
		warnf("topic %s has replication factor %d but only %d brokers are rendered; using RF %d", t.Name, rf, brokerCount, brokerCount)
		rf = brokerCount
	}
	return mappedTopic{real: t.Name, sim: sim, partitions: nParts, rf: rf, parts: kept, config: t.Config}, nil
}

// referencedBrokers lists every broker id a partition mentions.
func referencedBrokers(p snapshot.Partition) []string {
	out := make([]string, 0, len(p.Replicas)+len(p.ISR)+1)
	out = append(out, p.Replicas...)
	out = append(out, p.ISR...)
	if p.Leader != "" {
		out = append(out, p.Leader)
	}
	return out
}

// sortBrokerIDs orders broker ids numerically when every id is an integer
// (the Kafka node-id case), falling back to lexicographic otherwise, so
// broker-N assignment is deterministic and human-predictable.
func sortBrokerIDs(ids []string) {
	allNumeric := true
	nums := make(map[string]int, len(ids))
	for _, id := range ids {
		n, err := strconv.Atoi(id)
		if err != nil {
			allNumeric = false
			break
		}
		nums[id] = n
	}
	sort.Slice(ids, func(i, j int) bool {
		if allNumeric {
			return nums[ids[i]] < nums[ids[j]]
		}
		return ids[i] < ids[j]
	})
}

// mapReplicas translates a real replica list to sim broker ids, preserving
// order, dropping (and reporting) replicas on brokers the cap removed and any
// duplicates.
func mapReplicas(replicas []string, brokerNames map[string]string) (out []string, droppedAny bool) {
	seen := map[string]bool{}
	for _, id := range replicas {
		sim, ok := brokerNames[id]
		if !ok {
			droppedAny = true
			continue
		}
		if seen[sim] {
			continue
		}
		seen[sim] = true
		out = append(out, sim)
	}
	return out, droppedAny
}

func sameSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	set := make(map[string]bool, len(a))
	for _, s := range a {
		set[s] = true
	}
	for _, s := range b {
		if !set[s] {
			return false
		}
	}
	return true
}

// hasLiveReplica reports whether at least one of the partition's rendered
// replica brokers is online — the precondition for its produces to acknowledge.
func hasLiveReplica(p snapshot.Partition, brokerNames map[string]string, online map[string]bool) bool {
	for _, id := range p.Replicas {
		if _, kept := brokerNames[id]; kept && online[id] {
			return true
		}
	}
	return false
}
