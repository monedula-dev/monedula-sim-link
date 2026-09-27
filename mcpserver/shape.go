package mcpserver

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/monedula-dev/monedula-sim-link/mapping"
)

// This file maps a declarative cluster shape onto the free-play `cluster` +
// `size` URL parameters (docs/playground-url-api.md §9–§14). It targets the
// generator-safe subset of §13 — base (brokers x partitions x rf), the
// extraTopics tail segment, rackAware, and the primary-topic config — and
// VALIDATES against the §12 clamps instead of emitting a slug the playground
// would silently correct, so a generated link always loads exactly the shape
// that was asked for.

// ClusterShape declares the free-play starting cluster.
type ClusterShape struct {
	Brokers        int          `json:"brokers,omitempty" jsonschema:"total broker count for a single-zone topology (single-dc). For multi-zone topologies use brokersPerZone instead"`
	BrokersPerZone []int        `json:"brokersPerZone,omitempty" jsonschema:"broker count per zone in topology order (active-passive: [primary, dr]; active-active: [west, east]; stretched-3: [dc-a, dc-b, dc-c])"`
	Topics         []TopicShape `json:"topics,omitempty" jsonschema:"topics to create. A topic named 'orders' shapes the primary topic; every other topic becomes an extra topic (at most 6). Omit to keep the default single 'orders' topic"`
	RackAware      bool         `json:"rackAware,omitempty" jsonschema:"true turns on rack-aware placement (single-dc)"`
	// DisklessTiming is KIP-1163 draft (v2.0+): the session-wide
	// diskless timing override, feeding size tail index 17 alongside the
	// primary diskless flag. Mirrors FreePlaySize.disklessTiming in
	// freePlayTopologies.ts. Requires at least one topic with DisklessEnable
	// (validateDisklessTiming); rejected otherwise, mirroring the TS store's
	// "kept only when some topic is diskless" rule.
	DisklessTiming *DisklessTimingShape `json:"disklessTiming,omitempty" jsonschema:"optional KIP-1163 draft diskless timing override (v2.0+); requires a diskless topic (disklessEnable, or the diskless-3az preset)"`
}

// DisklessTimingShape is the editable diskless timing (ms), mirroring
// DisklessTiming in freePlayTopologies.ts. Both fields are optional and
// independently clamped to [100, 5000].
type DisklessTimingShape struct {
	CommitIntervalMs *int `json:"commitIntervalMs,omitempty" jsonschema:"diskless.append.commit.interval.ms override, in [100, 5000]"`
	UploadMs         *int `json:"uploadMs,omitempty" jsonschema:"object-storage WAL upload latency override, in [100, 5000]"`
}

// TopicShape is one topic's structure.
type TopicShape struct {
	Name              string `json:"name" jsonschema:"topic name: letters, digits, '.', '_', '-', at most 40 chars. Note: a name containing '.' cannot be referenced by produce/reassign actions (the action grammar reserves '.'), so prefer '_' or '-'"`
	Partitions        int    `json:"partitions" jsonschema:"partition count (>= 1)"`
	ReplicationFactor int    `json:"replicationFactor" jsonschema:"replication factor (>= 1, at most the broker count of the hosting zone set)"`
	MinInSyncReplicas *int   `json:"minInSyncReplicas,omitempty" jsonschema:"optional min.insync.replicas (1..replicationFactor)"`
	CleanupPolicy     string `json:"cleanupPolicy,omitempty" jsonschema:"optional cleanup policy: delete, compact, or compact,delete"`
	// DisklessEnable is KIP-1163 draft `diskless.enable` (v2.0+).
	// On the primary ("orders") topic it sets the free-play session's primary
	// diskless flag (size tail index 17); on any other topic it appends the
	// extraTopics chunk's 10th field. Rejected (validateTopicConfig) together
	// with a compacted cleanupPolicy or on a mirror topology
	// (active-passive/active-active), mirroring disklessAllowed in
	// freePlayTopologies.ts.
	DisklessEnable bool `json:"disklessEnable,omitempty" jsonschema:"optional KIP-1163 draft diskless.enable (v2.0+); not compatible with a compacted cleanupPolicy or a mirror topology"`
}

// topology mirrors the §10 table.
type topology struct {
	slug              string
	zones             []string
	defaultBrokers    []int
	defaultPartitions int
	defaultRF         int
	brokerCap         int // total across zones (§12: 10 single-dc, 9 otherwise)
	partitionCap      int // 0 = derived (stretched-3)
	mrc               bool
	// templateDiskless is true when the preset always materializes a fixed
	// diskless topic of its own (diskless-3az's `clicks`), independent of
	// anything the caller's shape.topics declare. Mirrors templateTopics(...)
	// .some((t) => t.disklessEnable) in freePlayTopologies.ts, which
	// freePlaySizeHasDiskless folds into its "any topic is diskless" check.
	templateDiskless bool
}

var topologies = map[string]topology{
	"single-dc":      {slug: "single-dc", zones: []string{"single"}, defaultBrokers: []int{3}, defaultPartitions: 3, defaultRF: 3, brokerCap: 10, partitionCap: 24},
	"active-passive": {slug: "active-passive", zones: []string{"primary", "dr"}, defaultBrokers: []int{2, 2}, defaultPartitions: 1, defaultRF: 2, brokerCap: 9, partitionCap: 6},
	"active-active":  {slug: "active-active", zones: []string{"west", "east"}, defaultBrokers: []int{2, 2}, defaultPartitions: 1, defaultRF: 2, brokerCap: 9, partitionCap: 6},
	"stretched-2-5":  {slug: "stretched-2-5", zones: []string{"dc-a", "dc-b"}, brokerCap: 9, mrc: true},
	"stretched-3":    {slug: "stretched-3", zones: []string{"dc-a", "dc-b", "dc-c"}, defaultBrokers: []int{1, 1, 1}, defaultPartitions: 3, defaultRF: 3, brokerCap: 9},
	// KIP-1163/1164 draft (v2.0+): one cluster whose AZs are racks,
	// not zones (FREE_PLAY_ZONES['diskless-3az'] in freePlayTopologies.ts is the
	// same single 'single' zone as single-dc) - the three AZs are the preset's
	// fixed rack cycle (az-a/az-b/az-c) and rack-aware placement, both intrinsic
	// to the type and never URL-encoded, plus a fixed diskless `clicks` extra
	// topic alongside the classic `orders` primary. None of that is a `size`
	// concern for this tool: it materializes automatically once `cluster
	// =diskless-3az` is set, the same way single-dc's template producer/consumer
	// do. brokerCap/partitionCap follow the shared multi-DC caps
	// (MAX_FREE_PLAY_BROKERS=9, MAX_FREE_PLAY_PARTITIONS=6) - diskless-3az does
	// NOT get single-dc's higher caps (freePlayBrokerCap/maxFreePlayPartitions
	// both gate on the exact string "single-dc").
	"diskless-3az": {slug: "diskless-3az", zones: []string{"single"}, defaultBrokers: []int{6}, defaultPartitions: 3, defaultRF: 3, brokerCap: 9, partitionCap: 6, templateDiskless: true},
}

// rfMax is §12's rfMaxFor: the broker set a single RF must fit into.
func rfMax(t topology, brokers []int) int {
	total := 0
	for _, b := range brokers {
		total += b
	}
	switch t.slug {
	case "active-passive":
		return brokers[0]
	case "active-active":
		min := brokers[0]
		for _, b := range brokers[1:] {
			if b < min {
				min = b
			}
		}
		return min
	default: // single-dc, stretched-3
		return total
	}
}

// maxPartitions is §12's partition clamp. For stretched-3 the doc bounds the
// busiest broker to 3 partition strips at the chosen RF; the conservative
// closed form is floor(3*brokers/rf).
func maxPartitions(t topology, brokers []int, rf int) int {
	if t.partitionCap > 0 {
		return t.partitionCap
	}
	total := 0
	for _, b := range brokers {
		total += b
	}
	return 3 * total / rf
}

var topicNameRe = regexp.MustCompile(`^[A-Za-z0-9._-]{1,40}$`)

const maxExtraTopics = 6

var cleanupCodes = map[string]string{"delete": "d", "compact": "c", "compact,delete": "cd"}

// escapeSizeName applies §11.4's name escaping: the slug splits on lowercase
// "x", so a name's "x" is emitted as "*" and restored on decode.
func escapeSizeName(name string) string { return strings.ReplaceAll(name, "x", "*") }

// shapeToParams turns (cluster, shape) into the cluster/size query params.
// Both returns are "" when the shape equals the topology default (the store
// omits default params, §10/§14.1). notes carry non-fatal explanations.
func shapeToParams(cluster string, shape *ClusterShape) (clusterParam, sizeParam string, notes []string, err error) {
	if cluster == "" {
		cluster = "single-dc"
	}
	topo, ok := topologies[cluster]
	if !ok {
		return "", "", nil, fmt.Errorf("cluster %q is not a free-play topology; expected one of single-dc, active-passive, active-active, stretched-2-5, stretched-3, diskless-3az", cluster)
	}
	if cluster != "single-dc" {
		clusterParam = cluster
	}
	if shape == nil {
		return clusterParam, "", nil, nil
	}
	if topo.mrc {
		return "", "", nil, fmt.Errorf("shape: cluster %q uses the MRC sync/observer size form, which this tool does not generate yet; omit shape to open its default (2 sync + 1 observer per DC), or use stretched-3 for a shaped multi-DC cluster", cluster)
	}

	// --- brokers per zone -----------------------------------------------
	brokers, err := resolveBrokers(topo, shape)
	if err != nil {
		return "", "", nil, err
	}

	// --- topics: primary ("orders") vs extras ----------------------------
	var primary *TopicShape
	var extras []TopicShape
	seen := map[string]bool{"orders": true}
	for i, t := range shape.Topics {
		if !topicNameRe.MatchString(t.Name) {
			return "", "", nil, fmt.Errorf("shape.topics[%d].name %q: must match [A-Za-z0-9._-], at most 40 chars", i, t.Name)
		}
		if strings.Contains(t.Name, "!") || strings.Contains(t.Name, "~") {
			return "", "", nil, fmt.Errorf("shape.topics[%d].name %q: contains a size-slug separator", i, t.Name)
		}
		if t.Name == "orders" {
			tt := t
			primary = &tt
			continue
		}
		if seen[t.Name] {
			return "", "", nil, fmt.Errorf("shape.topics[%d].name %q: duplicate topic name (the playground would rename it)", i, t.Name)
		}
		seen[t.Name] = true
		extras = append(extras, t)
	}
	if len(extras) > maxExtraTopics {
		return "", "", nil, fmt.Errorf("shape.topics: %d extra topics beyond the primary 'orders', the free-play cap is %d (MAX_FREE_PLAY_TOPICS)", len(extras), maxExtraTopics)
	}

	partitions, rf := topo.defaultPartitions, topo.defaultRF
	if primary != nil {
		partitions, rf = primary.Partitions, primary.ReplicationFactor
	} else if m := rfMax(topo, brokers); rf > m {
		// Default RF does not fit the requested (smaller) broker set; follow
		// the §12 clamp rather than erroring on a field the caller never set.
		rf = m
		notes = append(notes, fmt.Sprintf("primary topic replication factor lowered to %d to fit %d broker(s)", rf, m))
	}
	if err := validateTopic(topo, brokers, partitions, rf); err != nil {
		return "", "", nil, fmt.Errorf("shape.topics (primary 'orders'): %w", err)
	}
	if primary != nil {
		if err := validateTopicConfig(topo, *primary); err != nil {
			return "", "", nil, fmt.Errorf("shape.topics (primary 'orders'): %w", err)
		}
	}
	for _, t := range extras {
		if err := validateTopic(topo, brokers, t.Partitions, t.ReplicationFactor); err != nil {
			return "", "", nil, fmt.Errorf("shape.topics (%q): %w", t.Name, err)
		}
		if err := validateTopicConfig(topo, t); err != nil {
			return "", "", nil, fmt.Errorf("shape.topics (%q): %w", t.Name, err)
		}
	}

	// --- assemble the slug ------------------------------------------------
	zoneParts := make([]string, len(brokers))
	for i, b := range brokers {
		zoneParts[i] = strconv.Itoa(b)
	}
	base := strings.Join(zoneParts, "-") + "x" + strconv.Itoa(partitions) + "x" + strconv.Itoa(rf)

	// --- diskless timing (KIP-1163 draft, v2.0+) -------------
	anyDiskless := (primary != nil && primary.DisklessEnable) || topo.templateDiskless
	for _, t := range extras {
		if t.DisklessEnable {
			anyDiskless = true
		}
	}
	if err := validateDisklessTiming(shape.DisklessTiming, anyDiskless); err != nil {
		return "", "", nil, fmt.Errorf("shape.disklessTiming: %w", err)
	}

	// The tail is positional (§11.3): index 0 extraTopics, 3 rackAware,
	// 10 primary-topic config, and - KIP-1163 draft, v2.0+ -
	// 17 the primary diskless flag + diskless timing. Emit the shortest
	// accepted tail length that carries the highest present index.
	tail := map[int]string{}
	if len(extras) > 0 {
		chunks := make([]string, len(extras))
		for i, t := range extras {
			chunks[i] = topicChunk(t)
		}
		tail[0] = strings.Join(chunks, "!")
	}
	if shape.RackAware {
		tail[3] = "1"
	}
	if primary != nil {
		if chunk := primaryConfigChunk(*primary); chunk != "" {
			tail[10] = chunk
		}
	}
	var commitMs, uploadMs *int
	if shape.DisklessTiming != nil {
		commitMs, uploadMs = shape.DisklessTiming.CommitIntervalMs, shape.DisklessTiming.UploadMs
	}
	primaryDiskless := primary != nil && primary.DisklessEnable
	if disklessEnc := mapping.EncodeDisklessTail(primaryDiskless, commitMs, uploadMs); disklessEnc != "" {
		tail[17] = disklessEnc
	}

	slug := base
	if len(tail) > 0 {
		maxIdx := 0
		for i := range tail {
			if i > maxIdx {
				maxIdx = i
			}
		}
		// Accepted tail lengths (§11.3): 1, 3, 5, 7, 8, 10, 11, …, 17, 18 - the
		// index-17 diskless field needs the full 18-segment tail (index 17 is
		// the slug's 18th element), which "17" alone cannot hold.
		accepted := []int{1, 3, 5, 7, 8, 10, 11, 12, 13, 14, 15, 16, 17, 18}
		length := 0
		for _, l := range accepted {
			if l >= maxIdx+1 {
				length = l
				break
			}
		}
		segs := make([]string, length)
		for i, v := range tail {
			segs[i] = v
		}
		slug += "x" + strings.Join(segs, "x")
	}

	// A default-equal slug is omitted from the URL (§14.1).
	defParts := make([]string, len(topo.defaultBrokers))
	for i, b := range topo.defaultBrokers {
		defParts[i] = strconv.Itoa(b)
	}
	defaultBase := strings.Join(defParts, "-") + "x" + strconv.Itoa(topo.defaultPartitions) + "x" + strconv.Itoa(topo.defaultRF)
	if slug == defaultBase {
		return clusterParam, "", notes, nil
	}
	return clusterParam, slug, notes, nil
}

func resolveBrokers(topo topology, shape *ClusterShape) ([]int, error) {
	zones := len(topo.zones)
	var brokers []int
	switch {
	case shape.Brokers > 0 && len(shape.BrokersPerZone) > 0:
		return nil, fmt.Errorf("shape: set either brokers or brokersPerZone, not both")
	case len(shape.BrokersPerZone) > 0:
		if len(shape.BrokersPerZone) != zones {
			return nil, fmt.Errorf("shape.brokersPerZone has %d entries but cluster %q has %d zones (%s)", len(shape.BrokersPerZone), topo.slug, zones, strings.Join(topo.zones, ", "))
		}
		brokers = append(brokers, shape.BrokersPerZone...)
	case shape.Brokers > 0:
		if zones != 1 {
			return nil, fmt.Errorf("shape.brokers is for single-zone topologies; cluster %q has %d zones — use shape.brokersPerZone with exactly %d entries", topo.slug, zones, zones)
		}
		brokers = []int{shape.Brokers}
	default:
		brokers = append(brokers, topo.defaultBrokers...)
	}
	total := 0
	for i, b := range brokers {
		if b < 1 {
			return nil, fmt.Errorf("shape: zone %q broker count %d must be >= 1", topo.zones[i], b)
		}
		total += b
	}
	if total > topo.brokerCap {
		return nil, fmt.Errorf("shape: %d total brokers exceeds the %s cap of %d", total, topo.slug, topo.brokerCap)
	}
	return brokers, nil
}

func validateTopic(topo topology, brokers []int, partitions, rf int) error {
	if partitions < 1 {
		return fmt.Errorf("partitions %d must be >= 1", partitions)
	}
	if rf < 1 {
		return fmt.Errorf("replicationFactor %d must be >= 1", rf)
	}
	if m := rfMax(topo, brokers); rf > m {
		return fmt.Errorf("replicationFactor %d exceeds the maximum %d for this broker layout (§12)", rf, m)
	}
	if m := maxPartitions(topo, brokers, rf); partitions > m {
		return fmt.Errorf("partitions %d exceeds the %s cap of %d (§12)", partitions, topo.slug, m)
	}
	return nil
}

// isMirrorType mirrors isMirrorType in freePlayTopologies.ts: the two
// region-axis multi-DC topologies that replicate via mirror links, on which a
// diskless topic (KIP-1163 draft) is never allowed.
func isMirrorType(slug string) bool {
	return slug == "active-passive" || slug == "active-active"
}

func validateTopicConfig(topo topology, t TopicShape) error {
	if t.MinInSyncReplicas != nil {
		if *t.MinInSyncReplicas < 1 || *t.MinInSyncReplicas > t.ReplicationFactor {
			return fmt.Errorf("minInSyncReplicas %d must be within 1..replicationFactor (%d)", *t.MinInSyncReplicas, t.ReplicationFactor)
		}
	}
	if t.CleanupPolicy != "" {
		if _, ok := cleanupCodes[t.CleanupPolicy]; !ok {
			return fmt.Errorf("cleanupPolicy %q: expected delete, compact, or compact,delete", t.CleanupPolicy)
		}
	}
	// KIP-1163 draft `diskless.enable` (v2.0+): mirrors
	// disklessAllowed in freePlayTopologies.ts - not with a compacted cleanup
	// policy, not on a mirror topology. (This tool never sets
	// remoteStorageEnable, the third exclusion, so there is nothing to check
	// for it.)
	if t.DisklessEnable {
		if strings.Contains(t.CleanupPolicy, "compact") {
			return fmt.Errorf("disklessEnable: not compatible with cleanupPolicy %q (KIP-1163 draft, v2.0+)", t.CleanupPolicy)
		}
		if isMirrorType(topo.slug) {
			return fmt.Errorf("disklessEnable: not available on mirror topology %q (KIP-1163 draft, v2.0+)", topo.slug)
		}
	}
	return nil
}

// validateDisklessTiming mirrors the TS store's clampDisklessTiming rule
// (freePlayTopologies.ts): each set field must be an integer in [100, 5000],
// and a timing override is only meaningful when at least one topic is
// diskless - reject it outright instead of silently dropping it, so a caller
// never gets a slug that omits a field it asked for.
func validateDisklessTiming(dt *DisklessTimingShape, anyDiskless bool) error {
	if dt == nil || (dt.CommitIntervalMs == nil && dt.UploadMs == nil) {
		return nil
	}
	if !anyDiskless {
		return fmt.Errorf("requires a diskless topic (disklessEnable, or the diskless-3az preset) (KIP-1163 draft, v2.0+)")
	}
	if dt.CommitIntervalMs != nil && (*dt.CommitIntervalMs < 100 || *dt.CommitIntervalMs > 5000) {
		return fmt.Errorf("commitIntervalMs %d must be within [100, 5000]", *dt.CommitIntervalMs)
	}
	if dt.UploadMs != nil && (*dt.UploadMs < 100 || *dt.UploadMs > 5000) {
		return fmt.Errorf("uploadMs %d must be within [100, 5000]", *dt.UploadMs)
	}
	return nil
}

// topicChunk renders one extraTopics item: 9 "~"-joined fields (§11.4), the
// four retention/tier fields left empty, plus - KIP-1163 draft, v2.0+ -
// a 10th `~1` field appended when the topic is diskless (a
// classic topic omits it, so every pre-diskless chunk stays byte-identical).
func topicChunk(t TopicShape) string {
	minISR := ""
	if t.MinInSyncReplicas != nil {
		minISR = strconv.Itoa(*t.MinInSyncReplicas)
	}
	chunk := strings.Join([]string{
		escapeSizeName(t.Name),
		strconv.Itoa(t.Partitions),
		strconv.Itoa(t.ReplicationFactor),
		minISR,
		cleanupCodes[t.CleanupPolicy], // "" when unset
		"", "", "", "",
	}, "~")
	if t.DisklessEnable {
		chunk += "~1"
	}
	return chunk
}

// primaryConfigChunk renders tail index 10 (5 "~"-joined fields) when the
// primary topic overrides min.ISR or cleanup; "" when neither is set.
func primaryConfigChunk(t TopicShape) string {
	if t.MinInSyncReplicas == nil && t.CleanupPolicy == "" {
		return ""
	}
	minISR := ""
	if t.MinInSyncReplicas != nil {
		minISR = strconv.Itoa(*t.MinInSyncReplicas)
	}
	return strings.Join([]string{minISR, cleanupCodes[t.CleanupPolicy], "", "", ""}, "~")
}
