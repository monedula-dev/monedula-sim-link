package mapping

import (
	"fmt"
	"strconv"
	"strings"
)

// Free-play single-DC caps, mirroring the documented clamping rules in
// docs/playground-url-api.md (§9–§14) and their source constants in
// src/sim/freePlayTopologies.ts. Anything past these the simulator would clamp
// itself, so monedula-sim-link clamps (or fails) up front and says so.
const (
	// MaxBrokers is MAX_SINGLE_DC_FREE_PLAY_BROKERS: single-DC free play draws
	// at most 10 brokers.
	MaxBrokers = 10
	// MaxPartitionsPerTopic is MAX_SINGLE_DC_FREE_PLAY_PARTITIONS: 24
	// partitions per topic.
	MaxPartitionsPerTopic = 24
	// MaxExtraTopics is MAX_FREE_PLAY_TOPICS: up to 6 topics besides the fixed
	// "orders" primary.
	MaxExtraTopics = 6
	// MaxGroups mirrors MAX_FREE_PLAY_GROUPS: the free-play canvas renders at
	// most 4 consumer groups legibly. monedula-sim-link applies the same cap to the
	// real groups it maps, even though add_group/add_consumer actions carry no
	// engine-enforced limit of their own (docs/mapping.md).
	MaxGroups = 4
	// MaxGroupMembers mirrors MAX_FREE_PLAY_GROUP_MEMBERS: at most 4 members
	// rendered per mapped group.
	MaxGroupMembers = 4
)

// cleanupCode maps a canonical cleanup.policy value to the size slug's 1-2
// char code (CLEANUP_CODES in freePlayTopologies.ts); an unrecognized or empty
// policy falls back to "d" (delete), the simulator's own default.
func cleanupCode(policy string) string {
	switch policy {
	case "compact":
		return "c"
	case "compact,delete":
		return "cd"
	default:
		return "d"
	}
}

// topicShape is one topic's contribution to the size param: structural shape
// plus the config fields DescribeConfigs supplied (zero/empty when unread,
// which renders as the simulator's own default).
type topicShape struct {
	simName        string
	partitions     int
	rf             int
	minISR         int    // 0 ⇒ unknown; buildSize falls back to 1
	cleanupPolicy  string // "" ⇒ unknown; buildSize falls back to "delete"
	retentionMs    *int64
	retentionBytes *int64
	// disklessEnable is KIP-1163 draft `diskless.enable` (v2.0+),
	// mirroring FreePlayTopicSpec.disklessEnable in
	// freePlayTopologies.ts. Real Kafka has no such DescribeConfigs field
	// today, so this mapping never sets it from a live snapshot - it exists
	// so buildSize can encode it byte-identically when a caller does have
	// one (tests, or a future preview-aware source).
	disklessEnable bool
}

// disklessTail is the free-play size slug's session-wide diskless flag +
// timing: tail index 17 (§11.3/§11.4 of docs/playground-url-api.md), independent of
// any single topic's shape. Mirrors FreePlaySize.primaryDiskless /
// .disklessTiming in freePlayTopologies.ts.
type disklessTail struct {
	// primary is true when the PRIMARY ("orders") topic itself is diskless.
	primary bool
	// commitMs and uploadMs are the two editable diskless timings
	// (diskless.append.commit.interval.ms, WAL upload latency); nil ⇒ unset
	// ⇒ the simulator's own defaults. Either may be set without the other.
	commitMs *int
	uploadMs *int
}

// empty reports whether this diskless tail carries nothing to encode (no
// primary flag, no timing override) - buildSize then omits tail index 17
// entirely, keeping every pre-diskless slug byte-identical.
func (d disklessTail) empty() bool {
	return !d.primary && d.commitMs == nil && d.uploadMs == nil
}

// encode renders this diskless tail exactly like encodeDiskless in
// freePlayTopologies.ts: `d` when the primary is diskless, then - only when a
// timing is set - `<commitMs>.<uploadMs>` (either side empty when unset).
func (d disklessTail) encode() string {
	if d.empty() {
		return ""
	}
	var b strings.Builder
	if d.primary {
		b.WriteString("d")
	}
	if d.commitMs != nil || d.uploadMs != nil {
		if d.commitMs != nil {
			b.WriteString(strconv.Itoa(*d.commitMs))
		}
		b.WriteString(".")
		if d.uploadMs != nil {
			b.WriteString(strconv.Itoa(*d.uploadMs))
		}
	}
	return b.String()
}

// EncodeDisklessTail renders the free-play size slug's diskless tail segment
// (tail index 17, §11.3/§11.4 of playground-url-api.md): "d" when the primary
// topic is diskless, then - only when a timing is set - "<commitMs>.<uploadMs>"
// (either side empty when unset). "" when neither is set. Mirrors
// encodeDiskless in freePlayTopologies.ts. Exported so other callers in this
// module (mcpserver's declarative shape generator) reuse this exact format
// instead of re-deriving it, keeping the two byte-identical by construction.
func EncodeDisklessTail(primary bool, commitMs, uploadMs *int) string {
	return disklessTail{primary: primary, commitMs: commitMs, uploadMs: uploadMs}.encode()
}

// escapeSizeName applies the slug name escape from freePlayTopologies.ts
// (escapeNameForSlug): a literal "x" in a topic name would collide with the
// size param's segment separator, so it is stored as "*" and unescaped on
// decode.
func escapeSizeName(name string) string {
	return strings.ReplaceAll(name, "x", "*")
}

// retStr renders an optional retention field: empty when unset, else its
// decimal value (§11.4 leaves retention/tier fields empty when unset).
func retStr(v *int64) string {
	if v == nil {
		return ""
	}
	return strconv.FormatInt(*v, 10)
}

// buildSize renders the free-play single-DC size slug (playground-url-api.md
// §9/§12): base "<brokers>x<partitions>x<rf>" describing the fixed "orders"
// primary topic, plus tail segments for extra topics (index 0), when the
// primary carries a config override its min.ISR/cleanup/retention (index
// 10, §11.3/§11.4), and - KIP-1163 draft, v2.0+ - the primary
// diskless flag + diskless timing (index 17, §11.3/§11.4) - the three
// generator-safe tail fields monedula-sim-link emits. No other tail index is
// used (producers, other groups-in-size, racks, regions, seeds and engine
// timing stay at their faithful defaults).
//
// primary == nil means no real topic named "orders" was selected; the base
// then describes a minimal 1-partition/RF-1 placeholder primary so the slug
// stays well-formed (the primary cannot be removed via the URL; see
// docs/mapping.md). Its config fields are also unset in that case (nothing
// to encode).
//
// Each extra topic encodes as name~partitions~rf~minISR~cleanup~retMs~
// retBytes~tiered~localRetMs; the four config fields carry the topic's real
// DescribeConfigs values when known, falling back to min.ISR 1 / cleanup
// "delete" (the simulator's own defaults) when unread — remote-storage tiering
// is never fetched, so those two trailing fields stay empty. A diskless extra
// topic (t.disklessEnable) appends a 10th field `1`; a classic extra topic
// omits it, so every pre-diskless slug re-encodes byte-identically. All values
// are pre-clamped by the caller, so the slug decodes verbatim (it is a fixed
// point of the simulator's clampFreePlaySize).
func buildSize(brokerCount int, primary *topicShape, extras []topicShape, diskless disklessTail) string {
	pParts, pRF := 1, 1
	var pConfig *topicShape
	if primary != nil {
		pParts, pRF = primary.partitions, primary.rf
		pConfig = primary
	}
	base := fmt.Sprintf("%dx%dx%d", brokerCount, pParts, pRF)

	tail := map[int]string{}
	if len(extras) > 0 {
		enc := make([]string, len(extras))
		for i, t := range extras {
			minISR := t.minISR
			if minISR <= 0 {
				minISR = 1
			}
			enc[i] = fmt.Sprintf("%s~%d~%d~%d~%s~%s~%s~~",
				escapeSizeName(t.simName), t.partitions, t.rf, minISR, cleanupCode(t.cleanupPolicy),
				retStr(t.retentionMs), retStr(t.retentionBytes))
			if t.disklessEnable {
				enc[i] += "~1"
			}
		}
		tail[0] = strings.Join(enc, "!")
	}
	if pConfig != nil && (pConfig.minISR > 0 || pConfig.cleanupPolicy != "" || pConfig.retentionMs != nil || pConfig.retentionBytes != nil) {
		minISR := ""
		if pConfig.minISR > 0 {
			minISR = strconv.Itoa(pConfig.minISR)
		}
		cleanup := ""
		if pConfig.cleanupPolicy != "" {
			cleanup = cleanupCode(pConfig.cleanupPolicy)
		}
		tail[10] = strings.Join([]string{minISR, cleanup, retStr(pConfig.retentionMs), retStr(pConfig.retentionBytes), ""}, "~")
	}
	if !diskless.empty() {
		tail[17] = diskless.encode()
	}
	if len(tail) == 0 {
		return base
	}
	// Tail length is positional (§11.3): the shortest accepted length carrying
	// the highest present index. Only 0, 10 and 17 are ever set here, so the
	// only three possible lengths are 1 (index 0 only), 11 (index 10 present,
	// 17 absent) and 18 (index 17 present).
	length := 1
	if _, ok := tail[10]; ok {
		length = 11
	}
	if _, ok := tail[17]; ok {
		length = 18
	}
	segs := make([]string, length)
	for i, v := range tail {
		segs[i] = v
	}
	return base + "x" + strings.Join(segs, "x")
}
