package mcpserver

import (
	"fmt"
	"strconv"
	"strings"
)

// Ursa for Kafka (ursa-3az, v2.3, release stage next). The playground's engine
// refuses some actions on an Ursa cluster: it logs admin_rejected with
// InvalidConfigurationException and changes nothing. build_playground_url
// refuses the ones it can tell from the request alone, with the engine's own
// copy, so a link never carries an action that only shows a refusal; where the
// outcome depends on replica placement this tool does not compute, it notes
// the rule instead.

// ursaStorageRefusal is URSA_STORAGE_REFUSAL (website src/sim/objectStorage.ts).
const ursaStorageRefusal = "Ursa's sources do not describe what brokers do when object storage is unavailable or slow, so the simulator does not model it."

// ursaShareGroupRefusal is the simulator choice behind the engine's
// add_share_group refusal on a diskless topic (URSA_SIMULATOR_CHOICES,
// website src/sim/disklessImpl.ts).
const ursaShareGroupRefusal = "Share groups on diskless topics are refused as the simulator's limit (Ursa supports them)."

// ursaPauseRule is the start of the engine's pause_broker refusal detail
// (applyUrsaPause, website src/sim/ursaPause.ts).
const ursaPauseRule = "The simulator pauses Ursa brokers that hold only diskless partitions"

// ursaActionChecks applies the Ursa refusals to a build_playground_url request
// whose cluster and shape shapeToParams already accepted. It returns notes for
// the outcomes it cannot decide, and an error naming the first action the
// playground would refuse.
func ursaActionChecks(cluster string, shape *ClusterShape, actions []ActionInput) ([]string, error) {
	topo, ok := topologies[cluster]
	if !ok || topo.impl != "ursa" {
		return nil, nil
	}
	s := shape
	if s == nil {
		s = &ClusterShape{}
	}
	brokers, err := resolveBrokers(topo, s)
	if err != nil {
		return nil, err
	}
	total := 0
	for _, b := range brokers {
		total += b
	}
	diskless := map[string]bool{"orders": topo.primaryDisklessDefault}
	for _, t := range s.Topics {
		diskless[t.Name] = topicDiskless(topo, t, t.Name == "orders")
	}

	// A broker of the starting cluster holds a replica of every partition of
	// the disk topic payments while there are at most 3 brokers: payments has
	// RF min(3, brokers). Only a reassignment or a replication-factor change
	// before the pause can move them off it.
	var notes []string
	for i, a := range actions {
		fail := func(format string, args ...any) error {
			return fmt.Errorf("actions[%d] (kind %q): %s", i, a.Kind, fmt.Sprintf(format, args...))
		}
		switch a.Kind {
		case "stop_object_storage", "slow_object_storage":
			return nil, fail("refused on %s: %s The playground logs admin_rejected (InvalidConfigurationException) and changes nothing.", cluster, ursaStorageRefusal)
		case "add_share_group":
			if diskless[a.Topic] {
				return nil, fail("refused on %s: topic %q is diskless. %s Read it with a consumer group, or use a disk topic (disklessEnable false)", cluster, a.Topic, ursaShareGroupRefusal)
			}
		case "pause_broker":
			n, initial := initialBrokerIndex(a.BrokerID, total)
			if !initial {
				continue
			}
			if total <= 3 && !replicasMovedBefore(actions, i) {
				return nil, fail("refused on %s: %s; %s holds disk-topic replicas (payments-0, payments-1, payments-2: the preset's disk topic payments has replication factor %d on %d broker(s)). Add a broker with add_broker and pause that one", cluster, ursaPauseRule, a.BrokerID, total, total)
			}
			notes = append(notes, fmt.Sprintf("actions[%d] pause_broker %s (broker %d of %d at the start): the playground refuses the pause (admin_rejected) if %s holds any replica of a disk-topic partition, such as payments; this tool does not compute replica placement", i, a.BrokerID, n, total, a.BrokerID))
		}
	}
	return notes, nil
}

// initialBrokerIndex reports whether id names one of the starting cluster's
// brokers broker-1..broker-<total>, and its number.
func initialBrokerIndex(id string, total int) (int, bool) {
	rest, ok := strings.CutPrefix(id, "broker-")
	if !ok {
		return 0, false
	}
	n, err := strconv.Atoi(rest)
	if err != nil || n < 1 || n > total || strconv.Itoa(n) != rest {
		return 0, false
	}
	return n, true
}

// replicasMovedBefore reports whether an action the playground replays before
// actions[i] (sorted by at, ties in list order) reassigns a partition or
// changes a replication factor.
func replicasMovedBefore(actions []ActionInput, i int) bool {
	for j, b := range actions {
		if j == i || (b.At > actions[i].At) || (b.At == actions[i].At && j > i) {
			continue
		}
		if b.Kind == "reassign_partition" || b.Kind == "change_replication_factor" {
			return true
		}
	}
	return false
}
