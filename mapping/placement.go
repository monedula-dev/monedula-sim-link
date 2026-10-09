package mapping

import "sort"

// Faithful Go port of the simulator's replica-placement algorithm
// (src/sim/placement.ts — itself a deterministic port of Apache Kafka's
// rack-aware assignReplicasToBrokers with fixedStartIndex=0 /
// startPartitionId=0). The free-play engine routes EVERY topic through it:
// non-rack-aware clusters use a single synthetic rack ("single"), which
// degenerates to Kafka's real non-rack placement (consecutive replicas,
// rotating leader, shift bump every nBrokers partitions). monedula-sim-link needs the
// exact same function to know which partitions' real assignments differ from
// the simulator default (those get a reassign_partition action; identical ones
// are omitted). Verified against the TypeScript output by fixture tests.

type rackBroker struct {
	id   string
	rack string
}

// replicaIndex mirrors the Apache `replicaIndex` helper verbatim: the rotating
// secondary-replica index. Never called when nBrokers == 1 (callers guard via
// rf ≤ nBrokers).
func replicaIndex(firstReplicaIndex, secondReplicaShift, replicaNum, nBrokers int) int {
	shift := 1 + (secondReplicaShift+replicaNum)%(nBrokers-1)
	return (firstReplicaIndex + shift) % nBrokers
}

// rackAlternatedBrokerList groups brokers by rack (racks sorted ascending,
// brokers sorted ascending WITHIN a rack), then round-robins across racks so
// consecutive entries land on different racks. Broker ids sort as plain
// strings — the TypeScript comparator is a UTF-16 code-unit compare, so
// "broker-10" sorts BEFORE "broker-2"; this quirk is load-bearing for clusters
// past 9 brokers and is asserted by the fixture tests.
func rackAlternatedBrokerList(brokers []rackBroker) []string {
	sorted := make([]rackBroker, len(brokers))
	copy(sorted, brokers)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].id < sorted[j].id })

	byRack := make(map[string][]string)
	var racks []string
	for _, b := range sorted {
		if _, ok := byRack[b.rack]; !ok {
			racks = append(racks, b.rack)
		}
		byRack[b.rack] = append(byRack[b.rack], b.id)
	}
	sort.Strings(racks)

	cursors := make([]int, len(racks))
	result := make([]string, 0, len(sorted))
	r := 0
	for len(result) < len(sorted) {
		ri := r % len(racks)
		if cursors[ri] < len(byRack[racks[ri]]) {
			result = append(result, byRack[racks[ri]][cursors[ri]])
			cursors[ri]++
		}
		r++
	}
	return result
}

// assignRackAware returns replicas[partitionID] (leader first) for a topic's
// partitions. RF is clamped to the broker count. With a single rack the
// rack-skip is a no-op and the result degenerates to a distinct-broker
// round-robin.
func assignRackAware(brokers []rackBroker, nPartitions, replicationFactor int) [][]string {
	if len(brokers) == 0 {
		out := make([][]string, nPartitions)
		for i := range out {
			out[i] = []string{}
		}
		return out
	}
	rf := replicationFactor
	if rf > len(brokers) {
		rf = len(brokers)
	}
	arranged := rackAlternatedBrokerList(brokers)
	n := len(arranged)
	rackOf := make(map[string]string, len(brokers))
	rackSet := make(map[string]struct{})
	for _, b := range brokers {
		rackOf[b.id] = b.rack
		rackSet[b.rack] = struct{}{}
	}
	numRacks := len(rackSet)

	const startIndex = 0
	nextReplicaShift := 0
	ret := make([][]string, 0, nPartitions)

	for currentPartitionID := 0; currentPartitionID < nPartitions; currentPartitionID++ {
		if currentPartitionID > 0 && currentPartitionID%n == 0 {
			nextReplicaShift++
		}
		firstReplicaIndex := (currentPartitionID + startIndex) % n
		leader := arranged[firstReplicaIndex]
		replicaBuffer := []string{leader}
		racksWithReplicas := map[string]struct{}{rackOf[leader]: {}}
		brokersWithReplicas := map[string]struct{}{leader: {}}
		k := 0
		for j := 0; j < rf-1; j++ {
			for {
				broker := arranged[replicaIndex(firstReplicaIndex, nextReplicaShift*numRacks, k, n)]
				rack := rackOf[broker]
				_, rackTaken := racksWithReplicas[rack]
				_, brokerTaken := brokersWithReplicas[broker]
				skipForRack := rackTaken && len(racksWithReplicas) < numRacks
				skipForBroker := brokerTaken && len(brokersWithReplicas) < n
				k++
				if !skipForRack && !skipForBroker {
					replicaBuffer = append(replicaBuffer, broker)
					racksWithReplicas[rack] = struct{}{}
					brokersWithReplicas[broker] = struct{}{}
					break
				}
			}
		}
		ret = append(ret, replicaBuffer)
	}
	return ret
}

// defaultPlacement is the simulator's free-play default assignment for a
// non-rack-aware topic: the audited Apache port with every broker on ONE
// synthetic rack (ClusterConfig.faithfulPlacement — see engine.ts
// replicatedPartitionsForTopic). simBrokerIDs are the simulator broker ids
// (broker-1..broker-N) in any order; the algorithm re-sorts internally.
func defaultPlacement(simBrokerIDs []string, nPartitions, replicationFactor int) [][]string {
	brokers := make([]rackBroker, len(simBrokerIDs))
	for i, id := range simBrokerIDs {
		brokers[i] = rackBroker{id: id, rack: "single"}
	}
	return assignRackAware(brokers, nPartitions, replicationFactor)
}
