package mcpserver

import "fmt"

// Redpanda Cloud Topics (redpanda-3az, v2.3, release stage next). The
// playground's engine refuses to remove a broker that holds replicas of a
// Redpanda partition: it logs admin_rejected and changes nothing.
// build_playground_url refuses that up front with the engine's copy when the
// request alone shows the broker holds one, and notes the rule where replica
// placement decides.

// redpandaRemoveBrokerReason is REDPANDA_REMOVE_BROKER_REASON (website
// src/sim/redpanda.ts).
const redpandaRemoveBrokerReason = "a broker that holds replicas of Redpanda partitions leaves them by a Raft membership change, which the simulator does not model; move its replicas off first, or kill it"

// redpandaActionChecks applies the Redpanda refusal to a build_playground_url
// request whose cluster and shape shapeToParams already accepted. It returns
// notes for the outcomes it cannot decide, and an error naming the first
// remove_broker the playground would refuse.
func redpandaActionChecks(cluster string, shape *ClusterShape, actions []ActionInput) ([]string, error) {
	topo, ok := topologies[cluster]
	if !ok || topo.impl != "redpanda" {
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
	// A topic whose replication factor equals the broker count has a replica
	// of every partition on every starting broker. orders keeps the preset's
	// RF unless the shape sets it (shapeToParams lowers the default to fit).
	fullTopic := ""
	ordersRF := min(topo.defaultRF, rfMax(topo, brokers))
	for _, t := range s.Topics {
		if t.Name == "orders" {
			ordersRF = t.ReplicationFactor
		}
	}
	if ordersRF == total {
		fullTopic = "orders"
	}
	for _, t := range s.Topics {
		if fullTopic == "" && t.Name != "orders" && t.ReplicationFactor == total {
			fullTopic = t.Name
		}
	}

	var notes []string
	for i, a := range actions {
		if a.Kind != "remove_broker" {
			continue
		}
		n, initial := initialBrokerIndex(a.BrokerID, total)
		if !initial {
			continue
		}
		if fullTopic != "" && !replicasMovedBefore(actions, i) {
			return nil, fmt.Errorf("actions[%d] (kind %q): refused on %s: %s holds replicas of every partition of %s (replication factor %d on %d broker(s)), and %s. The playground logs admin_rejected and changes nothing; use kill_broker, or a reassign_partition before it", i, a.Kind, cluster, a.BrokerID, fullTopic, total, total, redpandaRemoveBrokerReason)
		}
		notes = append(notes, fmt.Sprintf("actions[%d] remove_broker %s (broker %d of %d at the start): the playground refuses the removal (admin_rejected) if %s still holds a replica of any partition, since %s; this tool does not compute replica placement", i, a.BrokerID, n, total, a.BrokerID, redpandaRemoveBrokerReason))
	}
	return notes, nil
}
