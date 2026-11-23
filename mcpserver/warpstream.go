package mcpserver

import (
	"fmt"
	"sort"
)

// WarpStream (warpstream-3az, v2.3, release stage next). The playground's
// engine refuses some actions on a WarpStream cluster: it logs admin_rejected
// and changes nothing. build_playground_url refuses them up front with the
// engine's own copy, so a link never carries an action that only shows a
// refusal.

// warpstreamStorageRefusal is WARPSTREAM_STORAGE_REFUSAL (website
// src/sim/objectStorage.ts).
const warpstreamStorageRefusal = "WarpStream's docs do not describe what Agents do when object storage is unavailable or slow, so the simulator does not model it."

// warpstreamAgentStartRefusal is WARPSTREAM_AGENT_START_REFUSAL (website
// src/sim/warpstream.ts): restart_broker and add_broker while the control
// plane is down.
const warpstreamAgentStartRefusal = "A WarpStream Agent cannot start while the control plane is unreachable."

// warpstreamShareGroupRefusal is the reason warpstreamConfigIssues gives for a
// share group on a WarpStream topic (website src/sim/warpstream.ts). The
// engine refuses add_share_group on any diskless topic, and every WarpStream
// topic is diskless.
const warpstreamShareGroupRefusal = "WarpStream does not support share groups (its protocol support page lists no share-group APIs)."

// warpstreamActionChecks applies the WarpStream refusals to a
// build_playground_url request whose cluster shapeToParams already accepted,
// and returns an error naming the first action the playground would refuse.
func warpstreamActionChecks(cluster string, actions []ActionInput) error {
	topo, ok := topologies[cluster]
	if !ok || topo.impl != "warpstream" {
		return nil
	}
	// The playground replays the actions sorted by at, ties in list order; the
	// control plane's state at an action is what came before it in that order.
	order := make([]int, len(actions))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(a, b int) bool { return actions[order[a]].At < actions[order[b]].At })

	down := false
	for _, i := range order {
		a := actions[i]
		fail := func(format string, args ...any) error {
			return fmt.Errorf("actions[%d] (kind %q): %s The playground logs admin_rejected and changes nothing.", i, a.Kind, fmt.Sprintf(format, args...))
		}
		switch a.Kind {
		case "stop_control_plane":
			down = true
		case "start_control_plane":
			down = false
		case "stop_object_storage", "slow_object_storage":
			return fail("refused on %s: %s", cluster, warpstreamStorageRefusal)
		case "restart_broker", "add_broker":
			if down {
				return fail("refused on %s: %s A stop_control_plane comes before it with no start_control_plane in between; start the control plane first.", cluster, warpstreamAgentStartRefusal)
			}
		case "reassign_partition":
			if len(a.Replicas) > 1 {
				return fail("refused on %s: %s Name one Agent, the partition's new home.", cluster, warpstreamNoReplicasReason)
			}
		case "change_replication_factor":
			return fail("refused on %s: %s", cluster, warpstreamNoReplicasReason)
		case "add_share_group":
			return fail("refused on %s: %s Read the topic with a consumer group.", cluster, warpstreamShareGroupRefusal)
		}
	}
	return nil
}
