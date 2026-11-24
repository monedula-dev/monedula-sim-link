package mcpserver

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/monedula-dev/monedula-sim-link/mapping"
)

// Nisshi (nisshi-3b, v2.3, not released yet; formerly Tansu). Every Nisshi
// broker answers as node 111 and keeps no data of its own: the storage engine,
// PostgreSQL by default or S3, holds every record, offset and group. The
// playground builds every topic at replication factor 1, and its engine
// refuses some actions on a Nisshi cluster: it logs admin_rejected (or
// config_rejected for a config change) and changes nothing.
// build_playground_url refuses them up front with the engine's own copy, so a
// link never carries an action that only shows a refusal, and notes the
// actions the playground takes but does nothing with.

// The engine's refusal copy (website src/sim/nisshi.ts, objectStorage.ts and
// disklessImpl.ts).
const (
	// nisshiPauseRefusal is NISSHI_PAUSE_REFUSAL.
	nisshiPauseRefusal = "A Nisshi broker keeps no state, so a paused one only makes its clients time out; the simulator does not model pausing a Nisshi broker."
	// nisshiPgStorageRefusal is NISSHI_PG_STORAGE_REFUSAL.
	nisshiPgStorageRefusal = "Nisshi's PostgreSQL engine keeps every record in PostgreSQL, not in object storage: stop PostgreSQL instead."
	// nisshiShareGroupRefusal is the reason nisshiConfigIssues gives for a
	// share group on a Nisshi topic.
	nisshiShareGroupRefusal = "Nisshi has no share group APIs (KIP-932)."
	// nisshiPartitionsRefusal and nisshiOneKindRefusal are the reasons the
	// website gives for the config changes nisshiTopicSettingRefusal refuses.
	nisshiPartitionsRefusal = "Nisshi does not implement CreatePartitions, so a topic keeps its partition count (UNSUPPORTED_VERSION)."
	nisshiOneKindRefusal    = "Nisshi has one kind of topic; its storage engine holds every record."
)

// nisshiEngineOf is the storage engine a shape picks on nisshi-3b: "postgres"
// unless shape.nisshi.engine says "s3".
func nisshiEngineOf(shape *ClusterShape) string {
	if shape != nil && shape.Nisshi != nil && shape.Nisshi.Engine == "s3" {
		return "s3"
	}
	return "postgres"
}

// nisshiNormalizeTopic follows nisshiNormalizeConfig on one topic of the
// shape: a replication factor above 1 is built as 1, with min.insync.replicas
// 1, and diskless.enable is implied. It returns a note for each change. A
// factor below 1 or a min.insync.replicas above the asked factor is left for
// the shape's own checks to refuse.
func nisshiNormalizeTopic(t *TopicShape) []string {
	var notes []string
	if t.ReplicationFactor > 1 {
		what := fmt.Sprintf("replicationFactor %d", t.ReplicationFactor)
		if t.MinInSyncReplicas != nil && *t.MinInSyncReplicas > 1 && *t.MinInSyncReplicas <= t.ReplicationFactor {
			what += fmt.Sprintf(" and minInSyncReplicas %d", *t.MinInSyncReplicas)
			one := 1
			t.MinInSyncReplicas = &one
		}
		notes = append(notes, fmt.Sprintf("topic %q: %s built as 1, as the playground does. %s", t.Name, what, nisshiNoReplicasReason))
		t.ReplicationFactor = 1
	}
	if t.DisklessEnable != nil && !*t.DisklessEnable {
		notes = append(notes, fmt.Sprintf("topic %q: disklessEnable false changes nothing on nisshi-3b, where every topic is kept by the storage engine", t.Name))
	}
	return notes
}

// encodeNisshi validates shape.nisshi against the playground's clamp
// (clampDisklessFields) and renders the Nisshi part of size tail index 17:
// `s` for the S3 engine, the upload latency, and `m<ms>` for a PostgreSQL
// maintenance interval. "" on any other cluster, or for PostgreSQL with the
// preset's timing. primary and extras are the shape's topics after
// nisshiNormalizeTopic, for the notes on settings S3 stores but does not apply.
func encodeNisshi(topo topology, shape *ClusterShape, primary *TopicShape, extras []TopicShape) (string, []string, error) {
	if topo.impl != "nisshi" {
		if shape.Nisshi != nil {
			return "", nil, fmt.Errorf("shape.nisshi: cluster %q is not Nisshi; shape.nisshi applies to nisshi-3b only (v2.3, not released yet)", topo.slug)
		}
		return "", nil, nil
	}
	var maintenance *int
	if n := shape.Nisshi; n != nil {
		if n.Engine != "" && n.Engine != "postgres" && n.Engine != "s3" {
			return "", nil, fmt.Errorf("shape.nisshi.engine %q: expected postgres or s3", n.Engine)
		}
		if n.MaintenanceMs != nil {
			if n.Engine == "s3" {
				return "", nil, fmt.Errorf("shape.nisshi.maintenanceMs: the S3 engine runs no maintenance sweep (it stores retention and compaction but does not apply them), and the playground drops the interval there; leave it unset or use engine postgres")
			}
			if *n.MaintenanceMs < nisshiMaintenanceMinMs || *n.MaintenanceMs > nisshiMaintenanceMaxMs {
				return "", nil, fmt.Errorf("shape.nisshi.maintenanceMs %d must be within [%d, %d]", *n.MaintenanceMs, nisshiMaintenanceMinMs, nisshiMaintenanceMaxMs)
			}
			maintenance = n.MaintenanceMs
		}
	}
	s3 := nisshiEngineOf(shape) == "s3"
	var uploadMs *int
	if shape.DisklessTiming != nil {
		uploadMs = shape.DisklessTiming.UploadMs
	}
	var notes []string
	if uploadMs != nil && !s3 {
		notes = append(notes, fmt.Sprintf("disklessTiming.uploadMs %d is the S3 engine's batch PUT; the PostgreSQL engine does not use it (the link keeps it, as the playground does)", *uploadMs))
	}
	if s3 {
		topics := extras
		if primary != nil {
			topics = append([]TopicShape{*primary}, extras...)
		}
		for _, t := range topics {
			if strings.Contains(t.CleanupPolicy, "compact") {
				notes = append(notes, fmt.Sprintf("topic %q: cleanupPolicy %s is stored but not applied on Nisshi's S3 engine (only its PostgreSQL and SQLite engines apply retention and compaction), so the topic keeps every record", t.Name, t.CleanupPolicy))
			}
		}
	}
	return mapping.EncodeNisshiTail(s3, uploadMs, maintenance), notes, nil
}

// nisshiActionChecks applies the Nisshi refusals to a build_playground_url
// request whose cluster and shape shapeToParams already accepted. It returns
// notes for the actions the playground takes but does nothing with, and an
// error naming the first action the playground would refuse.
func nisshiActionChecks(cluster string, shape *ClusterShape, actions []ActionInput) ([]string, error) {
	topo, ok := topologies[cluster]
	if !ok || topo.impl != "nisshi" {
		return nil, nil
	}
	engine := nisshiEngineOf(shape)
	// The starting partition count of each topic: a config change asking for
	// more is refused.
	partitions := map[string]int{"orders": topo.defaultPartitions}
	if shape != nil {
		for _, t := range shape.Topics {
			partitions[t.Name] = t.Partitions
		}
	}

	var notes []string
	for i, a := range actions {
		fail := func(format string, args ...any) error {
			return fmt.Errorf("actions[%d] (kind %q): refused on %s: %s", i, a.Kind, cluster, fmt.Sprintf(format, args...))
		}
		const rejected = " The playground logs admin_rejected and changes nothing."
		switch a.Kind {
		case "pause_broker":
			return nil, fail("%s%s Kill the broker instead.", nisshiPauseRefusal, rejected)
		case "stop_object_storage", "slow_object_storage":
			if engine == "postgres" {
				return nil, fail("%s%s Set shape.nisshi.engine s3 for the S3 engine, or use stop_coordinator_db.", nisshiPgStorageRefusal, rejected)
			}
		case "start_object_storage":
			if engine == "postgres" {
				notes = append(notes, fmt.Sprintf("actions[%d] start_object_storage: does nothing on the PostgreSQL engine, which keeps every record in PostgreSQL", i))
			}
		case "stop_coordinator_db", "start_coordinator_db":
			if engine == "s3" {
				notes = append(notes, fmt.Sprintf("actions[%d] %s: does nothing on the S3 engine, which has no PostgreSQL (stop_object_storage stops the S3 engine)", i, a.Kind))
			}
		case "reassign_partition":
			return nil, fail("%s%s", nisshiNoReplicasReason, rejected)
		case "change_replication_factor":
			if a.TargetReplicationFactor != nil && *a.TargetReplicationFactor > 1 {
				return nil, fail("%s%s", nisshiNoReplicasReason, rejected)
			}
		case "add_share_group":
			return nil, fail("%s The playground logs admin_rejected and creates no share group. Read the topic with a consumer group.", nisshiShareGroupRefusal)
		case "config_change":
			topic, field, ok := topicConfigPath(a.Path)
			if !ok || a.Value == nil {
				continue
			}
			v := *a.Value
			reason := ""
			switch field {
			case "partitions":
				n, err := strconv.Atoi(v)
				if have, known := partitions[topic]; known && err == nil && n > have {
					reason = nisshiPartitionsRefusal
				}
			case "remoteStorageEnable":
				if v == "true" {
					reason = nisshiOneKindRefusal
				}
			case "disklessEnable":
				reason = nisshiOneKindRefusal
			}
			if reason != "" {
				return nil, fail("%s=%s: %s The playground logs config_rejected and changes nothing.", a.Path, v, reason)
			}
		}
	}
	return notes, nil
}

// topicConfigPath splits a config_change path topics.<topic>.<field>. A topic
// name in an action path never contains '.', so the field is the last segment.
func topicConfigPath(path string) (topic, field string, ok bool) {
	rest, ok := strings.CutPrefix(path, "topics.")
	if !ok {
		return "", "", false
	}
	i := strings.LastIndex(rest, ".")
	if i <= 0 || i == len(rest)-1 {
		return "", "", false
	}
	return rest[:i], rest[i+1:], true
}
