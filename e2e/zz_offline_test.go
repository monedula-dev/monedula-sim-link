//go:build e2e

// Named zz_ so it runs last: it stops a node, and every other test expects
// the healthy three-node cluster.

package e2e

import (
	"context"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/monedula-dev/monedula-sim-link/actionlog"
	"github.com/monedula-dev/monedula-sim-link/internal/emit"
)

func compose(t *testing.T, file string, args ...string) {
	t.Helper()
	out, err := exec.Command("docker", append([]string{"compose", "-f", file}, args...)...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker compose %v: %v\n%s", args, err, out)
	}
}

// TestOfflineBroker stops node 3 and checks the snapshot and the link show a
// dead broker: node 3 kept as an offline broker (it is still an assigned
// replica), out of every ISR, and killed in the action log. It needs the
// compose file (E2E_COMPOSE) to stop and restart the node.
func TestOfflineBroker(t *testing.T) {
	file := os.Getenv(envCompose)
	if file == "" {
		t.Skipf("%s not set; the offline-broker test needs the compose file to stop a node", envCompose)
	}
	adm, err := newAdmin()
	if err != nil {
		t.Fatal(err)
	}
	defer adm.Close()

	compose(t, file, "stop", "kafka-3")
	t.Cleanup(func() {
		compose(t, file, "start", "kafka-3")
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if err := waitForBrokers(ctx, 3); err != nil {
			t.Errorf("node 3 did not come back: %v", err)
		}
	})

	// Wait for the controller to fence node 3 and shrink it out of every ISR
	// that has another member. A partition that loses its last in-sync
	// replica is leaderless either way, but Kafka 3.x keeps that replica in
	// the ISR while 4.x empties the ISR (KIP-966 eligible leader replicas).
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	err = poll(ctx, func() (bool, error) {
		meta, err := adm.Metadata(ctx, fixtureTopics...)
		if err != nil || len(meta.Brokers) != 2 {
			return false, nil
		}
		for _, td := range meta.Topics {
			for _, p := range td.Partitions {
				if slices.Contains(p.ISR, 3) && len(p.ISR) > 1 {
					return false, nil
				}
			}
		}
		return true, nil
	}, "node 3 fenced and out of every shared ISR")
	if err != nil {
		t.Fatal(err)
	}

	// A partition whose only replica was on node 3 has no leader now; its
	// offsets cannot be listed, which must be a warning, not a failed snapshot.
	// Bootstrap from the live nodes only: Docker keeps node 3's published port
	// open and resets each connection, which the client reports as an error
	// instead of moving on to the next seed.
	live := slices.DeleteFunc(brokers(), func(b string) bool { return strings.HasSuffix(b, ":39092") })
	snap, warnings, err := emit.Snapshot(ctx, emit.ConnectParams{Brokers: live})
	if err != nil {
		t.Fatalf("snapshot: %v (warnings %v)", err, warnings)
	}
	var online, offline []string
	for _, b := range snap.Brokers {
		if b.Online {
			online = append(online, b.ID)
		} else {
			offline = append(offline, b.ID)
		}
	}
	if !slices.Equal(online, []string{"1", "2"}) || !slices.Equal(offline, []string{"3"}) {
		t.Fatalf("online %v offline %v, want 1,2 online and 3 offline", online, offline)
	}
	for _, tp := range snap.Topics {
		for _, p := range tp.Partitions {
			if p.Leader == "3" || slices.Contains(p.ISR, "3") && len(p.ISR) > 1 {
				t.Errorf("%s/%d: dead node 3 still leader or in ISR: leader %q ISR %v", tp.Name, p.ID, p.Leader, p.ISR)
			}
		}
	}
	// audit.log's only replica is on node 3: no leader, no offsets, a warning.
	p := findTopic(t, snap, "audit.log").Partitions[0]
	if p.Leader != "" || !slices.Equal(p.Replicas, []string{"3"}) {
		t.Errorf("audit.log/0 = leader %q replicas %v, want leaderless on node 3", p.Leader, p.Replicas)
	}
	t.Logf("leaderless audit.log/0 ISR: %v", p.ISR)
	if !slices.ContainsFunc(warnings, func(w string) bool {
		return strings.HasPrefix(w, "audit.log/0: ") && strings.Contains(w, "offset unavailable")
	}) {
		t.Errorf("no offset warning for the leaderless audit.log/0; warnings %v", warnings)
	}

	res, err := emit.Build(snap, emit.BuildOptions{})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	var kills []string
	auditProduces := 0
	for _, e := range res.DecodedActions {
		switch a := e.Action.(type) {
		case actionlog.KillBroker:
			kills = append(kills, a.BrokerID)
		case actionlog.ProduceRecord:
			if a.Topic == "audit-log" {
				auditProduces++
			}
		}
	}
	if !slices.Equal(kills, []string{"broker-3"}) {
		t.Errorf("kills = %v, want [broker-3]", kills)
	}
	// A partition with every replica dead could never ack a record, so the
	// mapping skips its data volume.
	if auditProduces != 0 {
		t.Errorf("audit-log produces = %d, want 0 for a partition with no live replica", auditProduces)
	}
	if !strings.HasPrefix(res.Size, "3x") {
		t.Errorf("size = %q: the dead node must stay in the shape", res.Size)
	}
}
