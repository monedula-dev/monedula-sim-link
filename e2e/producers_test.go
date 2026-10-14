//go:build e2e

package e2e

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/monedula-dev/monedula-sim-link/actionlog"
	"github.com/monedula-dev/monedula-sim-link/internal/emit"
	"github.com/monedula-dev/monedula-sim-link/snapshot"
)

// takeProducersSnapshot is the snapshot behind --producers: the default reads
// plus DescribeProducers.
func takeProducersSnapshot(t *testing.T) (snapshot.ClusterSnapshot, []string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	snap, warnings, err := emit.Snapshot(ctx, emit.ConnectParams{Brokers: brokers(), Producers: true})
	if err != nil {
		t.Fatalf("snapshot with producers: %v (warnings %v)", err, warnings)
	}
	return snap, warnings
}

// seededProducerID finds the producer id the fixture's seed client used: the
// one that wrote orders/0, which every other partition the seed wrote must show
// too (one idempotent client, one producer id).
func seededProducerID(t *testing.T, snap snapshot.ClusterSnapshot) int64 {
	t.Helper()
	ps := findTopic(t, snap, "orders").Partitions[0].Producers
	if len(ps) != 1 {
		t.Fatalf("orders/0 producers = %+v, want exactly the seed client's", ps)
	}
	return ps[0].ProducerID
}

// TestSnapshotProducers: DescribeProducers on a real cluster lists the seed
// client, an idempotent (non-transactional) producer, on exactly the partitions
// it wrote, with a write time near now and no open transaction.
func TestSnapshotProducers(t *testing.T) {
	snap, warnings := takeProducersSnapshot(t)
	if len(warnings) > 0 {
		t.Errorf("unexpected warnings: %v", warnings)
	}
	read := snap.Producers
	if read == nil || read.At.IsZero() {
		t.Fatalf("Producers = %+v, want the read's outcome", read)
	}
	if read.Unsupported || read.ClusterDenied || len(read.DeniedTopics) != 0 || len(read.Errors) != 0 {
		t.Fatalf("DescribeProducers did not go through on this image: %+v", read)
	}

	pid := seededProducerID(t, snap)
	written := map[string]map[int]bool{
		"orders":    {0: true, 1: true}, // partition 2 stays empty
		"payments":  {0: true, 1: true},
		"audit.log": {0: true},
	}
	for topic, parts := range written {
		for _, p := range findTopic(t, snap, topic).Partitions {
			if !parts[p.ID] {
				if len(p.Producers) != 0 {
					t.Errorf("%s/%d was never written, yet lists producers %+v", topic, p.ID, p.Producers)
				}
				continue
			}
			if len(p.Producers) != 1 || p.Producers[0].ProducerID != pid {
				t.Errorf("%s/%d producers = %+v, want only the seed client %d", topic, p.ID, p.Producers, pid)
				continue
			}
			w := p.Producers[0]
			if age := read.At.UnixMilli() - w.LastTimestamp; w.LastTimestamp <= 0 || age < -60_000 || age > int64(time.Hour/time.Millisecond) {
				t.Errorf("%s/%d LastTimestamp = %d, read at %d: not a recent write", topic, p.ID, w.LastTimestamp, read.At.UnixMilli())
			}
			if w.CurrentTxnStartOffset >= 0 {
				t.Errorf("%s/%d: an idempotent producer has an open transaction at %d", topic, p.ID, w.CurrentTxnStartOffset)
			}
		}
	}
}

// TestURLProducers: the link adds the seed client as pid-<id>, pinned to the
// topic it wrote on the most partitions (orders and payments tie, the primary
// wins), with no config change after it (add_producer is idempotent already); a
// snapshot without the opt-in has no producers.
func TestURLProducers(t *testing.T) {
	snap, _ := takeProducersSnapshot(t)
	pid := "pid-" + strconv.FormatInt(seededProducerID(t, snap), 10)
	// A wide window: the seed ran minutes ago, and the suite must not race a clock.
	res, err := emit.Build(snap, emit.BuildOptions{ProducersWindow: time.Hour})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	var got []string
	for _, e := range res.DecodedActions {
		switch a := e.Action.(type) {
		case actionlog.AddProducer:
			if strings.HasPrefix(a.ProducerID, "pid-") {
				got = append(got, "AP:"+a.ProducerID+":"+a.Topic)
			}
		case actionlog.ConfigChange:
			if strings.HasPrefix(a.Path, "producers.pid-") {
				t.Errorf("unexpected producer config change %s=%s", a.Path, a.Value)
			}
		}
	}
	if len(got) != 1 || got[0] != "AP:"+pid+":orders" {
		t.Fatalf("producers in the link = %v, want [AP:%s:orders]", got, pid)
	}

	plain, _ := takeSnapshot(t)
	if plain.Producers != nil {
		t.Errorf("a default snapshot carries a producers read: %+v", plain.Producers)
	}
	res, err = emit.Build(plain, emit.BuildOptions{})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	for _, e := range res.DecodedActions {
		if ap, ok := e.Action.(actionlog.AddProducer); ok && strings.HasPrefix(ap.ProducerID, "pid-") {
			t.Errorf("default link adds %s", ap.ProducerID)
		}
	}
}
