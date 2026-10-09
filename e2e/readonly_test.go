//go:build e2e

package e2e

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/monedula-dev/monedula-sim-link/internal/emit"
)

const missingTopic = "e2e-missing"

// clusterState is what a snapshot must never change: the topic list, the
// groups' committed offsets and the billing group's membership.
type clusterState struct {
	topics  []string
	offsets map[string]string // group -> committed offsets, formatted
	members []string          // billing member ids
}

func readState(t *testing.T, adm *kadm.Client) clusterState {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	listed, err := adm.ListTopicsWithInternal(ctx)
	if err != nil {
		t.Fatalf("list topics: %v", err)
	}
	s := clusterState{topics: listed.Names(), offsets: map[string]string{}}
	slices.Sort(s.topics)
	for _, g := range []string{billingGroup, reportingGroup} {
		o, err := adm.FetchOffsets(ctx, g)
		if err != nil {
			t.Fatalf("fetch offsets %s: %v", g, err)
		}
		var parts []string
		o.Each(func(r kadm.OffsetResponse) {
			parts = append(parts, r.Topic+"/"+itoa(int64(r.Partition))+"="+itoa(r.At))
		})
		slices.Sort(parts)
		s.offsets[g] = strings.Join(parts, " ")
	}
	described, err := adm.DescribeGroups(ctx, billingGroup)
	if err != nil {
		t.Fatalf("describe %s: %v", billingGroup, err)
	}
	for _, m := range described[billingGroup].Members {
		s.members = append(s.members, m.MemberID)
	}
	return s
}

// TestReadOnly takes full snapshots and a snapshot of a topic that does not
// exist, on brokers with auto.create.topics.enable=true, and checks nothing
// moved: no topic created, no offset committed, no member joined or left.
func TestReadOnly(t *testing.T) {
	adm, err := newAdmin()
	if err != nil {
		t.Fatal(err)
	}
	defer adm.Close()
	before := readState(t, adm)

	takeSnapshot(t)
	buildURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, sel := range [][]string{{missingTopic}, {"orders", missingTopic}} {
		_, _, err := emit.Snapshot(ctx, emit.ConnectParams{Brokers: brokers(), Topics: sel})
		if err == nil || !strings.Contains(err.Error(), missingTopic) {
			t.Errorf("selection %v: err = %v, want one naming %s", sel, err, missingTopic)
		}
	}

	after := readState(t, adm)
	if !slices.Equal(before.topics, after.topics) {
		t.Errorf("topics changed:\nbefore %v\nafter  %v", before.topics, after.topics)
	}
	for g, o := range before.offsets {
		if after.offsets[g] != o {
			t.Errorf("%s offsets changed: %q -> %q", g, o, after.offsets[g])
		}
	}
	if !slices.Equal(before.members, after.members) {
		t.Errorf("billing members changed: %v -> %v", before.members, after.members)
	}

	listed, err := adm.ListTopics(ctx, missingTopic)
	if err != nil {
		t.Fatalf("list %s: %v", missingTopic, err)
	}
	if d := listed[missingTopic]; !errors.Is(d.Err, kerr.UnknownTopicOrPartition) {
		t.Errorf("%s exists after the snapshot (err %v): the tool created it", missingTopic, d.Err)
	}
}
