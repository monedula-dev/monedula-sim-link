//go:build e2e

// Package e2e runs monedula-sim-link against a real three-node Kafka cluster:
// the snapshot client, the mapping and URL pipeline, the CLI binary and the
// MCP server, end to end. It is excluded from `go test ./...` by the e2e build
// tag and needs a cluster started from e2e/compose.yaml:
//
//	KAFKA_IMAGE=apache/kafka:4.3.1 e2e/run.sh
//
// CI runs it once per image in the matrix (Apache Kafka, Confluent Platform
// community and Confluent Server).
//
// TestMain seeds a known fixture (topics, records, two consumer groups) and
// every test asserts against it. The fixture is reset on each run, so the
// suite can be re-run against a cluster that is already up.
package e2e

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Environment knobs, all optional.
const (
	envBrokers = "E2E_BROKERS" // bootstrap list; default the compose ports
	envCompose = "E2E_COMPOSE" // compose file for the offline-broker test
	envImage   = "KAFKA_IMAGE" // only logged, so a failure names the image
)

const defaultBrokers = "localhost:19092,localhost:29092,localhost:39092"

// The fixture. Offsets are per partition; records land on exact partitions
// via the manual partitioner.
var (
	fixtureTopics = []string{"audit.log", "orders", "payments"} // selected by default, sorted
	internalTopic = "_e2e-internal"                             // excluded by default

	ordersRecords   = map[int32]int{0: 5, 1: 2} // partition 2 stays empty
	paymentsRecords = map[int32]int{0: 3, 1: 1}
	auditRecords    = map[int32]int{0: 12} // over the default cap of 8 per partition

	// billing keeps one live member and commits behind the log end (lag 4);
	// reporting reads payments to the end and leaves (Empty, lag 0).
	billingCommits   = map[int32]int64{0: 2, 1: 1}
	reportingCommits = map[int32]int64{0: 3, 1: 1}
)

// auditNode hosts audit.log's only replica; the offline-broker test stops it.
const auditNode = 3

const (
	billingGroup    = "billing"
	billingClientID = "billing-worker"
	reportingGroup  = "reporting"
)

// billingMember stays joined for the whole run, so the snapshot sees a Stable
// group with a member.
var billingMember *kgo.Client

func brokers() []string {
	if v := os.Getenv(envBrokers); v != "" {
		return strings.Split(v, ",")
	}
	return strings.Split(defaultBrokers, ",")
}

func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	err := seed(ctx)
	cancel()
	if err != nil {
		fmt.Fprintf(os.Stderr, "e2e: seeding the fixture on %s (%s) failed: %v\n", strings.Join(brokers(), ","), os.Getenv(envImage), err)
		os.Exit(1)
	}
	code := m.Run()
	billingMember.Close()
	removeBinary()
	os.Exit(code)
}

func newAdmin() (*kadm.Client, error) {
	kc, err := kgo.NewClient(kgo.SeedBrokers(brokers()...))
	if err != nil {
		return nil, err
	}
	return kadm.NewClient(kc), nil
}

func seed(ctx context.Context) error {
	adm, err := newAdmin()
	if err != nil {
		return err
	}
	defer adm.Close()

	if err := waitForBrokers(ctx, 3); err != nil {
		return err
	}
	if err := resetFixture(ctx, adm); err != nil {
		return fmt.Errorf("reset: %w", err)
	}
	if err := createTopics(ctx, adm); err != nil {
		return fmt.Errorf("create topics: %w", err)
	}
	if err := produce(ctx); err != nil {
		return fmt.Errorf("produce: %w", err)
	}
	if err := runReporting(ctx); err != nil {
		return fmt.Errorf("reporting group: %w", err)
	}
	if billingMember, err = startBilling(ctx, adm); err != nil {
		return fmt.Errorf("billing group: %w", err)
	}
	return nil
}

// waitForBrokers polls metadata until n brokers are registered. Each poll uses
// a fresh client: a long-lived one kept reporting a restarted node as absent
// for minutes after it had rejoined.
func waitForBrokers(ctx context.Context, n int) error {
	return poll(ctx, func() (bool, error) {
		adm, err := newAdmin()
		if err != nil {
			return false, err
		}
		defer adm.Close()
		meta, err := adm.BrokerMetadata(ctx)
		if err != nil {
			return false, nil // not up yet
		}
		return len(meta.Brokers) == n, nil
	}, fmt.Sprintf("%d brokers in metadata", n))
}

// resetFixture drops the fixture's topics and groups left by a previous run.
func resetFixture(ctx context.Context, adm *kadm.Client) error {
	all := append(append([]string{}, fixtureTopics...), internalTopic)
	if _, err := adm.DeleteTopics(ctx, all...); err != nil {
		return err
	}
	// On a fresh cluster the group coordinator (__consumer_offsets) is still
	// coming up, so retry retriable errors; a group that does not exist is fine.
	err := poll(ctx, func() (bool, error) {
		resp, err := adm.DeleteGroups(ctx, billingGroup, reportingGroup)
		if err != nil {
			if kerr.IsRetriable(err) {
				return false, nil
			}
			return false, err
		}
		for _, g := range resp {
			switch {
			case g.Err == nil, errors.Is(g.Err, kerr.GroupIDNotFound):
			case kerr.IsRetriable(g.Err):
				return false, nil
			default:
				return false, fmt.Errorf("group %s: %w", g.Group, g.Err)
			}
		}
		return true, nil
	}, "fixture groups deleted")
	if err != nil {
		return err
	}
	return poll(ctx, func() (bool, error) {
		listed, err := adm.ListTopics(ctx, all...)
		if err != nil {
			return false, err
		}
		for _, t := range listed {
			if t.Err == nil {
				return false, nil
			}
		}
		return true, nil
	}, "fixture topics deleted")
}

func createTopics(ctx context.Context, adm *kadm.Client) error {
	kc, err := kgo.NewClient(kgo.SeedBrokers(brokers()...))
	if err != nil {
		return err
	}
	defer kc.Close()
	topics := []struct {
		name  string
		parts int32
		rf    int16
		cfg   map[string]string
		// onNode pins the single replica of every partition to one node;
		// 0 lets Kafka place the replicas.
		onNode int32
	}{
		{"orders", 3, 3, map[string]string{"min.insync.replicas": "2", "retention.ms": "86400000"}, 0},
		{"payments", 2, 2, map[string]string{"cleanup.policy": "compact"}, 0},
		// On node 3, so the offline-broker test always gets a leaderless partition.
		{"audit.log", 1, 1, map[string]string{"retention.bytes": "1048576"}, auditNode},
		{internalTopic, 1, 1, nil, 0},
	}
	for _, tp := range topics {
		req := kmsg.NewPtrCreateTopicsRequest()
		rt := kmsg.NewCreateTopicsRequestTopic()
		rt.Topic = tp.name
		rt.NumPartitions, rt.ReplicationFactor = tp.parts, tp.rf
		if tp.onNode != 0 {
			rt.NumPartitions, rt.ReplicationFactor = -1, -1
			for p := int32(0); p < tp.parts; p++ {
				a := kmsg.NewCreateTopicsRequestTopicReplicaAssignment()
				a.Partition, a.Replicas = p, []int32{tp.onNode}
				rt.ReplicaAssignment = append(rt.ReplicaAssignment, a)
			}
		}
		for k, v := range tp.cfg {
			c := kmsg.NewCreateTopicsRequestTopicConfig()
			c.Name, c.Value = k, kmsg.StringPtr(v)
			rt.Configs = append(rt.Configs, c)
		}
		req.Topics = append(req.Topics, rt)
		req.TimeoutMillis = 30000
		// A topic deleted a moment ago can still be finishing its deletion.
		err := poll(ctx, func() (bool, error) {
			resp, err := req.RequestWith(ctx, kc)
			if err == nil {
				err = kerr.ErrorForCode(resp.Topics[0].ErrorCode)
			}
			if errors.Is(err, kerr.TopicAlreadyExists) {
				return false, nil
			}
			return err == nil, err
		}, "create "+tp.name)
		if err != nil {
			return err
		}
	}
	// Wait until every fixture partition has a leader and a full ISR.
	return poll(ctx, func() (bool, error) {
		details, err := adm.ListTopics(ctx, fixtureTopics...)
		if err != nil {
			return false, err
		}
		for _, name := range fixtureTopics {
			t, ok := details[name]
			if !ok || t.Err != nil || len(t.Partitions) == 0 {
				return false, nil
			}
			for _, p := range t.Partitions {
				if p.Leader < 0 || len(p.ISR) != len(p.Replicas) {
					return false, nil
				}
			}
		}
		return true, nil
	}, "fixture partitions fully in sync")
}

func produce(ctx context.Context) error {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers()...),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	if err != nil {
		return err
	}
	defer cl.Close()
	var recs []*kgo.Record
	add := func(topic string, counts map[int32]int) {
		for p, n := range counts {
			for i := 0; i < n; i++ {
				key := fmt.Sprintf("%s-%d-%d", topic, p, i) // distinct keys, so compaction keeps them all
				recs = append(recs, &kgo.Record{Topic: topic, Partition: p, Key: []byte(key), Value: []byte("v")})
			}
		}
	}
	add("orders", ordersRecords)
	add("payments", paymentsRecords)
	add("audit.log", auditRecords)
	return cl.ProduceSync(ctx, recs...).FirstErr()
}

// runReporting consumes payments to the end, commits and leaves the group.
func runReporting(ctx context.Context) error {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers()...),
		kgo.ConsumerGroup(reportingGroup),
		kgo.ConsumeTopics("payments"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return err
	}
	defer cl.Close()
	if err := consume(ctx, cl, total(paymentsRecords)); err != nil {
		return err
	}
	return cl.CommitUncommittedOffsets(ctx)
}

// startBilling joins the billing group, reads orders and commits behind the
// log end. The returned client stays in the group.
func startBilling(ctx context.Context, adm *kadm.Client) (*kgo.Client, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers()...),
		kgo.ClientID(billingClientID),
		kgo.ConsumerGroup(billingGroup),
		kgo.ConsumeTopics("orders"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, err
	}
	if err := consume(ctx, cl, total(ordersRecords)); err != nil {
		cl.Close()
		return nil, err
	}
	offsets := map[string]map[int32]kgo.EpochOffset{"orders": {}}
	for p, o := range billingCommits {
		offsets["orders"][p] = kgo.EpochOffset{Epoch: -1, Offset: o}
	}
	var commitErr error
	cl.CommitOffsetsSync(ctx, offsets, func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {
		commitErr = commitError(resp, err)
	})
	if commitErr != nil {
		cl.Close()
		return nil, commitErr
	}
	err = poll(ctx, func() (bool, error) {
		described, err := adm.DescribeGroups(ctx, billingGroup)
		if err != nil {
			return false, nil
		}
		g := described[billingGroup]
		return g.State == "Stable" && len(g.Members) == 1, nil
	}, "billing group Stable with one member")
	if err != nil {
		cl.Close()
		return nil, err
	}
	return cl, nil
}

// commitError folds a request error and every per-partition error code of an
// OffsetCommit response into one error.
func commitError(resp *kmsg.OffsetCommitResponse, err error) error {
	if err != nil {
		return err
	}
	for _, t := range resp.Topics {
		for _, p := range t.Partitions {
			if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
				return fmt.Errorf("commit %s/%d: %w", t.Topic, p.Partition, err)
			}
		}
	}
	return nil
}

func consume(ctx context.Context, cl *kgo.Client, want int) error {
	got := 0
	for got < want {
		fetches := cl.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("consumed %d of %d records: %w", got, want, err)
		}
		for _, fe := range fetches.Errors() {
			return fmt.Errorf("fetch %s/%d: %w", fe.Topic, fe.Partition, fe.Err)
		}
		got += fetches.NumRecords()
	}
	return nil
}

func total(counts map[int32]int) int {
	n := 0
	for _, c := range counts {
		n += c
	}
	return n
}

// poll retries check once a second until it reports done, returns an error,
// or ctx expires.
func poll(ctx context.Context, check func() (bool, error), what string) error {
	for {
		done, err := check()
		if err != nil {
			return fmt.Errorf("%s: %w", what, err)
		}
		if done {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for %s", what)
		case <-time.After(time.Second):
		}
	}
}
