package snapshot

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"
)

// requestLog counts the requests a kfake cluster handles, by API key, without
// intercepting any of them. Unhandled control functions stay registered, so one
// observer sees every request.
type requestLog struct {
	mu   sync.Mutex
	keys map[int16]int
}

func recordRequests(c *kfake.Cluster) *requestLog {
	l := &requestLog{keys: map[int16]int{}}
	c.Control(func(req kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		l.mu.Lock()
		l.keys[req.Key()]++
		l.mu.Unlock()
		return nil, nil, false
	})
	return l
}

func (l *requestLog) seen() []int16 {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []int16
	for k := range l.keys {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

func (l *requestLog) count(key int16) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.keys[key]
}

const describeProducersKey = 61 // kmsg.DescribeProducersRequest.Key()

// readOnlyKeys is every request a default Fetch is allowed to send: the six
// documented reads, the ApiVersions handshake and the FindCoordinator lookups
// the group reads go through. Anything else, a write above all, fails the test.
var readOnlyKeys = []int16{
	18, // ApiVersions
	3,  // Metadata
	2,  // ListOffsets
	16, // ListGroups
	15, // DescribeGroups
	9,  // OffsetFetch
	32, // DescribeConfigs
	10, // FindCoordinator
}

// TestFetchWithoutProducersSendsNoDescribeProducers: the producers read is
// opt-in. A default Fetch, and FetchWith with the zero options, send no
// DescribeProducers request, nothing but the read-only ones, and leave
// ClusterSnapshot.Producers nil.
func TestFetchWithoutProducersSendsNoDescribeProducers(t *testing.T) {
	for name, run := range map[string]func(*kfake.Cluster) ClusterSnapshot{
		"Fetch": func(c *kfake.Cluster) ClusterSnapshot {
			snap, _, err := fetch(t, c, Selection{})
			if err != nil {
				t.Fatal(err)
			}
			return snap
		},
		"FetchWith zero options": func(c *kfake.Cluster) ClusterSnapshot {
			snap, _, err := fetchWith(t, c, Selection{}, FetchOptions{})
			if err != nil {
				t.Fatal(err)
			}
			return snap
		},
	} {
		cluster := startFake(t)
		log := recordRequests(cluster)
		snap := run(cluster)
		if n := log.count(describeProducersKey); n != 0 {
			t.Errorf("%s: %d DescribeProducers requests sent without --producers", name, n)
		}
		for _, k := range log.seen() {
			if !slices.Contains(readOnlyKeys, k) {
				t.Errorf("%s: sent request key %d, not on the read-only list %v", name, k, readOnlyKeys)
			}
		}
		if len(log.seen()) < 5 {
			t.Errorf("%s: only saw request keys %v, the recorder is not observing", name, log.seen())
		}
		if snap.Producers != nil {
			t.Errorf("%s: Producers = %+v, want nil", name, snap.Producers)
		}
		for _, tp := range snap.Topics {
			for _, p := range tp.Partitions {
				if len(p.Producers) != 0 {
					t.Errorf("%s: %s/%d carries producers %v", name, tp.Name, p.ID, p.Producers)
				}
			}
		}
	}
}

// TestFetchWithProducersSendsDescribeProducers: opting in adds DescribeProducers,
// and nothing that writes.
func TestFetchWithProducersSendsDescribeProducers(t *testing.T) {
	cluster := startFake(t)
	log := recordRequests(cluster)
	snap, _, err := fetchWith(t, cluster, Selection{}, FetchOptions{Producers: true})
	if err != nil {
		t.Fatal(err)
	}
	if log.count(describeProducersKey) == 0 {
		t.Fatalf("no DescribeProducers sent with Producers set; saw keys %v", log.seen())
	}
	for _, k := range log.seen() {
		if k != describeProducersKey && !slices.Contains(readOnlyKeys, k) {
			t.Errorf("sent request key %d, not on the read-only list", k)
		}
	}
	if snap.Producers == nil || snap.Producers.At.IsZero() {
		t.Fatalf("Producers = %+v, want a read stamped with its time", snap.Producers)
	}
	if p := snap.Producers; p.Unsupported || p.ClusterDenied || len(p.DeniedTopics) != 0 || len(p.Errors) != 0 {
		t.Errorf("a clean read reported problems: %+v", p)
	}
}

// TestFetchProducersOpenTransaction reads a real producer back: kfake lists the
// producers with an open transaction, with the first offset of the transaction and
// the time of its last write.
func TestFetchProducersOpenTransaction(t *testing.T) {
	cluster := startFake(t)
	tx, err := kgo.NewClient(
		kgo.SeedBrokers(cluster.ListenAddrs()...),
		kgo.TransactionalID("snapshot-test-txn"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := tx.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := tx.ProduceSync(ctx, &kgo.Record{Topic: "orders", Partition: 1, Value: []byte("in a transaction")}).FirstErr(); err != nil {
		t.Fatal(err)
	}

	before := time.Now()
	snap, _, err := fetchWith(t, cluster, Selection{}, FetchOptions{Producers: true})
	if err != nil {
		t.Fatal(err)
	}
	var orders Topic
	for _, tp := range snap.Topics {
		if tp.Name == "orders" {
			orders = tp
		}
	}
	if len(orders.Partitions) != 2 {
		t.Fatalf("orders partitions: %+v", orders.Partitions)
	}
	if got := orders.Partitions[0].Producers; len(got) != 0 {
		t.Errorf("orders/0 never saw the producer, got %+v", got)
	}
	got := orders.Partitions[1].Producers
	if len(got) != 1 {
		t.Fatalf("orders/1 producers = %+v, want the transactional one", got)
	}
	if got[0].CurrentTxnStartOffset < 0 {
		t.Errorf("CurrentTxnStartOffset = %d, want the open transaction's first offset", got[0].CurrentTxnStartOffset)
	}
	if age := before.UnixMilli() - got[0].LastTimestamp; got[0].LastTimestamp <= 0 || age > 60_000 {
		t.Errorf("LastTimestamp = %d, want within a minute before the read (%d)", got[0].LastTimestamp, before.UnixMilli())
	}
	if snap.Producers == nil || snap.Producers.At.Before(before) {
		t.Errorf("Producers.At = %v, want a time at or after %v", snap.Producers, before)
	}
}

// answerDescribeProducers replaces the cluster's answer to DescribeProducers: fn
// gets each request topic's partitions and returns the partition answers.
func answerDescribeProducers(c *kfake.Cluster, fn func(topic string, partition int32) kmsg.DescribeProducersResponseTopicPartition) {
	c.ControlKey(describeProducersKey, func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		req := kreq.(*kmsg.DescribeProducersRequest)
		resp := req.ResponseKind().(*kmsg.DescribeProducersResponse)
		for _, rt := range req.Topics {
			st := kmsg.NewDescribeProducersResponseTopic()
			st.Topic = rt.Topic
			for _, p := range rt.Partitions {
				ans := fn(rt.Topic, p)
				ans.Partition = p
				st.Partitions = append(st.Partitions, ans)
			}
			resp.Topics = append(resp.Topics, st)
		}
		return resp, nil, true
	})
}

func producerAnswer(id int64, lastTimestamp, txnStart int64) kmsg.DescribeProducersResponseTopicPartitionActiveProducer {
	ap := kmsg.NewDescribeProducersResponseTopicPartitionActiveProducer()
	ap.ProducerID, ap.LastTimestamp, ap.CurrentTxnStartOffset = id, lastTimestamp, txnStart
	return ap
}

// TestFetchProducersIdempotentAndSorted: a broker lists idempotent producers (no
// open transaction) too; they come back sorted by producer id.
func TestFetchProducersIdempotentAndSorted(t *testing.T) {
	cluster := startFake(t)
	answerDescribeProducers(cluster, func(topic string, partition int32) kmsg.DescribeProducersResponseTopicPartition {
		ans := kmsg.NewDescribeProducersResponseTopicPartition()
		if topic == "orders" {
			ans.ActiveProducers = []kmsg.DescribeProducersResponseTopicPartitionActiveProducer{
				producerAnswer(9001, 1_700_000_000_000, -1),
				producerAnswer(8000, 1_700_000_100_000, 12),
			}
		}
		return ans
	})
	snap, _, err := fetchWith(t, cluster, Selection{}, FetchOptions{Producers: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, tp := range snap.Topics {
		for _, p := range tp.Partitions {
			switch tp.Name {
			case "orders":
				want := []ProducerWrite{
					{ProducerID: 8000, LastTimestamp: 1_700_000_100_000, CurrentTxnStartOffset: 12},
					{ProducerID: 9001, LastTimestamp: 1_700_000_000_000, CurrentTxnStartOffset: -1},
				}
				if !slices.Equal(p.Producers, want) {
					t.Errorf("orders/%d producers = %+v, want %+v", p.ID, p.Producers, want)
				}
			default:
				if len(p.Producers) != 0 {
					t.Errorf("%s/%d producers = %+v, want none", tp.Name, p.ID, p.Producers)
				}
			}
		}
	}
}

// TestFetchProducersFailuresAreRecordedNotFatal: a refused or unsupported read
// leaves the snapshot intact and says what happened in ProducersRead.
func TestFetchProducersFailuresAreRecordedNotFatal(t *testing.T) {
	coded := func(code int16) func(string, int32) kmsg.DescribeProducersResponseTopicPartition {
		return func(string, int32) kmsg.DescribeProducersResponseTopicPartition {
			ans := kmsg.NewDescribeProducersResponseTopicPartition()
			ans.ErrorCode = code
			return ans
		}
	}
	cases := []struct {
		name  string
		setup func(*kfake.Cluster)
		check func(t *testing.T, p *ProducersRead)
	}{
		{
			name: "topic authorization failed on every topic",
			setup: func(c *kfake.Cluster) {
				answerDescribeProducers(c, coded(kerr.TopicAuthorizationFailed.Code))
			},
			check: func(t *testing.T, p *ProducersRead) {
				if want := []string{"orders", "payments"}; !slices.Equal(p.DeniedTopics, want) {
					t.Errorf("DeniedTopics = %v, want %v", p.DeniedTopics, want)
				}
			},
		},
		{
			name: "topic authorization failed on one topic only",
			setup: func(c *kfake.Cluster) {
				answerDescribeProducers(c, func(topic string, partition int32) kmsg.DescribeProducersResponseTopicPartition {
					if topic == "orders" {
						return coded(kerr.TopicAuthorizationFailed.Code)(topic, partition)
					}
					return kmsg.NewDescribeProducersResponseTopicPartition()
				})
			},
			check: func(t *testing.T, p *ProducersRead) {
				if want := []string{"orders"}; !slices.Equal(p.DeniedTopics, want) {
					t.Errorf("DeniedTopics = %v, want %v", p.DeniedTopics, want)
				}
			},
		},
		{
			name:  "cluster authorization failed",
			setup: func(c *kfake.Cluster) { answerDescribeProducers(c, coded(kerr.ClusterAuthorizationFailed.Code)) },
			check: func(t *testing.T, p *ProducersRead) {
				if !p.ClusterDenied {
					t.Errorf("ClusterDenied = false: %+v", p)
				}
			},
		},
		{
			name:  "unsupported version answered by the broker",
			setup: func(c *kfake.Cluster) { answerDescribeProducers(c, coded(kerr.UnsupportedVersion.Code)) },
			check: func(t *testing.T, p *ProducersRead) {
				if !p.Unsupported {
					t.Errorf("Unsupported = false: %+v", p)
				}
			},
		},
		{
			name: "another partition error",
			setup: func(c *kfake.Cluster) {
				answerDescribeProducers(c, coded(kerr.UnknownServerError.Code))
			},
			check: func(t *testing.T, p *ProducersRead) {
				if len(p.Errors) != 1 || !strings.Contains(p.Errors[0], "UNKNOWN_SERVER_ERROR (-1) on orders/0, orders/1, payments/0, payments/1, payments/2") {
					t.Errorf("Errors = %v, want the code and the partitions", p.Errors)
				}
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cluster := startFake(t)
			c.setup(cluster)
			snap, _, err := fetchWith(t, cluster, Selection{}, FetchOptions{Producers: true})
			if err != nil {
				t.Fatalf("a failed producers read must not fail the snapshot: %v", err)
			}
			if len(snap.Topics) != 2 || len(snap.Brokers) != 3 {
				t.Fatalf("snapshot incomplete: %d topics, %d brokers", len(snap.Topics), len(snap.Brokers))
			}
			if snap.Producers == nil {
				t.Fatal("Producers = nil, want the read's outcome")
			}
			c.check(t, snap.Producers)
		})
	}
}

// TestFetchProducersBrokerTooOld: a cluster that does not advertise
// DescribeProducers (before Kafka 2.8) is Unsupported, not an error.
func TestFetchProducersBrokerTooOld(t *testing.T) {
	cluster := startFakeWith(t, kfake.MaxVersions(kversion.V2_7_0()))
	log := recordRequests(cluster)
	snap, warnings, err := fetchWith(t, cluster, Selection{}, FetchOptions{Producers: true})
	if err != nil {
		t.Fatalf("a too-old cluster must not fail the snapshot: %v (warnings %v)", err, warnings)
	}
	if snap.Producers == nil || !snap.Producers.Unsupported {
		t.Fatalf("Producers = %+v, want Unsupported", snap.Producers)
	}
	if n := log.count(describeProducersKey); n != 0 {
		t.Errorf("%d DescribeProducers requests reached a broker that does not support it", n)
	}
	if len(snap.Topics) != 2 {
		t.Errorf("snapshot incomplete: %+v", snap.Topics)
	}
}

// TestFetchProducersNoTopics: nothing selected has nothing to ask.
func TestFetchProducersNoTopics(t *testing.T) {
	cluster := startFake(t)
	log := recordRequests(cluster)
	client, err := Connect(ConnectConfig{Brokers: cluster.ListenAddrs()})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	read := client.fetchProducers(context.Background(), nil)
	if read == nil || read.At.IsZero() || fmt.Sprint(read.Errors) != "[]" {
		t.Errorf("read = %+v", read)
	}
	if n := log.count(describeProducersKey); n != 0 {
		t.Errorf("%d requests for no partitions", n)
	}
}

// TestFetchProducersSlowLeaderIsBounded: franz-go retries NOT_LEADER_OR_FOLLOWER,
// so a partition that keeps answering it would hold the read until the overall
// snapshot timeout. The read has its own bound and reports the stall instead.
func TestFetchProducersSlowLeaderIsBounded(t *testing.T) {
	old := producersReadTimeout
	producersReadTimeout = 400 * time.Millisecond
	defer func() { producersReadTimeout = old }()

	cluster := startFake(t)
	answerDescribeProducers(cluster, func(string, int32) kmsg.DescribeProducersResponseTopicPartition {
		ans := kmsg.NewDescribeProducersResponseTopicPartition()
		ans.ErrorCode = kerr.NotLeaderForPartition.Code
		return ans
	})
	start := time.Now()
	snap, _, err := fetchWith(t, cluster, Selection{}, FetchOptions{Producers: true})
	if err != nil {
		t.Fatalf("a stalled producers read must not fail the snapshot: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("the read took %s, want it bounded by its own timeout", elapsed)
	}
	if snap.Producers == nil || len(snap.Producers.Errors) == 0 {
		t.Fatalf("want the stall reported, got %+v", snap.Producers)
	}
	t.Logf("errors: %v", snap.Producers.Errors)
}
