package mcpserver

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestClusterToURLProducers: the producers input is opt-in. Left out (or false),
// the cluster is never sent DescribeProducers and the link has no producers; set,
// the producers that wrote lately are added (with the simulator's default,
// idempotent config: no config change follows) and the echo carries them.
func TestClusterToURLProducers(t *testing.T) {
	cluster, err := kfake.NewCluster(
		kfake.NumBrokers(3),
		kfake.SeedTopics(2, "orders"),
		kfake.SeedTopics(3, "payments"),
	)
	if err != nil {
		t.Fatalf("kfake: %v", err)
	}
	t.Cleanup(cluster.Close)

	var mu sync.Mutex
	asked := 0
	cluster.ControlKey(61, func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		mu.Lock()
		asked++
		mu.Unlock()
		req := kreq.(*kmsg.DescribeProducersRequest)
		resp := req.ResponseKind().(*kmsg.DescribeProducersResponse)
		for _, rt := range req.Topics {
			st := kmsg.NewDescribeProducersResponseTopic()
			st.Topic = rt.Topic
			for _, p := range rt.Partitions {
				sp := kmsg.NewDescribeProducersResponseTopicPartition()
				sp.Partition = p
				if rt.Topic == "payments" {
					ap := kmsg.NewDescribeProducersResponseTopicPartitionActiveProducer()
					ap.ProducerID, ap.LastTimestamp = 4242, time.Now().Add(-30*time.Second).UnixMilli()
					sp.ActiveProducers = []kmsg.DescribeProducersResponseTopicPartitionActiveProducer{ap}
				}
				st.Partitions = append(st.Partitions, sp)
			}
			resp.Topics = append(resp.Topics, st)
		}
		return resp, nil, true
	})
	brokers := cluster.ListenAddrs()
	askedCount := func() int {
		mu.Lock()
		defer mu.Unlock()
		return asked
	}
	s := newSession(t)

	for name, args := range map[string]map[string]any{
		"omitted": {"brokers": brokers},
		"false":   {"brokers": brokers, "producers": false},
	} {
		var out ClusterToURLOutput
		structured(t, callTool(t, s, "cluster_to_url", args), &out)
		for _, a := range out.DecodedActions {
			if strings.HasPrefix(a.ProducerID, "pid-") {
				t.Errorf("producers %s: the link has %+v", name, a)
			}
		}
	}
	if n := askedCount(); n != 0 {
		t.Fatalf("producers off: %d DescribeProducers requests were sent", n)
	}

	var out ClusterToURLOutput
	structured(t, callTool(t, s, "cluster_to_url", map[string]any{"brokers": brokers, "producers": true}), &out)
	if askedCount() == 0 {
		t.Fatal("producers on: no DescribeProducers request was sent")
	}
	var kinds []string
	for _, a := range out.DecodedActions {
		switch {
		case a.Kind == "add_producer" && a.ProducerID == "pid-4242":
			kinds = append(kinds, "add_producer:"+a.Topic)
		case a.Kind == "config_change" && strings.HasPrefix(a.Path, "producers.pid-"):
			t.Errorf("unexpected producer config change: %+v", a)
		}
	}
	if want := "add_producer:payments"; strings.Join(kinds, ",") != want {
		t.Fatalf("producer actions = %v, want %s\nactions: %+v", kinds, want, out.DecodedActions)
	}
}

// TestClusterToURLProducersRefusedIsAWarning: a cluster that refuses the read
// still yields a link, with the warning in the result.
func TestClusterToURLProducersRefusedIsAWarning(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(3), kfake.SeedTopics(2, "orders"))
	if err != nil {
		t.Fatalf("kfake: %v", err)
	}
	t.Cleanup(cluster.Close)
	cluster.ControlKey(61, func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		req := kreq.(*kmsg.DescribeProducersRequest)
		resp := req.ResponseKind().(*kmsg.DescribeProducersResponse)
		for _, rt := range req.Topics {
			st := kmsg.NewDescribeProducersResponseTopic()
			st.Topic = rt.Topic
			for _, p := range rt.Partitions {
				sp := kmsg.NewDescribeProducersResponseTopicPartition()
				sp.Partition, sp.ErrorCode = p, 29 // TOPIC_AUTHORIZATION_FAILED
				st.Partitions = append(st.Partitions, sp)
			}
			resp.Topics = append(resp.Topics, st)
		}
		return resp, nil, true
	})
	s := newSession(t)
	var out ClusterToURLOutput
	structured(t, callTool(t, s, "cluster_to_url", map[string]any{"brokers": cluster.ListenAddrs(), "producers": true}), &out)
	if out.URL == "" {
		t.Fatal("no URL for a refused producers read")
	}
	if !strings.Contains(strings.Join(out.Warnings, "\n"), "TOPIC_AUTHORIZATION_FAILED for orders; the link has no producers") {
		t.Errorf("warnings = %q, want the refusal named", out.Warnings)
	}
}
