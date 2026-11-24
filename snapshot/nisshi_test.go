package snapshot

import (
	"context"
	"errors"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestNisshiSignature(t *testing.T) {
	broker := func(id int32) kmsg.MetadataResponseBroker {
		b := kmsg.NewMetadataResponseBroker()
		b.NodeID = id
		return b
	}
	for _, c := range []struct {
		name    string
		brokers []kmsg.MetadataResponseBroker
		want    bool
	}{
		{"one broker, node 111", []kmsg.MetadataResponseBroker{broker(111)}, true},
		{"one broker, node 1", []kmsg.MetadataResponseBroker{broker(1)}, false},
		{"two brokers with 111", []kmsg.MetadataResponseBroker{broker(111), broker(2)}, false},
		{"no broker", nil, false},
	} {
		if got := nisshiCandidate(c.brokers); got != c.want {
			t.Errorf("%s: nisshiCandidate = %v, want %v", c.name, got, c.want)
		}
	}

	apis := func(errCode int16, keys ...int16) *kmsg.ApiVersionsResponse {
		r := kmsg.NewPtrApiVersionsResponse()
		r.ErrorCode = errCode
		for _, k := range keys {
			ak := kmsg.NewApiVersionsResponseApiKey()
			ak.ApiKey = k
			r.ApiKeys = append(r.ApiKeys, ak)
		}
		return r
	}
	for _, c := range []struct {
		name string
		resp *kmsg.ApiVersionsResponse
		want bool
	}{
		{"no CreatePartitions", apis(0, 0, 1, 3, 18, 19), true},
		{"CreatePartitions offered", apis(0, 0, 1, 3, 18, 19, 37), false},
		{"error answer", apis(35, 0, 1, 3), false},
		{"no keys", apis(0), false},
		{"no answer", nil, false},
	} {
		if got := nisshiAPIs(c.resp); got != c.want {
			t.Errorf("%s: nisshiAPIs = %v, want %v", c.name, got, c.want)
		}
	}
}

// TestFetchRefusesNisshi: a fake cluster answering Metadata with the single
// broker 111 and ApiVersions without CreatePartitions, as Nisshi does, is
// refused with ErrNisshi before any other request.
func TestFetchRefusesNisshi(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "orders"))
	if err != nil {
		t.Fatalf("kfake: %v", err)
	}
	t.Cleanup(cluster.Close)
	host, portStr, err := net.SplitHostPort(cluster.ListenAddrs()[0])
	if err != nil {
		t.Fatalf("listen addr: %v", err)
	}
	port, _ := strconv.Atoi(portStr)

	cluster.ControlKey(int16(kmsg.ApiVersions), func(req kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		resp := req.ResponseKind().(*kmsg.ApiVersionsResponse)
		resp.Version = req.GetVersion()
		for key := int16(0); key <= kmsg.MaxKey; key++ {
			r := kmsg.RequestForKey(key)
			if r == nil || key == int16(kmsg.CreatePartitions) {
				continue
			}
			ak := kmsg.NewApiVersionsResponseApiKey()
			ak.ApiKey = key
			ak.MaxVersion = r.MaxVersion()
			resp.ApiKeys = append(resp.ApiKeys, ak)
		}
		return resp, nil, true
	})
	var mu sync.Mutex
	var others []int16
	cluster.Control(func(req kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		switch req.Key() {
		case int16(kmsg.ApiVersions):
			return nil, nil, false
		case int16(kmsg.Metadata):
			resp := req.ResponseKind().(*kmsg.MetadataResponse)
			resp.Version = req.GetVersion()
			b := kmsg.NewMetadataResponseBroker()
			b.NodeID, b.Host, b.Port = NisshiNodeID, host, int32(port)
			resp.Brokers = []kmsg.MetadataResponseBroker{b}
			resp.ControllerID = NisshiNodeID
			topic := kmsg.NewMetadataResponseTopic()
			name := "orders"
			topic.Topic = &name
			p := kmsg.NewMetadataResponseTopicPartition()
			p.Leader, p.Replicas, p.ISR = NisshiNodeID, []int32{NisshiNodeID}, []int32{NisshiNodeID}
			topic.Partitions = []kmsg.MetadataResponseTopicPartition{p}
			resp.Topics = []kmsg.MetadataResponseTopic{topic}
			return resp, nil, true
		}
		mu.Lock()
		others = append(others, req.Key())
		mu.Unlock()
		return nil, nil, false
	})

	_, _, err = fetch(t, cluster, Selection{})
	if !errors.Is(err, ErrNisshi) {
		t.Fatalf("err %v, want ErrNisshi", err)
	}
	for _, n := range []string{"node 111", "build_playground_url", "nisshi-3b", "shape.brokers"} {
		if !strings.Contains(err.Error(), n) {
			t.Errorf("error %q does not mention %q", err, n)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(others) != 0 {
		t.Errorf("requests after the refusal: %v", others)
	}
}

// TestKafkaIsNotNisshi: a Kafka broker's own ApiVersions answer (kfake's)
// offers CreatePartitions, so a Kafka broker that happens to have node.id 111
// is mapped, not refused.
func TestKafkaIsNotNisshi(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		t.Fatalf("kfake: %v", err)
	}
	t.Cleanup(cluster.Close)
	cl, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...))
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	t.Cleanup(cl.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := kmsg.NewPtrApiVersionsRequest().RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("ApiVersions: %v", err)
	}
	if nisshiAPIs(resp) {
		t.Errorf("kfake's ApiVersions answer reads as Nisshi's: %d keys", len(resp.ApiKeys))
	}
}
