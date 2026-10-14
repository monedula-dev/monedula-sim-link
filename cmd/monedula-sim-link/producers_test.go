package main

import (
	"bytes"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/monedula-dev/monedula-sim-link/actionlog"
	"github.com/monedula-dev/monedula-sim-link/simurl"
)

// producerAPs lists the AP:pid-... entries of a dry-run output, in order.
func producerAPs(out string) []string {
	var aps []string
	for _, line := range strings.Split(out, "\n") {
		if line = strings.TrimSpace(line); strings.HasPrefix(line, "AP:pid-") {
			aps = append(aps, line[:strings.LastIndex(line, "@")])
		}
	}
	return aps
}

// TestDemoProducers: demo --producers shows the feature without a cluster. The
// top producer is pinned to orders, the one in an open transaction is added as an
// idempotent producer with a warning, and the one outside the window is left out.
func TestDemoProducers(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"demo", "--producers", "--dry-run"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit %d\n%s\n%s", code, stdout.String(), stderr.String())
	}
	if got, want := strings.Join(producerAPs(stdout.String()), ","), "AP:pid-1001:orders,AP:pid-1002:payments"; got != want {
		t.Fatalf("producers = %v, want %v\n%s", got, want, stdout.String())
	}
	if strings.Contains(stdout.String(), "C:producers.") {
		t.Errorf("a new producer is idempotent already, yet the dry run has a producer config change:\n%s", stdout.String())
	}
	if strings.Contains(stdout.String(), "pid-1003") {
		t.Errorf("pid-1003 wrote three hours ago, outside the default window:\n%s", stdout.String())
	}
	if !strings.Contains(stderr.String(), "warning: producer 1002 (shown as pid-1002) has an open transaction on payments/0") {
		t.Errorf("want the open-transaction warning, got:\n%s", stderr.String())
	}
	// The link itself carries them and decodes back.
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	parsed, err := url.Parse(lines[len(lines)-1])
	if err != nil {
		t.Fatal(err)
	}
	entries, err := simurl.DecodeActionsFromURL(parsed.Query().Get("actions"))
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, e := range entries {
		if ap, ok := e.Action.(actionlog.AddProducer); ok && strings.HasPrefix(ap.ProducerID, "pid-") {
			ids = append(ids, ap.ProducerID)
		}
	}
	if strings.Join(ids, ",") != "pid-1001,pid-1002" {
		t.Errorf("URL carries producers %v", ids)
	}
}

// TestDemoWithoutProducersFlagChangesNothing: the flag is opt-in.
func TestDemoWithoutProducersFlagChangesNothing(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"demo", "--dry-run"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit %d\n%s", code, stderr.String())
	}
	if strings.Contains(stdout.String(), "pid-") || strings.Contains(stderr.String(), "--producers") {
		t.Fatalf("producers leaked into a run without --producers:\n%s\n%s", stdout.String(), stderr.String())
	}
}

func TestProducersWindowFlag(t *testing.T) {
	// 1m: only pid-1002 (30s ago) is inside; pid-1001 wrote 2 to 4 minutes ago.
	var stdout, stderr bytes.Buffer
	if code := run([]string{"demo", "--producers", "--producers-window", "1m", "--dry-run"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit %d\n%s", code, stderr.String())
	}
	if got := strings.Join(producerAPs(stdout.String()), ","); got != "AP:pid-1002:payments" {
		t.Fatalf("1m window: producers = %v", got)
	}
	// 4h: pid-1003 (3h ago) is inside too, with one partition.
	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"demo", "--producers", "--producers-window", "4h", "--dry-run"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit %d\n%s", code, stderr.String())
	}
	if got := strings.Join(producerAPs(stdout.String()), ","); got != "AP:pid-1001:orders,AP:pid-1002:payments,AP:pid-1003:orders" {
		t.Fatalf("4h window: producers = %v", got)
	}
	// A window that is not positive is a usage error.
	for _, bad := range []string{"0s", "-5m"} {
		stdout.Reset()
		stderr.Reset()
		if code := run([]string{"demo", "--producers", "--producers-window", bad}, &stdout, &stderr); code != 2 {
			t.Errorf("--producers-window %s: exit %d, want 2", bad, code)
		}
		if !strings.Contains(stderr.String(), "--producers-window must be positive") {
			t.Errorf("--producers-window %s: stderr %q", bad, stderr.String())
		}
	}
	// A window without --producers is ignored, with a note.
	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"demo", "--producers-window", "1h"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit %d\n%s", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "--producers-window has no effect without --producers") {
		t.Errorf("want the no-effect note, got %q", stderr.String())
	}
	if strings.Contains(stdout.String(), "pid-") {
		t.Errorf("a window alone must not add producers:\n%s", stdout.String())
	}
}

// TestConnectProducers drives connect --producers against kfake, answering
// DescribeProducers with one idempotent producer that wrote a minute ago and one
// that wrote an hour ago. Without the flag the same cluster is never asked.
func TestConnectProducers(t *testing.T) {
	cluster, err := kfake.NewCluster(
		kfake.NumBrokers(3),
		kfake.SeedTopics(2, "orders"),
		kfake.SeedTopics(3, "payments"),
	)
	if err != nil {
		t.Fatalf("kfake: %v", err)
	}
	defer cluster.Close()

	var mu sync.Mutex
	asked := 0
	cluster.ControlKey(61, func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		mu.Lock()
		asked++
		mu.Unlock()
		req := kreq.(*kmsg.DescribeProducersRequest)
		resp := req.ResponseKind().(*kmsg.DescribeProducersResponse)
		now := time.Now()
		for _, rt := range req.Topics {
			st := kmsg.NewDescribeProducersResponseTopic()
			st.Topic = rt.Topic
			for _, p := range rt.Partitions {
				sp := kmsg.NewDescribeProducersResponseTopicPartition()
				sp.Partition = p
				if rt.Topic == "orders" {
					recent := kmsg.NewDescribeProducersResponseTopicPartitionActiveProducer()
					recent.ProducerID, recent.LastTimestamp = 77, now.Add(-time.Minute).UnixMilli()
					old := kmsg.NewDescribeProducersResponseTopicPartitionActiveProducer()
					old.ProducerID, old.LastTimestamp = 88, now.Add(-time.Hour).UnixMilli()
					sp.ActiveProducers = []kmsg.DescribeProducersResponseTopicPartitionActiveProducer{recent, old}
				}
				st.Partitions = append(st.Partitions, sp)
			}
			resp.Topics = append(resp.Topics, st)
		}
		return resp, nil, true
	})
	addrs := strings.Join(cluster.ListenAddrs(), ",")
	askedCount := func() int {
		mu.Lock()
		defer mu.Unlock()
		return asked
	}

	var stdout, stderr bytes.Buffer
	if code := run([]string{"connect", "--brokers", addrs, "--dry-run"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit %d\n%s", code, stderr.String())
	}
	if n := askedCount(); n != 0 || strings.Contains(stdout.String(), "pid-") {
		t.Fatalf("without --producers: %d DescribeProducers requests, output:\n%s", n, stdout.String())
	}

	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"connect", "--brokers", addrs, "--producers", "--dry-run"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit %d\n%s", code, stderr.String())
	}
	if got := strings.Join(producerAPs(stdout.String()), ","); got != "AP:pid-77:orders" {
		t.Fatalf("producers = %q, want only pid-77 (pid-88 is an hour old)\n%s", got, stdout.String())
	}
	if strings.Contains(stdout.String(), "C:producers.") {
		t.Errorf("unexpected producer config change:\n%s", stdout.String())
	}
	if askedCount() == 0 {
		t.Error("--producers sent no DescribeProducers request")
	}

	// A wider window brings the older producer in, behind the recent one.
	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"connect", "--brokers", addrs, "--producers", "--producers-window", "2h", "--dry-run"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit %d\n%s", code, stderr.String())
	}
	if got := strings.Join(producerAPs(stdout.String()), ","); got != "AP:pid-77:orders,AP:pid-88:orders" {
		t.Fatalf("2h window: producers = %q", got)
	}
}
