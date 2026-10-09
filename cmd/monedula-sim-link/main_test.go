package main

import (
	"bytes"
	"context"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/monedula-dev/monedula-sim-link/actionlog"
	"github.com/monedula-dev/monedula-sim-link/internal/emit"
	"github.com/monedula-dev/monedula-sim-link/mapping"
	"github.com/monedula-dev/monedula-sim-link/simurl"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TestConnectEndToEnd drives the real CLI path against an in-process kfake
// cluster: connect → snapshot → mapping → self-validation → URL, then decodes
// the emitted URL back and asserts the action log and size param match the
// cluster that was faked.
func TestConnectEndToEnd(t *testing.T) {
	cluster, err := kfake.NewCluster(
		kfake.NumBrokers(3),
		kfake.SeedTopics(2, "orders"),
		kfake.SeedTopics(3, "payments"),
		kfake.SeedTopics(1, "_hidden"),
	)
	if err != nil {
		t.Fatalf("kfake: %v", err)
	}
	defer cluster.Close()

	producer, err := kgo.NewClient(
		kgo.SeedBrokers(cluster.ListenAddrs()...),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	defer producer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	seed := []*kgo.Record{
		{Topic: "orders", Partition: 0, Value: []byte("a")},
		{Topic: "orders", Partition: 0, Value: []byte("b")},
		{Topic: "orders", Partition: 1, Value: []byte("c")},
		{Topic: "payments", Partition: 2, Value: []byte("d")},
	}
	if err := producer.ProduceSync(ctx, seed...).FirstErr(); err != nil {
		t.Fatalf("seed produce: %v", err)
	}

	var stdout, stderr bytes.Buffer
	code := run([]string{
		"connect",
		"--brokers", strings.Join(cluster.ListenAddrs(), ","),
		"--dry-run",
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit %d\nstdout:\n%s\nstderr:\n%s", code, stdout.String(), stderr.String())
	}

	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	link := lines[len(lines)-1]
	parsed, err := url.Parse(link)
	if err != nil {
		t.Fatalf("emitted URL unparseable: %v\n%s", err, link)
	}
	q := parsed.Query()
	if q.Get("scenario") != "free" {
		t.Fatalf("scenario = %q", q.Get("scenario"))
	}
	if q.Get("cluster") != "" {
		t.Fatalf("cluster param should be omitted for single-dc, got %q", q.Get("cluster"))
	}
	// kfake: RF = min(3, brokers) = 3, orders has 2 partitions (primary),
	// payments 3 (extra topic), _hidden excluded as internal. kfake's default
	// topic config resolves retention.ms=604800000 (7d, Kafka's own default)
	// for every topic, min.insync.replicas=1 and cleanup.policy=delete — the
	// tail carries the primary's (index 10) and payments' (index 0) real
	// DescribeConfigs values; retention.bytes stays empty (kfake's -1/disabled
	// sentinel is treated as unset, see snapshot/kafka.go).
	const wantSize = "3x2x3xpayments~3~3~1~d~604800000~~~xxxxxxxxxx1~d~604800000~~"
	if got := q.Get("size"); got != wantSize {
		t.Fatalf("size = %q, want %q", got, wantSize)
	}

	entries, err := simurl.DecodeActionsFromURL(q.Get("actions"))
	if err != nil {
		t.Fatalf("actions decode: %v", err)
	}
	// Deterministic log: with 3 brokers at RF 3 every replica SET is all
	// brokers, so no reassign; all brokers online and in-sync, so no kills or
	// slowdowns. Just the template-producer removal and the per-topic produce
	// bursts: orders p0×2 + p1×1, payments p2×1.
	var kinds []string
	producesPerTopicPartition := map[string]int{}
	for _, e := range entries {
		enc, err := actionlog.EncodeEntry(e)
		if err != nil {
			t.Fatalf("re-encode: %v", err)
		}
		kinds = append(kinds, strings.SplitN(enc, ":", 2)[0])
		if p, ok := e.Action.(actionlog.ProduceRecord); ok {
			if p.Key == nil {
				t.Fatalf("keyless produce: %s", enc)
			}
			count := 2
			if p.Topic == "payments" {
				count = 3
			}
			pid := mapping.PartitionForKey(*p.Key, count)
			producesPerTopicPartition[p.Topic+"/"+strconv.Itoa(pid)]++
		}
	}
	wantKinds := []string{"RP", "AP", "P", "P", "P", "RP", "AP", "P", "RP"}
	if strings.Join(kinds, ",") != strings.Join(wantKinds, ",") {
		t.Fatalf("action kinds = %v, want %v", kinds, wantKinds)
	}
	wantProduces := map[string]int{"orders/0": 2, "orders/1": 1, "payments/2": 1}
	for k, want := range wantProduces {
		if producesPerTopicPartition[k] != want {
			t.Fatalf("produce distribution = %v, want %v", producesPerTopicPartition, wantProduces)
		}
	}
	if len(producesPerTopicPartition) != len(wantProduces) {
		t.Fatalf("stray produces: %v", producesPerTopicPartition)
	}

	// The dry run printed the decoded entries and the name mappings.
	if !strings.Contains(stdout.String(), "RP:p1@500") || !strings.Contains(stdout.String(), "orders -> orders") {
		t.Fatalf("dry-run output incomplete:\n%s", stdout.String())
	}
}

func TestConnectRequiresBrokers(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"connect"}, &stdout, &stderr); code != 2 {
		t.Fatalf("exit = %d, want 2", code)
	}
	if !strings.Contains(stderr.String(), "--brokers") {
		t.Fatalf("stderr: %s", stderr.String())
	}
}

func TestVersionCommand(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"version"}, &stdout, &stderr); code != 0 {
		t.Fatalf("version exit %d, stderr %q", code, stderr.String())
	}
	if got := strings.TrimSpace(stdout.String()); !strings.HasPrefix(got, "monedula-sim-link ") {
		t.Fatalf("version printed %q", got)
	}
}

func TestDemoEmitsValidatedURL(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"demo", "--dry-run"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit %d\n%s\n%s", code, stdout.String(), stderr.String())
	}
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	link := lines[len(lines)-1]
	parsed, err := url.Parse(link)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := simurl.DecodeActionsFromURL(parsed.Query().Get("actions"))
	if err != nil || len(entries) == 0 {
		t.Fatalf("demo URL actions: %v (%d entries)", err, len(entries))
	}
	// The fixture exercises kill (offline broker 4), reassign (payments off
	// default placement) and slow (ISR gaps on orders).
	out := stdout.String()
	for _, needle := range []string{"K:broker-4@", "SR:", "RA:", "AP:kp1:orders@"} {
		if !strings.Contains(out, needle) {
			t.Fatalf("demo dry-run missing %q:\n%s", needle, out)
		}
	}
}

// TestValidateEmitsCompressedForm proves the reconciled behavior: a log past the
// compression threshold now ships in the "~1" form WITHOUT force (the playground
// decodes the raw-DEFLATE layout this tool emits — docs/playground-url-api.md §4),
// and the emitted value round-trips losslessly. The compressed-form gate moved
// into package emit (BuildFromMapping) with the cluster_to_url refactor.
func TestValidateEmitsCompressedForm(t *testing.T) {
	res := &mapping.Result{Size: "3x3x3"}
	key := "k"
	for i := 0; i < 400; i++ {
		res.Entries = append(res.Entries, actionlog.Entry{
			At:     500 + i*250,
			Action: actionlog.ProduceRecord{ProducerID: "kp1", Topic: "orders", Key: &key},
		})
	}
	// Confirm this log really is past the plain threshold and thus compressed.
	value, err := simurl.EncodeActionsForURL(res.Entries)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if !strings.HasPrefix(value, "~1") {
		t.Fatalf("expected the log to compress to the ~1 form, got %.12q…", value)
	}

	out, err := emit.BuildFromMapping(res, emit.BuildOptions{})
	if err != nil {
		t.Fatalf("compressed form must now emit without force: %v", err)
	}
	if len(out.Warnings) != 0 {
		t.Fatalf("no warning expected for a compressed link that fits: %v", out.Warnings)
	}
	if !out.Compressed {
		t.Fatalf("expected Result.Compressed to report the ~1 form")
	}

	// The emitted "~1" value round-trips back to the same entries.
	decoded, err := simurl.DecodeActionsFromURL(value)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(decoded) != len(res.Entries) {
		t.Fatalf("round-trip lost data: got %d entries, want %d", len(decoded), len(res.Entries))
	}
}
