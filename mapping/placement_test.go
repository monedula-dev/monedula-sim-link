package mapping

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
)

// tsFixtures streams the non-comment lines of testdata/ts-fixtures.txt — the
// ground truth dumped from the website's TypeScript implementation (see the
// file header for provenance).
func tsFixtures(t *testing.T, prefix string, fn func(fields []string)) {
	t.Helper()
	f, err := os.Open("testdata/ts-fixtures.txt")
	if err != nil {
		t.Fatalf("open fixtures: %v", err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	n := 0
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") || !strings.HasPrefix(line, prefix+" ") {
			continue
		}
		n++
		fn(strings.SplitN(line[len(prefix)+1:], " ", 4))
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan fixtures: %v", err)
	}
	if n == 0 {
		t.Fatalf("no %s fixtures found — fixture file corrupted?", prefix)
	}
}

// TestAssignRackAwareMatchesTypeScript replays every placement fixture: the Go
// port must be byte-for-byte identical to src/sim/placement.ts on the
// single-rack (free-play default) path, including the lexicographic broker-id
// sort that puts broker-10 before broker-2 and the replica shift bump every
// nBrokers partitions.
func TestAssignRackAwareMatchesTypeScript(t *testing.T) {
	tsFixtures(t, "PLACEMENT", func(fields []string) {
		if len(fields) != 4 {
			t.Fatalf("malformed PLACEMENT fixture: %v", fields)
		}
		var n, parts, rf int
		fmt.Sscanf(fields[0]+" "+fields[1]+" "+fields[2], "%d %d %d", &n, &parts, &rf)
		var want [][]string
		if err := json.Unmarshal([]byte(fields[3]), &want); err != nil {
			t.Fatalf("fixture json: %v", err)
		}
		ids := make([]string, n)
		for i := range ids {
			ids[i] = fmt.Sprintf("broker-%d", i+1)
		}
		got := defaultPlacement(ids, parts, rf)
		if !reflect.DeepEqual(got, want) {
			t.Errorf("defaultPlacement(%d brokers, %d parts, rf %d):\n got %v\nwant %v", n, parts, rf, got, want)
		}
	})
}

func TestAssignRackAwareEmptyBrokers(t *testing.T) {
	got := defaultPlacement(nil, 3, 3)
	if len(got) != 3 {
		t.Fatalf("want 3 empty rows, got %v", got)
	}
	for _, row := range got {
		if len(row) != 0 {
			t.Fatalf("want empty rows, got %v", got)
		}
	}
}

// TestRackAlternatedMultiRack pins the multi-rack arrangement (not exercised
// by free play, but part of the faithful port): racks sorted, brokers
// round-robined across them.
func TestRackAlternatedMultiRack(t *testing.T) {
	got := rackAlternatedBrokerList([]rackBroker{
		{"b4", "r2"}, {"b1", "r1"}, {"b3", "r1"}, {"b2", "r2"},
	})
	want := []string{"b1", "b2", "b3", "b4"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}
