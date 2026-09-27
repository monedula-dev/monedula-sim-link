package mapping

import (
	"strconv"
	"testing"
)

// TestPartitionForKeyMatchesTypeScript replays every HASH fixture: the Go port
// must agree with src/sim/producer.ts partitionForKey for every (key, count)
// pair dumped from the real implementation.
func TestPartitionForKeyMatchesTypeScript(t *testing.T) {
	tsFixtures(t, "HASH", func(fields []string) {
		if len(fields) != 3 {
			t.Fatalf("malformed HASH fixture: %v", fields)
		}
		key := fields[0]
		count, _ := strconv.Atoi(fields[1])
		want, _ := strconv.Atoi(fields[2])
		if got := PartitionForKey(key, count); got != want {
			t.Errorf("PartitionForKey(%q, %d) = %d, want %d", key, count, got, want)
		}
	})
}

func TestPartitionForKeyDegenerate(t *testing.T) {
	if got := PartitionForKey("anything", 0); got != 0 {
		t.Fatalf("count 0: got %d", got)
	}
	if got := PartitionForKey("", 7); got < 0 || got >= 7 {
		t.Fatalf("empty key out of range: %d", got)
	}
}

// TestKeysForPartitions proves the brute force: for several partition counts,
// every requested partition receives the requested number of DISTINCT keys and
// each key really hashes to its partition.
func TestKeysForPartitions(t *testing.T) {
	for _, count := range []int{1, 2, 3, 8, 24} {
		need := make([]int, count)
		for p := range need {
			need[p] = 3
		}
		keys, err := keysForPartitions(count, need)
		if err != nil {
			t.Fatalf("count %d: %v", count, err)
		}
		seen := map[string]bool{}
		for p := 0; p < count; p++ {
			if len(keys[p]) != 3 {
				t.Fatalf("count %d partition %d: got %d keys, want 3", count, p, len(keys[p]))
			}
			for _, k := range keys[p] {
				if seen[k] {
					t.Fatalf("count %d: duplicate key %q", count, k)
				}
				seen[k] = true
				if got := PartitionForKey(k, count); got != p {
					t.Fatalf("count %d: key %q hashes to %d, want %d", count, k, got, p)
				}
			}
		}
	}
}

func TestKeysForPartitionsSparseNeed(t *testing.T) {
	need := make([]int, 24)
	need[0], need[23] = 2, 1
	keys, err := keysForPartitions(24, need)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys[0]) != 2 || len(keys[23]) != 1 {
		t.Fatalf("sparse need not honored: %v", keys)
	}
	for p := 1; p < 23; p++ {
		if len(keys[p]) != 0 {
			t.Fatalf("partition %d should have no keys", p)
		}
	}
}
