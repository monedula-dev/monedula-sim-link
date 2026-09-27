package mapping

import (
	"fmt"
	"unicode/utf16"
)

// PartitionForKey is a bit-exact Go port of the simulator's default keyed
// partitioner — `partitionForKey` in src/sim/producer.ts (the website repo).
// It is a cyrb53-derived hash: both 32-bit halves of the mixing round are
// recovered and XORed for avalanche across small partition counts, then
// reduced mod partitionCount. Keyed records hash through this function in
// EVERY simulator partitioner mode, so a key found by KeysForPartitions lands
// on a chosen partition deterministically.
//
// JS semantics preserved: charCodeAt iterates UTF-16 code units, Math.imul is
// a low-32-bit multiply, and ">>>" is an unsigned shift — all of which map to
// plain uint32 arithmetic here. Verified against the TypeScript output by
// fixture tests.
func PartitionForKey(key string, partitionCount int) int {
	if partitionCount <= 0 {
		return 0
	}
	h1 := uint32(0xdeadbeef)
	h2 := uint32(0x41c6ce57)
	for _, ch := range utf16.Encode([]rune(key)) {
		c := uint32(ch)
		h1 = (h1 ^ c) * 2654435761
		h2 = (h2 ^ c) * 1597334677
	}
	h1 = (h1^(h1>>16))*2246822507 ^ (h2^(h2>>13))*3266489909
	h2 = (h2^(h2>>16))*2246822507 ^ (h1^(h1>>13))*3266489909
	return int((h1 ^ h2) % uint32(partitionCount))
}

// keyAlphabet orders the brute-force candidate space. Lowercase alphanumerics
// only: always action-log safe (no reserved separators) and short.
const keyAlphabet = "abcdefghijklmnopqrstuvwxyz0123456789"

// maxKeyLen bounds the brute force; 36^4 ≈ 1.68M candidates covers any need
// (≤ 24 partitions × a handful of keys each) by a huge margin.
const maxKeyLen = 4

// keysForPartitions brute-forces `need[p]` DISTINCT keys hashing to each
// partition p (0 ≤ p < partitionCount), enumerating candidates in a fixed
// order (a…z0…9, then aa, ab, …) so the result is deterministic. Returns
// keys[p] with len(keys[p]) == need[p].
func keysForPartitions(partitionCount int, need []int) ([][]string, error) {
	keys := make([][]string, partitionCount)
	remaining := 0
	for p, n := range need {
		if n > 0 {
			keys[p] = make([]string, 0, n)
			remaining += n
		}
	}
	if remaining == 0 {
		return keys, nil
	}
	buf := make([]byte, maxKeyLen)
	for length := 1; length <= maxKeyLen; length++ {
		idx := make([]int, length)
		for {
			for i, a := range idx {
				buf[i] = keyAlphabet[a]
			}
			candidate := string(buf[:length])
			p := PartitionForKey(candidate, partitionCount)
			if p < len(need) && len(keys[p]) < need[p] {
				keys[p] = append(keys[p], candidate)
				remaining--
				if remaining == 0 {
					return keys, nil
				}
			}
			// Odometer increment over the alphabet.
			i := length - 1
			for ; i >= 0; i-- {
				idx[i]++
				if idx[i] < len(keyAlphabet) {
					break
				}
				idx[i] = 0
			}
			if i < 0 {
				break
			}
		}
	}
	return nil, fmt.Errorf("could not find keys for every partition of %d within %d-char candidates", partitionCount, maxKeyLen)
}
