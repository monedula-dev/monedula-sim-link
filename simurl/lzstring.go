package simurl

import (
	"errors"
	"unicode/utf16"
)

// This file ports lz-string's `decompressFromBase64` so the Go tool can DECODE
// the `~1` compressed layout that the playground EMITS (see actionLog.ts): the
// playground compresses with lz-string, while this tool emits raw DEFLATE. Both
// layouts share the `~1` version and both must decode (docs/playground-url-api.md
// §4). Only the decompressor is ported — this tool never emits lz-string.
//
// Faithful port of pieffe's LZString.decompressFromBase64 / _decompress. Strings
// are UTF-16 in lz-string, so entries are modeled as []uint16 and the result is
// decoded via utf16.Decode; for the ASCII action-log grammar this is a plain
// byte-for-byte round-trip.

const lzKeyStrBase64 = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/="

var lzBaseReverse = func() map[rune]int {
	m := make(map[rune]int, len(lzKeyStrBase64))
	for i, r := range lzKeyStrBase64 {
		m[r] = i
	}
	return m
}()

// decompressLZBase64 decompresses an lz-string base64 value. Input must be in
// the keyStrBase64 alphabet (…+/); callers map base64url (-_) to +/ first. An
// empty input yields an empty string. Returns an error on malformed data.
func decompressLZBase64(input string) (string, error) {
	if input == "" {
		return "", nil
	}
	runes := []rune(input)
	getNext := func(index int) int {
		if index < 0 || index >= len(runes) {
			return 0
		}
		if v, ok := lzBaseReverse[runes[index]]; ok {
			return v
		}
		return 0
	}
	return lzDecompress(len(runes), 32, getNext)
}

func lzDecompress(length, resetValue int, getNextValue func(int) int) (string, error) {
	// entries 0..2 are reserved sentinels (never dereferenced as strings).
	dictionary := make([][]uint16, 3, 8)
	enlargeIn := 4
	dictSize := 4
	numBits := 3
	var result []uint16

	val := getNextValue(0)
	position := resetValue
	index := 1

	readBits := func(n int) int {
		bits := 0
		power := 1
		maxpower := 1 << n
		for power != maxpower {
			resb := val & position
			position >>= 1
			if position == 0 {
				position = resetValue
				val = getNextValue(index)
				index++
			}
			if resb > 0 {
				bits |= power
			}
			power <<= 1
		}
		return bits
	}

	var c []uint16
	switch readBits(2) {
	case 0:
		c = []uint16{uint16(readBits(8))}
	case 1:
		c = []uint16{uint16(readBits(16))}
	case 2:
		return "", nil
	}
	dictionary = append(dictionary, c) // dictionary[3]
	w := c
	result = append(result, c...)

	for {
		if index > length {
			return "", nil
		}
		cval := readBits(numBits)
		switch cval {
		case 0:
			dictionary = append(dictionary, []uint16{uint16(readBits(8))})
			dictSize++
			cval = dictSize - 1
			enlargeIn--
		case 1:
			dictionary = append(dictionary, []uint16{uint16(readBits(16))})
			dictSize++
			cval = dictSize - 1
			enlargeIn--
		case 2:
			return string(utf16.Decode(result)), nil
		}

		if enlargeIn == 0 {
			enlargeIn = 1 << numBits
			numBits++
		}

		var entry []uint16
		if cval < len(dictionary) && dictionary[cval] != nil {
			entry = dictionary[cval]
		} else if cval == dictSize {
			entry = append(append([]uint16{}, w...), w[0])
		} else {
			return "", errors.New("lz-string: invalid dictionary reference")
		}
		result = append(result, entry...)

		// dictionary[dictSize++] = w + entry[0]
		dictionary = append(dictionary, append(append([]uint16{}, w...), entry[0]))
		dictSize++
		enlargeIn--

		w = entry

		if enlargeIn == 0 {
			enlargeIn = 1 << numBits
			numBits++
		}
	}
}
