package rdb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLZ77Decompression(t *testing.T) {
	tests := map[string]struct {
		compressed []byte
		expected   string
	}{
		// An input that can't really be compressed with LZ77, due to lack of repetition.
		// Hence the compressed size is greater than the decompressed size, due to extra
		// metadata encoded in the compressed block to describe literal runs or matches.
		"lorem ipsum": {
			compressed: []byte{
				31, 76, 111, 114, 101, 109, 32, 105, 112, 115, 117, 109, 32, 100, 111,
				108, 111, 114, 32, 115, 105, 116, 32, 97, 109, 101, 116, 32, 110, 117,
				108, 108, 97, 9, 109, 32, 115, 111, 100, 97, 108, 101, 115, 46,
			},
			expected: "Lorem ipsum dolor sit amet nullam sodales.",
		},

		"lots of repetition": {
			compressed: []byte{
				6, 117, 112, 115, 116, 97, 115, 104, 224, 35, 6, 4, 115, 116, 97, 115, 104,
			},
			expected: "upstashupstashupstashupstashupstashupstashupstashupstash",
		},

		"some repetition": {
			compressed: []byte{
				31, 65, 110, 121, 119, 97, 121, 44, 32, 108, 105, 107, 101, 32, 73, 32, 119,
				97, 115, 32, 115, 97, 121, 105, 110, 39, 44, 32, 115, 104, 114, 105, 109, 16,
				112, 32, 105, 115, 32, 116, 104, 101, 32, 102, 114, 117, 105, 116, 32, 111,
				102, 96, 12, 29, 115, 101, 97, 46, 32, 89, 111, 117, 32, 99, 97, 110, 32, 98,
				97, 114, 98, 101, 99, 117, 101, 32, 105, 116, 44, 32, 98, 111, 105, 108, 128,
				8, 0, 114, 224, 0, 9, 0, 97, 32, 90, 64, 27, 3, 115, 97, 117, 116, 64, 9, 9,
				46, 32, 68, 101, 121, 39, 115, 32, 117, 104, 32, 19, 96, 100, 6, 45, 107, 97,
				98, 111, 98, 115, 192, 14, 6, 32, 99, 114, 101, 111, 108, 101, 224, 0, 14, 7,
				103, 117, 109, 98, 111, 46, 32, 80, 32, 108, 10, 102, 114, 105, 101, 100, 44,
				32, 100, 101, 101, 112, 192, 11, 4, 115, 116, 105, 114, 45, 96, 23, 6, 46, 32,
				84, 104, 101, 114, 101, 32, 90, 7, 112, 105, 110, 101, 97, 112, 112, 108, 32,
				170, 96, 96, 32, 224, 3, 101, 109, 111, 110, 224, 0, 13, 6, 99, 111, 99, 111,
				110, 117, 116, 224, 0, 15, 5, 112, 101, 112, 112, 101, 114, 224, 0, 14, 128, 52,
				3, 32, 115, 111, 117, 32, 57, 192, 12, 2, 116, 101, 119, 224, 1, 12, 2, 97, 108,
				97, 32, 134, 160, 26, 10, 97, 110, 100, 32, 112, 111, 116, 97, 116, 111, 101,
				224, 1, 195, 5, 98, 117, 114, 103, 101, 114, 224, 0, 35, 0, 115, 32, 36, 3, 119,
				105, 99, 104, 64, 163, 2, 97, 116, 45, 33, 70, 13, 97, 116, 39, 115, 32, 97, 98,
				111, 117, 116, 32, 105, 116, 46,
			},
			expected: "Anyway, like I was sayin', shrimp is the fruit of the sea. You can " +
				"barbecue it, boil it, broil it, bake it, saute it. Dey's uh, shrimp-kabobs, " +
				"shrimp creole, shrimp gumbo. Pan fried, deep fried, stir-fried. There's " +
				"pineapple shrimp, lemon shrimp, coconut shrimp, pepper shrimp, shrimp soup, " +
				"shrimp stew, shrimp salad, shrimp and potatoes, shrimp burger, shrimp sandwich. " +
				"That- that's about it.",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			decompressed, err := decompressLZ77(test.compressed, len(test.expected))
			require.NoError(t, err)

			require.Equal(t, test.expected, string(decompressed))
		})
	}
}

func TestLZ77Decompression_corruptInput(t *testing.T) {
	tests := map[string]struct {
		compressed []byte
		outLen     int
	}{
		"corrupt input": {
			// Text: upupupupstash supports rdbrdbrdbrdbrdbrdb
			// The first byte should be 1 instead of 2
			compressed: []byte{
				2, 117, 112, 128, 1, 17, 115, 116, 97, 115, 104, 32, 115, 117, 112,
				112, 111, 114, 116, 115, 32, 114, 100, 98, 224, 1, 2, 4, 100, 98,
				114, 100, 98,
			},
			outLen: 41,
		},

		"wrong out len": {
			// Text: abcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabc
			// outLen should be 54
			compressed: []byte{
				2, 97, 98, 99, 224, 37, 2, 4, 98, 99, 97, 98, 99,
			},
			outLen: 100,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			decompressed, err := decompressLZ77(test.compressed, test.outLen)
			require.Nil(t, decompressed)
			require.Error(t, err)
		})
	}
}
