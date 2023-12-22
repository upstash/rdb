package rdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVerifyChecksum(t *testing.T) {
	tests := map[string]struct {
		payload    []byte
		shouldFail bool
	}{
		"too small payload": {
			payload:    []byte{1, 2, 3},
			shouldFail: true,
		},

		"empty payload": {
			payload:    []byte{11, 0, 52, 68, 225, 51, 242, 224, 75, 83},
			shouldFail: false,
		},

		"not yet supported version": {
			// versin 42 is not supported yet
			payload:    []byte{42, 0, 255, 50, 213, 243, 8, 202, 213, 26},
			shouldFail: true,
		},

		"invalid crc": {
			// first byte should be 0
			payload:    []byte{1, 8, 33, 85, 80, 115, 116, 97, 115, 104, 10, 0, 118, 38, 238, 102, 71, 149, 199, 18},
			shouldFail: true,
		},

		"valid crc": {
			payload:    []byte{0, 12, 117, 112, 115, 116, 97, 115, 104, 114, 111, 99, 107, 115, 10, 0, 219, 124, 214, 167, 201, 155, 113, 148},
			shouldFail: false,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := VerifyValueChecksum(test.payload)
			if test.shouldFail {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestCRC64(t *testing.T) {
	tests := map[string]struct {
		payload  []byte
		expected uint64
	}{
		"empty payload": {
			payload:  []byte{},
			expected: 0,
		},

		"non-empty payload": {
			payload:  []byte{1, 2, 3, 4, 44, 42, 252},
			expected: 816497613141667909,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			crc := getCRC(0, test.payload)
			assert.Equal(t, test.expected, crc)
		})
	}
}
