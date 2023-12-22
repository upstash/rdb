package rdb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc64"
	"io"
	"math/bits"
	"sync"
)

// Size of the checksum block. 2 (RDB version) + 8 (CRC-64) bytes in total.
const ValueChecksumSize = 10

// Polynomial used in the CRC64 table construction. This must be
// the same polynomial used in the Redis implementation so that
// we will be able to verify the checksum of the dumps prepared
// by Redis.
const poly uint64 = 0xAD93D23594C935A9

var buildOnce sync.Once
var crc64Table *crc64.Table

// VerifyValueChecksum uses the checksum block at the end of the
// payload(last 10 bytes) to verify that the CRC64 of the
// payload matches with the CRC value encoded at the last 8 bytes
// of the payload. It also verifies that the RDB version encoded
// in the checksum block is less than or equal to the RDB version
// supported by Upstash.
func VerifyValueChecksum(payload []byte) error {
	n := len(payload)
	if n < ValueChecksumSize {
		// The payload should be at least 2 (RDB version) + 8 (CRC64)
		// bytes long.
		return io.ErrUnexpectedEOF
	}

	version := binary.LittleEndian.Uint16(payload[n-ValueChecksumSize:])
	if version > Version {
		return fmt.Errorf("RDB version %d is not supported by Upstash", version)
	}

	crc := binary.LittleEndian.Uint64(payload[n-8:])
	expected := getCRC(0, payload[:n-8])
	if crc != expected {
		return errors.New("invalid CRC value for the payload")
	}

	return nil
}

// getCRC returns the CRC-64 of the payload, using the given CRC as a base.
func getCRC(crc uint64, payload []byte) uint64 {
	buildOnce.Do(buildCrc64Table)

	// Go implementation uses pre and post inversions while calculating the
	// CRC, but Redis does not. To make sure that we calculate the same
	// CRC we pass XOR of the initial CRC, and XOR the return value
	// to have the same effect.
	return ^crc64.Update(^crc, crc64Table, payload)
}

func buildCrc64Table() {
	table := new(crc64.Table)

	for i := 0; i < 256; i++ {
		var bit, crc uint64
		for j := uint8(1); j&0xFF != 0; j <<= 1 {
			bit = crc & 0x8000000000000000
			if uint8(i)&j != 0 {
				if bit == 0 {
					bit = 1
				} else {
					bit = 0
				}
			}

			crc <<= 1
			if bit != 0 {
				crc ^= poly
			}
		}

		table[i] = bits.Reverse64(crc)
	}

	crc64Table = table
}
