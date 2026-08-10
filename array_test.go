package rdb

import (
	"encoding/binary"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestReadArray(t *testing.T) {
	indexes, values, insertIndex := readArrayDump(t, "array.bin")

	require.Equal(t, ArrayInsertIndexNone, insertIndex)
	require.Equal(t, []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 1000, 1000000}, indexes)
	require.Equal(t, []string{
		"42",
		"-4242",
		"0",
		"2305843009213693951",
		"-2305843009213693952",
		"3.14",
		"-0.5",
		"1.0",
		"-0.0",
		"abc",
		"abcdefg",
		"",
		"4611686018427387904",
		"hello there world",
		"sparse",
		"far away",
	}, values)
}

func TestReadArray_withInsertCursor(t *testing.T) {
	indexes, values, insertIndex := readArrayDump(t, "array-with-cursor.bin")

	require.Equal(t, uint64(4), insertIndex)
	require.Equal(t, []uint64{0, 1, 2, 3, 4}, indexes)
	require.Equal(t, []string{"a", "b", "c", "1", "2.5"}, values)
}

func TestReadArray_lzfCompressedElement(t *testing.T) {
	indexes, values, insertIndex := readArrayDump(t, "array-lzf.bin")

	require.Equal(t, ArrayInsertIndexNone, insertIndex)
	require.Equal(t, []uint64{0}, indexes)
	require.Equal(t, []string{strings.Repeat("a", 200)}, values)
}

func TestReadArray_corrupt(t *testing.T) {
	tests := map[string]struct {
		payload []byte
		err     string
	}{
		"empty array": {
			// <len=0>
			payload: []byte{0x00},
			err:     "array must have at least one element",
		},

		"unexpected insert cursor marker": {
			// <len=1><has-cursor=2>
			payload: []byte{0x01, 0x02},
			err:     "unexpected array insert cursor marker 2",
		},

		"reserved insert cursor": {
			// <len=1><has-cursor=1><cursor=UINT64_MAX>
			payload: []byte{0x01, 0x01, len64Bit, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
			err:     "invalid array insert cursor",
		},

		"specially encoded insert cursor": {
			// <len=1><has-cursor=1><cursor=int8 encoding>
			payload: []byte{0x01, 0x01, 0xC0},
			err:     "invalid array insert cursor",
		},

		"length overflowing a signed integer": {
			// <len=1<<63><has-cursor=0>, with no elements to read
			payload: []byte{len64Bit, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			err:     "EOF",
		},

		"specially encoded index": {
			// <len=1><has-cursor=0><index=int8 encoding>
			payload: []byte{0x01, 0x00, 0xC0},
			err:     "invalid array index",
		},

		"unknown element type": {
			// <len=1><has-cursor=0><index=0><tag=4>
			payload: []byte{0x01, 0x00, 0x00, 0x04},
			err:     "unknown array element type 4",
		},

		"too long small string": {
			// <len=1><has-cursor=0><index=0><tag=3><len=8>abcdefgh
			payload: []byte{0x01, 0x00, 0x00, 0x03, 0x08, 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'},
			err:     "too long small string of length 8 in array",
		},

		"truncated element": {
			// <len=2><has-cursor=0><index=0><tag=1><incomplete integer>
			payload: []byte{0x02, 0x00, 0x00, 0x01, 0x00},
			err:     "EOF",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			r := valueReader{
				buf: newMemoryBackedBuffer(tc.payload),
			}

			_, _, err := r.ReadArray(func(index uint64, value string) error {
				return nil
			})
			require.ErrorContains(t, err, tc.err)
		})
	}
}

func TestWriteArray(t *testing.T) {
	tests := map[string]string{
		"without insert cursor": "array.bin",
		"with insert cursor":    "array-with-cursor.bin",
	}

	for name, file := range tests {
		t.Run(name, func(t *testing.T) {
			indexes, values, insertIndex := readArrayDump(t, file)

			writer := NewWriter()

			err := writer.WriteType(TypeArray)
			require.NoError(t, err)

			err = writer.WriteArray(indexes, values, insertIndex)
			require.NoError(t, err)

			// Redis stamps a newer RDB version into the checksum block of its
			// dumps than the one we write, so only the object itself can be
			// compared.
			require.Equal(t, loadValueDump(t, file), writer.GetBuffer())
		})
	}
}

func TestWriteArray_invalid(t *testing.T) {
	tests := map[string]struct {
		indexes []uint64
		values  []string
		err     string
	}{
		"mismatched lengths": {
			indexes: []uint64{0, 1},
			values:  []string{"a"},
			err:     "indexes and values must be of the same length",
		},

		"no elements": {
			indexes: []uint64{},
			values:  []string{},
			err:     "array must have at least one element",
		},

		"reserved index": {
			indexes: []uint64{ArrayInsertIndexNone},
			values:  []string{"a"},
			err:     "invalid array index",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			writer := NewWriter()
			err := writer.WriteArray(tc.indexes, tc.values, ArrayInsertIndexNone)
			require.ErrorContains(t, err, tc.err)
		})
	}
}

func TestEncodeArrayValue(t *testing.T) {
	tests := map[string]struct {
		value string
		tag   uint64
	}{
		"zero":                      {value: "0", tag: arrayTagInt},
		"positive integer":          {value: "42", tag: arrayTagInt},
		"negative integer":          {value: "-4242", tag: arrayTagInt},
		"biggest inline integer":    {value: "2305843009213693951", tag: arrayTagInt},
		"smallest inline integer":   {value: "-2305843009213693952", tag: arrayTagInt},
		"too big integer":           {value: "2305843009213693952", tag: arrayTagStr},
		"too small integer":         {value: "-2305843009213693953", tag: arrayTagStr},
		"integer with leading zero": {value: "007", tag: arrayTagSmallStr},
		"integer with plus sign":    {value: "+7", tag: arrayTagSmallStr},
		"negative zero integer":     {value: "-0", tag: arrayTagSmallStr},
		"whole float":               {value: "1.0", tag: arrayTagFloat},
		"negative zero float":       {value: "-0.0", tag: arrayTagFloat},
		"exact float":               {value: "-0.5", tag: arrayTagFloat},
		"Redis fpconv float":        {value: "171422365113461.13", tag: arrayTagFloat},
		"alternate shortest float":  {value: "171422365113461.12", tag: arrayTagStr},
		"inexact float":             {value: "3.14", tag: arrayTagSmallStr},
		"float in exponent form":    {value: "1e5", tag: arrayTagSmallStr},
		"not a number":              {value: "nan", tag: arrayTagSmallStr},
		"infinity":                  {value: "inf", tag: arrayTagSmallStr},
		"empty string":              {value: "", tag: arrayTagSmallStr},
		"biggest small string":      {value: "abcdefg", tag: arrayTagSmallStr},
		"string":                    {value: "abcdefgh", tag: arrayTagStr},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			tag, ival, fval := encodeArrayValue(tc.value)
			require.Equal(t, tc.tag, tag)

			// Whatever the tag is, the value must be read back as it is.
			writer := NewWriter()
			require.NoError(t, writer.WriteType(TypeArray))
			require.NoError(t, writer.WriteArray([]uint64{7}, []string{tc.value}, ArrayInsertIndexNone))

			indexes := make([]uint64, 0)
			values := make([]string, 0)
			r := valueReader{
				buf: newMemoryBackedBuffer(writer.GetBuffer()[1:]),
			}
			_, _, err := r.ReadArray(func(index uint64, value string) error {
				indexes = append(indexes, index)
				values = append(values, value)
				return nil
			})
			require.NoError(t, err)
			require.Equal(t, []uint64{7}, indexes)
			require.Equal(t, []string{tc.value}, values)

			switch tag {
			case arrayTagInt:
				require.Equal(t, tc.value, strconv.FormatInt(ival, 10))
			case arrayTagFloat:
				require.Equal(t, tc.value, formatArrayFloat(fval))
			}
		})
	}
}

func TestFormatArrayFloat(t *testing.T) {
	// The expected values are the ones Redis replies with for the same floats.
	tests := map[float64]string{
		0:                           "0.0",
		math.Copysign(0, -1):        "-0.0",
		1:                           "1.0",
		-1:                          "-1.0",
		0.5:                         "0.5",
		-0.5:                        "-0.5",
		0.1:                         "0.1",
		0.0625:                      "0.0625",
		2.5:                         "2.5",
		3.14:                        "3.14",
		1024:                        "1024.0",
		1e6:                         "1000000.0",
		1e15:                        "1000000000000000.0",
		1e17:                        "100000000000000000.0",
		9007199254740992:            "9007199254740992.0",
		1e21:                        "1e+21",
		1e22:                        "1e+22",
		123456789012345680000:       "123456789012345680000.0",
		6.02e23:                     "6.02e+23",
		1e-4:                        "0.0001",
		1e-5:                        "0.00001",
		1e-6:                        "0.000001",
		1.5e-7:                      "1.5e-7",
		-3.75e-9:                    "-3.75e-9",
		1e100:                       "1e+100",
		1e-100:                      "1e-100",
		math.MaxFloat64:             "1.7976931348623157e+308",
		1.7976931348623157e-308:     "1.7976931348623155e-308",
		math.SmallestNonzeroFloat64: "5e-324",
		171422365113461.13:          "171422365113461.13",
		math.Inf(1):                 "inf",
		math.Inf(-1):                "-inf",
		math.NaN():                  "nan",
	}

	for value, expected := range tests {
		require.Equal(t, expected, formatArrayFloat(value), "for %v", value)
	}
}

func TestReadValue_arrayFloatUsesFpconv(t *testing.T) {
	payload := []byte{byte(TypeArray), 1, 0, 0, byte(arrayTagFloat)}
	bits := make([]byte, 8)
	binary.LittleEndian.PutUint64(bits, 0x42e37d0c25bb8ea4)
	payload = append(payload, bits...)

	db := newDummyDB()
	require.NoError(t, ReadValue("array", payload, db))
	require.Equal(t, "171422365113461.13", db.arrays["array"][0])
}

func TestWriteArray_usesFpconv(t *testing.T) {
	tests := map[string]struct {
		value string
		tag   byte
	}{
		"Redis representation": {
			value: "171422365113461.13",
			tag:   byte(arrayTagFloat),
		},
		"alternate representation": {
			value: "171422365113461.12",
			tag:   byte(arrayTagStr),
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			writer := NewWriter()
			require.NoError(t, writer.WriteType(TypeArray))
			require.NoError(t, writer.WriteArray([]uint64{0}, []string{tc.value}, ArrayInsertIndexNone))

			// <type><len=1><has-cursor=0><index=0><tag>
			require.Equal(t, tc.tag, writer.GetBuffer()[4])
		})
	}
}

func TestEncoder_Array(t *testing.T) {
	tests := map[string]struct {
		key         string
		indexes     []uint64
		values      []string
		insertIndex uint64
	}{
		"without insert cursor": {
			key:         "array",
			indexes:     []uint64{0, 5, 1 << 40},
			values:      []string{"42", "1.0", "a long enough element"},
			insertIndex: ArrayInsertIndexNone,
		},

		"with insert cursor": {
			key:         "array-with-cursor",
			indexes:     []uint64{0, 1, 2},
			values:      []string{"a", "b", "c"},
			insertIndex: 2,
		},
	}

	rdbFile := filepath.Join(t.TempDir(), "array.rdb")

	encoder, err := NewFileEncoder(rdbFile, version)
	require.NoError(t, err)
	require.NoError(t, encoder.Begin())

	for _, tc := range tests {
		arrayEncoder, err := encoder.BeginArray(tc.key, tc.insertIndex, time.Time{})
		require.NoError(t, err)

		for i, index := range tc.indexes {
			require.NoError(t, arrayEncoder.WriteFieldUint64Str(index, tc.values[i]))
		}

		require.NoError(t, arrayEncoder.Close())
	}

	require.NoError(t, encoder.Close())

	db := newDummyDB()
	require.NoError(t, ReadFile(rdbFile, db))

	for name, tc := range tests {
		expected := make(map[uint64]string, len(tc.indexes))
		for i, index := range tc.indexes {
			expected[index] = tc.values[i]
		}

		require.Equal(t, expected, db.arrays[tc.key], name)
		require.Equal(t, uint64(len(tc.indexes)), db.arrayEntriesRead[tc.key], name)
		require.Equal(t, tc.insertIndex, db.arrayInsertIndexes[tc.key], name)
	}
}

func TestEncoder_emptyArray(t *testing.T) {
	rdbFile := filepath.Join(t.TempDir(), "empty-array.rdb")

	encoder, err := NewFileEncoder(rdbFile, version)
	require.NoError(t, err)
	require.NoError(t, encoder.Begin())

	arrayEncoder, err := encoder.BeginArray("array", ArrayInsertIndexNone, time.Time{})
	require.NoError(t, err)

	// Redis rejects the empty arrays, so closing one without any elements must
	// not produce a file we cannot read back.
	require.ErrorIs(t, arrayEncoder.Close(), errEmptyArray)
}

func TestVerifyValue_arrayMaxEntrySize(t *testing.T) {
	dump, err := os.ReadFile(filepath.Join(valueDumpsPath, "array.bin"))
	require.NoError(t, err)

	err = VerifyValue(dump, VerifyValueOptions{
		MaxEntrySize: 32,
	})
	require.ErrorContains(t, err, "max entry size")

	err = VerifyValue(dump, VerifyValueOptions{})
	require.NoError(t, err)
}

func loadValueDump(t *testing.T, file string) []byte {
	dump, err := os.ReadFile(filepath.Join(valueDumpsPath, file))
	require.NoError(t, err)
	require.Greater(t, len(dump), ValueChecksumSize)

	crc := binary.LittleEndian.Uint64(dump[len(dump)-8:])
	require.Equal(t, getCRC(0, dump[:len(dump)-8]), crc)

	return dump[:len(dump)-ValueChecksumSize]
}

func readArrayDump(t *testing.T, file string) ([]uint64, []string, uint64) {
	dump := loadValueDump(t, file)

	r := valueReader{
		buf: newMemoryBackedBuffer(dump),
	}

	ot, err := r.ReadType()
	require.NoError(t, err)
	require.Equal(t, TypeArray, ot)

	indexes := make([]uint64, 0)
	values := make([]string, 0)
	cb := func(index uint64, value string) error {
		indexes = append(indexes, index)
		values = append(values, value)
		return nil
	}

	read, insertIndex, err := r.ReadArray(cb)
	require.NoError(t, err)
	require.Equal(t, uint64(len(indexes)), read)

	return indexes, values, insertIndex
}
