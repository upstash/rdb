package rdb

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/ohler55/ojg/oj"
	"github.com/stretchr/testify/require"
)

const alpahabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"

var valueDumpsPath = filepath.Join("testdata", "valuedumps")

func generateAlphabetCycle(length int) string {
	var sb strings.Builder
	sb.Grow(length)

	for i := 0; i < length; i++ {
		_ = sb.WriteByte(alpahabet[i%len(alpahabet)])
	}

	return sb.String()
}

func TestReadString(t *testing.T) {
	tests := map[string]struct {
		file     string
		expected string
	}{
		"empty string": {
			file:     "string-empty.bin",
			expected: "",
		},

		"int8 as string": {
			file:     "string-int8.bin",
			expected: "42",
		},

		"int16 as string": {
			file:     "string-int16.bin",
			expected: "-4242",
		},

		"int32 as string": {
			file:     "string-int32.bin",
			expected: "42424242",
		},

		"LZF compressed": {
			file:     "string-lzf.bin",
			expected: generateAlphabetCycle(1000),
		},

		"normal string": {
			file:     "string.bin",
			expected: generateAlphabetCycle(142),
		},

		"long string": {
			file:     "string-long.bin",
			expected: generateAlphabetCycle(20_000),
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			dump, err := os.ReadFile(filepath.Join(valueDumpsPath, tc.file))
			require.NoError(t, err)

			err = VerifyValueChecksum(dump)
			require.NoError(t, err)

			dump = dump[:len(dump)-10]

			r := valueReader{
				buf: newMemoryBackedBuffer(dump),
			}

			ot, err := r.ReadType()
			require.NoError(t, err)

			require.Equal(t, TypeString, ot)

			value, err := r.ReadString()
			require.NoError(t, err)
			require.Equal(t, tc.expected, value)
		})
	}
}

func TestReadString_withMaxLz77StrLen(t *testing.T) {
	dump, err := os.ReadFile(filepath.Join(valueDumpsPath, "string-lzf.bin"))
	require.NoError(t, err)

	err = VerifyValueChecksum(dump)
	require.NoError(t, err)

	dump = dump[:len(dump)-10]

	r := valueReader{
		buf:           newMemoryBackedBuffer(dump),
		maxLz77StrLen: 10,
	}

	ot, err := r.ReadType()
	require.NoError(t, err)

	require.Equal(t, TypeString, ot)

	_, err = r.ReadString()
	require.ErrorContains(t, err, "uncompressed length")
}

func TestReadList(t *testing.T) {
	path := filepath.Join(valueDumpsPath, "list.bin")

	dump, err := os.ReadFile(path)
	require.NoError(t, err)

	err = VerifyValueChecksum(dump)
	require.NoError(t, err)

	dump = dump[:len(dump)-10]

	r := valueReader{
		buf: newMemoryBackedBuffer(dump),
	}

	ot, err := r.ReadType()
	require.NoError(t, err)

	require.Equal(t, TypeList, ot)

	list := make([]string, 0)
	cb := func(elem string) error {
		list = append(list, elem)
		return nil
	}
	read, err := r.ReadList(cb)
	require.NoError(t, err)
	require.Equal(t, uint64(7), read)
	require.Equal(t, []string{"a", "b", "c", "1", "2", "3", "def"}, list)
}

func TestReadSet(t *testing.T) {
	path := filepath.Join(valueDumpsPath, "set.bin")

	dump, err := os.ReadFile(path)
	require.NoError(t, err)

	err = VerifyValueChecksum(dump)
	require.NoError(t, err)

	dump = dump[:len(dump)-10]

	r := valueReader{
		buf: newMemoryBackedBuffer(dump),
	}

	ot, err := r.ReadType()
	require.NoError(t, err)

	require.Equal(t, TypeSet, ot)

	set := make([]string, 0)
	cb := func(elem string) error {
		set = append(set, elem)
		return nil
	}
	read, err := r.ReadList(cb)
	require.NoError(t, err)
	require.Equal(t, uint64(6), read)
	require.ElementsMatch(t, []string{"hello", "world", "1", "2", "3", "upstash"}, set)
}

func TestReadZset(t *testing.T) {
	path := filepath.Join(valueDumpsPath, "zset.bin")

	dump, err := os.ReadFile(path)
	require.NoError(t, err)

	err = VerifyValueChecksum(dump)
	require.NoError(t, err)

	dump = dump[:len(dump)-10]

	r := valueReader{
		buf: newMemoryBackedBuffer(dump),
	}

	ot, err := r.ReadType()
	require.NoError(t, err)

	require.Equal(t, TypeZset, ot)

	zset := make(map[string]float64)
	cb := func(elem string, score float64) error {
		zset[elem] = score
		return nil
	}
	read, err := r.ReadZset(cb)
	require.NoError(t, err)
	require.Equal(t, uint64(6), read)
	require.Equal(t, map[string]float64{
		"score0": 0,
		"score1": 1.5,
		"score2": 2.5,
		"score3": 42.42,
		"neginf": math.Inf(-1),
		"posinf": math.Inf(1),
	}, zset)
}

func TestReadHash(t *testing.T) {
	path := filepath.Join(valueDumpsPath, "hash.bin")

	dump, err := os.ReadFile(path)
	require.NoError(t, err)

	err = VerifyValueChecksum(dump)
	require.NoError(t, err)

	dump = dump[:len(dump)-10]

	r := valueReader{
		buf: newMemoryBackedBuffer(dump),
	}

	ot, err := r.ReadType()
	require.NoError(t, err)

	require.Equal(t, TypeHash, ot)

	hash := make(map[string]string)
	cb := func(field, value string) error {
		hash[field] = value
		return nil
	}
	err = r.ReadHash(cb)
	require.NoError(t, err)
	require.Equal(t,
		map[string]string{"a": "1", "b": "2", "cd": "34", "efgh": "upstash"},
		hash,
	)
}

func TestReadZset2(t *testing.T) {
	path := filepath.Join(valueDumpsPath, "zset2.bin")

	dump, err := os.ReadFile(path)
	require.NoError(t, err)

	err = VerifyValueChecksum(dump)
	require.NoError(t, err)

	dump = dump[:len(dump)-10]

	r := valueReader{
		buf: newMemoryBackedBuffer(dump),
	}

	ot, err := r.ReadType()
	require.NoError(t, err)

	require.Equal(t, TypeZset2, ot)

	zset := make(map[string]float64)
	cb := func(elem string, score float64) error {
		zset[elem] = score
		return nil
	}
	read, err := r.ReadZset2(cb)
	require.NoError(t, err)
	require.Equal(t, uint64(7), read)
	require.Equal(t, map[string]float64{
		"s1": 23.23,
		"s2": -32.32,
		"s3": math.Inf(-1),
		"s4": math.Inf(1),
		"s5": 0,
		"s6": 1,
		"s7": 42,
	}, zset)
}

func TestReadModule2_JSON(t *testing.T) {
	expected := map[string]any{
		"str":    "upstash",
		"number": 42.42,
		"int":    424242.0,
		"bool":   true,
		"nested": map[string]any{
			"null": nil,
			"str":  "rdb",
			"arr":  []any{1.0, 2.0, "upstash"},
		},
		"arr": []any{42.0, []any{42.0}, map[string]any{"arr": []any{"x", "y", []any{"z"}, false, nil, -4242.4242}}},
	}

	tests := map[string]string{
		"v0": "module2-jsonv0.bin",
		"v3": "module2-jsonv3.bin",
	}

	for name, file := range tests {
		t.Run(name, func(t *testing.T) {

			dump, err := os.ReadFile(filepath.Join(valueDumpsPath, file))
			require.NoError(t, err)

			err = VerifyValueChecksum(dump)
			require.NoError(t, err)

			dump = dump[:len(dump)-10]

			r := valueReader{
				buf: newMemoryBackedBuffer(dump),
			}

			ot, err := r.ReadType()
			require.NoError(t, err)

			require.Equal(t, TypeModule2, ot)

			value, moduleMarker, err := r.ReadModule2(false)
			require.Equal(t, JSONModuleMarker, moduleMarker)
			require.NoError(t, err)

			var actual map[string]any
			err = oj.Unmarshal([]byte(value), &actual)
			require.NoError(t, err)

			require.Equal(t, expected, actual)
		})
	}
}

func TestReadModule2_UnsupportedModule(t *testing.T) {
	path := filepath.Join(valueDumpsPath, "module2-bloomfilter.bin")

	dump, err := os.ReadFile(path)
	require.NoError(t, err)

	err = VerifyValueChecksum(dump)
	require.NoError(t, err)

	dump = dump[:len(dump)-10]

	r := valueReader{
		buf: newMemoryBackedBuffer(dump),
	}

	ot, err := r.ReadType()
	require.NoError(t, err)

	require.Equal(t, TypeModule2, ot)

	_, _, err = r.ReadModule2(false)
	require.ErrorContains(t, err, "MBbloom--")
}

func TestReadModule2_UnsupportedModule_withSkip(t *testing.T) {
	path := filepath.Join(valueDumpsPath, "module2-bloomfilter.bin")

	dump, err := os.ReadFile(path)
	require.NoError(t, err)

	err = VerifyValueChecksum(dump)
	require.NoError(t, err)

	dump = dump[:len(dump)-10]

	r := valueReader{
		buf: newMemoryBackedBuffer(dump),
	}

	ot, err := r.ReadType()
	require.NoError(t, err)

	require.Equal(t, TypeModule2, ot)

	_, _, err = r.ReadModule2(true)
	require.NoError(t, err)
}

func TestReadHashZipmap(t *testing.T) {
	bigLenExpected := make(map[string]string)
	for i := 0; i < 300; i++ {
		bigLenExpected[strconv.Itoa(i)] = "x"
	}

	tests := map[string]struct {
		file     string
		expected map[string]string
	}{
		"small len": {
			file: "hash-zipmap-small.bin",
			expected: map[string]string{
				"a": "1", "b": "2", "up": "stash",
				generateAlphabetCycle(253): generateAlphabetCycle(253),
				generateAlphabetCycle(254): generateAlphabetCycle(255),
				generateAlphabetCycle(255): generateAlphabetCycle(254),
				generateAlphabetCycle(333): generateAlphabetCycle(300),
			},
		},

		"big len": {
			file:     "hash-zipmap-big.bin",
			expected: bigLenExpected,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			dump, err := os.ReadFile(filepath.Join(valueDumpsPath, tc.file))
			require.NoError(t, err)

			err = VerifyValueChecksum(dump)
			require.NoError(t, err)

			dump = dump[:len(dump)-10]

			r := valueReader{
				buf: newMemoryBackedBuffer(dump),
			}

			ot, err := r.ReadType()
			require.NoError(t, err)

			require.Equal(t, TypeHashZipmap, ot)

			hash := make(map[string]string)
			cb := func(field, value string) error {
				hash[field] = value
				return nil
			}
			err = r.ReadHashZipmap(cb)
			require.NoError(t, err)
			require.Equal(t, tc.expected, hash)
		})
	}
}

func TestReadListZiplist(t *testing.T) {
	bigLenExpected := make([]string, 70_000)
	for i := 0; i < 70_000; i++ {
		bigLenExpected[i] = "x"
	}

	tests := map[string]struct {
		file         string
		expected     []string
		expectedRead uint64
	}{
		"small len": {
			file: "list-ziplist-small.bin",
			expected: []string{
				generateAlphabetCycle(18_000),
				generateAlphabetCycle(100),
				"upstash",
				"3147483648",
				"-9388608",
				"40422",
				"-12345",
				"13",
				"100",
				"12",
				"0",
				"-1",
			},
			expectedRead: 12,
		},

		"big len": {
			file:         "list-ziplist-big.bin",
			expected:     bigLenExpected,
			expectedRead: 70_000,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			dump, err := os.ReadFile(filepath.Join(valueDumpsPath, tc.file))
			require.NoError(t, err)

			err = VerifyValueChecksum(dump)
			require.NoError(t, err)

			dump = dump[:len(dump)-10]

			r := valueReader{
				buf: newMemoryBackedBuffer(dump),
			}

			ot, err := r.ReadType()
			require.NoError(t, err)

			require.Equal(t, TypeListZiplist, ot)

			list := make([]string, 0)
			cb := func(elem string) error {
				list = append(list, elem)
				return nil
			}
			read, err := r.ReadListZiplist(cb)
			require.NoError(t, err)
			require.Equal(t, tc.expectedRead, read)
			require.Equal(t, tc.expected, list)
		})
	}
}

func TestReadSetIntset(t *testing.T) {
	tests := map[string]struct {
		file     string
		expected []string
	}{
		"int16": {
			file:     "set-intset-int16.bin",
			expected: []string{"-42", "0", "42", "2323", "-12342"},
		},

		"int32": {
			file:     "set-intset-int32.bin",
			expected: []string{"-424242", "191919", "42000", "-1234567"},
		},

		"int64": {
			file:     "set-intset-int64.bin",
			expected: []string{"4294967296", "-14234167290", "4444444444", "-2323232323"},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			dump, err := os.ReadFile(filepath.Join(valueDumpsPath, tc.file))
			require.NoError(t, err)

			err = VerifyValueChecksum(dump)
			require.NoError(t, err)

			dump = dump[:len(dump)-10]

			r := valueReader{
				buf: newMemoryBackedBuffer(dump),
			}

			ot, err := r.ReadType()
			require.NoError(t, err)

			require.Equal(t, TypeSetIntset, ot)

			set := make([]string, 0)
			cb := func(elem string) error {
				set = append(set, elem)
				return nil
			}
			err = r.ReadSetIntset(cb)
			require.NoError(t, err)
			require.ElementsMatch(t, tc.expected, set)
		})
	}
}

func TestReadZsetZiplist(t *testing.T) {
	path := filepath.Join(valueDumpsPath, "zset-ziplist.bin")

	dump, err := os.ReadFile(path)
	require.NoError(t, err)

	err = VerifyValueChecksum(dump)
	require.NoError(t, err)

	dump = dump[:len(dump)-10]

	r := valueReader{
		buf: newMemoryBackedBuffer(dump),
	}

	ot, err := r.ReadType()
	require.NoError(t, err)

	require.Equal(t, TypeZsetZiplist, ot)

	zset := make(map[string]float64)
	cb := func(elem string, score float64) error {
		zset[elem] = score
		return nil
	}
	read, err := r.ReadZsetZiplist(cb)
	require.NoError(t, err)
	require.Equal(t, uint64(6), read)
	require.Equal(t, map[string]float64{
		"s0": 12.21,
		"s1": -1.1,
		"s2": math.Inf(-1),
		"s3": 4242.4242,
		"s4": math.Inf(1),
		"s5": 0,
	}, zset)
}

func TestReadHashZiplist(t *testing.T) {
	path := filepath.Join(valueDumpsPath, "hash-ziplist.bin")

	dump, err := os.ReadFile(path)
	require.NoError(t, err)

	err = VerifyValueChecksum(dump)
	require.NoError(t, err)

	dump = dump[:len(dump)-10]

	r := valueReader{
		buf: newMemoryBackedBuffer(dump),
	}

	ot, err := r.ReadType()
	require.NoError(t, err)

	require.Equal(t, TypeHashZiplist, ot)

	hash := make(map[string]string)
	cb := func(field, value string) error {
		hash[field] = value
		return nil
	}
	err = r.ReadHashZiplist(cb)
	require.NoError(t, err)
	require.Equal(t,
		map[string]string{"foo": "bar", "bar": "baz", "a": "42", "up": "stash", "b": "23", "c": "-34"},
		hash,
	)
}

func TestReadListQuicklist(t *testing.T) {
	bigLenExpected := make([]string, 1_000)
	for i := 0; i < 1_000; i++ {
		bigLenExpected[i] = "x"
	}

	tests := map[string]struct {
		file         string
		expected     []string
		expectedRead uint64
	}{
		"small len": {
			file:         "list-quicklist-small.bin",
			expected:     []string{"1", "up", "2", "stash"},
			expectedRead: 4,
		},

		"big len": {
			file:         "list-quicklist-big.bin",
			expected:     bigLenExpected,
			expectedRead: 1_000,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			dump, err := os.ReadFile(filepath.Join(valueDumpsPath, tc.file))
			require.NoError(t, err)

			err = VerifyValueChecksum(dump)
			require.NoError(t, err)

			dump = dump[:len(dump)-10]

			r := valueReader{
				buf: newMemoryBackedBuffer(dump),
			}

			ot, err := r.ReadType()
			require.NoError(t, err)

			require.Equal(t, TypeListQuicklist, ot)

			list := make([]string, 0)
			cb := func(elem string) error {
				list = append(list, elem)
				return nil
			}
			read, err := r.ReadListQuicklist(cb)
			require.NoError(t, err)
			require.Equal(t, tc.expectedRead, read)
			require.Equal(t, tc.expected, list)
		})
	}
}

func TestReadHashListpack(t *testing.T) {
	path := filepath.Join(valueDumpsPath, "hash-listpack.bin")

	dump, err := os.ReadFile(path)
	require.NoError(t, err)

	err = VerifyValueChecksum(dump)
	require.NoError(t, err)

	dump = dump[:len(dump)-10]

	r := valueReader{
		buf: newMemoryBackedBuffer(dump),
	}

	ot, err := r.ReadType()
	require.NoError(t, err)

	require.Equal(t, TypeHashListpack, ot)

	hash := make(map[string]string)
	cb := func(field, value string) error {
		hash[field] = value
		return nil
	}
	err = r.ReadHashListpack(cb)
	require.NoError(t, err)
	require.Equal(t,
		map[string]string{
			"uint7":    "42",
			"int13":    "-1234",
			"int16":    "42000",
			"int24":    "-424242",
			"int32":    "96777216",
			"int64":    "-42949672960",
			"6bitstr":  "upstash",
			"12bitstr": generateAlphabetCycle(100),
			"32bitstr": generateAlphabetCycle(5_000),
		},
		hash,
	)
}

func TestReadZsetListpack(t *testing.T) {
	path := filepath.Join(valueDumpsPath, "zset-listpack.bin")

	dump, err := os.ReadFile(path)
	require.NoError(t, err)

	err = VerifyValueChecksum(dump)
	require.NoError(t, err)

	dump = dump[:len(dump)-10]

	r := valueReader{
		buf: newMemoryBackedBuffer(dump),
	}

	ot, err := r.ReadType()
	require.NoError(t, err)

	require.Equal(t, TypeZsetListpack, ot)

	zset := make(map[string]float64)
	cb := func(elem string, score float64) error {
		zset[elem] = score
		return nil
	}
	read, err := r.ReadZsetListpack(cb)
	require.NoError(t, err)
	require.Equal(t, uint64(9), read)
	require.Equal(t, map[string]float64{
		"-42":                        0,
		"1234":                       -1.542,
		"-42000":                     42123.23,
		"424242":                     123123131,
		"-96777216":                  -9999999,
		"42949672960":                math.Inf(-1),
		"upstash":                    math.Inf(1),
		generateAlphabetCycle(99):    99,
		generateAlphabetCycle(5_001): -50.01,
	}, zset)
}

func TestReadListQuicklist2(t *testing.T) {
	smallExpected := make([]string, 20)
	for i := 0; i < 20; i++ {
		smallExpected[i] = generateAlphabetCycle(i * 10)
	}

	bigExpected := make([]string, 70_000)
	for i := 0; i < 70_000; i++ {
		bigExpected[i] = "x"
	}

	tests := map[string]struct {
		file         string
		expected     []string
		expectedRead uint64
	}{
		"small len": {
			file:         "list-quicklist2-small.bin",
			expected:     smallExpected,
			expectedRead: 20,
		},

		"big len": {
			file:         "list-quicklist2-big.bin",
			expected:     bigExpected,
			expectedRead: 70_000,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			dump, err := os.ReadFile(filepath.Join(valueDumpsPath, tc.file))
			require.NoError(t, err)

			err = VerifyValueChecksum(dump)
			require.NoError(t, err)

			dump = dump[:len(dump)-10]

			r := valueReader{
				buf: newMemoryBackedBuffer(dump),
			}

			ot, err := r.ReadType()
			require.NoError(t, err)

			require.Equal(t, TypeListQuicklist2, ot)

			list := make([]string, 0)
			cb := func(elem string) error {
				list = append(list, elem)
				return nil
			}
			read, err := r.ReadListQuicklist2(cb)
			require.NoError(t, err)
			require.Equal(t, tc.expectedRead, read)
			require.Equal(t, tc.expected, list)
		})
	}
}

func TestReadSetListpack(t *testing.T) {
	path := filepath.Join(valueDumpsPath, "set-listpack.bin")

	dump, err := os.ReadFile(path)
	require.NoError(t, err)

	err = VerifyValueChecksum(dump)
	require.NoError(t, err)

	dump = dump[:len(dump)-10]

	r := valueReader{
		buf: newMemoryBackedBuffer(dump),
	}

	ot, err := r.ReadType()
	require.NoError(t, err)

	require.Equal(t, TypeSetListpack, ot)

	set := make([]string, 0)
	cb := func(elem string) error {
		set = append(set, elem)
		return nil
	}
	err = r.ReadSetListpack(cb)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"up", "stash", "rdb", "23343423", "-42"}, set)
}

func TestReadStreamListpacks(t *testing.T) {
	path := filepath.Join(valueDumpsPath, "stream-listpacks.bin")

	dump, err := os.ReadFile(path)
	require.NoError(t, err)

	err = VerifyValueChecksum(dump)
	require.NoError(t, err)

	dump = dump[:len(dump)-10]

	r := valueReader{
		buf: newMemoryBackedBuffer(dump),
	}

	ot, err := r.ReadType()
	require.NoError(t, err)

	require.Equal(t, TypeStreamListpacks, ot)

	entries := make([]StreamEntry, 0)
	entryCB := func(entry StreamEntry) error {
		entries = append(entries, entry)
		return nil
	}

	groups := make([]StreamConsumerGroup, 0)
	groupCB := func(group StreamConsumerGroup) error {
		groups = append(groups, group)
		return nil
	}

	read, err := r.ReadStreamListpacks(entryCB, groupCB)
	require.NoError(t, err)
	require.Equal(t, uint64(3), read)

	expectedEntries := make([]StreamEntry, 3)
	expectedEntries[0] = StreamEntry{
		ID:    StreamID{Millis: 1693576721400, Seq: 0},
		Value: []string{"up", "up1", "stash", "stash1", "upstash", "upstash1"},
	}

	expectedEntries[1] = StreamEntry{
		ID:    StreamID{Millis: 1693576731925, Seq: 0},
		Value: []string{"up", "up2", "stash", "stash2", "upstash", "upstash2"},
	}
	expectedEntries[2] = StreamEntry{
		ID:    StreamID{Millis: 1693576737806, Seq: 0},
		Value: []string{"up", "up3", "stash", "stash3", "upstash", "upstash3"},
	}
	require.Equal(t, expectedEntries, entries)

	expectedGroups := make([]StreamConsumerGroup, 1)
	expectedGroups[0] = StreamConsumerGroup{
		Name:        "gg",
		LastID:      StreamID{Millis: 1693576737806, Seq: 0},
		EntriesRead: 0,
		Consumers: []StreamConsumer{
			{
				Name:           "cc",
				SeenTime:       1693576868734,
				ActiveTime:     0,
				PendingEntries: []*StreamPendingEntry{},
			},
		},
	}

	require.Equal(t, expectedGroups, groups)
}

func TestReadStreamListpacks2(t *testing.T) {
	path := filepath.Join(valueDumpsPath, "stream-listpacks2.bin")

	dump, err := os.ReadFile(path)
	require.NoError(t, err)

	err = VerifyValueChecksum(dump)
	require.NoError(t, err)

	dump = dump[:len(dump)-10]

	r := valueReader{
		buf: newMemoryBackedBuffer(dump),
	}

	ot, err := r.ReadType()
	require.NoError(t, err)

	require.Equal(t, TypeStreamListpacks2, ot)

	entries := make([]StreamEntry, 0)
	entryCB := func(entry StreamEntry) error {
		entries = append(entries, entry)
		return nil
	}

	groups := make([]StreamConsumerGroup, 0)
	groupCB := func(group StreamConsumerGroup) error {
		groups = append(groups, group)
		return nil
	}

	read, err := r.ReadStreamListpacks2(entryCB, groupCB)
	require.NoError(t, err)
	require.Equal(t, uint64(6), read)

	expectedEntries := make([]StreamEntry, 6)
	expectedEntries[0] = StreamEntry{
		ID:    StreamID{Millis: 1693571577029, Seq: 0},
		Value: []string{"up", "up", "stash", "stash"},
	}
	expectedEntries[1] = StreamEntry{
		ID:    StreamID{Millis: 1693571578033, Seq: 0},
		Value: []string{"bar", "1", "baz", "2"},
	}
	expectedEntries[2] = StreamEntry{
		ID:    StreamID{Millis: 1693571578034, Seq: 0},
		Value: []string{"up", "upstash", "1", "1"},
	}
	expectedEntries[3] = StreamEntry{
		ID:    StreamID{Millis: 1693571578038, Seq: 0},
		Value: []string{"up", "up1"},
	}
	expectedEntries[4] = StreamEntry{
		ID:    StreamID{Millis: 1693571578039, Seq: 0},
		Value: []string{"up", "up2"},
	}
	expectedEntries[5] = StreamEntry{
		ID:    StreamID{Millis: 1693571578040, Seq: 0},
		Value: []string{"up", "up3"},
	}
	require.Equal(t, expectedEntries, entries)

	expectedGroups := make([]StreamConsumerGroup, 1)
	expectedGroups[0] = StreamConsumerGroup{
		Name:        "group!",
		LastID:      StreamID{Millis: 1693571578039, Seq: 0},
		EntriesRead: -1,
		Consumers: []StreamConsumer{
			{
				Name:       "consumer!",
				SeenTime:   1693571578041,
				ActiveTime: 0,
				PendingEntries: []*StreamPendingEntry{
					{
						Entry: StreamEntry{
							ID:    StreamID{Millis: 1693571578039, Seq: 0},
							Value: []string{"up", "up2"},
						},
						DeliveryTime:  1693571578041,
						DeliveryCount: 1,
					},
				},
			},
		},
	}

	require.Equal(t, expectedGroups, groups)
}

func TestReadStreamListpacks3(t *testing.T) {
	path := filepath.Join(valueDumpsPath, "stream-listpacks3.bin")

	dump, err := os.ReadFile(path)
	require.NoError(t, err)

	err = VerifyValueChecksum(dump)
	require.NoError(t, err)

	dump = dump[:len(dump)-10]

	r := valueReader{
		buf: newMemoryBackedBuffer(dump),
	}

	ot, err := r.ReadType()
	require.NoError(t, err)

	require.Equal(t, TypeStreamListpacks3, ot)

	entries := make([]StreamEntry, 0)
	entryCB := func(entry StreamEntry) error {
		entries = append(entries, entry)
		return nil
	}

	groups := make([]StreamConsumerGroup, 0)
	groupCB := func(group StreamConsumerGroup) error {
		groups = append(groups, group)
		return nil
	}

	read, err := r.ReadStreamListpacks3(entryCB, groupCB)
	require.NoError(t, err)
	require.Equal(t, uint64(4), read)

	expectedEntries := make([]StreamEntry, 4)
	expectedEntries[0] = StreamEntry{
		ID:    StreamID{Millis: 1693566931036, Seq: 0},
		Value: []string{"foo", "bar0"},
	}
	expectedEntries[1] = StreamEntry{
		ID:    StreamID{Millis: 1693566931036, Seq: 1},
		Value: []string{"foo", "bar1"},
	}
	expectedEntries[2] = StreamEntry{
		ID:    StreamID{Millis: 1693566931036, Seq: 2},
		Value: []string{"foo", "bar2"},
	}
	expectedEntries[3] = StreamEntry{
		ID:    StreamID{Millis: 1693566932041, Seq: 0},
		Value: []string{"foo", "bar3", "up", "stash", "upstash", "123"},
	}
	require.Equal(t, expectedEntries, entries)

	expectedGroups := make([]StreamConsumerGroup, 2)
	expectedGroups[0] = StreamConsumerGroup{
		Name:        "g0",
		LastID:      StreamID{Millis: 1693566931036, Seq: 1},
		EntriesRead: 2,
		Consumers: []StreamConsumer{
			{
				Name:           "c0",
				SeenTime:       1693566932042,
				ActiveTime:     -1,
				PendingEntries: []*StreamPendingEntry{},
			},
			{
				Name:       "c1",
				SeenTime:   1693566932042,
				ActiveTime: 1693566932042,
				PendingEntries: []*StreamPendingEntry{
					{
						Entry: StreamEntry{
							ID:    StreamID{Millis: 1693566931036, Seq: 0},
							Value: []string{"foo", "bar0"},
						},
						DeliveryTime:  1693566932042,
						DeliveryCount: 1,
					},
					{
						Entry: StreamEntry{
							ID:    StreamID{Millis: 1693566931036, Seq: 1},
							Value: []string{"foo", "bar1"},
						},
						DeliveryTime:  1693566932042,
						DeliveryCount: 1,
					},
				},
			},
		},
	}
	expectedGroups[1] = StreamConsumerGroup{
		Name:        "g1",
		LastID:      StreamID{Millis: 1693566932041, Seq: 0},
		EntriesRead: -1,
		Consumers: []StreamConsumer{
			{
				Name:           "c2",
				SeenTime:       1693566932042,
				ActiveTime:     -1,
				PendingEntries: []*StreamPendingEntry{},
			},
		},
	}

	require.Equal(t, expectedGroups, groups)
}

func TestReadStreamListpacks3_big(t *testing.T) {
	path := filepath.Join(valueDumpsPath, "stream-listpacks3-big.bin")

	dump, err := os.ReadFile(path)
	require.NoError(t, err)

	err = VerifyValueChecksum(dump)
	require.NoError(t, err)

	dump = dump[:len(dump)-10]

	r := valueReader{
		buf: newMemoryBackedBuffer(dump),
	}

	ot, err := r.ReadType()
	require.NoError(t, err)

	require.Equal(t, TypeStreamListpacks3, ot)

	entries := make([]StreamEntry, 0)
	entryCB := func(entry StreamEntry) error {
		entries = append(entries, entry)
		return nil
	}

	groups := make([]StreamConsumerGroup, 0)
	groupCB := func(group StreamConsumerGroup) error {
		groups = append(groups, group)
		return nil
	}

	read, err := r.ReadStreamListpacks3(entryCB, groupCB)
	require.NoError(t, err)
	require.Equal(t, uint64(950), read)

	expectedEntries := make([]StreamEntry, 0)
	for i := uint64(51); i <= 1000; i++ {
		fields := make([]string, 0)
		for j := uint64(0); j < 1+(i-1)%10; j++ {
			fields = append(fields, fmt.Sprintf("field-%d", j), "x")
		}

		entry := StreamEntry{
			ID:    StreamID{Millis: i, Seq: 0},
			Value: fields,
		}
		expectedEntries = append(expectedEntries, entry)
	}

	require.Equal(t, expectedEntries, entries)

	group1Consumer1PE := make([]*StreamPendingEntry, 10)
	group1Consumer2PE := make([]*StreamPendingEntry, 20)

	for i := uint64(591); i < 601; i++ {
		value := make([]string, 2*(1+(i-1)%10))
		for j := 0; j < len(value); j += 2 {
			value[j] = fmt.Sprintf("field-%d", j/2)
			value[j+1] = "x"
		}
		pe := &StreamPendingEntry{
			Entry: StreamEntry{
				ID:    StreamID{Millis: i, Seq: 0},
				Value: value,
			},
			DeliveryTime:  1693595249313,
			DeliveryCount: 1,
		}
		group1Consumer1PE[int(i-591)] = pe
	}

	for i := uint64(681); i < 701; i++ {
		value := make([]string, 2*(1+(i-1)%10))
		for j := 0; j < len(value); j += 2 {
			value[j] = fmt.Sprintf("field-%d", j/2)
			value[j+1] = "x"
		}
		pe := &StreamPendingEntry{
			Entry: StreamEntry{
				ID:    StreamID{Millis: i, Seq: 0},
				Value: value,
			},
			DeliveryTime:  1693595249318,
			DeliveryCount: 1,
		}
		group1Consumer2PE[int(i-681)] = pe
	}

	group2Consumer1PE := make([]*StreamPendingEntry, 0)
	group2Consumer2PE := make([]*StreamPendingEntry, 10)
	group2Consumer3PE := make([]*StreamPendingEntry, 0)

	for i := uint64(941); i < 951; i++ {
		value := make([]string, 2*(1+(i-1)%10))
		for j := 0; j < len(value); j += 2 {
			value[j] = fmt.Sprintf("field-%d", j/2)
			value[j+1] = "x"
		}
		pe := &StreamPendingEntry{
			Entry: StreamEntry{
				ID:    StreamID{Millis: i, Seq: 0},
				Value: value,
			},
			DeliveryTime:  1693595249326,
			DeliveryCount: 1,
		}
		group2Consumer2PE[int(i-941)] = pe
	}

	expectedGroups := make([]StreamConsumerGroup, 2)
	expectedGroups[0] = StreamConsumerGroup{
		Name:        "group-1",
		LastID:      StreamID{Millis: 700, Seq: 0},
		EntriesRead: -1,
		Consumers: []StreamConsumer{
			{
				Name:           "consumer-1",
				SeenTime:       1693595249313,
				ActiveTime:     1693595249313,
				PendingEntries: group1Consumer1PE,
			},
			{
				Name:           "consumer-2",
				SeenTime:       1693595249318,
				ActiveTime:     1693595249318,
				PendingEntries: group1Consumer2PE,
			},
		},
	}

	expectedGroups[1] = StreamConsumerGroup{
		Name:        "group-2",
		LastID:      StreamID{Millis: 1000, Seq: 0},
		EntriesRead: 1000,
		Consumers: []StreamConsumer{
			{
				Name:           "consumer-1",
				SeenTime:       1693595249320,
				ActiveTime:     1693595249320,
				PendingEntries: group2Consumer1PE,
			},
			{
				Name:           "consumer-2",
				SeenTime:       1693595249326,
				ActiveTime:     1693595249326,
				PendingEntries: group2Consumer2PE,
			},
			{
				Name:           "consumer-3",
				SeenTime:       1693595249329,
				ActiveTime:     1693595249329,
				PendingEntries: group2Consumer3PE,
			},
		},
	}

	require.Equal(t, expectedGroups, groups)
}
