package rdb

import (
	"github.com/stretchr/testify/require"
	"math"
	"path/filepath"
	"testing"
	"time"
)

const version = "7.2.4"

func TestEncoder_String(t *testing.T) {
	rdbFile := filepath.Join(t.TempDir(), "string.rdb")

	encoder, err := NewFileEncoder(rdbFile, version)
	require.NoError(t, err)
	require.NoError(t, encoder.Begin())

	tests := []struct {
		name   string
		key    string
		value  string
		expiry *time.Time
	}{
		{
			name:  "empty string",
			key:   "empty-str",
			value: "",
		},
		{
			name:  "int8 as string",
			key:   "int8",
			value: "42",
		},
		{
			name:  "int16 as string",
			key:   "int16",
			value: "-4242",
		},
		{
			name:  "int32 as string",
			key:   "int32",
			value: "42424242",
		},
		{
			name:  "normal string",
			key:   "normal-str",
			value: generateAlphabetCycle(142),
		},
		{
			name:  "with expiry",
			key:   "with-expiry",
			value: "expires soon",
			expiry: func() *time.Time {
				t := time.Now().Add(time.Hour)
				return &t
			}(),
		},
	}

	for i, tc := range tests {
		key := tc.key
		err = encoder.WriteStringEntry(key, tc.value, tc.expiry)
		require.NoError(t, err, "Test case %d: %s", i, tc.name)
	}

	require.NoError(t, encoder.Close())

	db := newDummyDB()
	err = ReadFile(filepath.Join(rdbFile), db)
	require.NoError(t, err)

	for _, tc := range tests {
		require.Equal(t, db.strings[tc.key], tc.value)
		if tc.expiry != nil {
			require.WithinDuration(t, time.Now().Add(db.expireTimes[tc.key]), *tc.expiry, time.Second*30)
		}
	}

}

func TestEncoder_List(t *testing.T) {
	tempDir := t.TempDir()
	rdbFile := filepath.Join(tempDir, "list.rdb")

	encoder, err := NewFileEncoder(rdbFile, version)
	require.NoError(t, err)
	require.NoError(t, encoder.Begin())

	listKey := "test-list"
	values := []string{"a", "b", "c", "1", "2", "3", "def"}

	listEncoder, err := encoder.BeginList(listKey, nil)
	require.NoError(t, err)

	for _, val := range values {
		err = listEncoder.WriteFieldStr(val)
		require.NoError(t, err)
	}

	require.NoError(t, listEncoder.Close())
	require.NoError(t, encoder.Close())

	db := newDummyDB()
	err = ReadFile(filepath.Join(rdbFile), db)
	require.NoError(t, err)

	require.Equal(t, db.lists[listKey], values)

}

func TestEncoder_Set(t *testing.T) {
	tempDir := t.TempDir()
	rdbFile := filepath.Join(tempDir, "set.rdb")

	encoder, err := NewFileEncoder(rdbFile, version)
	require.NoError(t, err)
	require.NoError(t, encoder.Begin())

	setKey := "test-set"
	values := []string{"hello", "world", "1", "2", "3", "upstash"}

	setEncoder, err := encoder.BeginSet(setKey, nil)
	require.NoError(t, err)

	for _, val := range values {
		err = setEncoder.WriteFieldStr(val)
		require.NoError(t, err)
	}

	require.NoError(t, setEncoder.Close())
	require.NoError(t, encoder.Close())

	db := newDummyDB()
	err = ReadFile(filepath.Join(rdbFile), db)
	require.NoError(t, err)

	require.Equal(t, db.sets[setKey], values)
}

func TestEncoder_Hash(t *testing.T) {
	tempDir := t.TempDir()
	rdbFile := filepath.Join(tempDir, "hash.rdb")

	encoder, err := NewFileEncoder(rdbFile, version)
	require.NoError(t, err)

	require.NoError(t, encoder.Begin())

	hashKey := "test-hash"
	hashValues := map[string]string{
		"a":    "1",
		"b":    "2",
		"cd":   "34",
		"efgh": "upstash",
	}

	hashEncoder, err := encoder.BeginHash(hashKey, nil)
	require.NoError(t, err)

	for field, value := range hashValues {
		err = hashEncoder.WriteFieldStrStr(field, value)
		require.NoError(t, err)
	}

	require.NoError(t, hashEncoder.Close())
	require.NoError(t, encoder.Close())

	db := newDummyDB()
	err = ReadFile(filepath.Join(rdbFile), db)
	require.NoError(t, err)

	require.Equal(t, db.hashes[hashKey], hashValues)
}

func TestEncoder_HashWithMetadata(t *testing.T) {
	tempDir := t.TempDir()
	rdbFile := filepath.Join(tempDir, "hash-metadata.rdb")

	encoder, err := NewFileEncoder(rdbFile, version)
	require.NoError(t, err)

	require.NoError(t, encoder.Begin())

	now := time.Now()
	hashKey := "test-hash-metadata"
	fieldValues := []struct {
		field  string
		value  string
		expiry *time.Time
	}{
		{field: "a", value: "1", expiry: nil},
		{field: "b", value: "2", expiry: getTimePtr(now.Add(time.Hour))},
		{field: "c", value: "3", expiry: getTimePtr(now.Add(2 * time.Hour))},
	}

	hashEncoder, err := encoder.BeginHashWithMetadata(hashKey, nil)
	require.NoError(t, err)

	for _, fv := range fieldValues {
		err = hashEncoder.WriteFieldStrStrWithExpiry(fv.field, fv.value, fv.expiry)
		require.NoError(t, err)
	}

	err = hashEncoder.Close()
	require.NoError(t, err)

	require.NoError(t, encoder.Close())

	db := newDummyDB()
	err = ReadFile(filepath.Join(rdbFile), db)
	require.NoError(t, err)

	require.Equal(t, db.hashes[hashKey]["a"], "1")
	require.Equal(t, db.hashes[hashKey]["b"], "2")
	require.Equal(t, db.hashes[hashKey]["c"], "3")
	require.Equal(t, db.hashExpireTimes[hashKey]["a"], time.UnixMilli(0))
	require.WithinDuration(t, db.hashExpireTimes[hashKey]["b"], now.Add(time.Hour), time.Second)
	require.WithinDuration(t, db.hashExpireTimes[hashKey]["c"], now.Add(time.Hour*2), time.Second)
	require.WithinDuration(t, db.hashExpireTimes[hashKey]["c"], now.Add(time.Hour*2), time.Second)
}

func TestEncoder_SortedSet(t *testing.T) {
	tempDir := t.TempDir()
	rdbFile := filepath.Join(tempDir, "sorted-set.rdb")

	encoder, err := NewFileEncoder(rdbFile, version)
	require.NoError(t, err)
	require.NoError(t, encoder.Begin())

	zsetKey := "test-zset"
	zsetValues := map[string]float64{
		"score0": 0,
		"score1": 1.5,
		"score2": 2.5,
		"score3": 42.42,
		"neginf": math.Inf(-1),
		"posinf": math.Inf(1),
	}

	zsetEncoder, err := encoder.BeginSortedSet(zsetKey, nil)
	require.NoError(t, err)

	for elem, score := range zsetValues {
		err = zsetEncoder.WriteFieldStrFloat64(elem, score)
		require.NoError(t, err)
	}

	require.NoError(t, zsetEncoder.Close())
	require.NoError(t, encoder.Close())

	db := newDummyDB()
	err = ReadFile(filepath.Join(rdbFile), db)
	require.NoError(t, err)

	require.Equal(t, db.zsets[zsetKey], zsetValues)
}

func TestEncoder_Stream(t *testing.T) {
	tempDir := t.TempDir()
	rdbFile := filepath.Join(tempDir, "stream.rdb")

	encoder, err := NewFileEncoder(rdbFile, version)
	require.NoError(t, err)

	require.NoError(t, encoder.Begin())

	streamKey := "test-stream"
	entries := []StreamEntry{
		{
			ID:    StreamID{Millis: 1693576721400, Seq: 0},
			Value: []string{"up", "up1", "stash", "stash1", "upstash", "upstash1"},
		},
		{
			ID:    StreamID{Millis: 1693576731925, Seq: 0},
			Value: []string{"up", "up2", "stash", "stash2", "upstash", "upstash2"},
		},
		{
			ID:    StreamID{Millis: 1693576737806, Seq: 0},
			Value: []string{"up", "up3", "stash", "stash3", "upstash", "upstash3"},
		},
	}

	streamEncoder, err := encoder.BeginStream(streamKey, nil)
	require.NoError(t, err)

	for _, entry := range entries {
		err = streamEncoder.WriteEntry(entry)
		require.NoError(t, err)
	}

	group := StreamConsumerGroup{
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

	err = streamEncoder.WriteMetadata(uint64(len(entries)), entries[len(entries)-1].ID)
	require.NoError(t, err)

	err = streamEncoder.WriteGroups([]StreamConsumerGroup{group})
	require.NoError(t, err)

	err = streamEncoder.Close()
	require.NoError(t, err)

	require.NoError(t, encoder.Close())

	db := newDummyDB()
	err = ReadFile(filepath.Join(rdbFile), db)
	require.NoError(t, err)

	require.Equal(t, db.streamEntriesRead[streamKey], uint64(len(entries)))
	require.Equal(t, db.streamEntries[streamKey], entries)
	require.Equal(t, db.streamGroups[streamKey], []StreamConsumerGroup{group})
}

func TestEncoder_JSON(t *testing.T) {
	tempDir := t.TempDir()
	rdbFile := filepath.Join(tempDir, "json.rdb")

	encoder, err := NewFileEncoder(rdbFile, version)
	require.NoError(t, err)

	require.NoError(t, encoder.Begin())

	jsonKey := "test-json"
	jsonValue := `{
		"str": "upstash",
		"number": 42.42,
		"int": 424242,
		"bool": true,
		"nested": {
			"null": null,
			"str": "rdb",
			"arr": [1, 2, "upstash"]
		},
		"arr": [
			42,
			[42],
			{"arr": ["x", "y", ["z"], false, null, -4242.4242]}
		]
	}`

	err = encoder.WriteJSON(jsonKey, jsonValue, nil)
	require.NoError(t, err)

	require.NoError(t, encoder.Close())

	db := newDummyDB()
	err = ReadFile(filepath.Join(rdbFile), db)
	require.NoError(t, err)

	require.Equal(t, db.modules[jsonKey], jsonValue)
}

func getTimePtr(t time.Time) *time.Time {
	return &t
}
