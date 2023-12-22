package rdb

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type dummyDB struct {
	partialRead       bool
	strings           map[string]string
	lists             map[string][]string
	sets              map[string][]string
	zsets             map[string]map[string]float64
	hashes            map[string]map[string]string
	modules           map[string]string
	streamEntries     map[string][]StreamEntry
	streamGroups      map[string][]StreamConsumerGroup
	expireTimes       map[string]time.Duration
	listEntriesRead   map[string]uint64
	zsetEntriesRead   map[string]uint64
	streamEntriesRead map[string]uint64
}

func newDummyDB() *dummyDB {
	return &dummyDB{
		partialRead:       false,
		strings:           make(map[string]string),
		lists:             make(map[string][]string),
		sets:              make(map[string][]string),
		zsets:             make(map[string]map[string]float64),
		hashes:            make(map[string]map[string]string),
		modules:           make(map[string]string),
		streamEntries:     make(map[string][]StreamEntry),
		streamGroups:      make(map[string][]StreamConsumerGroup),
		expireTimes:       make(map[string]time.Duration),
		listEntriesRead:   make(map[string]uint64),
		zsetEntriesRead:   make(map[string]uint64),
		streamEntriesRead: make(map[string]uint64),
	}
}

func (db *dummyDB) AllowPartialRead() bool {
	return db.partialRead
}

func (db *dummyDB) HandleString(key, value string) error {
	db.strings[key] = value
	return nil
}

func (db *dummyDB) ListEntryHandler(key string) func(string) error {
	return func(elem string) error {
		list, ok := db.lists[key]
		if !ok {
			list = make([]string, 0)
		}

		list = append(list, elem)
		db.lists[key] = list
		return nil
	}
}

func (db *dummyDB) HandleListEnding(key string, entriesRead uint64) {
	db.listEntriesRead[key] = entriesRead
}

func (db *dummyDB) SetEntryHandler(key string) func(string) error {
	return func(elem string) error {
		set, ok := db.sets[key]
		if !ok {
			set = make([]string, 0)
		}

		set = append(set, elem)
		db.sets[key] = set
		return nil
	}
}

func (db *dummyDB) ZsetEntryHandler(key string) func(string, float64) error {
	return func(elem string, score float64) error {
		zset, ok := db.zsets[key]
		if !ok {
			zset = make(map[string]float64)
		}

		zset[elem] = score
		db.zsets[key] = zset
		return nil
	}
}

func (db *dummyDB) HandleZsetEnding(key string, entriesRead uint64) {
	db.zsetEntriesRead[key] = entriesRead
}

func (db *dummyDB) HashEntryHandler(key string) func(string, string) error {
	return func(field, value string) error {
		hash, ok := db.hashes[key]
		if !ok {
			hash = make(map[string]string)
		}

		hash[field] = value
		db.hashes[key] = hash
		return nil
	}
}

func (db *dummyDB) HandleModule(key, value string, marker ModuleMarker) error {
	if marker != JSONModuleMarker {
		return errors.New("unexpected module value")
	}

	db.modules[key] = value
	return nil
}

func (db *dummyDB) StreamEntryHandler(key string) func(StreamEntry) error {
	return func(entry StreamEntry) error {
		entries, ok := db.streamEntries[key]
		if !ok {
			entries = make([]StreamEntry, 0)
		}

		entries = append(entries, entry)
		db.streamEntries[key] = entries
		return nil
	}
}

func (db *dummyDB) StreamGroupHandler(key string) func(StreamConsumerGroup) error {
	return func(group StreamConsumerGroup) error {
		groups, ok := db.streamGroups[key]
		if !ok {
			groups = make([]StreamConsumerGroup, 0)
		}

		groups = append(groups, group)
		db.streamGroups[key] = groups
		return nil
	}
}

func (db *dummyDB) HandleStreamEnding(key string, entriesRead uint64) {
	db.streamEntriesRead[key] = entriesRead
}

func (db *dummyDB) HandleExpireTime(key string, expireTime time.Duration) error {
	db.expireTimes[key] = expireTime
	return nil
}

var dumpsPath = filepath.Join("testdata", "dumps")

func TestFileReader_PreV5_withoutCRC(t *testing.T) {
	db := newDummyDB()
	err := ReadFile(filepath.Join(dumpsPath, "no-crc.rdb"), db)
	require.NoError(t, err)
}

func TestFileReader_PostV5_withChecksumDisabled(t *testing.T) {
	db := newDummyDB()
	err := ReadFile(filepath.Join(dumpsPath, "disabled-crc.rdb"), db)
	require.NoError(t, err)
}

func TestFileReader_unsupportedVersion(t *testing.T) {
	db := newDummyDB()
	err := ReadFile(filepath.Join(dumpsPath, "from-future.rdb"), db)
	require.ErrorContains(t, err, "cannot handle RDB format version 42")
}

func TestFileReader_badHeader(t *testing.T) {
	db := newDummyDB()
	err := ReadFile(filepath.Join(dumpsPath, "bad-header.rdb"), db)
	require.ErrorContains(t, err, "wrong signature trying to load DB from file")
}

func TestFileReader_badCRC(t *testing.T) {
	db := newDummyDB()
	err := ReadFile(filepath.Join(dumpsPath, "bad-crc.rdb"), db)
	require.ErrorContains(t, err, "wrong CRC at the end of the RDB file")
}

func TestFileReader_multiDB(t *testing.T) {
	db := newDummyDB()
	err := ReadFile(filepath.Join(dumpsPath, "multi-db.rdb"), db)
	require.ErrorContains(t, err, "multiple databases are not supported when the partial restore is not allowed")

	db.partialRead = true
	err = ReadFile(filepath.Join(dumpsPath, "multi-db.rdb"), db)
	require.NoError(t, err)
	db.partialRead = false

	expected := newDummyDB()
	expected.strings["00"] = "a"

	require.Equal(t, expected, db)
}

func TestFileReader_moduleAux(t *testing.T) {
	db := newDummyDB()
	err := ReadFile(filepath.Join(dumpsPath, "module-aux.rdb"), db)
	require.NoError(t, err)

	expected := newDummyDB()
	expected.modules["doc"] = "{\"a\":2}"

	require.Equal(t, expected, db)
}

func TestFileReader_withIdleInfo(t *testing.T) {
	db := newDummyDB()
	err := ReadFile(filepath.Join(dumpsPath, "idle.rdb"), db)
	require.NoError(t, err)

	expected := newDummyDB()
	expected.strings["up"] = "stash"
	expected.expireTimes["up"] = time.Duration(1694542150330000000)

	require.Equal(t, expected, db)
}

func TestFileReader_withFreqInfo(t *testing.T) {
	db := newDummyDB()
	err := ReadFile(filepath.Join(dumpsPath, "freq.rdb"), db)
	require.NoError(t, err)

	expected := newDummyDB()
	expected.strings["up"] = "stash"
	expected.expireTimes["up"] = time.Duration(1694542238686000000)

	require.Equal(t, expected, db)
}

func TestFileReader_function(t *testing.T) {
	db := newDummyDB()
	err := ReadFile(filepath.Join(dumpsPath, "function.rdb"), db)
	require.ErrorContains(t, err, "restoring function payload is not supported when the partial restore is not allowed")

	db.partialRead = true
	err = ReadFile(filepath.Join(dumpsPath, "function.rdb"), db)
	require.NoError(t, err)
}

func TestFileReader_expireTimeSec(t *testing.T) {
	db := newDummyDB()
	err := ReadFile(filepath.Join(dumpsPath, "expiretime-sec.rdb"), db)
	require.NoError(t, err)

	expected := newDummyDB()
	expected.strings["up"] = "stash"
	expected.expireTimes["up"] = time.Duration(2325035706000000000)

	require.Equal(t, expected, db)
}

func TestFileReader_allTypes(t *testing.T) {
	db := newDummyDB()
	err := ReadFile(filepath.Join(dumpsPath, "all-types.rdb"), db)
	require.NoError(t, err)

	expected := newDummyDB()
	expected.strings["00"] = "a"
	expected.lists["01"] = []string{"a"}
	expected.sets["02"] = []string{"a"}
	expected.zsets["03"] = map[string]float64{"a": 0}
	expected.hashes["04"] = map[string]string{"a": "a"}
	expected.zsets["05"] = map[string]float64{"a": 0}
	expected.modules["07"] = "{\"a\":0}"
	expected.hashes["09"] = map[string]string{"a": "a"}
	expected.lists["10"] = []string{"a"}
	expected.sets["11"] = []string{"0"}
	expected.zsets["12"] = map[string]float64{"a": 0}
	expected.hashes["13"] = map[string]string{"a": "a"}
	expected.lists["14"] = []string{"a"}
	expected.streamEntries["15"] = []StreamEntry{
		{
			ID:    StreamID{Millis: 0, Seq: 1},
			Value: []string{"a", "a"},
		},
	}
	expected.hashes["16"] = map[string]string{"a": "a"}
	expected.zsets["17"] = map[string]float64{"a": 0}
	expected.lists["18"] = []string{"a"}
	expected.streamEntries["19"] = []StreamEntry{
		{
			ID:    StreamID{Millis: 0, Seq: 1},
			Value: []string{"a", "a"},
		},
	}
	expected.sets["20"] = []string{"a"}
	expected.streamEntries["21"] = []StreamEntry{
		{
			ID:    StreamID{Millis: 0, Seq: 1},
			Value: []string{"a", "a"},
		},
	}
	expected.streamGroups["21"] = []StreamConsumerGroup{
		{
			Name:        "a",
			LastID:      StreamID{Millis: 0, Seq: 1},
			EntriesRead: 1,
			Consumers: []StreamConsumer{
				{
					Name:           "a",
					SeenTime:       1694596401972,
					ActiveTime:     1694596401972,
					PendingEntries: []*StreamPendingEntry{},
				},
			},
		},
	}

	expected.listEntriesRead["01"] = 1
	expected.listEntriesRead["10"] = 1
	expected.listEntriesRead["14"] = 1
	expected.listEntriesRead["18"] = 1

	expected.zsetEntriesRead["03"] = 1
	expected.zsetEntriesRead["05"] = 1
	expected.zsetEntriesRead["12"] = 1
	expected.zsetEntriesRead["17"] = 1

	expected.streamEntriesRead["15"] = 1
	expected.streamEntriesRead["19"] = 1
	expected.streamEntriesRead["21"] = 1

	require.Equal(t, expected, db)
}
