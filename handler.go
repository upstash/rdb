package rdb

import "time"

// ValueHandler is used to handle RDB objects while reading a file.
type ValueHandler interface {
	// whether the handler can skip known but not yet supported types or not.
	AllowPartialRead() bool

	// whether the handler expects the file to end with the eof opcode, i.e,
	// has no more bytes to the right of it.
	RequireStrictEOF() bool

	// called when a string value is read for the key.
	HandleString(key, value string) error

	// returned function is called for the each enty read for the key.
	ListEntryHandler(key string) func(elem string) error

	// called when the list is read completely, with the name and the number of entries read.
	HandleListEnding(key string, entriesRead uint64)

	// returned function is called for the each enty read for the key.
	SetEntryHandler(key string) func(elem string) error

	// returned function is called for the each enty read for the key.
	ZsetEntryHandler(key string) func(elem string, score float64) error

	// called when the zset is read completely, with the name and the number of entries read.
	HandleZsetEnding(key string, entriesRead uint64)

	// returned function is called for the each enty read for the key.
	HashEntryHandler(key string) func(field, value string) error

	// called when a module value is read for the key.
	HandleModule(key, value string, marker ModuleMarker) error

	// returned function is called for the each stream enty read for the key.
	StreamEntryHandler(key string) func(entry StreamEntry) error

	// returned function is called for the each stream group read for the key.
	StreamGroupHandler(key string) func(group StreamConsumerGroup) error

	// called when the stream entries and groups are read completely,
	// with the name and the number of entries read.
	HandleStreamEnding(key string, entriesRead uint64)

	// returned function is called for each read hash entry with expiration info for the key.
	HashWithExpEntryHandler(key string) func(field string, value string, exp time.Time) error

	// returned function is called for the each element read for the key, with
	// the index of that element and its value.
	ArrayEntryHandler(key string) func(index uint64, value string) error

	// called when the array is read completely, with the name, the number of
	// elements read, and the insert cursor of the array. The cursor is equal to
	// ArrayInsertIndexNone for the arrays that have none.
	HandleArrayEnding(key string, entriesRead uint64, insertIndex uint64)

	// called when the a function code is read
	HandleLibrary(code string) error
}

// FileHandler is an extension of ValueHandler, which can handle RDB objects
// and their expiration information from RDB files.
type FileHandler interface {
	ValueHandler
	// called when a key expiration timestamp is read.
	HandleExpireTime(key string, expireTime time.Time)
	HandleLibrary(code string) error
}

// nopHandler is used to ignore the RDB objects read so that
// the file can be read while skipping the values we don't need
// to read.
type nopHandler struct {
}

func (nopHandler) AllowPartialRead() bool {
	return true
}

func (nopHandler) RequireStrictEOF() bool {
	return false
}

func (nopHandler) HandleString(key, value string) error {
	return nil
}

func (nopHandler) ListEntryHandler(key string) func(elem string) error {
	return func(elem string) error {
		return nil
	}
}

func (nopHandler) HandleListEnding(key string, entriesRead uint64) {
}

func (nopHandler) SetEntryHandler(key string) func(elem string) error {
	return func(elem string) error {
		return nil
	}
}

func (nopHandler) ZsetEntryHandler(key string) func(elem string, score float64) error {
	return func(elem string, score float64) error {
		return nil
	}
}

func (nopHandler) HandleZsetEnding(key string, entriesRead uint64) {
}

func (nopHandler) HashEntryHandler(key string) func(field, value string) error {
	return func(field, value string) error {
		return nil
	}
}

func (nopHandler) HandleModule(key, value string, marker ModuleMarker) error {
	return nil
}

func (nopHandler) StreamEntryHandler(key string) func(entry StreamEntry) error {
	return func(entry StreamEntry) error {
		return nil
	}
}

func (nopHandler) StreamGroupHandler(key string) func(group StreamConsumerGroup) error {
	return func(group StreamConsumerGroup) error {
		return nil
	}
}

func (nopHandler) HandleStreamEnding(key string, entriesRead uint64) {
}

func (nopHandler) HandleExpireTime(key string, expireTime time.Time) {
}

func (h nopHandler) HashWithExpEntryHandler(key string) func(field string, value string, exp time.Time) error {
	return nil
}

func (nopHandler) ArrayEntryHandler(key string) func(index uint64, value string) error {
	return func(index uint64, value string) error {
		return nil
	}
}

func (nopHandler) HandleArrayEnding(key string, entriesRead uint64, insertIndex uint64) {
}

func (nopHandler) HandleLibrary(code string) error {
	return nil
}
