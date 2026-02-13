package rdb

import (
	"fmt"
	"io"
	"math"
	"os"
	"time"
)

var defaultMaxDataSize = 256 << 20  // 256 MB
var defaultMaxEntrySize = 100 << 20 // 100 MB
var defaultMaxKeySize = 32 << 10    // 32 KB
var defaultMaxStreamPELSize = 1000

const maxStreamStrSize = math.MaxUint32

type VerifyFileOptions struct {
	MaxDataSize        int
	MaxEntrySize       int
	MaxKeySize         int
	MaxStreamPELSize   int
	AllowPartialVerify bool
	RequireStrictEOF   bool
}

func (o *VerifyFileOptions) maybeSetDefaults() {
	if o.MaxDataSize <= 0 {
		o.MaxDataSize = defaultMaxDataSize
	}

	if o.MaxEntrySize <= 0 {
		o.MaxEntrySize = defaultMaxEntrySize
	}

	if o.MaxKeySize <= 0 {
		o.MaxKeySize = defaultMaxKeySize
	}

	if o.MaxStreamPELSize <= 0 {
		o.MaxStreamPELSize = defaultMaxStreamPELSize
	}
}

// VerifyFile verifies that the given RDB file is not corrupt,
// or does not exceed the limits in the given options.
func VerifyFile(path string, opts VerifyFileOptions) error {
	opts.maybeSetDefaults()
	v := &verifier{
		maxDataSize:        opts.MaxDataSize,
		maxEntrySize:       opts.MaxEntrySize,
		maxKeySize:         opts.MaxKeySize,
		maxStreamPELSize:   opts.MaxStreamPELSize,
		allowPartialVerify: opts.AllowPartialVerify,
		requireStrictEOF:   opts.RequireStrictEOF,
	}

	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return err
	}

	fileLen := info.Size()
	buf := newFileBackedBuffer(file, int(fileLen), minInt(int(fileLen), 1<<20))

	return readFile(buf, v, uint64(opts.MaxEntrySize))
}

type VerifyReaderOptions struct {
	MaxDataSize        int
	MaxEntrySize       int
	MaxKeySize         int
	MaxStreamPELSize   int
	AllowPartialVerify bool
	RequireStrictEOF   bool
}

func (o *VerifyReaderOptions) maybeSetDefaults() {
	if o.MaxDataSize <= 0 {
		o.MaxDataSize = defaultMaxDataSize
	}

	if o.MaxEntrySize <= 0 {
		o.MaxEntrySize = defaultMaxEntrySize
	}

	if o.MaxKeySize <= 0 {
		o.MaxKeySize = defaultMaxKeySize
	}

	if o.MaxStreamPELSize <= 0 {
		o.MaxStreamPELSize = defaultMaxStreamPELSize
	}
}

// VerifyReader verifies that the given RDB reader is not corrupt,
// or does not exceed the limits in the given options.
func VerifyReader(r io.Reader, opts VerifyReaderOptions) error {
	opts.maybeSetDefaults()
	v := &verifier{
		maxDataSize:        opts.MaxDataSize,
		maxEntrySize:       opts.MaxEntrySize,
		maxKeySize:         opts.MaxKeySize,
		maxStreamPELSize:   opts.MaxStreamPELSize,
		allowPartialVerify: opts.AllowPartialVerify,
		requireStrictEOF:   opts.RequireStrictEOF,
	}

	buf := newForwardOnlyBuffer(r)

	return readFile(buf, v, uint64(opts.MaxEntrySize))
}

type VerifyValueOptions struct {
	MaxEntrySize     int
	MaxStreamPELSize int
}

func (o *VerifyValueOptions) maybeSetDefaults() {
	if o.MaxEntrySize <= 0 {
		o.MaxEntrySize = defaultMaxEntrySize
	}

	if o.MaxStreamPELSize <= 0 {
		o.MaxStreamPELSize = defaultMaxStreamPELSize
	}
}

// VerifyValue verifies that the given RDB value is not corrupt,
// or does not exceed the limits in the given options.
func VerifyValue(payload []byte, opts VerifyValueOptions) error {
	opts.maybeSetDefaults()
	v := &verifier{
		maxEntrySize:     opts.MaxEntrySize,
		maxStreamPELSize: opts.MaxStreamPELSize,
		// We don't care about the values below, as they don't
		// really apply to RDB values.
		maxDataSize: math.MaxInt,
		maxKeySize:  math.MaxInt,
	}

	return readValue("", payload, v, uint64(opts.MaxEntrySize))
}

func errMaxDataSizeExceeded(current int, limit int) error {
	return fmt.Errorf("max data size is exceeded. current: %d, limit: %d", current, limit)
}

func errMaxEntrySizeExceeded(current int, limit int) error {
	return fmt.Errorf("max entry size is exceeded. current: %d, limit: %d", current, limit)
}

func errMaxKeySizeExceeded(current int, limit int) error {
	return fmt.Errorf("max key size is exceeded. current: %d, limit: %d", current, limit)
}

func errMaxStreamPELSizeExceeded(current int, limit int) error {
	return fmt.Errorf("max stream pel size is exceeded. current: %d, limit: %d", current, limit)
}

func errMaxStreamStrSizeExceeded(current int, limit int) error {
	return fmt.Errorf("max stream string item size is exceeded. current: %d, limit: %d", current, limit)
}

type verifier struct {
	maxDataSize        int
	maxEntrySize       int
	maxKeySize         int
	maxStreamPELSize   int
	allowPartialVerify bool
	requireStrictEOF   bool
	dataSize           int
}

func (v *verifier) HandleString(key string, value string) error {
	if len(key) > v.maxKeySize {
		return errMaxKeySizeExceeded(len(key), v.maxKeySize)
	}

	if len(value) > v.maxEntrySize {
		return errMaxEntrySizeExceeded(len(value), v.maxEntrySize)
	}

	v.dataSize += len(key) + len(value)
	if v.dataSize > v.maxDataSize {
		return errMaxDataSizeExceeded(v.dataSize, v.maxDataSize)
	}

	return nil
}

func (v *verifier) HashEntryHandler(key string) func(field string, value string) error {
	if len(key) > v.maxKeySize {
		return func(field, value string) error {
			return errMaxKeySizeExceeded(len(key), v.maxKeySize)
		}
	}

	v.dataSize += len(key)
	if v.dataSize > v.maxDataSize {
		return func(field, value string) error {
			return errMaxDataSizeExceeded(v.dataSize, v.maxDataSize)
		}
	}

	var entrySize int
	return func(field, value string) error {
		elementSize := len(field) + len(value)
		entrySize += elementSize
		if entrySize > v.maxEntrySize {
			return errMaxEntrySizeExceeded(entrySize, v.maxEntrySize)
		}

		v.dataSize += elementSize
		if v.dataSize > v.maxDataSize {
			return errMaxDataSizeExceeded(v.dataSize, v.maxDataSize)
		}

		return nil
	}
}

func (v *verifier) HashWithExpEntryHandler(key string) func(field string, value string, exp time.Time) error {
	if len(key) > v.maxKeySize {
		return func(field, value string, exp time.Time) error {
			return errMaxKeySizeExceeded(len(key), v.maxKeySize)
		}
	}

	v.dataSize += len(key)
	if v.dataSize > v.maxDataSize {
		return func(field, value string, exp time.Time) error {
			return errMaxDataSizeExceeded(v.dataSize, v.maxDataSize)
		}
	}

	var entrySize int
	return func(field, value string, exp time.Time) error {
		elementSize := len(field) + len(value) + 8
		entrySize += elementSize
		if entrySize > v.maxEntrySize {
			return errMaxEntrySizeExceeded(entrySize, v.maxEntrySize)
		}

		v.dataSize += elementSize
		if v.dataSize > v.maxDataSize {
			return errMaxDataSizeExceeded(v.dataSize, v.maxDataSize)
		}

		return nil
	}
}

func (v *verifier) ListEntryHandler(key string) func(elem string) error {
	if len(key) > v.maxKeySize {
		return func(elem string) error {
			return errMaxKeySizeExceeded(len(key), v.maxKeySize)
		}
	}

	v.dataSize += len(key)
	if v.dataSize > v.maxDataSize {
		return func(elem string) error {
			return errMaxDataSizeExceeded(v.dataSize, v.maxDataSize)
		}
	}

	var entrySize int
	return func(elem string) error {
		elementSize := len(elem)
		entrySize += elementSize
		if entrySize > v.maxEntrySize {
			return errMaxEntrySizeExceeded(entrySize, v.maxEntrySize)
		}

		v.dataSize += elementSize
		if v.dataSize > v.maxDataSize {
			return errMaxDataSizeExceeded(v.dataSize, v.maxDataSize)
		}

		return nil
	}
}

func (v *verifier) SetEntryHandler(key string) func(elem string) error {
	if len(key) > v.maxKeySize {
		return func(elem string) error {
			return errMaxKeySizeExceeded(len(key), v.maxKeySize)
		}
	}

	v.dataSize += len(key)
	if v.dataSize > v.maxDataSize {
		return func(elem string) error {
			return errMaxDataSizeExceeded(v.dataSize, v.maxDataSize)
		}
	}

	var entrySize int
	return func(elem string) error {
		elementSize := len(elem)
		entrySize += elementSize
		if entrySize > v.maxEntrySize {
			return errMaxEntrySizeExceeded(entrySize, v.maxEntrySize)
		}

		v.dataSize += elementSize
		if v.dataSize > v.maxDataSize {
			return errMaxDataSizeExceeded(v.dataSize, v.maxDataSize)
		}

		return nil
	}
}

func (v *verifier) ZsetEntryHandler(key string) func(elem string, score float64) error {
	if len(key) > v.maxKeySize {
		return func(elem string, score float64) error {
			return errMaxKeySizeExceeded(len(key), v.maxKeySize)
		}
	}

	v.dataSize += len(key)
	if v.dataSize > v.maxDataSize {
		return func(elem string, score float64) error {
			return errMaxDataSizeExceeded(v.dataSize, v.maxDataSize)
		}
	}

	var entrySize int
	return func(elem string, score float64) error {
		elementSize := len(elem) + 8
		entrySize += elementSize
		if entrySize > v.maxEntrySize {
			return errMaxEntrySizeExceeded(entrySize, v.maxEntrySize)
		}

		v.dataSize += elementSize
		if v.dataSize > v.maxDataSize {
			return errMaxDataSizeExceeded(v.dataSize, v.maxDataSize)
		}

		return nil
	}
}

func (v *verifier) HandleModule(key string, value string, marker ModuleMarker) error {
	if len(key) > v.maxKeySize {
		return errMaxKeySizeExceeded(len(key), v.maxKeySize)
	}

	if len(value) > v.maxEntrySize {
		return errMaxEntrySizeExceeded(len(value), v.maxEntrySize)
	}

	v.dataSize += len(key) + len(value)
	if v.dataSize > v.maxDataSize {
		return errMaxDataSizeExceeded(v.dataSize, v.maxDataSize)
	}

	return nil
}

func (v *verifier) StreamEntryHandler(key string) func(entry StreamEntry) error {
	if len(key) > v.maxKeySize {
		return func(entry StreamEntry) error {
			return errMaxKeySizeExceeded(len(key), v.maxKeySize)
		}
	}

	v.dataSize += len(key)
	if v.dataSize > v.maxDataSize {
		return func(entry StreamEntry) error {
			return errMaxDataSizeExceeded(v.dataSize, v.maxDataSize)
		}
	}

	return func(entry StreamEntry) error {
		var valueSize int
		for _, value := range entry.Value {
			if len(value) > maxStreamStrSize {
				return errMaxStreamStrSizeExceeded(len(value), maxStreamStrSize)
			}

			valueSize += len(value)
		}

		// we don't check for the max entry size here as we store
		// stream entries on disk.

		v.dataSize += valueSize
		v.dataSize += 16 // 8: ID#Seq + 8: ID#Millis
		if v.dataSize > v.maxDataSize {
			return errMaxDataSizeExceeded(v.dataSize, v.maxDataSize)
		}

		return nil
	}
}

func (v *verifier) StreamGroupHandler(key string) func(group StreamConsumerGroup) error {
	var entrySize int
	return func(group StreamConsumerGroup) error {
		var groupSize int
		if len(group.Name) > maxStreamStrSize {
			return errMaxStreamStrSizeExceeded(len(group.Name), maxStreamStrSize)
		}

		groupSize += len(group.Name) + 24 // 8: LastID#Seq + 8: LastID#Millis + 8: EntriesRead

		for _, consumer := range group.Consumers {
			if len(consumer.Name) > maxStreamStrSize {
				return errMaxStreamStrSizeExceeded(len(consumer.Name), maxStreamStrSize)
			}

			groupSize += len(consumer.Name) + 16 // 8: SeenTime + 8: ActiveTime

			if len(consumer.PendingEntries) > v.maxStreamPELSize {
				return errMaxStreamPELSizeExceeded(len(consumer.PendingEntries), v.maxStreamPELSize)
			}

			for _, pe := range consumer.PendingEntries {
				groupSize += 32 // 8: ID#Seq + 8: ID#Millis + 8: DeliveryCount + 8: DeliveryTime

				for _, val := range pe.Entry.Value {
					if len(val) > maxStreamStrSize {
						return errMaxStreamStrSizeExceeded(len(val), maxStreamStrSize)
					}

					groupSize += len(val)
				}
			}
		}
		entrySize += groupSize

		// unlike normal stream entries, pending entries are stored both on disk
		// and memory.

		if entrySize > v.maxEntrySize {
			return errMaxEntrySizeExceeded(entrySize, v.maxEntrySize)
		}

		v.dataSize += groupSize
		if v.dataSize > v.maxDataSize {
			return errMaxDataSizeExceeded(v.dataSize, v.maxDataSize)
		}

		return nil
	}
}

func (v *verifier) AllowPartialRead() bool {
	return v.allowPartialVerify
}

func (v *verifier) RequireStrictEOF() bool {
	return v.requireStrictEOF
}

func (v *verifier) HandleExpireTime(key string, expireTime time.Duration) {
}

func (v *verifier) HandleListEnding(key string, entriesRead uint64) {
}

func (v *verifier) HandleZsetEnding(key string, entriesRead uint64) {
}

func (v *verifier) HandleStreamEnding(key string, entriesRead uint64) {
}
