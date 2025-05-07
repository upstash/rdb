package rdb

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriterLimit(t *testing.T) {
	writer := &Writer{
		buf:   make([]byte, 1<<10),
		limit: 1 << 20, // 1 MB
	}

	err := writer.write(make([]byte, 512<<10)) // 512 KB
	require.NoError(t, err)

	err = writer.write(make([]byte, 512<<10)) // 512 KB
	require.NoError(t, err)

	err = writer.write(make([]byte, 1))
	require.Error(t, err)
}

func TestWriteString(t *testing.T) {
	tests := map[string]string{
		"empty string":  "string-empty.bin",
		"normal string": "string.bin",
		"long string":   "string-long.bin",
	}

	for name, file := range tests {
		t.Run(name, func(t *testing.T) {
			dump, err := os.ReadFile(filepath.Join(valueDumpsPath, file))
			require.NoError(t, err)

			reader := valueReader{
				buf: newMemoryBackedBuffer(dump[1 : len(dump)-10]),
			}

			value, err := reader.ReadString()
			require.NoError(t, err)

			writer := NewWriter()

			err = writer.WriteType(TypeString)
			require.NoError(t, err)

			err = writer.WriteString(value)
			require.NoError(t, err)

			err = writer.WriteChecksum(Version)
			require.NoError(t, err)

			require.Equal(t, dump, writer.GetBuffer())
		})
	}
}

func TestWriteList(t *testing.T) {
	path := filepath.Join(valueDumpsPath, "list-str.bin")

	dump, err := os.ReadFile(path)
	require.NoError(t, err)

	reader := valueReader{
		buf: newMemoryBackedBuffer(dump[1 : len(dump)-10]),
	}

	list := make([]string, 0)
	cb := func(elem string) error {
		list = append(list, elem)
		return nil
	}
	_, err = reader.ReadList(cb)
	require.NoError(t, err)

	writer := NewWriter()

	err = writer.WriteType(TypeList)
	require.NoError(t, err)

	err = writer.WriteList(list)
	require.NoError(t, err)

	err = writer.WriteChecksum(Version)
	require.NoError(t, err)

	require.Equal(t, dump, writer.GetBuffer())
}

func TestWriteSet(t *testing.T) {
	path := filepath.Join(valueDumpsPath, "set-str.bin")

	dump, err := os.ReadFile(path)
	require.NoError(t, err)

	reader := valueReader{
		buf: newMemoryBackedBuffer(dump[1 : len(dump)-10]),
	}

	set := make([]string, 0)
	cb := func(elem string) error {
		set = append(set, elem)
		return nil
	}
	err = reader.ReadSet(cb)
	require.NoError(t, err)

	writer := NewWriter()

	err = writer.WriteType(TypeSet)
	require.NoError(t, err)

	err = writer.WriteSet(set)
	require.NoError(t, err)

	err = writer.WriteChecksum(Version)
	require.NoError(t, err)

	require.Equal(t, dump, writer.GetBuffer())
}

func TestWriteZset(t *testing.T) {
	path := filepath.Join(valueDumpsPath, "zset2.bin")

	dump, err := os.ReadFile(path)
	require.NoError(t, err)

	reader := valueReader{
		buf: newMemoryBackedBuffer(dump[1 : len(dump)-10]),
	}

	elements := make([]string, 0)
	scores := make([]float64, 0)
	cb := func(elem string, score float64) error {
		elements = append(elements, elem)
		scores = append(scores, score)
		return nil
	}
	_, err = reader.ReadZset2(cb)
	require.NoError(t, err)

	writer := NewWriter()

	err = writer.WriteType(TypeZset2)
	require.NoError(t, err)

	err = writer.WriteZset(elements, scores)
	require.NoError(t, err)

	err = writer.WriteChecksum(Version)
	require.NoError(t, err)

	require.Equal(t, dump, writer.GetBuffer())
}

func TestWriteHash(t *testing.T) {
	path := filepath.Join(valueDumpsPath, "hash-str.bin")

	dump, err := os.ReadFile(path)
	require.NoError(t, err)

	reader := valueReader{
		buf: newMemoryBackedBuffer(dump[1 : len(dump)-10]),
	}

	hash := make(map[string]string)
	cb := func(field, value string) error {
		hash[field] = value
		return nil
	}
	err = reader.ReadHash(cb)
	require.NoError(t, err)

	writer := NewWriter()

	err = writer.WriteType(TypeHash)
	require.NoError(t, err)

	err = writer.WriteHash(hash)
	require.NoError(t, err)

	err = writer.WriteChecksum(Version)
	require.NoError(t, err)

	require.Equal(t, dump, writer.GetBuffer())
}

func TestJSON(t *testing.T) {
	path := filepath.Join(valueDumpsPath, "module2-jsonv3.bin")

	dump, err := os.ReadFile(path)
	require.NoError(t, err)

	reader := valueReader{
		buf: newMemoryBackedBuffer(dump[1 : len(dump)-10]),
	}

	value, marker, err := reader.ReadModule2(false)
	require.Equal(t, JSONModuleMarker, marker)
	require.NoError(t, err)

	writer := NewWriter()

	err = writer.WriteType(TypeModule2)
	require.NoError(t, err)

	err = writer.WriteJSON(value)
	require.NoError(t, err)

	err = writer.WriteChecksum(Version)
	require.NoError(t, err)

	require.Equal(t, dump, writer.GetBuffer())
}

func TestStream(t *testing.T) {
	path := filepath.Join(valueDumpsPath, "stream-listpacks-upstash.bin")

	dump, err := os.ReadFile(path)
	require.NoError(t, err)

	reader := valueReader{
		buf: newMemoryBackedBuffer(dump[1 : len(dump)-10]),
	}

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

	_, err = reader.ReadStreamListpacks(entryCB, groupCB)
	require.NoError(t, err)

	writer := NewWriter()

	err = writer.WriteType(TypeStreamListpacks)
	require.NoError(t, err)

	stream := &Stream{
		LastID:  entries[len(entries)-1].ID,
		Entries: entries,
		Length:  uint64(len(entries)),
		Groups:  groups,
	}
	err = writer.WriteStream(stream)
	require.NoError(t, err)

	err = writer.WriteChecksum(Version)
	require.NoError(t, err)

	require.Equal(t, dump, writer.GetBuffer())
}
