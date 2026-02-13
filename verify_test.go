package rdb

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var allTypesRDBPath = filepath.Join(dumpsPath, "all-types.rdb")
var streamWithPELRDBPath = filepath.Join(dumpsPath, "stream-with-pel.rdb")
var badCrcRDBPath = filepath.Join(dumpsPath, "bad-crc.rdb")
var stringRDBValuePath = filepath.Join(valueDumpsPath, "string.bin")
var streamWithPELRDBValuePath = filepath.Join(valueDumpsPath, "stream-listpacks3.bin")
var multiDBRDBPath = filepath.Join(dumpsPath, "multi-db.rdb")
var withPaddingRDBPath = filepath.Join(dumpsPath, "with-padding.rdb")
var bigDumpPath = filepath.Join(dumpsPath, "big.rdb")

func TestVerifyFile(t *testing.T) {
	err := VerifyFile(allTypesRDBPath, VerifyFileOptions{})
	require.NoError(t, err)
}

func TestVerifyFile_withPEL(t *testing.T) {
	err := VerifyFile(streamWithPELRDBPath, VerifyFileOptions{})
	require.NoError(t, err)
}

func TestVerifyFile_AllowPartialRead(t *testing.T) {
	err := VerifyFile(multiDBRDBPath, VerifyFileOptions{
		AllowPartialVerify: true,
	})
	require.NoError(t, err)

	err = VerifyFile(multiDBRDBPath, VerifyFileOptions{
		AllowPartialVerify: false,
	})
	require.ErrorContains(t, err, "partial restore")
}

func TestVerifyFile_RequireStrictEOF(t *testing.T) {
	err := VerifyFile(withPaddingRDBPath, VerifyFileOptions{
		RequireStrictEOF: false,
	})
	require.NoError(t, err)

	err = VerifyFile(withPaddingRDBPath, VerifyFileOptions{
		RequireStrictEOF: true,
	})
	require.ErrorContains(t, err, "eof")
}

func TestVerifyFile_BadCrc(t *testing.T) {
	err := VerifyFile(badCrcRDBPath, VerifyFileOptions{})
	require.ErrorContains(t, err, "CRC")
}

func TestVerifyFile_maxDataSize(t *testing.T) {
	err := VerifyFile(allTypesRDBPath, VerifyFileOptions{
		MaxDataSize: 10,
	})
	require.ErrorContains(t, err, "max data size")
}

func TestVerifyFile_maxEntrySize(t *testing.T) {
	err := VerifyFile(allTypesRDBPath, VerifyFileOptions{
		MaxEntrySize: 5,
	})
	require.ErrorContains(t, err, "max entry size")
}

func TestVerifyFile_maxKeySize(t *testing.T) {
	err := VerifyFile(allTypesRDBPath, VerifyFileOptions{
		MaxKeySize: 1,
	})
	require.ErrorContains(t, err, "max key size")
}

func TestVerifyFile_maxStreamPELSize(t *testing.T) {
	err := VerifyFile(streamWithPELRDBPath, VerifyFileOptions{
		MaxStreamPELSize: 1,
	})
	require.ErrorContains(t, err, "max stream pel size")
}

func TestVerifyFile_biggerThanBufferSizeFile(t *testing.T) {
	err := VerifyFile(bigDumpPath, VerifyFileOptions{})
	require.NoError(t, err)
}

func TestVerifyValue(t *testing.T) {
	dump, err := os.ReadFile(stringRDBValuePath)
	require.NoError(t, err)

	err = VerifyValue(dump, VerifyValueOptions{})
	require.NoError(t, err)

	dump, err = os.ReadFile(streamWithPELRDBValuePath)
	require.NoError(t, err)

	err = VerifyValue(dump, VerifyValueOptions{})
	require.NoError(t, err)
}

func TestVerifyValue_maxEntrySize(t *testing.T) {
	dump, err := os.ReadFile(stringRDBValuePath)
	require.NoError(t, err)

	err = VerifyValue(dump, VerifyValueOptions{
		MaxEntrySize: 12,
	})
	require.ErrorContains(t, err, "max entry size")
}

func TestVerifyValue_maxStreamPELSize(t *testing.T) {
	dump, err := os.ReadFile(streamWithPELRDBValuePath)
	require.NoError(t, err)

	err = VerifyValue(dump, VerifyValueOptions{
		MaxStreamPELSize: 1,
	})
	require.ErrorContains(t, err, "max stream pel size")
}

func TestVerifyReader(t *testing.T) {
	file, err := os.Open(allTypesRDBPath)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = file.Close()
	})

	err = VerifyReader(file, VerifyReaderOptions{})
	require.NoError(t, err)
}

func TestVerifyReader_withPEL(t *testing.T) {
	file, err := os.Open(streamWithPELRDBPath)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = file.Close()
	})

	err = VerifyReader(file, VerifyReaderOptions{})
	require.NoError(t, err)
}

func TestReaderFile_maxDataSize(t *testing.T) {
	file, err := os.Open(allTypesRDBPath)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = file.Close()
	})

	err = VerifyReader(file, VerifyReaderOptions{
		MaxDataSize: 10,
	})
	require.ErrorContains(t, err, "max data size")
}

func TestVerifyReader_maxEntrySize(t *testing.T) {
	file, err := os.Open(allTypesRDBPath)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = file.Close()
	})

	err = VerifyReader(file, VerifyReaderOptions{
		MaxEntrySize: 5,
	})
	require.ErrorContains(t, err, "max entry size")
}

func TestVerifyReader_maxKeySize(t *testing.T) {
	file, err := os.Open(allTypesRDBPath)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = file.Close()
	})

	err = VerifyReader(file, VerifyReaderOptions{
		MaxKeySize: 1,
	})
	require.ErrorContains(t, err, "max key size")
}

func TestVerifyReader_maxStreamPELSize(t *testing.T) {
	file, err := os.Open(streamWithPELRDBPath)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = file.Close()
	})

	err = VerifyReader(file, VerifyReaderOptions{
		MaxStreamPELSize: 1,
	})
	require.ErrorContains(t, err, "max stream pel size")
}

func TestVerifyReader_BadCrc(t *testing.T) {
	file, err := os.Open(badCrcRDBPath)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = file.Close()
	})

	err = VerifyReader(file, VerifyReaderOptions{})
	require.ErrorContains(t, err, "CRC")
}

func TestVerifyReader_AllowPartialRead(t *testing.T) {
	file, err := os.Open(multiDBRDBPath)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = file.Close()
	})

	err = VerifyReader(file, VerifyReaderOptions{
		AllowPartialVerify: true,
	})
	require.NoError(t, err)

	_, err = file.Seek(0, 0)
	require.NoError(t, err)

	err = VerifyReader(file, VerifyReaderOptions{
		AllowPartialVerify: false,
	})
	require.ErrorContains(t, err, "partial restore")
}

func TestVerifyReader_RequireStrictEOF(t *testing.T) {
	file, err := os.Open(withPaddingRDBPath)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = file.Close()
	})

	err = VerifyReader(file, VerifyReaderOptions{
		RequireStrictEOF: false,
	})
	require.NoError(t, err)

	_, err = file.Seek(0, 0)
	require.NoError(t, err)

	err = VerifyReader(file, VerifyReaderOptions{
		RequireStrictEOF: true,
	})
	require.ErrorContains(t, err, "eof")
}

func TestVerifier_String_MaxDataSize(t *testing.T) {
	v := &verifier{
		maxDataSize:  3,
		maxEntrySize: 100,
		maxKeySize:   100,
	}

	require.NoError(t, v.HandleString("k", "v"))
	require.Equal(t, 2, v.dataSize)

	require.ErrorContains(t, v.HandleString("k", "v"), "max data size")
}

func TestVerifier_String_MaxKeySize(t *testing.T) {
	v := &verifier{
		maxDataSize:  100,
		maxEntrySize: 100,
		maxKeySize:   2,
	}

	require.ErrorContains(t, v.HandleString("longkey", "v"), "max key size")
}

func TestVerifier_String_MaxEntrySize(t *testing.T) {
	v := &verifier{
		maxDataSize:  100,
		maxEntrySize: 2,
		maxKeySize:   100,
	}

	require.ErrorContains(t, v.HandleString("k", "longvalue"), "max entry size")
}

func TestVerifier_HashEntryHandler_MaxDataSize(t *testing.T) {
	v := &verifier{
		maxDataSize:      5,
		maxEntrySize:     100,
		maxKeySize:       100,
		maxStreamPELSize: 100,
	}

	h := v.HashEntryHandler("k")
	require.NoError(t, h("f", "v"))
	require.NoError(t, h("x", "y"))
	require.Equal(t, 5, v.dataSize)

	require.ErrorContains(t, h("a", "b"), "max data size")
}

func TestVerifier_HashEntryHandler_MaxKeySize(t *testing.T) {
	v := &verifier{
		maxDataSize:  100,
		maxEntrySize: 100,
		maxKeySize:   2,
	}

	h := v.HashEntryHandler("longkey")
	require.ErrorContains(t, h("f", "v"), "max key size")
}

func TestVerifier_HashEntryHandler_MaxEntrySize(t *testing.T) {
	v := &verifier{
		maxDataSize:  100,
		maxEntrySize: 3,
		maxKeySize:   100,
	}

	h := v.HashEntryHandler("k")
	require.NoError(t, h("f", "v"))
	require.ErrorContains(t, h("x", "y"), "max entry size")
}

func TestVerifier_HashWithExpEntryHandler_MaxDataSize(t *testing.T) {
	v := &verifier{
		maxDataSize:      21,
		maxEntrySize:     100,
		maxKeySize:       100,
		maxStreamPELSize: 100,
	}

	h := v.HashWithExpEntryHandler("k")
	require.NoError(t, h("f", "v", time.Now()))
	require.NoError(t, h("x", "y", time.Now()))
	require.Equal(t, 21, v.dataSize)

	require.ErrorContains(t, h("a", "b", time.Now()), "max data size")
}

func TestVerifier_HashWithExpEntryHandler_MaxKeySize(t *testing.T) {
	v := &verifier{
		maxDataSize:  100,
		maxEntrySize: 100,
		maxKeySize:   2,
	}

	h := v.HashWithExpEntryHandler("longkey")
	require.ErrorContains(t, h("f", "v", time.Now()), "max key size")
}

func TestVerifier_HashWithExpEntryHandler_MaxEntrySize(t *testing.T) {
	v := &verifier{
		maxDataSize:  100,
		maxEntrySize: 10,
		maxKeySize:   100,
	}

	h := v.HashWithExpEntryHandler("k")
	// "f" + "v" + 8 = 10, at limit
	require.NoError(t, h("f", "v", time.Now()))
	// cumulative: 20, exceeds 10
	require.ErrorContains(t, h("x", "y", time.Now()), "max entry size")
}

func TestVerifier_ListEntryHandler_MaxDataSize(t *testing.T) {
	v := &verifier{
		maxDataSize:      3,
		maxEntrySize:     100,
		maxKeySize:       100,
		maxStreamPELSize: 100,
	}

	h := v.ListEntryHandler("k")
	require.NoError(t, h("a"))
	require.NoError(t, h("b"))
	require.Equal(t, 3, v.dataSize)

	require.ErrorContains(t, h("c"), "max data size")
}

func TestVerifier_ListEntryHandler_MaxKeySize(t *testing.T) {
	v := &verifier{
		maxDataSize:  100,
		maxEntrySize: 100,
		maxKeySize:   2,
	}

	h := v.ListEntryHandler("longkey")
	require.ErrorContains(t, h("a"), "max key size")
}

func TestVerifier_ListEntryHandler_MaxEntrySize(t *testing.T) {
	v := &verifier{
		maxDataSize:  100,
		maxEntrySize: 1,
		maxKeySize:   100,
	}

	h := v.ListEntryHandler("k")
	require.NoError(t, h("a"))
	require.ErrorContains(t, h("b"), "max entry size")
}

func TestVerifier_SetEntryHandler_MaxDataSize(t *testing.T) {
	v := &verifier{
		maxDataSize:      3,
		maxEntrySize:     100,
		maxKeySize:       100,
		maxStreamPELSize: 100,
	}

	h := v.SetEntryHandler("k")
	require.NoError(t, h("a"))
	require.NoError(t, h("b"))
	require.Equal(t, 3, v.dataSize)

	require.ErrorContains(t, h("c"), "max data size")
}

func TestVerifier_SetEntryHandler_MaxKeySize(t *testing.T) {
	v := &verifier{
		maxDataSize:  100,
		maxEntrySize: 100,
		maxKeySize:   2,
	}

	h := v.SetEntryHandler("longkey")
	require.ErrorContains(t, h("a"), "max key size")
}

func TestVerifier_SetEntryHandler_MaxEntrySize(t *testing.T) {
	v := &verifier{
		maxDataSize:  100,
		maxEntrySize: 1,
		maxKeySize:   100,
	}

	h := v.SetEntryHandler("k")
	require.NoError(t, h("a"))
	require.ErrorContains(t, h("b"), "max entry size")
}

func TestVerifier_ZsetEntryHandler_MaxDataSize(t *testing.T) {
	v := &verifier{
		maxDataSize:      19,
		maxEntrySize:     100,
		maxKeySize:       100,
		maxStreamPELSize: 100,
	}

	h := v.ZsetEntryHandler("k")
	require.NoError(t, h("a", 1))
	require.NoError(t, h("b", 2))
	require.Equal(t, 19, v.dataSize)

	require.ErrorContains(t, h("c", 3), "max data size")
}

func TestVerifier_ZsetEntryHandler_MaxKeySize(t *testing.T) {
	v := &verifier{
		maxDataSize:  100,
		maxEntrySize: 100,
		maxKeySize:   2,
	}

	h := v.ZsetEntryHandler("longkey")
	require.ErrorContains(t, h("a", 1), "max key size")
}

func TestVerifier_ZsetEntryHandler_MaxEntrySize(t *testing.T) {
	v := &verifier{
		maxDataSize:  100,
		maxEntrySize: 9,
		maxKeySize:   100,
	}

	h := v.ZsetEntryHandler("k")
	// "a" + 8 = 9, at limit
	require.NoError(t, h("a", 1))
	// cumulative: 18, exceeds 9
	require.ErrorContains(t, h("b", 2), "max entry size")
}

func TestVerifier_StreamEntryHandler_MaxDataSize(t *testing.T) {
	v := &verifier{
		maxDataSize:      42,
		maxEntrySize:     1000,
		maxKeySize:       100,
		maxStreamPELSize: 10,
	}

	h := v.StreamEntryHandler("stream")
	require.NoError(t, h(StreamEntry{Value: []string{"a", "b"}}))
	require.NoError(t, h(StreamEntry{Value: []string{"x", "y"}}))
	require.Equal(t, 42, v.dataSize)

	require.ErrorContains(t, h(StreamEntry{Value: []string{"z"}}), "max data size")
}

func TestVerifier_StreamEntryHandler_MaxKeySize(t *testing.T) {
	v := &verifier{
		maxDataSize:  100,
		maxEntrySize: 1000,
		maxKeySize:   2,
	}

	h := v.StreamEntryHandler("stream")
	require.ErrorContains(t, h(StreamEntry{Value: []string{"a"}}), "max key size")
}

func TestVerifier_StreamGroupHandler_MaxDataSize(t *testing.T) {
	v := &verifier{
		maxDataSize:      156,
		maxEntrySize:     1000,
		maxKeySize:       100,
		maxStreamPELSize: 10,
	}

	h := v.StreamGroupHandler("stream")

	group1 := StreamConsumerGroup{
		Name: "g1",
		Consumers: []StreamConsumer{
			{
				Name: "c1",
				PendingEntries: []*StreamPendingEntry{
					{
						Entry: StreamEntry{Value: []string{"a", "b"}},
					},
				},
			},
		},
	}

	group2 := StreamConsumerGroup{
		Name: "g2",
		Consumers: []StreamConsumer{
			{
				Name: "c2",
				PendingEntries: []*StreamPendingEntry{
					{
						Entry: StreamEntry{Value: []string{"x", "y"}},
					},
				},
			},
		},
	}

	require.NoError(t, h(group1))
	require.NoError(t, h(group2))
	require.Equal(t, 156, v.dataSize)

	require.ErrorContains(t, h(group1), "max data size")
}

func TestVerifier_StreamGroupHandler_MaxEntrySize(t *testing.T) {
	// group size: name "g" (1) + 24 + consumer name "c" (1) + 16 + pending 32 + value "a" (1) = 75
	// two groups cumulative entrySize = 150, limit = 100
	v := &verifier{
		maxDataSize:      10000,
		maxEntrySize:     100,
		maxKeySize:       100,
		maxStreamPELSize: 10,
	}

	h := v.StreamGroupHandler("stream")

	group := StreamConsumerGroup{
		Name: "g",
		Consumers: []StreamConsumer{
			{
				Name: "c",
				PendingEntries: []*StreamPendingEntry{
					{Entry: StreamEntry{Value: []string{"a"}}},
				},
			},
		},
	}
	require.NoError(t, h(group))

	require.ErrorContains(t, h(group), "max entry size")
}

func TestVerifier_StreamGroupHandler_MaxStreamPELSize(t *testing.T) {
	v := &verifier{
		maxDataSize:      10000,
		maxEntrySize:     10000,
		maxKeySize:       100,
		maxStreamPELSize: 0,
	}

	h := v.StreamGroupHandler("stream")
	group := StreamConsumerGroup{
		Name: "g1",
		Consumers: []StreamConsumer{
			{
				Name: "c1",
				PendingEntries: []*StreamPendingEntry{
					{Entry: StreamEntry{Value: []string{"a"}}},
				},
			},
		},
	}
	require.ErrorContains(t, h(group), "max stream pel size")
}
