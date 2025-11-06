package rdb

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

var allTypesRDBPath = filepath.Join(dumpsPath, "all-types.rdb")
var streamWithPELRDBPath = filepath.Join(dumpsPath, "stream-with-pel.rdb")
var badCrcRDBPath = filepath.Join(dumpsPath, "bad-crc.rdb")
var stringRDBValuePath = filepath.Join(valueDumpsPath, "string.bin")
var streamWithPELRDBValuePath = filepath.Join(valueDumpsPath, "stream-listpacks3.bin")
var multiDBRDBPath = filepath.Join(dumpsPath, "multi-db.rdb")
var withPaddingRDBPath = filepath.Join(dumpsPath, "with-padding.rdb")

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
		file.Close()
	})

	err = VerifyReader(file, VerifyReaderOptions{})
	require.NoError(t, err)
}

func TestVerifyReader_withPEL(t *testing.T) {
	file, err := os.Open(streamWithPELRDBPath)
	require.NoError(t, err)
	t.Cleanup(func() {
		file.Close()
	})

	err = VerifyReader(file, VerifyReaderOptions{})
	require.NoError(t, err)
}

func TestReaderFile_maxDataSize(t *testing.T) {
	file, err := os.Open(allTypesRDBPath)
	require.NoError(t, err)
	t.Cleanup(func() {
		file.Close()
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
		file.Close()
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
		file.Close()
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
		file.Close()
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
		file.Close()
	})

	err = VerifyReader(file, VerifyReaderOptions{})
	require.ErrorContains(t, err, "CRC")
}

func TestVerifyReader_AllowPartialRead(t *testing.T) {
	file, err := os.Open(multiDBRDBPath)
	require.NoError(t, err)
	t.Cleanup(func() {
		file.Close()
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
		file.Close()
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
