package rdb

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

var allTypesRDBPath = filepath.Join(dumpsPath, "all-types.rdb")
var stringRDBValuePath = filepath.Join(valueDumpsPath, "string.bin")

func TestVerifyFile(t *testing.T) {
	err := VerifyFile(allTypesRDBPath, VerifyFileOptions{})
	require.NoError(t, err)
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

func TestVerifyValue(t *testing.T) {
	dump, err := os.ReadFile(stringRDBValuePath)
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
