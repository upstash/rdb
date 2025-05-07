package rdb

import (
	"bufio"
	"bytes"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func createTestFileWriter(f *os.File) *FileWriter {
	return &FileWriter{w: bufio.NewWriter(f), f: f}
}

func TestFileWriter_PosAndSeek(t *testing.T) {
	tempDir := t.TempDir()
	tempFile, err := os.CreateTemp(tempDir, "rdb_test_*.rdb")
	assert.NoError(t, err)

	writer := createTestFileWriter(tempFile)

	pos, err := writer.Pos()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), pos)

	data0 := []byte("*****")
	n0, err := writer.Write(data0)
	assert.NoError(t, err)
	assert.Equal(t, len(data0), n0)
	pos, err = writer.Pos()
	assert.NoError(t, err)
	assert.Equal(t, int64(len(data0)), pos)

	data1 := []byte("Hello")
	n1, err := writer.Write(data1)
	assert.NoError(t, err)
	assert.Equal(t, len(data1), n1)
	pos, err = writer.Pos()
	assert.NoError(t, err)
	assert.Equal(t, int64(len(data0)+len(data1)), pos)

	data2 := []byte("World")
	n2, err := writer.Write(data2)
	assert.NoError(t, err)
	assert.Equal(t, len(data2), n2)
	pos, err = writer.Pos()
	assert.NoError(t, err)
	assert.Equal(t, int64(len(data0)+len(data1)+len(data2)), pos)

	assert.NoError(t, writer.Flush())

	pos, err = writer.SeekPos(5)
	assert.NoError(t, err)
	assert.Equal(t, int64(5), pos)

	data3 := []byte("Hi   ")
	n3, err := writer.Write(data3)
	assert.NoError(t, err)
	assert.Equal(t, len(data3), n3)
	pos, err = writer.Pos()
	assert.NoError(t, err)
	assert.Equal(t, int64(5+len(data3)), pos)

	assert.NoError(t, writer.Flush())

	pos, err = writer.SeekPos(10)
	assert.NoError(t, err)
	assert.Equal(t, int64(10), pos)
	data4 := []byte("There")
	n4, err := writer.Write(data4)
	assert.NoError(t, err)
	assert.Equal(t, len(data4), n4)
	pos, err = writer.Pos()
	assert.NoError(t, err)
	assert.Equal(t, int64(15), pos)

	assert.NoError(t, writer.Flush())

	content, err := os.ReadFile(tempFile.Name())
	assert.NoError(t, err)
	expected := []byte("*****Hi   There")
	assert.True(t, bytes.Equal(expected, content))
}
