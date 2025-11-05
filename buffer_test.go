package rdb

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMemoryBackedBuffer(t *testing.T) {
	buf := newMemoryBackedBuffer([]byte{1, 2, 3, 4})

	b, err := buf.Get(1)
	require.NoError(t, err)
	require.Equal(t, []byte{1}, b)

	b, err = buf.Get(3)
	require.NoError(t, err)
	require.Equal(t, []byte{2, 3, 4}, b)
}

func TestMemoryBackedBuffer_outOfBoundsAccess(t *testing.T) {
	buf := newMemoryBackedBuffer(make([]byte, 10))

	_, err := buf.Get(11)
	require.Error(t, err)
}

func TestMemoryBackedBuffer_view(t *testing.T) {
	buf := newMemoryBackedBuffer([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9})
	view, err := buf.View(3)
	require.NoError(t, err)

	b, err := view.Get(3 + buf.Pos())
	require.NoError(t, err)
	require.Equal(t, []byte{4, 5, 6}, b)

	require.False(t, view.SupportsView())

	_, err = view.Get(10)
	require.Error(t, err)

	err = view.Close()
	require.NoError(t, err)

	// view should not affect the iteration of buf
	b, err = buf.Get(5)
	require.NoError(t, err)
	require.Equal(t, []byte{1, 2, 3, 4, 5}, b)
}

func open() (*os.File, int64, error) {
	file, err := os.Open(filepath.Join("testdata", "buffer.bin"))
	if err != nil {
		return nil, 0, err
	}

	info, err := file.Stat()
	if err != nil {
		err2 := file.Close()
		return nil, 0, errors.Join(err, err2)
	}

	return file, info.Size(), nil
}

func TestFileBackedBuffer(t *testing.T) {
	file, fileLen, err := open()
	require.NoError(t, err)
	defer file.Close()

	buf := newFileBackedBuffer(file, int(fileLen), 1024)

	b, err := buf.Get(8)
	require.NoError(t, err)
	require.Equal(t, []byte{0, 1, 2, 3, 4, 5, 6, 7}, b)

	expected := make([]byte, 1024)
	for i := 0; i < 1024; i++ {
		expected[i] = byte((i + 8) % 256) // +8 because we read 8 bytes above
	}

	b, err = buf.Get(1024)
	require.NoError(t, err)
	require.Equal(t, expected, b)

	require.Equal(t, uint64(0xfe908173263acf4a), buf.Crc())
}

func TestFileBackedBuffer_readMoreThanBufCap(t *testing.T) {
	file, fileLen, err := open()
	require.NoError(t, err)
	defer file.Close()

	buf := newFileBackedBuffer(file, int(fileLen), 100)

	expected := make([]byte, 1024)
	for i := 0; i < 1024; i++ {
		expected[i] = byte(i % 256)
	}

	b, err := buf.Get(1024)
	require.NoError(t, err)
	require.Equal(t, expected, b)

	require.Equal(t, uint64(0x3bfa104f9b118f4d), buf.Crc())
}

func TestFileBackedBuffer_outOfBoundsAccess(t *testing.T) {
	file, fileLen, err := open()
	require.NoError(t, err)
	defer file.Close()

	buf := newFileBackedBuffer(file, int(fileLen), 2048)

	expected := make([]byte, 2048)
	for i := 0; i < 2048; i++ {
		expected[i] = byte(i % 256)
	}

	b, err := buf.Get(2048)
	require.NoError(t, err)
	require.Equal(t, expected, b)

	_, err = buf.Get(1)
	require.Error(t, err)
}

func TestFileBackedBuffer_crc(t *testing.T) {
	file, fileLen, err := open()
	require.NoError(t, err)
	defer file.Close()

	buf := newFileBackedBuffer(file, int(fileLen+crcLen), 2048)

	expected := make([]byte, 2048)
	for i := 0; i < 2048; i++ {
		expected[i] = byte(i % 256)
	}

	b, err := buf.Get(2048)
	require.NoError(t, err)
	require.Equal(t, expected, b)

	require.Equal(t, uint64(9267225763363821280), buf.Crc())
}

func TestFileBackedBuffer_view(t *testing.T) {
	file, fileLen, err := open()
	require.NoError(t, err)
	defer file.Close()

	buf := newFileBackedBuffer(file, int(fileLen), 42)

	b, err := buf.Get(255)
	require.NoError(t, err)
	require.Equal(t, 255, len(b))

	view, err := buf.View(buf.Pos())
	require.NoError(t, err)
	defer func() {
		err = view.Close()
		require.NoError(t, err)
	}()

	expected := make([]byte, 1042)
	for i := 255; i < 1297; i++ {
		expected[i-255] = byte(i % 256)
	}

	b, err = view.Get(100)
	require.NoError(t, err)
	require.Equal(t, expected[:100], b)

	b, err = view.Get(942)
	require.NoError(t, err)
	require.Equal(t, expected[100:], b)

	require.False(t, view.SupportsView())

	_, err = view.Get(10_000)
	require.Error(t, err)

	// view should not affect the iteration of buf
	b, err = buf.Get(5)
	require.NoError(t, err)
	require.Equal(t, []byte{255, 0, 1, 2, 3}, b)
}
