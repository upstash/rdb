package rdb

import (
	"errors"
	"fmt"
	"io"
	"os"
)

// a partial view of buffer, starting from a particular position.
type bufferView interface {
	buffer
	Close() error
}

type buffer interface {
	Get(n int) ([]byte, error)
	Pos() int
	View(pos int) (bufferView, error)
}

type memoryBackedBuffer struct {
	buf []byte
	len int
	pos int
}

func newMemoryBackedBuffer(buf []byte) *memoryBackedBuffer {
	return &memoryBackedBuffer{
		buf: buf,
		len: len(buf),
	}
}

func (b *memoryBackedBuffer) Get(n int) ([]byte, error) {
	if b.len < b.pos+n {
		return nil, io.ErrUnexpectedEOF
	}

	value := b.buf[b.pos : b.pos+n]
	b.pos += n
	return value, nil
}

func (b *memoryBackedBuffer) Pos() int {
	return b.pos
}

type memoryBackedBufferView struct {
	buf *memoryBackedBuffer
}

func (b *memoryBackedBuffer) View(pos int) (bufferView, error) {
	return &memoryBackedBufferView{
		buf: &memoryBackedBuffer{
			buf: b.buf,
			len: b.len,
			pos: pos,
		},
	}, nil
}

func (v *memoryBackedBufferView) Get(n int) ([]byte, error) {
	return v.buf.Get(n)
}

func (v *memoryBackedBufferView) Pos() int {
	return v.buf.Pos()
}

func (v *memoryBackedBufferView) View(pos int) (bufferView, error) {
	return nil, errors.New("cannot take a view of a view")
}

func (v *memoryBackedBufferView) Close() error {
	return nil
}

type fileBackedBuffer struct {
	file    *os.File
	fileLen int
	filePos int
	bufCap  int
	buf     []byte
	len     int
	pos     int
	calcCRC bool
	crc     uint64
}

func newFileBackedBuffer(file *os.File, fileLen int, bufCap int) *fileBackedBuffer {
	return &fileBackedBuffer{
		file:    file,
		fileLen: fileLen,
		bufCap:  bufCap,
		buf:     make([]byte, 0),
	}
}

func (b *fileBackedBuffer) Get(n int) ([]byte, error) {
	if b.fileLen < b.filePos+n {
		// we use the file pos as the source of truth
		return nil, io.ErrUnexpectedEOF
	}

	if b.len < b.pos+n {
		// there are enough bytes in the file, but not in the buffer.
		// we need to read some more bytes into buffer. after this call
		// it is guaranteed that b.pos + n <= b.len
		err := b.read(n)
		if err != nil {
			return nil, err
		}
	}

	value := b.buf[b.pos : b.pos+n]
	b.pos += n
	b.filePos += n
	return value, nil
}

func (b *fileBackedBuffer) Pos() int {
	return b.filePos
}

type fileBackedBufferView struct {
	file *os.File
	buf  *fileBackedBuffer
}

func (b *fileBackedBuffer) View(pos int) (bufferView, error) {
	// reopen the same file, and seek to the current position
	file, err := os.Open(b.file.Name())
	if err != nil {
		return nil, err
	}

	shouldSeek := int64(headerLen + pos)
	seek, err := file.Seek(shouldSeek, 0) // from the start
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	if seek != shouldSeek {
		_ = file.Close()
		return nil, fmt.Errorf("expected to seek %d, but it was %d", shouldSeek, seek)
	}

	buf := newFileBackedBuffer(file, b.fileLen, b.bufCap)
	buf.filePos = pos

	return &fileBackedBufferView{
		file: file,
		buf:  buf,
	}, nil
}

func (v *fileBackedBufferView) Get(n int) ([]byte, error) {
	return v.buf.Get(n)
}

func (v *fileBackedBufferView) Pos() int {
	return v.buf.Pos()
}

func (v *fileBackedBufferView) View(pos int) (bufferView, error) {
	return nil, errors.New("cannot take a view of a view")
}

func (v *fileBackedBufferView) Close() error {
	return v.file.Close()
}

func (b *fileBackedBuffer) initCRC(payload []byte) {
	b.calcCRC = true
	b.crc = getCRC(b.crc, payload)
}

// caller must guarantee that there are at least n bytes in the file starting
// at file pos, and there are not enough not-yet-read bytes in the buffer.
func (b *fileBackedBuffer) read(n int) error {
	remaining := b.len - b.pos // not-yet-read bytes in the buffer

	// the reason we are using a new buffer each time is that
	// the downstream users of the buffer might use methods like
	// unsafe.ToString() which is backed by this buffer. Using a
	// single buffer and reading into it can be accomplished easily,
	// but that would require us to copy bytes instead of unsafe.ToString()
	// because otherwise we would break the invariant that the string
	// is immutable. So, we would either have to copy downstream or
	// here.

	dst := make([]byte, maxInt(b.bufCap, n)) // n might be >> bufCap
	copied := copy(dst, b.buf[b.pos:])       // copy remaining bytes into new buffer
	if copied != remaining {
		return fmt.Errorf("expected to copy %d bytes, but it was %d", remaining, copied)
	}
	b.buf = dst
	b.len = len(dst)
	b.pos = 0

	// we don't want to read more than the file len we know. there might
	// be more bytes after file len, but reading them would result
	// in a wrong CRC calculation.
	readLen := minInt(b.len, b.fileLen-b.filePos) - remaining
	read, err := b.file.Read(b.buf[remaining : remaining+readLen])
	if err != nil {
		return err
	}

	if read != readLen {
		return fmt.Errorf("expected to read %d bytes, but it was %d", readLen, read)
	}

	if b.calcCRC {
		b.crc = getCRC(b.crc, b.buf[remaining:remaining+readLen])
	}

	return nil
}
