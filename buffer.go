package rdb

import (
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
	SupportsView() bool
	View(pos int) (bufferView, error)
	DoNotCalcCrc()
	Crc() uint64
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

func (b *memoryBackedBuffer) DoNotCalcCrc() {
	// nop
}

func (b *memoryBackedBuffer) Crc() uint64 {
	return 0
}

type memoryBackedBufferView struct {
	buf *memoryBackedBuffer
}

func (b *memoryBackedBuffer) SupportsView() bool {
	return true
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

func (v *memoryBackedBufferView) DoNotCalcCrc() {
	// nop
}

func (v *memoryBackedBufferView) Crc() uint64 {
	return 0
}

func (v *memoryBackedBufferView) SupportsView() bool {
	return false
}

func (v *memoryBackedBufferView) View(_ int) (bufferView, error) {
	return nil, nil
}

func (v *memoryBackedBufferView) Close() error {
	return nil
}

type crcCalculator struct {
	crc         uint64
	checkedLen  int
	fileLen     int
	doNotUpdate bool
}

func (c *crcCalculator) Update(b []byte) {
	if c.doNotUpdate {
		return
	}

	limit := c.fileLen - crcLen
	if c.checkedLen+len(b) > limit {
		// we don't want to update crc with more than the limit we know.
		// there might be more bytes after limit(i.e. crc footer),
		// but updating the crc with them would result in a wrong crc.
		b = b[:limit-c.checkedLen]
	}

	c.crc = getCRC(c.crc, b)
}

func (c *crcCalculator) Crc() uint64 {
	return c.crc
}

type fileBackedBuffer struct {
	file    *os.File
	fileLen int
	filePos int
	bufCap  int
	buf     []byte
	len     int
	pos     int
	crcCalc *crcCalculator
}

func newFileBackedBuffer(file *os.File, fileLen int, bufCap int) *fileBackedBuffer {
	return &fileBackedBuffer{
		file:    file,
		fileLen: fileLen,
		bufCap:  bufCap,
		buf:     make([]byte, 0),
		crcCalc: &crcCalculator{
			crc:         0,
			checkedLen:  0,
			fileLen:     fileLen,
			doNotUpdate: false,
		},
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

func (b *fileBackedBuffer) SupportsView() bool {
	return true
}

func (b *fileBackedBuffer) View(pos int) (bufferView, error) {
	// reopen the same file, and seek to the current position
	file, err := os.Open(b.file.Name())
	if err != nil {
		return nil, err
	}

	shouldSeek := int64(pos)
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

func (v *fileBackedBufferView) DoNotCalcCrc() {
	// nop
}

func (v *fileBackedBufferView) Crc() uint64 {
	return 0
}

func (v *fileBackedBufferView) SupportsView() bool {
	return false
}

func (v *fileBackedBufferView) View(_ int) (bufferView, error) {
	return nil, nil
}

func (v *fileBackedBufferView) Close() error {
	return v.file.Close()
}

func (b *fileBackedBuffer) DoNotCalcCrc() {
	b.crcCalc.doNotUpdate = true
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

	readLen := minInt(b.len, b.fileLen-b.filePos) - remaining
	read, err := b.file.Read(b.buf[remaining : remaining+readLen])
	if err != nil {
		return err
	}

	if read != readLen {
		return fmt.Errorf("expected to read %d bytes, but it was %d", readLen, read)
	}

	if b.crcCalc != nil {
		b.crcCalc.Update(b.buf[remaining : remaining+readLen])
	}

	return nil
}

func (b *fileBackedBuffer) Crc() uint64 {
	return b.crcCalc.Crc()
}

func newForwardOnlyBuffer(r io.Reader) buffer {
	return &forwardOnlyBuffer{
		reader:  r,
		calcCRC: true,
		crc:     0,
	}
}

type forwardOnlyBuffer struct {
	reader  io.Reader
	calcCRC bool
	crc     uint64
}

func (f *forwardOnlyBuffer) Get(n int) ([]byte, error) {
	b := make([]byte, n)
	remaining := n

	for remaining > 0 {
		read, err := f.reader.Read(b[n-remaining:])
		if err != nil {
			return nil, err
		}

		if read == 0 {
			return nil, io.ErrUnexpectedEOF
		}

		remaining -= read
	}

	if f.calcCRC {
		f.crc = getCRC(f.crc, b)
	}

	return b, nil
}

func (f *forwardOnlyBuffer) Pos() int {
	return 0
}

func (f *forwardOnlyBuffer) DoNotCalcCrc() {
	f.calcCRC = false
}

func (f *forwardOnlyBuffer) Crc() uint64 {
	return f.crc
}

func (f *forwardOnlyBuffer) SupportsView() bool {
	return false
}

func (f *forwardOnlyBuffer) View(_ int) (bufferView, error) {
	return nil, nil
}
