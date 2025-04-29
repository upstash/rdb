package rdb

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
)

type FileWriter struct {
	w *bufio.Writer
	f *os.File
}

func newFileWriter(path string) (*FileWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &FileWriter{w: bufio.NewWriter(f), f: f}, nil
}

func (fw FileWriter) WriteUint8(n uint8) error {
	err := fw.w.WriteByte(n)
	if err != nil {
		return err
	}
	return err
}

func (fw FileWriter) WriteUint16(n uint16) error {
	b := make([]byte, 2)
	binary.LittleEndian.PutUint16(b, n)
	_, err := fw.w.Write(b)
	return err
}

func (fw FileWriter) WriteUint16BE(n uint16) error {
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, n)
	_, err := fw.w.Write(b)
	return err
}

func (fw FileWriter) WriteUint32(n uint32) error {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, n)
	_, err := fw.w.Write(b)
	return err
}

func (fw FileWriter) WriteUint32BE(n uint32) error {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, n)
	_, err := fw.w.Write(b)
	return err
}

func (fw FileWriter) WriteUint64(n uint64) error {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, n)
	_, err := fw.w.Write(b)
	return err
}

func (fw FileWriter) WriteUint64BE(n uint64) error {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, n)
	_, err := fw.w.Write(b)
	return err
}

func (fw FileWriter) Write(data []byte) (int, error) {
	return fw.w.Write(data)
}

func (fw FileWriter) WriteByte(b byte) error {
	err := fw.w.WriteByte(b)
	return err
}

func (fw FileWriter) WriteLength(length uint64) error {
	if length <= 0x3F {
		return fw.WriteUint8(uint8(length) | len6Bit)
	}
	if length <= 0x3FFF {
		if err := fw.WriteUint8(uint8(length>>8) | len14Bit); err != nil {
			return err
		}
		return fw.WriteUint8(uint8(length & 0xFF))
	}
	if length <= 0xFFFFFFFF {
		if err := fw.WriteUint8(len32Bit); err != nil {
			return err
		}
		return fw.WriteUint32BE(uint32(length))
	}
	if err := fw.WriteUint8(len64Bit); err != nil {
		return err
	}
	return fw.WriteUint64BE(length)
}

func (fw FileWriter) WriteLengthUint64(length uint64) error {
	if err := fw.WriteUint8(len64Bit); err != nil {
		return err
	}
	return fw.WriteUint64BE(length)
}

func (fw FileWriter) Flush() error {
	return fw.w.Flush()
}

func (fw FileWriter) Close() error {
	return fw.w.Flush()
}

func (fw FileWriter) Pos() (int64, error) {
	err := fw.w.Flush()
	if err != nil {
		return 0, err
	}

	return fw.f.Seek(0, io.SeekCurrent)
}

// SeekPos only supports seeking to a previous position from the latest flush.
// Seeking beyond the underlying file does not work.
func (fw FileWriter) SeekPos(offset int64) (int64, error) {
	err := fw.w.Flush()
	if err != nil {
		return 0, err
	}
	pos, err := fw.f.Seek(offset, io.SeekStart)
	if err != nil {
		return 0, err
	}

	fw.w.Reset(fw.f)
	return pos, nil
}
