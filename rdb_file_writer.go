package rdb

import (
	"encoding/binary"
	"io"
	"os"
)

type FileWriter struct {
	file *os.File
}

func newFileWriter(path string) (*FileWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &FileWriter{file: f}, nil
}

func (fw FileWriter) WriteUint8(n uint8) error {
	_, err := fw.file.Write([]byte{byte(n)})
	return err
}

func (fw FileWriter) WriteUint16(n uint16) error {
	b := make([]byte, 2)
	binary.LittleEndian.PutUint16(b, n)
	_, err := fw.file.Write(b)
	return err
}

func (fw FileWriter) WriteUint16BE(n uint16) error {
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, n)
	_, err := fw.file.Write(b)
	return err
}

func (fw FileWriter) WriteUint32(n uint32) error {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, n)
	_, err := fw.file.Write(b)
	return err
}

func (fw FileWriter) WriteUint32BE(n uint32) error {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, n)
	_, err := fw.file.Write(b)
	return err
}

func (fw FileWriter) WriteUint64(n uint64) error {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, n)
	_, err := fw.file.Write(b)
	return err
}

func (fw FileWriter) WriteUint64BE(n uint64) error {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, n)
	_, err := fw.file.Write(b)
	return err
}

func (fw FileWriter) Write(data []byte) (int, error) {
	return fw.file.Write(data)
}

func (fw FileWriter) WriteByte(b byte) error {
	_, err := fw.file.Write([]byte{b})
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
	return fw.file.Sync()
}

func (fw FileWriter) Close() error {
	return fw.file.Close()
}

func (fw FileWriter) Pos() (int64, error) {
	return fw.file.Seek(0, io.SeekCurrent)
}

func (fw FileWriter) SeekPos(offset int64) (int64, error) {
	return fw.file.Seek(offset, io.SeekStart)
}

func (fw FileWriter) Read(offset int64, len int64, fn func(b byte)) error {
	initialPos, err := fw.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	_, err = fw.file.Seek(offset, io.SeekStart)
	if err != nil {
		return err
	}
	for len > 0 {
		buf := make([]byte, 10)
		read, err := fw.file.Read(buf)
		if err != nil {
			return err
		}
		for i := 0; i < read; i++ {
			fn(buf[i])
		}
		if read < 10 {
			break
		}
		len -= int64(read)
	}
	_, err = fw.file.Seek(initialPos, io.SeekStart)
	return err
}
