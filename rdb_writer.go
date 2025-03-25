package rdb

type RDBWriter interface {
	Write(data []byte) (int, error)

	WriteByte(b byte) error

	WriteLength(length uint64) error

	WriteLengthUint64(length uint64) error

	WriteUint8(n uint8) error

	WriteUint16(n uint16) error

	WriteUint16BE(n uint16) error

	WriteUint32(n uint32) error

	WriteUint32BE(n uint32) error

	WriteUint64(n uint64) error

	WriteUint64BE(n uint64) error

	Pos() (int64, error)

	SeekPos(offset int64) (int64, error)

	Flush() error

	Close() error

	Read(seek int64, length int64, fn func(v byte)) error
}
