package rdb

import (
	"encoding/binary"
	"errors"
	"math"
)

type Writer struct {
	buf   []byte
	pos   int
	limit int
}

func NewWriter() *Writer {
	return &Writer{
		buf:   make([]byte, 1<<10), // 1 KB
		limit: 1 << 20,             // 1 MB
	}
}

// GetBuffer returns the payload written to the writer.
func (w *Writer) GetBuffer() []byte {
	return w.buf[:w.pos]
}

// WriteChecksum writes the given RDB version and the CRC64 for the
// payload.
func (w *Writer) WriteChecksum(version uint16) error {
	err := w.writeUint16(version)
	if err != nil {
		return err
	}

	crc := getCRC(0, w.GetBuffer())
	return w.writeUint64(crc)
}

// WriteString writes the given string as the ObjectTypeString.
func (w *Writer) WriteString(str string) error {
	bytes := stringToBytes(str)
	err := w.writeLen(uint64(len(bytes)))
	if err != nil {
		return err
	}

	return w.write(bytes)
}

// WriteList writes the given list as the ObjectTypeList.
func (w *Writer) WriteList(list []string) error {
	n := len(list)
	err := w.writeLen(uint64(n))
	if err != nil {
		return err
	}

	for i := 0; i < n; i++ {
		err = w.WriteString(list[i])
		if err != nil {
			return err
		}
	}

	return nil
}

// WriteSet writes the given set as the ObjectTypeSet.
func (w *Writer) WriteSet(set []string) error {
	n := len(set)
	err := w.writeLen(uint64(n))
	if err != nil {
		return err
	}

	for i := 0; i < n; i++ {
		err = w.WriteString(set[i])
		if err != nil {
			return err
		}
	}

	return nil
}

// WriteZset writes the given sorted set as the ObjectTypeZset2.
func (w *Writer) WriteZset(elements []string, scores []float64) error {
	n := len(elements)
	if n != len(scores) {
		return errors.New("elements and scores must be of the same length")
	}

	err := w.writeLen(uint64(n))
	if err != nil {
		return err
	}

	for i := 0; i < n; i++ {
		err = w.WriteString(elements[i])
		if err != nil {
			return err
		}

		score := math.Float64bits(scores[i])
		err = w.writeUint64(score)
		if err != nil {
			return err
		}
	}

	return nil
}

// WriteHash writes the given hash as the ObjectTypeHash.
func (w *Writer) WriteHash(hash map[string]string) error {
	n := len(hash)
	err := w.writeLen(uint64(n))
	if err != nil {
		return err
	}

	for key, value := range hash {
		err = w.WriteString(key)
		if err != nil {
			return err
		}

		err = w.WriteString(value)
		if err != nil {
			return err
		}
	}

	return nil
}

// WriteJSON writes the JSON string as the ObjectTypeModule2, with the
// JSON module type with version 3.
func (w *Writer) WriteJSON(json string) error {
	mWriter := moduleWriter{
		writer: w,
	}

	return mWriter.WriteJSON(json)
}

// WriteStream writes the stream as the ObjectTypeStreamListpacks.
func (w *Writer) WriteStream(stream *Stream) error {
	writer := StreamWriter{writer: w}
	err := writer.WriteEntries(stream.Entries)
	if err != nil {
		return err
	}

	err = writer.WriteMetadata(stream.Length, stream.LastID)
	if err != nil {
		return err
	}

	return writer.WriteConsumerGroups(stream.Groups)
}

// WriteType writes the given object type.
func (w *Writer) WriteType(objType Type) error {
	return w.writeUint8(uint8(objType))
}

func (w *Writer) writeLen(length uint64) error {
	if length <= len6BitMax {
		value := uint8(length) | len6Bit
		return w.writeUint8(value)
	} else if length <= len14BitMax {
		value := uint16(length) | uint16(len14Bit)<<8
		return w.writeUint16BE(value)
	} else if length <= len32BitMax {
		err := w.writeUint8(len32Bit)
		if err != nil {
			return err
		}

		return w.writeUint32BE(uint32(length))
	} else {
		err := w.writeUint8(len64Bit)
		if err != nil {
			return err
		}

		return w.writeUint64BE(length)
	}
}

func (w *Writer) writeLenUint64(length uint64) error {
	err := w.writeUint8(len64Bit)
	if err != nil {
		return err
	}

	return w.writeUint64BE(length)

}

// Returns the size of the listpack entry
func (w *Writer) writeListpackEntry(value string, backLenBuf [5]byte) (uint32, error) {
	// we always write 32 bit long strings for simplicity
	err := w.writeUint8(listpackEnc32bitStrLen)
	if err != nil {
		return 0, err
	}

	bytes := stringToBytes(value)
	// length of <lpentry-data>
	err = w.writeUint32(uint32(len(bytes)))
	if err != nil {
		return 0, err
	}

	// <lpentry-data>
	err = w.write(bytes)
	if err != nil {
		return 0, err
	}

	// <backlen>
	backLen := 5 + len(bytes)
	var backLenLen uint32
	if backLen <= 127 {
		backLenLen = 1
		backLenBuf[0] = byte(backLen)
		err = w.write(backLenBuf[:1])
	} else if backLen < 16383 {
		backLenLen = 2
		backLenBuf[0] = byte(backLen >> 7)
		backLenBuf[1] = byte((backLen & 127) | 128)
		err = w.write(backLenBuf[:2])
	} else if backLen < 2097151 {
		backLenLen = 3
		backLenBuf[0] = byte(backLen >> 14)
		backLenBuf[1] = byte(((backLen >> 7) & 127) | 128)
		backLenBuf[2] = byte((backLen & 127) | 128)
		err = w.write(backLenBuf[:3])
	} else if backLen < 268435455 {
		backLenLen = 4
		backLenBuf[0] = byte(backLen >> 21)
		backLenBuf[1] = byte(((backLen >> 14) & 127) | 128)
		backLenBuf[2] = byte(((backLen >> 7) & 127) | 128)
		backLenBuf[3] = byte((backLen & 127) | 128)
		err = w.write(backLenBuf[:4])
	} else {
		backLenLen = 5
		backLenBuf[0] = byte(backLen >> 28)
		backLenBuf[1] = byte(((backLen >> 21) & 127) | 128)
		backLenBuf[2] = byte(((backLen >> 14) & 127) | 128)
		backLenBuf[3] = byte(((backLen >> 7) & 127) | 128)
		backLenBuf[4] = byte((backLen & 127) | 128)
		err = w.write(backLenBuf[:5])
	}

	if err != nil {
		return 0, err
	}

	// encoding + entry data len + entry data + back len
	return 1 + 4 + uint32(len(bytes)) + backLenLen, nil
}

// Returns the size of the listpack entry
func (w *Writer) writeListpackIntEntry(value int64) (uint32, error) {
	var encoding, encodingLen uint8
	if math.MinInt16 <= value && value <= math.MaxInt16 {
		encoding = listpackEncInt16
		encodingLen = 2
	} else if math.MinInt32 <= value && value <= math.MaxInt32 {
		encoding = listpackEncInt32
		encodingLen = 4
	} else {
		encoding = listpackEncInt64
		encodingLen = 8
	}

	err := w.writeUint8(encoding)
	if err != nil {
		return 0, err
	}

	switch encodingLen {
	case 2:
		err = w.writeUint16(uint16(value))
	case 4:
		err = w.writeUint32(uint32(value))
	case 8:
		err = w.writeUint64(uint64(value))
	}

	if err != nil {
		return 0, err
	}

	// +1 for the first byte, specifying the encoding
	err = w.writeUint8(1 + encodingLen)
	if err != nil {
		return 0, err
	}

	if err != nil {
		return 0, err
	}

	// encoding + encoding len + back len
	// back len is 1 because encoding is always 1 byte and encoding
	// len is at most 8, which makes back len less than 127, so 1
	// byte is enough to represent it.
	return uint32(1 + encodingLen + 1), nil
}

func (w *Writer) writeUint8(value uint8) error {
	if w.pos+1 >= len(w.buf) {
		err := w.grow(1)
		if err != nil {
			return err
		}
	}

	w.buf[w.pos] = value
	w.pos++
	return nil
}

func (w *Writer) writeUint16(value uint16) error {
	if w.pos+2 >= len(w.buf) {
		err := w.grow(2)
		if err != nil {
			return err
		}
	}

	binary.LittleEndian.PutUint16(w.buf[w.pos:w.pos+2], value)
	w.pos += 2
	return nil
}

func (w *Writer) writeUint16BE(value uint16) error {
	if w.pos+2 >= len(w.buf) {
		err := w.grow(2)
		if err != nil {
			return err
		}
	}

	binary.BigEndian.PutUint16(w.buf[w.pos:w.pos+2], value)
	w.pos += 2
	return nil
}

func (w *Writer) writeUint32(value uint32) error {
	if w.pos+4 >= len(w.buf) {
		err := w.grow(4)
		if err != nil {
			return err
		}
	}

	binary.LittleEndian.PutUint32(w.buf[w.pos:w.pos+4], value)
	w.pos += 4
	return nil
}

func (w *Writer) writeUint32BE(value uint32) error {
	if w.pos+4 >= len(w.buf) {
		err := w.grow(4)
		if err != nil {
			return err
		}
	}

	binary.BigEndian.PutUint32(w.buf[w.pos:w.pos+4], value)
	w.pos += 4
	return nil
}

func (w *Writer) writeUint64(value uint64) error {
	if w.pos+8 >= len(w.buf) {
		err := w.grow(8)
		if err != nil {
			return err
		}
	}

	binary.LittleEndian.PutUint64(w.buf[w.pos:w.pos+8], value)
	w.pos += 8
	return nil
}

func (w *Writer) writeUint64BE(value uint64) error {
	if w.pos+8 >= len(w.buf) {
		err := w.grow(8)
		if err != nil {
			return err
		}
	}

	binary.BigEndian.PutUint64(w.buf[w.pos:w.pos+8], value)
	w.pos += 8
	return nil
}

func (w *Writer) write(value []byte) error {
	n := len(value)
	if w.pos+n >= len(w.buf) {
		err := w.grow(n)
		if err != nil {
			return err
		}
	}

	copy(w.buf[w.pos:w.pos+n], value)
	w.pos += n
	return nil
}

func (w *Writer) grow(atLeast int) error {
	if w.pos+atLeast > w.limit {
		return errors.New("exceeded write buffer limit")
	}

	newLen := 2 * len(w.buf)
	if newLen-w.pos < atLeast {
		newLen = w.pos + atLeast
	}

	if newLen > w.limit {
		newLen = w.limit
	}

	newBuf := make([]byte, newLen)
	copy(newBuf, w.buf)
	w.buf = newBuf
	return nil
}
