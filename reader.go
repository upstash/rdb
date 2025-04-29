package rdb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"
)

// ReadValue reads the single RDB value given in the payload into the handler.
// The given key is passed into the handler methods directly.
func ReadValue(key string, payload []byte, handler ValueHandler) error {
	return readValue(key, payload, handler, 0)
}

func readValue(key string, payload []byte, handler ValueHandler, maxLz77StrLen uint64) error {
	reader := valueReader{
		buf:           newMemoryBackedBuffer(payload),
		maxLz77StrLen: maxLz77StrLen,
	}

	t, err := reader.ReadType()
	if err != nil {
		return err
	}

	return reader.readObject(key, t, handler)
}

var errZMUnexpectedEnd = errors.New("unexpected end of zipmap")
var errZLUnexpectedEnd = errors.New("unexpected end of ziplist")
var errLPUnexpectedEnd = errors.New("unexpected end of listpack")
var errTooBigLz77String = errors.New("uncompressed length of the string is too big")

// valueReader provides ways of reading different RDB objects.
// All reader methods advance the pointer by the amount of data read.
type valueReader struct {
	buf           buffer
	maxLz77StrLen uint64
}

func (r *valueReader) readObject(key string, t Type, handler ValueHandler) error {
	var err error
	var read uint64
	switch t {
	case TypeString:
		var value string
		value, err = r.ReadString()
		if err == nil {
			err = handler.HandleString(key, value)
		}
	case TypeList:
		h := handler.ListEntryHandler(key)
		read, err = r.ReadList(h)
		if err == nil {
			handler.HandleListEnding(key, read)
		}
	case TypeSet:
		h := handler.SetEntryHandler(key)
		err = r.ReadSet(h)
	case TypeZset:
		h := handler.ZsetEntryHandler(key)
		read, err = r.ReadZset(h)
		if err == nil {
			handler.HandleZsetEnding(key, read)
		}
	case TypeHash:
		h := handler.HashEntryHandler(key)
		err = r.ReadHash(h)
	case TypeZset2:
		h := handler.ZsetEntryHandler(key)
		read, err = r.ReadZset2(h)
		if err == nil {
			handler.HandleZsetEnding(key, read)
		}
	case TypeModule2:
		var value string
		var marker ModuleMarker
		value, marker, err = r.ReadModule2(handler.AllowPartialRead())
		if err == nil {
			err = handler.HandleModule(key, value, marker)
		}
	case TypeHashZipmap:
		h := handler.HashEntryHandler(key)
		err = r.ReadHashZipmap(h)
	case TypeListZiplist:
		h := handler.ListEntryHandler(key)
		read, err = r.ReadListZiplist(h)
		if err == nil {
			handler.HandleListEnding(key, read)
		}
	case TypeSetIntset:
		h := handler.SetEntryHandler(key)
		err = r.ReadSetIntset(h)
	case TypeZsetZiplist:
		h := handler.ZsetEntryHandler(key)
		read, err = r.ReadZsetZiplist(h)
		if err == nil {
			handler.HandleZsetEnding(key, read)
		}
	case TypeHashZiplist:
		h := handler.HashEntryHandler(key)
		err = r.ReadHashZiplist(h)
	case TypeListQuicklist:
		h := handler.ListEntryHandler(key)
		read, err = r.ReadListQuicklist(h)
		if err == nil {
			handler.HandleListEnding(key, read)
		}
	case TypeStreamListpacks:
		eh := handler.StreamEntryHandler(key)
		gh := handler.StreamGroupHandler(key)
		read, err = r.ReadStreamListpacks(eh, gh)
		if err == nil {
			handler.HandleStreamEnding(key, read)
		}
	case TypeHashListpack:
		h := handler.HashEntryHandler(key)
		err = r.ReadHashListpack(h)
	case TypeZsetListpack:
		h := handler.ZsetEntryHandler(key)
		read, err = r.ReadZsetListpack(h)
		if err == nil {
			handler.HandleZsetEnding(key, read)
		}
	case TypeListQuicklist2:
		h := handler.ListEntryHandler(key)
		read, err = r.ReadListQuicklist2(h)
		if err == nil {
			handler.HandleListEnding(key, read)
		}
	case TypeStreamListpacks2:
		eh := handler.StreamEntryHandler(key)
		gh := handler.StreamGroupHandler(key)
		read, err = r.ReadStreamListpacks2(eh, gh)
		if err == nil {
			handler.HandleStreamEnding(key, read)
		}
	case TypeSetListpack:
		h := handler.SetEntryHandler(key)
		err = r.ReadSetListpack(h)
	case TypeStreamListpacks3:
		eh := handler.StreamEntryHandler(key)
		gh := handler.StreamGroupHandler(key)
		read, err = r.ReadStreamListpacks3(eh, gh)
		if err == nil {
			handler.HandleStreamEnding(key, read)
		}
	case TypeHashMetadata:
		h := handler.HashWithExpEntryHandler(key)
		err = r.ReadHashMetadata(h)
	case TypeHashListpackEx:
		h := handler.HashWithExpEntryHandler(key)
		err = r.ReadHashListpackEx(h)
	default:
		err = fmt.Errorf("unknown RDB object type %d", t)
	}

	return err
}

// ReadType returns the type of the RDB object.
func (r *valueReader) ReadType() (Type, error) {
	objType, err := r.readUint8()
	if err != nil {
		return 0, err
	}

	return Type(objType), nil
}

// readLen returns the value of the length-encoded integer object
// and whether it is a specially encoded object or not.
//
// The length is encoded in variable size, which is signified by the first byte:
//
// 00xxxxxx => 6 bit unsigned length
// 01xxxxxx => 14 bit unsigned length, constructed by reading one more byte
// 10000000 => 32 bit unsigned big endian length, from the next 4 bytes
// 10000001 => 64 bit unsigned big endian length, from the next 8 bytes
// 11000000 => Special encoding, next object is an 8 bit signed integer
// 11000001 => Special encoding, next object is a 16 bit signed integer
// 11000010 => Special encoding, next object is a 32 bit signed integer
// 11000011 => Special encoding, next object is a FastLZ(LZ77) compressed string
func (r *valueReader) readLen() (uint64, bool, error) {
	b0, err := r.readUint8()
	if err != nil {
		return 0, false, err
	}

	switch b0 & 0xC0 {
	case len6Bit:
		return uint64(b0 & 0x3F), false, nil
	case len14Bit:
		b1, err := r.readUint8()
		if err != nil {
			return 0, false, err
		}

		return uint64(b0&0x3F)<<8 | uint64(b1), false, nil
	case len32Or64Bit:
		switch b0 {
		case len32Bit:
			length, err := r.readUint32BE()
			if err != nil {
				return 0, false, err
			}

			return uint64(length), false, nil
		case len64Bit:
			length, err := r.readUint64BE()
			if err != nil {
				return 0, false, err
			}

			return length, false, nil
		}
	case lenEncodedValue:
		return uint64(b0 & 0x3F), true, nil
	}

	return 0, false, errors.New("unexpected length encoding")
}

// ReadString reads the next string object.
// The string is prefixed by a length, which is described in the
// ReadLen method.
//
// When the length has no special encoding, the next length bytes are
// returned as a string.
//
// When the string has a specially encoded length, it is one of the following,
// depending on the encoding:
// - 8 bit signed integer, when the length is 0
// - 16 bit signed integer, when the length is 1
// - 32 bit signed integer, when the length is 2
// - LZ77 compressed string, when the length is 3
//
// When the string is a signed integer, next 1, 2, or 4 bytes are read and
// interpreted as the string representation of the integer read.
//
// When the string is a compressed one, it has the following structure:
// - compressed length, as length encoding described above
// - uncompressed length, as length encoding described above
// - compressed length many bytes, which is the LZ77 compressed string
func (r *valueReader) ReadString() (string, error) {
	length, encoded, err := r.readLen()
	if err != nil {
		return "", err
	}

	if encoded {
		switch length {
		case lenEncodingInt8:
			value, err := r.readUint8()
			if err != nil {
				return "", err
			}

			return strconv.Itoa(int(int8(value))), nil
		case lenEncodingInt16:
			value, err := r.readUint16()
			if err != nil {
				return "", err
			}

			return strconv.Itoa(int(int16(value))), nil
		case lenEncodingInt32:
			value, err := r.readUint32()
			if err != nil {
				return "", err
			}

			return strconv.Itoa(int(int32(value))), nil
		case lenEncodingLZF:
			compressedLen, _, err := r.readLen()
			if err != nil {
				return "", err
			}

			uncompressedLen, _, err := r.readLen()
			if err != nil {
				return "", err
			}

			if r.maxLz77StrLen > 0 && uncompressedLen > r.maxLz77StrLen {
				return "", errTooBigLz77String
			}

			compressed, err := r.read(int(compressedLen))
			if err != nil {
				return "", err
			}

			decompressed, err := decompressLZ77(compressed, int(uncompressedLen))
			if err != nil {
				return "", err
			}

			return bytesToString(decompressed), nil
		default:
			return "", errors.New("unexpected string encoding")
		}
	}

	data, err := r.read(int(length))
	if err != nil {
		return "", err
	}

	return bytesToString(data), nil
}

// ReadList reads the next list object and returns the number of elements read.
// For each list element read, the cb is called with that element.
// The list has the following form:
// <len><elem>...<elem>
// where
// <len> is a length encoded integer, and there are exactly <len> <elem>s.
// <elem> is a string
func (r *valueReader) ReadList(cb func(string) error) (uint64, error) {
	length, _, err := r.readLen()
	if err != nil {
		return 0, err
	}

	for i := 0; i < int(length); i++ {
		elem, err := r.ReadString()
		if err != nil {
			return 0, err
		}

		err = cb(elem)
		if err != nil {
			return 0, err
		}
	}

	return length, nil
}

// ReadSet reads the next set object.
// For each set element read, the cb is called with that element.
// The set has the following form:
// <len><elem>...<elem>
// where
// <len> is a length encoded integer, and there are exactly <len> <elem>s
// <elem> is a string
func (r *valueReader) ReadSet(cb func(string) error) error {
	length, _, err := r.readLen()
	if err != nil {
		return err
	}

	for i := 0; i < int(length); i++ {
		elem, err := r.ReadString()
		if err != nil {
			return err
		}

		err = cb(elem)
		if err != nil {
			return err
		}
	}

	return nil
}

// ReadZset reads the next sorted set object and returns the number of elements read.
// For each sorted set element score pair read, the cb is called with that pair.
// The sorted set has the following form:
// <len><elem><score>...<elem><score>
// where
// <len> is a length encoded integer, and there are exactly <len> <elem><score> pairs.
// <elem> is a string
// <score> is described by its first byte:
//   - If it is equal to 255, the score is negative infinity
//   - If it is equal to 254, the score is positive infinity
//   - If it is equal to 253, the score is NaN
//   - Else, this byte is interpreted as an unsigned 8 bit integer,
//     describing the length of the score. Then, length many bytes are read, which is
//     an ASCII-encoded string representation of a float64.
func (r *valueReader) ReadZset(cb func(string, float64) error) (uint64, error) {
	length, _, err := r.readLen()
	if err != nil {
		return 0, err
	}

	for i := 0; i < int(length); i++ {
		elem, err := r.ReadString()
		if err != nil {
			return 0, err
		}

		scoreLen, err := r.readUint8()
		if err != nil {
			return 0, err
		}

		var score float64
		switch scoreLen {
		case 255:
			score = math.Inf(-1)
		case 254:
			score = math.Inf(1)
		case 253:
			score = math.NaN()
		default:
			data, err := r.read(int(scoreLen))
			if err != nil {
				return 0, err
			}

			score, err = strconv.ParseFloat(bytesToString(data), 64)
			if err != nil {
				return 0, err
			}
		}

		err = cb(elem, score)
		if err != nil {
			return 0, err
		}
	}

	return length, nil
}

// ReadHash reads the next hash object.
// For each hash field value pair read, the cb is called with that pair.
// The hash has the following form:
// <len><field><value>...<field><value>
// where
// <len> is a length encoded integer, and there are exactly <len> <field><value> pairs.
// <field> is a string
// <value> is a string:
func (r *valueReader) ReadHash(cb func(string, string) error) error {
	length, _, err := r.readLen()
	if err != nil {
		return err
	}

	for i := 0; i < int(length); i++ {
		field, err := r.ReadString()
		if err != nil {
			return err
		}

		value, err := r.ReadString()
		if err != nil {
			return err
		}

		err = cb(field, value)
		if err != nil {
			return err
		}
	}

	return nil
}

// ReadZset2 reads the next sorted set object and returns the number of elements read.
// For each sorted set element score pair read, the cb is called with that pair.
// The sorted set has the following form:
// <len><elem><score>...<elem><score>
// where
// <len> is a length encoded integer, and there are exactly <len> <elem><score> pairs.
// <elem> is a string
// <score> is 8 byte long number, encoded as the IEEE 754 binary representation of a float64
func (r *valueReader) ReadZset2(cb func(string, float64) error) (uint64, error) {
	length, _, err := r.readLen()
	if err != nil {
		return 0, err
	}

	for i := 0; i < int(length); i++ {
		elem, err := r.ReadString()
		if err != nil {
			return 0, err
		}

		bits, err := r.readUint64()
		if err != nil {
			return 0, err
		}

		score := math.Float64frombits(bits)

		err = cb(elem, score)
		if err != nil {
			return 0, err
		}
	}

	return length, nil
}

// ReadModule2 reads the next module object.
// Module2s are described by a module id, which is an 64-bit integer,
// encoded with length encoding.
// A module name and version is extracted from this number in the following manner:
//   - The 64 bit number is divided into 10 parts of the following bit lengths:
//     6|6|6|6|6|6|6|6|6|10|
//   - The first 9 6 bit long numbers represent the index of the character from the
//     charset [A-Z][a-z][0-9][-_]. A string module name is constructed from these characters.
//   - The last 10 bits represents the module version.
//
// Then a matching module with that name is found, module version is passed into that module,
// and that module reads the rest of the module content. Each module should be terminated with
// the EOF marker.
func (r *valueReader) ReadModule2(skipUnsupported bool) (string, ModuleMarker, error) {
	id, _, err := r.readLen()
	if err != nil {
		return "", EmptyModuleMarker, err
	}

	version := id & 0x000000000000003FF
	mReader := moduleReader{
		reader: r,
	}

	switch id & 0xFFFFFFFFFFFFFC00 {
	case jsonModuleID:
		value, err := mReader.ReadJSON(version)
		if err != nil {
			return "", EmptyModuleMarker, err
		}

		return value, JSONModuleMarker, nil
	}

	if skipUnsupported {
		err = mReader.Skip()
		return "", EmptyModuleMarker, err
	}

	name := constructModuleName(id)
	return "", EmptyModuleMarker, errors.New("unsupported module " + name)
}

// ReadHashZipmap reads the next hash object.
// For each hash field value pair read, the cb is called with that pair.
// Zipmap is a string that represents field value pairs.
// The byte array representation of the string has the following form:
// <zmlen><len><field><len><free><value><free-bytes>...<len><field><len><free><value><free-bytes><zmend>
// where
// <zmlen> is a 1 byte number that describes the length of the zipmap.
//   - If it is less than 254, this is the length of the zipmap.
//   - If not, the zipmap has to traversed to find out the length.
//
// <len> is the length of the <field> or <value>, and it is different than
// the length encoded integer. It is either 1 or 5 bytes long.
//   - If the first byte is less than or equal to 253, it is the length of the
//     <field> or <value>.
//   - If it is equal to 254, the next 4 bytes describes the length.
//   - 255 is reserved for <zmend> and it is not a valid first byte for the <len>.
//
// <free> is the number of unused bytes after the <value>.
// These bytes should be skipped.
// <free-bytes> is the bytes that should be skipped.
// <zmend> is always 255.
func (r *valueReader) ReadHashZipmap(cb func(string, string) error) error {
	zipmap, err := r.ReadString()
	if err != nil {
		return err
	}

	reader := valueReader{
		buf:           newMemoryBackedBuffer(stringToBytes(zipmap)),
		maxLz77StrLen: r.maxLz77StrLen,
	}

	zmlen, err := reader.readUint8()
	if err != nil {
		return err
	}

	var limit int
	if zmlen < zipmapLenBig {
		limit = int(zmlen)
	} else {
		limit = math.MaxInt
	}

	for i := 0; i < limit; i++ {
		len0, err := reader.readUint8()
		if err != nil {
			return err
		}

		if len0 == zipmapEnd {
			if limit == math.MaxInt {
				return nil
			} else {
				return errZMUnexpectedEnd
			}
		}

		var fieldLen uint32
		if len0 < zipmapLenBig {
			fieldLen = uint32(len0)
		} else {
			fieldLen, err = reader.readUint32()
			if err != nil {
				return err
			}
		}

		fieldData, err := reader.read(int(fieldLen))
		if err != nil {
			return err
		}

		len0, err = reader.readUint8()
		if err != nil {
			return err
		}

		if len0 == zipmapEnd {
			return errZMUnexpectedEnd
		}

		var valueLen uint32
		if len0 < zipmapLenBig {
			valueLen = uint32(len0)
		} else {
			valueLen, err = reader.readUint32()
			if err != nil {
				return err
			}
		}

		freeLen, err := reader.readUint8()
		if err != nil {
			return err
		}

		valueData, err := reader.read(int(valueLen))
		if err != nil {
			return err
		}

		if err := reader.skip(int(freeLen)); err != nil {
			return err
		}

		err = cb(bytesToString(fieldData), bytesToString(valueData))
		if err != nil {
			return err
		}
	}

	// <zmlen> was < 254, we should read the <zmend>
	zmend, err := reader.readUint8()
	if err != nil {
		return err
	}

	if zmend != zipmapEnd {
		return errZMUnexpectedEnd
	}

	return nil
}

// ReadListZiplist reads the next list object and returns the number of elements read..
// For each list element read, the cb is called with that element.
// Ziplist is a string that represents the list entries.
// The byte array representation of the string has the following form:
// <zlbytes><zltail><zllen><zlentry>...<zlentry><zlend>
// where
// <zlbytes> is a 4 byte unsigned integer that holds the number of bytes that
// the ziplist occupies.
// <zltail> is a 4 byte unsigned integer that holds the offset to the last
// entry in the ziplist.
// <zllen> is the 2 byte unsigned integer that describes the length of the ziplist.
// If there are more than 2^16 - 2 entries in the ziplist, this value is set to
// 2^16 - 1. In that case, the whole ziplist must be traversed to find out the length.
//
// Each <zlentry> has one of the two following forms:
// - <prevlen><encoding>, where the encoding describes the entry
// - <prevlen><encoding><zlentry-data>, otherwise
//
// <prevlen> is either 1 or 5 bytes. If the first byte is less than or equal to 253,
// it is the previous entry length. If it is equal to 254, the next 4 bytes as
// unsigned integer describes the length. 255 is reserved for <zlend>, and not a
// valid first byte for the <prevlen>.
//
// <encoding> has the following form:
//   - 00xxxxxx => The <zlentry-data> is a string, and these 6 bits are the length.
//   - 01xxxxxx => The <zlentry-data> is a string, and these 6 bits plus the next
//     8 bits are the length(14 in total), in big endian byte order.
//   - 10000000 => The <zlentry-data> is a string, and the next 4 bytes are the length,
//     in big endian byte order.
//   - 11000000 => The <zlentry-data> is a 2 bytes long signed integer.
//   - 11010000 => The <zlentry-data> is a 4 bytes long signed integer.
//   - 11100000 => The <zlentry-data> is a 8 bytes long signed integer.
//   - 11110000 => The <zlentry-data> is a 3 bytes long signed integer.
//   - 11111110 => The <zlentry-data> is a 1 byte long signed integer.
//   - 1111xxxx => Where xxxx is between 0001 and 1101(both inclusive). It represents
//     the integers between 0 to 12, where the value is equal to the xxxx - 0001.
//     This value is the <zlentry-data>, no extra data will be read for <zlentry-data>.
//
// <zlend> is always 255
func (r *valueReader) ReadListZiplist(cb func(string) error) (uint64, error) {
	ziplist, err := r.ReadString()
	if err != nil {
		return 0, err
	}

	reader := valueReader{
		buf:           newMemoryBackedBuffer(stringToBytes(ziplist)),
		maxLz77StrLen: r.maxLz77StrLen,
	}

	// <zlbytes> + <zltail>
	if err := reader.skip(8); err != nil {
		return 0, err
	}

	zllen, err := reader.readUint16()
	if err != nil {
		return 0, err
	}

	var limit int
	if zllen == ziplistLenBig {
		limit = math.MaxInt
	} else {
		limit = int(zllen)
	}

	for i := 0; i < limit; i++ {
		elem, err := reader.readZiplistEntry()

		if err == errZLUnexpectedEnd && limit == math.MaxInt {
			// The ziplist size was unbounded and we read <zlend>, as expected
			return uint64(i), nil
		}

		if err != nil {
			return 0, err
		}

		err = cb(elem)
		if err != nil {
			return 0, err
		}
	}

	// <zllen> was < 65535, we should read the <zlend>
	zlend, err := reader.readUint8()
	if err != nil {
		return 0, err
	}

	if zlend != ziplistEnd {
		return 0, errZLUnexpectedEnd
	}

	return uint64(zllen), nil
}

// ReadSetIntset reads the next set object.
// For each set element read, the cb is called with that element.
// Intset is a string that represents the set elements.
// The byte array representation of the string has the following form:
// <encoding><len><elem>...<elem>
// where
// <encoding> is a 4 byte unsigned integer that is either equal to 2, 4, or 8
// that describes the length of the elements.
// <len> is a 4 byte unsigned integer that describes the length of the set.
// <elem> is either a 2, 4, or 8 bytes long signed integer.
func (r *valueReader) ReadSetIntset(cb func(string) error) error {
	intset, err := r.ReadString()
	if err != nil {
		return err
	}

	reader := valueReader{
		buf:           newMemoryBackedBuffer(stringToBytes(intset)),
		maxLz77StrLen: r.maxLz77StrLen,
	}

	encoding, err := reader.readUint32()
	if err != nil {
		return err
	}

	length, err := reader.readUint32()
	if err != nil {
		return err
	}

	for i := 0; i < int(length); i++ {
		var elem int
		switch encoding {
		case intsetEncInt16:
			elem0, err := reader.readUint16()
			if err != nil {
				return err
			}
			elem = int(int16(elem0))
		case intsetEncInt32:
			elem0, err := reader.readUint32()
			if err != nil {
				return err
			}
			elem = int(int32(elem0))
		case intsetEncInt64:
			elem0, err := reader.readUint64()
			if err != nil {
				return err
			}
			elem = int(elem0)
		default:
			return errors.New("unexpected intset encoding")
		}

		err = cb(strconv.Itoa(elem))
		if err != nil {
			return err
		}
	}

	return nil
}

// ReadZsetZiplist reads the next sorted set object and returns the number of elements read.
// For each sorted set element score pair read, the cb is called with that pair.
// It has the same structure as the ziplist. The ziplist consists of
// element score pairs, which are <zlentry> tuples stored back to back.
func (r *valueReader) ReadZsetZiplist(cb func(string, float64) error) (uint64, error) {
	ziplist, err := r.ReadString()
	if err != nil {
		return 0, err
	}

	reader := valueReader{
		buf:           newMemoryBackedBuffer(stringToBytes(ziplist)),
		maxLz77StrLen: r.maxLz77StrLen,
	}

	// <zlbytes> + <zltail>
	if err := reader.skip(8); err != nil {
		return 0, err
	}

	zllen, err := reader.readUint16()
	if err != nil {
		return 0, err
	}

	var limit int
	if zllen == ziplistLenBig {
		limit = math.MaxInt
	} else {
		limit = int(zllen)
	}

	for i := 0; i < limit; i += 2 {
		elem, err := reader.readZiplistEntry()

		if err == errZLUnexpectedEnd && limit == math.MaxInt {
			// The ziplist size was unbounded and we read <zlend>, as expected
			return uint64(i / 2), nil
		}

		if err != nil {
			return 0, err
		}

		score0, err := reader.readZiplistEntry()
		if err != nil {
			return 0, err
		}

		score, err := strconv.ParseFloat(score0, 64)
		if err != nil {
			return 0, err
		}

		err = cb(elem, score)
		if err != nil {
			return 0, err
		}
	}

	// <zllen> was < 65535, we should read the <zlend>
	zlend, err := reader.readUint8()
	if err != nil {
		return 0, err
	}

	if zlend != ziplistEnd {
		return 0, errZLUnexpectedEnd
	}

	return uint64(zllen / 2), nil
}

// ReadHashZiplist reads the next hash object.
// For each hash field value pair read, the cb is called with that pair.
// It has the same structure as the ziplist. The ziplist consists of
// field value pairs, which are <zlentry> tuples stored back to back.
func (r *valueReader) ReadHashZiplist(cb func(string, string) error) error {
	ziplist, err := r.ReadString()
	if err != nil {
		return err
	}

	reader := valueReader{
		buf:           newMemoryBackedBuffer(stringToBytes(ziplist)),
		maxLz77StrLen: r.maxLz77StrLen,
	}

	// <zlbytes> + <zltail>
	if err := reader.skip(8); err != nil {
		return err
	}

	zllen, err := reader.readUint16()
	if err != nil {
		return err
	}

	var limit int
	if zllen == ziplistLenBig {
		limit = math.MaxInt
	} else {
		limit = int(zllen)
	}

	for i := 0; i < limit; i += 2 {
		field, err := reader.readZiplistEntry()

		if err == errZLUnexpectedEnd && limit == math.MaxInt {
			// The ziplist size was unbounded and we read <zlend>, as expected
			return nil
		}

		if err != nil {
			return err
		}

		value, err := reader.readZiplistEntry()
		if err != nil {
			return err
		}

		err = cb(field, value)
		if err != nil {
			return err
		}
	}

	// <zllen> was < 65535, we should read the <zlend>
	zlend, err := reader.readUint8()
	if err != nil {
		return err
	}

	if zlend != ziplistEnd {
		return errZLUnexpectedEnd
	}

	return nil
}

// ReadListQuicklist reads the next list object and returns the number of elements read.
// For each list element read, the cb is called with that element.
// Quicklist is a sequence of ziplists and has the following form:
// <len><ziplist>...<ziplist>
// where
// <len> is the number of ziplists, as a length encoded integer.
// <ziplist> has the same structure as the ziplist defined in the ListZipList
//
// The list is the concatenation of all the elements in all the ziplists.
func (r *valueReader) ReadListQuicklist(cb func(string) error) (uint64, error) {
	length, _, err := r.readLen()
	if err != nil {
		return 0, err
	}

	var totalRead uint64
	for i := 0; i < int(length); i++ {
		read, err := r.ReadListZiplist(cb)
		if err != nil {
			return 0, err
		}
		totalRead += read
	}

	return totalRead, nil
}

// ReadHashListpack reads the next hash object.
// For each hash field value pair read, the cb is called with that pair.
// It has the same structure as the listpack. The listpack consists of
// field value pairs, which are <lpentry> tuples stored back to back.
func (r *valueReader) ReadHashListpack(cb func(string, string) error) error {
	listpack, err := r.ReadString()
	if err != nil {
		return err
	}

	reader := valueReader{
		buf:           newMemoryBackedBuffer(stringToBytes(listpack)),
		maxLz77StrLen: r.maxLz77StrLen,
	}

	// <lpbytes>
	if err := reader.skip(4); err != nil {
		return err
	}

	lplen, err := reader.readUint16()
	if err != nil {
		return err
	}

	var limit int
	if lplen == listpackLenBig {
		limit = math.MaxInt
	} else {
		limit = int(lplen)
	}

	for i := 0; i < limit; i += 2 {
		field, err := reader.readListpackEntry()

		if err == errLPUnexpectedEnd && limit == math.MaxInt {
			// The listpack size was unbounded and we read <lpend>, as expected
			return nil
		}

		if err != nil {
			return err
		}

		value, err := reader.readListpackEntry()
		if err != nil {
			return err
		}

		err = cb(field, value)
		if err != nil {
			return err
		}
	}

	// <lplen> was < 65535, we should read the <lpend>
	lpend, err := reader.readUint8()
	if err != nil {
		return err
	}

	if lpend != listpackEnd {
		return errLPUnexpectedEnd
	}

	return nil
}

// ReadZsetListpack reads the next sorted set object and returns the number of elements read.
// For each sorted set element score pair read, the cb is called with that pair.
// It has the same structure as the listpack. The listpack consists of
// element score pairs, which are <lpentry> tuples stored back to back.
func (r *valueReader) ReadZsetListpack(cb func(string, float64) error) (uint64, error) {
	listpack, err := r.ReadString()
	if err != nil {
		return 0, err
	}

	reader := valueReader{
		buf:           newMemoryBackedBuffer(stringToBytes(listpack)),
		maxLz77StrLen: r.maxLz77StrLen,
	}

	// <lpbytes>
	if err := reader.skip(4); err != nil {
		return 0, err
	}

	lplen, err := reader.readUint16()
	if err != nil {
		return 0, err
	}

	var limit int
	if lplen == listpackLenBig {
		limit = math.MaxInt
	} else {
		limit = int(lplen)
	}

	for i := 0; i < limit; i += 2 {
		elem, err := reader.readListpackEntry()

		if err == errLPUnexpectedEnd && limit == math.MaxInt {
			// The listpack size was unbounded and we read <lpend>, as expected
			return uint64(i / 2), nil
		}

		if err != nil {
			return 0, err
		}

		score0, err := reader.readListpackEntry()
		if err != nil {
			return 0, err
		}

		score, err := strconv.ParseFloat(score0, 64)
		if err != nil {
			return 0, err
		}

		err = cb(elem, score)
		if err != nil {
			return 0, err
		}
	}

	// <lplen> was < 65535, we should read the <lpend>
	lpend, err := reader.readUint8()
	if err != nil {
		return 0, err
	}

	if lpend != listpackEnd {
		return 0, errLPUnexpectedEnd
	}

	return uint64(lplen / 2), nil
}

// ReadListQuicklist2 reads the next list object and returns the number of elements read.
// For each list element read, the cb is called with that element.
// Quicklist2 is a sequence of list nodes and has the following form:
// <len><list-node>...<list-node>
// where
// <len> is the number of list nodes, as a length encoded integer.
// <list-node> has the following form
// <container-type><node-content>
// where
// <container-type> is a length encoded integer and either equals to 1
// which means the <node-content> is a plain string, or equals to 2
// which means the <node-content> is a listpack.
// The list is the concatenation of all the elements in all the list nodes.
func (r *valueReader) ReadListQuicklist2(cb func(string) error) (uint64, error) {
	length, _, err := r.readLen()
	if err != nil {
		return 0, err
	}

	var totalRead uint64
	for i := 0; i < int(length); i++ {
		container, _, err := r.readLen()
		if err != nil {
			return 0, err
		}

		data, err := r.ReadString()
		if err != nil {
			return 0, err
		}

		switch container {
		case quicklist2NodePlain:
			err = cb(data)
			if err != nil {
				return 0, err
			}
			totalRead++
		case quicklist2NodePacked:
			read, err := r.readListpack(data, cb)
			if err != nil {
				return 0, err
			}
			totalRead += read
		default:
			return 0, errors.New("unexpected quicklist2 container")
		}
	}
	return totalRead, nil
}

// ReadSetListpack reads the next set object.
// For each set element read, the cb is called with that element.
// It has the same structure as the listpack. The listpack consists of
// set elements.
func (r *valueReader) ReadSetListpack(cb func(string) error) error {
	listpack, err := r.ReadString()
	if err != nil {
		return err
	}

	_, err = r.readListpack(listpack, cb)
	return err
}

// ReadStreamListpacks reads the next stream object and returns the number of elements read.
// For each stream entry and group read, the corresponding cb is called with that entry or group.
func (r *valueReader) ReadStreamListpacks(
	entryCB func(StreamEntry) error,
	groupCB func(StreamConsumerGroup) error,
) (uint64, error) {
	return r.readStreamListpacks0(TypeStreamListpacks, entryCB, groupCB)
}

// ReadStreamListpacks2 reads the next stream object and returns the number of elements read.
// For each stream entry and group read, the corresponding cb is called with that entry or group.
func (r *valueReader) ReadStreamListpacks2(
	entryCB func(StreamEntry) error,
	groupCB func(StreamConsumerGroup) error,
) (uint64, error) {
	return r.readStreamListpacks0(TypeStreamListpacks2, entryCB, groupCB)
}

// ReadStreamListpacks3 reads the next stream object and returns the number of elements read.
// For each stream entry and group read, the corresponding cb is called with that entry or group.
// Stream has the following form:
// <entries><metadata><consumer-groups>
// where
// <entries> has the following form:
// <entry-pack-count><entry-pack>...<entry-pack>
// where
// <entry-pack-count> is the number of entry packs, which is a length encoded integer
// <entry-pack> is a pack of one or more entries, which has the delta encoded keys
// and has the following form:
// <master-entry-id><entry-lp>
// where
// <master-entry-id> is a string whose byte array representation stores two 64 bit big
// endian numbers back to back, which describes the milliseconds and sequence components
// of the stream id. The entries in the <entry-lp> has their stream ids and possibly their
// stream field name delta encoded according to this id and following field names.
// <entry-lp> is a regular listpack with the following entries:
// <count><deleted><num-fields><master-entry-field-name>...<master-entry-field-name><0><delta-encoded-entry>...<delta-encoded-entry>
// where
// <count> is how many entries are there in the listpack, which are present in the stream.
// <deleted> is how many entries that were in the stream, but now deleted. They are still
// represented in the listpack, although being deleted. Hence, the <entry-lp> consists of
// <count> + <deleted> many entries.
// <num-fields> is the number of field names the master entry consist of.
// each <master-entry-field-name> is one of the field names of the master stream entry.
// this section is ended with a listpack entry consists of the integer 0.
// there are <count> + <deleted> <delta-encoded-entry> and each has the following form,
// which are all individual listpack entries:
// <flag><millis-delta><seq-delta>[<value>...<value>|<num-fields><field-name><value>...<field-name><value>]<lp-entry-count>
// where
// <flag> is an integer for the state of the entry. If the first bit of the <flag> is set
// that means the entry is deleted. If the second bit of the <flag> is set, that means
// the <delta-encoded-entry> has the same fields with the master entry, hence we can just
// read <values> for it. (The former form represented above). If the second bit is not set,
// it menas the entry has different fields than the master entry, so we need to read each field
// name along with the value.
// Each entry ends with <lp-entry-count> which describes how many listpack entries we have for that entry,
// so that we can easily jump back to <flag> while traversing backward.
// The <metadata> sections has the following form
// <length><last-id-millis><last-id-seq>[<first-id-millis><first-id-seq><max-deleted-id-millis><max-deleted-id-seq><entries-added>]
// where
// <length> is the total number of entries in the stream, which is a length encoded integer.
// <last-id-millis> and <last-id-seq> are length encoded integers that describes the id of the last entry written into stream.
// The next portion is optinal, and only present in StreamListpacks2 and StreamListpacks3 object types.
// <first-id-millis>, <first-id-seq>, <max-deleted-id-millis>, and <max-deleted-id-seq> are
// all length encoded integers describing the stream ids of the first entry and max deleted entry.
// <entries-added> is the total number of added into the stream, which is anoter length encoded integer.
// Lastly, consumer groups section has the following form:
// <count><group>...<group>
// where
// <count> is the total number of consumer groups, which is a length encoded integer.
// each <group> has the following form:
// <name><last-id-millis><last-id-seq>[<entries-read>]<global-pel-len><pe>...<pe><consumer-count><consumer>...<consumer>
// where
// <name> is the name of the group, which is a string.
// <last-id-millis> and <last-id-seq> are length encoded integers that describe the last id delivered to this group.
// <entries-read> is optional and only present in StreamListpacks2 and StreamListpacks3 object types. It is a length
// encoded integer describing the number of entries read for this group.
// <global-pel-len> is a length encoded integer that describes how many pending entries in total for
// this group, including all the consumers.
// each <pe> represents a pending entry in this global list and has the following form:
// <pe-id-millis><pe-id-seq><delivery-time><delivery-count>
// where
// <pe-id-millis> and <pe-id-seq> are 64 bit big endian unsigned integes that describe
// the stream id of this pending entry.
// <delivery-time> is 64 bit signed integer that describes the unix time in milliseconds when
// this pending entry is delivered.
// <delivery-count> is a length encoded integer which describes the amount of times this entry is delivered.
// After writing each pending entry like this, the consumer groups section continues with the consumers.
// <consumer-count> is a length encoded integer that describes how many consumers are there in this group.
// each <consumer> has the following form:
// <name><seen-time>[<active-time>]<pel-len><consumer-pe-id>...<consumer-pe-id>
// where
// <name> is the name of the consumer, which is a string.
// <seen-time> is a 64 bit signed integer, which describes the last time in unix milliseconds the consumer
// had an interaction like xreadgroup, xclaim, or xautoclaim.
// <active-time> is optional and only present in StreamListpacks3 object types. It is a 64 bit signed
// integer describing the last time in unix milliseconds the consumer had a succesful interaction
// like reading entries with xreadgroup or xclaim that actually claimed some entries.
// <pel-len> is a length encoded integer which describes the number of pending entries this customer has.
// each <consumer-pe-id> is the id of the pending entries which has the following form:
// <pe-id-millis><pe-id-seq>
// where
// <pe-id-millis> and <pe-id-seq> are 64 bit unsigned big endian integeres that describes the
// stream id of the pending entry. The attributes like delivery time or count are in the global PEL
// encoded before this. This is just a reference to the id of a pending entry in that list.
func (r *valueReader) ReadStreamListpacks3(
	entryCB func(StreamEntry) error,
	groupCB func(StreamConsumerGroup) error,
) (uint64, error) {
	return r.readStreamListpacks0(TypeStreamListpacks3, entryCB, groupCB)
}

// ReadHashMetadata reads the next hash object with per-field TTLs.
// For each hash field value pair read, the cb is called with that pair and its TTL.
// The hash has the following form:
// <len><ttl><field><value>...<ttl><field><value>
// where
// <len> is a length encoded integer, and there are exactly <len> <ttl><field><value> triplets.
// <ttl> is a length encoded integer representing the expiration time of the field (0 means no TTL)
// <field> is a string
// <value> is a string
func (r *valueReader) ReadHashMetadata(cb func(string, string, time.Time) error) error {
	minExpirationTs, err := r.readUint64()
	if err != nil {
		return err
	}

	length, _, err := r.readLen()
	if err != nil {
		return err
	}

	for i := 0; i < int(length); i++ {
		expVal, _, err := r.readLen()
		if err != nil {
			return err
		}
		var exp time.Time
		if expVal > 0 {
			exp = time.UnixMilli(int64(minExpirationTs + expVal))
		}
		field, err := r.ReadString()
		if err != nil {
			return err
		}

		value, err := r.ReadString()
		if err != nil {
			return err
		}

		err = cb(field, value, exp)
		if err != nil {
			return err
		}
	}

	return nil
}

// ReadHashListpackEx reads the next hash object with per-field TTLs stored in a listpack.
// For each hash field value TTL triplet read, the cb is called with that triplet.
// It has the same structure as the listpack. The listpack consists of
// field-value-ttl triplets, which are <lpentry> values stored back to back.
func (r *valueReader) ReadHashListpackEx(cb func(string, string, time.Time) error) error {
	t, err := r.readUint64()
	if err != nil {
		return err
	}
	// minExpire
	// This value was serialized for future use-case of streaming the object
	// directly to FLASH (while keeping in mem its next expiration time)
	_ = time.UnixMilli(int64(t))
	listpack, err := r.ReadString()
	if err != nil {
		return err
	}

	reader := valueReader{
		buf:           newMemoryBackedBuffer(stringToBytes(listpack)),
		maxLz77StrLen: r.maxLz77StrLen,
	}

	// <lpbytes>
	if err := reader.skip(4); err != nil {
		return err
	}

	lplen, err := reader.readUint16()
	if err != nil {
		return err
	}

	var limit int
	if lplen == listpackLenBig {
		limit = math.MaxInt
	} else {
		limit = int(lplen)
	}

	// Read entries in triplets (field, value, TTL)
	for i := 0; i < limit; i += 3 {
		field, err := reader.readListpackEntry()
		if err == errLPUnexpectedEnd && limit == math.MaxInt {
			// The listpack size was unbounded and we read <lpend>, as expected
			return nil
		}
		if err != nil {
			return err
		}

		value, err := reader.readListpackEntry()
		if err != nil {
			return err
		}

		expStr, err := reader.readListpackEntry()
		if err != nil {
			return err
		}
		expVal, err := strconv.ParseInt(expStr, 10, 64)
		if err != nil {
			return err
		}

		err = cb(field, value, time.UnixMilli(expVal))
		if err != nil {
			return err
		}
	}

	// <lplen> was < 65535, we should read the <lpend>
	lpend, err := reader.readUint8()
	if err != nil {
		return err
	}

	if lpend != listpackEnd {
		return errLPUnexpectedEnd
	}

	return nil
}

// Listpack is a string that represents the list entries.
//
// The byte array representation of the string has the following form:
// <lpbytes><lplen><lpentry>...<lpentry><lpend>
// where
// <lpbytes> is a 4 byte unsigned integer that holds the number of bytes that
// the listpack occupies
// <lplen> is the 2 byte unsigned integer that describes the length of the listpack.
// If there are more than 2^16 - 2 entries in the listpack, this value is set to
// 2^16 - 1. In that case, the whole listpack must be traversed to find out the length.
//
// Each <lpentry> has one of the two following forms:
// - <encoding><backlen>, where the encoding describes the entry
// - <encoding><lpentry-data><backlen>, otherwise
//
// <encoding> has the following form:
//   - 0xxxxxxx => The <lpentry-data> is these 7bits as an unsigned integer.
//     No extra data will be read for <lpentry-data>.
//   - 10xxxxxx => The <lpentry-data> is a string, and these 6 bits are the length.
//   - 110xxxxx => The <lpentry-data> is these 5 bits plus the next 8 bits (13 in total)
//     as a signed integer, in big endian byte order.
//   - 1110xxxx => The <lpentry-data> is a string, and these 4 bits plust the next 8 bits
//     (12 in total) are the length, in big endian byte order.
//   - 11110001 => The <lpentry-data> is 2 bytes long signed integer.
//   - 11110010 => The <lpentry-data> is 3 bytes long signed integer.
//   - 11110011 => The <lpentry-data> is 4 bytes long signed integer.
//   - 11110100 => The <lpentry-data> is 8 bytes long signed integer.
//   - 11110000 => The <lpentry-data> is a string, and the next 4 bytes are the length.
//
// <backlen> is the length of the <encoding> and <lpentry-data>. It can be 1 to 5 bytes
// long, based on the following limits:
//   - If the total length is less than or equal to 127, it is 1 byte long
//   - If the total length is less than 16383, it is 2 bytes long
//   - If the total length is less than 2097151, it is 3 bytes long
//   - If the total length is less than 268435455, it is 4 bytes long
//   - Otherwise, it is 5 bytes long
//
// <lpend> is always 255
func (r *valueReader) readListpack(listpack string, cb func(string) error) (uint64, error) {
	reader := valueReader{
		buf:           newMemoryBackedBuffer(stringToBytes(listpack)),
		maxLz77StrLen: r.maxLz77StrLen,
	}

	// <lpbytes>
	if err := reader.skip(4); err != nil {
		return 0, err
	}

	lplen, err := reader.readUint16()
	if err != nil {
		return 0, err
	}

	var limit int
	if lplen == listpackLenBig {
		limit = math.MaxInt
	} else {
		limit = int(lplen)
	}

	for i := 0; i < limit; i++ {
		entry, err := reader.readListpackEntry()

		if err == errLPUnexpectedEnd && limit == math.MaxInt {
			// The listpack size was unbounded and we read <lpend>, as expected
			return uint64(i), nil
		}

		if err != nil {
			return 0, err
		}

		err = cb(entry)
		if err != nil {
			return 0, err
		}
	}

	// <lplen> was < 65535, we should read the <lpend>
	lpend, err := reader.readUint8()
	if err != nil {
		return 0, err
	}

	if lpend != listpackEnd {
		return 0, errLPUnexpectedEnd
	}

	return uint64(lplen), nil
}

func (r *valueReader) readListpackEntry() (string, error) {
	encoding, err := r.readUint8()
	if err != nil {
		return "", err
	}

	if encoding == listpackEnd {
		return "", errLPUnexpectedEnd
	}

	var entry string
	if encoding&0x80 == listpackEncUint7 {
		value := encoding & 0x7F
		entry = strconv.Itoa(int(value))
	} else if encoding&0xE0 == listpackEncInt13 {
		valueLsb, err := r.readUint8()
		if err != nil {
			return "", err
		}

		value := int16(encoding&0x1F) << 8
		value |= int16(valueLsb)
		// This is a signed integer, we need to shift right after setting the sign bit
		value = (value << 3) >> 3

		entry = strconv.Itoa(int(value))
	} else if encoding == listpackEncInt16 {
		val, err := r.readUint16()
		if err != nil {
			return "", err
		}

		entry = strconv.Itoa(int(int16(val)))
	} else if encoding == listpackEncInt24 {
		valueBytes, err := r.read(3)
		if err != nil {
			return "", nil
		}

		value := int32(valueBytes[0])
		value |= int32(valueBytes[1]) << 8
		value |= int32(valueBytes[2]) << 16
		// This is a signed integer, we need to shift right after setting the sign bit
		value = (value << 8) >> 8

		entry = strconv.Itoa(int(value))

	} else if encoding == listpackEncInt32 {
		value, err := r.readUint32()
		if err != nil {
			return "", err
		}

		entry = strconv.Itoa(int(int32(value)))
	} else if encoding == listpackEncInt64 {
		value, err := r.readUint64()
		if err != nil {
			return "", err
		}

		entry = strconv.Itoa(int(int64(value)))
	}

	if entry != "" {
		// read an integer as the entry, we should skip
		// 1 byte (because backlen is < 127) and return

		if err := r.skip(1); err != nil {
			return "", err
		}

		return entry, nil
	}

	var valueLen, backLen int
	if encoding&0xC0 == listpackEnc6bitStrLen {
		valueLen = int(encoding & 0x3F)
		backLen = 1 + valueLen
	} else if encoding&0xF0 == listpackEnc12bitStrLen {
		valueLenLsb, err := r.readUint8()
		if err != nil {
			return "", nil
		}

		valueLen = int(encoding&0x0F)<<8 | int(valueLenLsb)
		backLen = 2 + valueLen
	} else if encoding == listpackEnc32bitStrLen {
		valueLen0, err := r.readUint32()
		if err != nil {
			return "", nil
		}

		valueLen = int(valueLen0)
		backLen = 5 + valueLen
	} else {
		return "", errors.New("unexpected listpack encoding")
	}

	data, err := r.read(valueLen)
	if err != nil {
		return "", nil
	}

	var skip int
	if backLen <= 127 {
		skip = 1
	} else if backLen < 16383 {
		skip = 2
	} else if backLen < 2097151 {
		skip = 3
	} else if backLen < 268435455 {
		skip = 4
	} else {
		skip = 5
	}

	if err := r.skip(skip); err != nil {
		return "", err
	}

	return bytesToString(data), nil
}

func (r *valueReader) readZiplistEntry() (string, error) {
	prevLen0, err := r.readUint8()
	if err != nil {
		return "", err
	}

	if prevLen0 == ziplistPrevLenBig {
		err := r.skip(4)
		if err != nil {
			return "", err
		}
	} else if prevLen0 == ziplistEnd {
		return "", errZLUnexpectedEnd
	}

	encoding, err := r.readUint8()
	if err != nil {
		return "", err
	}

	length := -1
	switch encoding & 0xC0 {
	case ziplistEnc6BitStrLen:
		length = int(encoding & 0x3F)
	case ziplistEnc14BitStrLen:
		lengthLsb, err := r.readUint8()
		if err != nil {
			return "", nil
		}

		length = int(encoding&0x3F) << 8
		length = length | int(lengthLsb)
	case ziplistEnc32BitStrLen:
		length0, err := r.readUint32BE()
		if err != nil {
			return "", nil
		}
		length = int(length0)
	}

	if length != -1 {
		data, err := r.read(int(length))
		if err != nil {
			return "", nil
		}

		return bytesToString(data), nil
	}

	// encoding & 0xC0 == 3, since length is read

	switch encoding {
	case ziplistEncInt8:
		entry, err := r.readUint8()
		if err != nil {
			return "", nil
		}

		return strconv.Itoa(int(int8(entry))), nil
	case ziplistEncInt16:
		entry, err := r.readUint16()
		if err != nil {
			return "", nil
		}

		return strconv.Itoa(int(int16(entry))), nil
	case ziplistEncInt24:
		raw, err := r.read(3)
		if err != nil {
			return "", nil
		}

		val := int32(raw[0]) << 8
		val |= int32(raw[1]) << 16
		val |= int32(raw[2]) << 24
		// This is a signed integer, we need to shift right after setting the sign bit
		val >>= 8

		return strconv.Itoa(int(val)), nil
	case ziplistEncInt32:
		val, err := r.readUint32()
		if err != nil {
			return "", nil
		}

		return strconv.Itoa(int(int32(val))), nil
	case ziplistEncInt64:
		val, err := r.readUint64()
		if err != nil {
			return "", nil
		}

		return strconv.Itoa(int(int64(val))), nil
	default:
		// 1111xxxx
		// Unsigned int between 0 and 12, after extracting 1 from the last 4 bits
		return strconv.Itoa(int(encoding - 0xF1)), nil
	}
}

func (r *valueReader) readUint8() (uint8, error) {
	b, err := r.buf.Get(1)
	if err != nil {
		return 0, err
	}

	value := b[0]
	return value, nil
}

func (r *valueReader) readUint16() (uint16, error) {
	b, err := r.buf.Get(2)
	if err != nil {
		return 0, err
	}

	value := binary.LittleEndian.Uint16(b)
	return value, nil
}

func (r *valueReader) readUint32() (uint32, error) {
	b, err := r.buf.Get(4)
	if err != nil {
		return 0, err
	}

	value := binary.LittleEndian.Uint32(b)
	return value, nil
}

func (r *valueReader) readUint32BE() (uint32, error) {
	b, err := r.buf.Get(4)
	if err != nil {
		return 0, err
	}

	value := binary.BigEndian.Uint32(b)
	return value, nil
}

func (r *valueReader) readUint64() (uint64, error) {
	b, err := r.buf.Get(8)
	if err != nil {
		return 0, err
	}

	value := binary.LittleEndian.Uint64(b)
	return value, nil
}

func (r *valueReader) readUint64BE() (uint64, error) {
	b, err := r.buf.Get(8)
	if err != nil {
		return 0, err
	}

	value := binary.BigEndian.Uint64(b)
	return value, nil
}

func (r *valueReader) read(n int) ([]byte, error) {
	return r.buf.Get(n)
}

func (r *valueReader) skip(n int) error {
	_, err := r.buf.Get(n)
	return err
}
