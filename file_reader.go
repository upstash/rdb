package rdb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"
)

const magicStr = "REDIS"
const magicLen = 5
const versionLen = 4
const headerLen = magicLen + versionLen
const crcLen = 8

// ReadFile reads the RDB file in the given path, and calls the appropriate methods
// of the handler for the objects read. It also allows partial read of the file which
// means the function will skip some parts of the file, which is not compatible
// with Upstash yet. These parts include function data, multiple databases(any database other than 0),
// and unsupported modules.
func ReadFile(name string, handler FileHandler) error {
	// An RDB file has the following form:
	// <magic><version>[<select-db>[<resize-db>]<entry>*]*[<aux>*][<module-aux>*][<function>*]<eof>[<crc>]
	// where
	// <magic> is always 82, 69, 68, 73, 83 which is the string REDIS
	// <version> is a 4 digit string in a form similar to 30, 30, 31, 31 which is the 0011 string
	// Then, there comes the optional parts of the RDB file
	// <aux> has the following form, which contains auxilary metadata about the database:
	// <opcode><aux-key><aux-val>
	// where <opcode> is 250 and <aux-key> and <aux-val> are RDB string objects.
	// <module-aux> contains auxilary information about the modules the database started with
	// and it has the same form with the RDB module2 object, prefixed by the opcode 247.
	// <function> contains function payload as a RDB string object, followed by opcode 245.
	// Then, there comes the actual data stored in the RDB file.
	// It always start with the <select-db>, which has a length-encoded integer describing the database
	// index, prefixed by opcode 254. The database index starts from 0.
	// <resize-db> is an optinal section, which has two length-encoded integers which describes
	// the size of the database and size of the database entries that has expiration information, which
	// is prefixed by opcode 251.
	// Then, there comes the actual <entry>s, which has the following form:
	// [<expire-time>[<freq>][<idle>]]<type><key><value>
	// Each entry might be prefixed by an optional <expire-time> info, which has two flavors:
	// - Opcode 253, followed by a 4 byte unsigned integer, which is the UNIX epoch of the expiration time in seconds
	// - Opcode 252, followed by an 8 byte unsigned integer, which is the UNIX epoch of the expiration time in milliseconds
	// Then, there might be extra information regarding the expiration, if the database is configured
	// with "maxmemory-policy". If it is some form of LRU, keys migh have an <idle> information,
	// which is a 1 byte unsigned integer, prefixed with the opcode 248. Else if it is some form of LFU, keys migh
	// have an <freq> information, which is a 1 byte unsigned integer, prefixed with the opcode 249.
	// The optional expiration information is followed by a <type>.
	// <type> is same with the RDB types, and describes the type of the <value>.
	// <key> is always a RDB string.
	// <value> might be any RDB object, depending on the <type>.
	// After all databases and their entries are read, we read the final opcode, which is <eof>, with the value of 255.
	// After that, there might be a 8 byte unsigned integer describing the CRC-64 of the file content.
	// The <crc> is added in RDB version 5, and after that version, it is always there. The RDB CRC calculation
	// might be disabled in the database configuration. In that case, it has the value of 0.
	file, err := os.Open(name)
	if err != nil {
		return err
	}
	defer file.Close()

	header := make([]byte, headerLen)
	n, err := file.Read(header)
	if err != nil {
		return err
	}

	if n != headerLen {
		return io.ErrUnexpectedEOF
	}

	if bytesToString(header[:magicLen]) != magicStr {
		return errors.New("wrong signature trying to load DB from file")
	}

	version, err := strconv.Atoi(bytesToString(header[magicLen:]))
	if err != nil {
		return err
	}

	if version < 1 || version > int(Version) {
		return fmt.Errorf("cannot handle RDB format version %d", version)
	}

	endsWithCRC := version >= 5

	info, err := file.Stat()
	if err != nil {
		return err
	}

	fileLen := info.Size() - headerLen
	if endsWithCRC {
		// We don't want to read CRC bytes.
		// CRC is calculated excluding the last 8 bytes of the payload.
		fileLen -= crcLen
	}

	buf := newFileBackedBuffer(file, int(fileLen), minInt(int(fileLen), 1<<20))
	if endsWithCRC {
		buf.initCRC(header)
	}

	reader := &valueReader{
		buf: buf,
	}

	handler0 := handler

	var hasExpireTime bool
	var expireTime time.Duration
	for {
		t, err := reader.ReadType()
		if err != nil {
			return err
		}

		switch t {
		case typeOpCodeEOF:
			if !endsWithCRC {
				return nil
			}

			n, err := file.Read(header)
			if err != nil {
				return err
			}

			if n != crcLen {
				return errors.New("unexpected CRC length at the end of the RDB file")
			}

			crc := binary.LittleEndian.Uint64(header[:crcLen])
			if crc == 0 {
				// crc calculation can be disabled by the redis config.
				// if it is disabled, the crc bytes are still there but
				// it is equai to 0.
				return nil
			}

			if buf.crc != crc {
				return errors.New("wrong CRC at the end of the RDB file")
			}

			return nil
		case typeOpCodeSelectDB:
			dbnum, _, err := reader.readLen()
			if err != nil {
				return err
			}

			if dbnum != 0 {
				if !handler.AllowPartialRead() {
					return errors.New("multiple databases are not supported when the partial restore is not allowed")
				}

				handler = nopHandler{}
			} else {
				handler = handler0
			}
		case typeOpCodeExpireTime:
			t, err := reader.readUint32()
			if err != nil {
				return err
			}
			hasExpireTime = true
			expireTime = time.Duration(t) * time.Second
		case typeOpCodeExpireTimeMS:
			t, err := reader.readUint64()
			if err != nil {
				return err
			}

			hasExpireTime = true
			expireTime = time.Duration(t) * time.Millisecond
		case typeOpCodeResizeDB:
			_, _, err = reader.readLen() // db size
			if err != nil {
				return err
			}

			_, _, err = reader.readLen() // expires size
			if err != nil {
				return err
			}
		case typeOpCodeAux:
			_, err = reader.ReadString() // aux key
			if err != nil {
				return err
			}

			_, err = reader.ReadString() // aux value
			if err != nil {
				return err
			}
		case typeOpCodeFreq:
			_, err = reader.readUint8() // lfu freq
			if err != nil {
				return err
			}
		case typeOpCodeIdle:
			_, _, err = reader.readLen() // lru idle
			if err != nil {
				return err
			}
		case typeOpCodeModuleAux:
			_, _, err = reader.readLen() // module id
			if err != nil {
				return err
			}

			mReader := moduleReader{
				reader: reader,
			}

			err = mReader.Skip()
			if err != nil {
				return err
			}
		case typeOpCodeFunctionPreGA:
			return errors.New("pre-release function format not supported")
		case typeOpCodeFunction2:
			if !handler.AllowPartialRead() {
				return errors.New("restoring function payload is not supported when the partial restore is not allowed")
			}

			_, err = reader.ReadString() // function payload
			if err != nil {
				return err
			}
		default:
			if t > TypeStreamListpacks3 {
				return fmt.Errorf("unknown RDB encoding type %d", t)
			}

			err = readObject(reader, handler, t, hasExpireTime, expireTime)
			if err != nil {
				return err
			}

			hasExpireTime = false
		}
	}
}

func readObject(reader *valueReader, handler FileHandler, t Type, hasExpireTime bool, expireTime time.Duration) error {
	key, err := reader.ReadString()
	if err != nil {
		return err
	}

	err = reader.readObject(key, t, handler)
	if err != nil {
		return err
	}

	if hasExpireTime {
		err = handler.HandleExpireTime(key, expireTime)
		if err != nil {
			return err
		}
	}

	return nil
}
