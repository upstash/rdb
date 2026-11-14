package rdb

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"unicode/utf8"
)

const moduleTypeNameCharSet string = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"

func constructModuleName(id uint64) string {
	id >>= 10
	name := make([]byte, 9)
	for i := len(name) - 1; i >= 0; i-- {
		name[i] = moduleTypeNameCharSet[id&0x3F]
		id >>= 6
	}

	return bytesToString(name)
}

type moduleReader struct {
	reader *valueReader
}

func (r *moduleReader) ReadJSON(version uint64) (string, error) {
	value, err := r.readJSON(version)
	if err != nil {
		return "", err
	}

	err = r.readEOF()
	if err != nil {
		return "", err
	}

	return value, nil
}

func (r *moduleReader) Skip() error {
	for {
		opcode, _, err := r.reader.readLen()
		if err != nil {
			return err
		}

		switch opcode {
		case moduleOpCodeEOF:
			return nil
		case moduleOpCodeSInt, moduleOpCodeUInt:
			_, _, err = r.reader.readLen()
		case moduleOpCodeFloat:
			err = r.reader.skip(4)
		case moduleOpCodeDouble:
			err = r.reader.skip(8)
		case moduleOpCodeString:
			_, err = r.reader.ReadString()
		default:
			err = errors.New("unexpected module opcode")
		}

		if err != nil {
			return err
		}
	}
}

func (r *moduleReader) readJSON(version uint64) (string, error) {
	switch version {
	case jsonModuleV0:
		value, err := r.readJSONV0()
		if err != nil {
			return "", err
		}

		return marshalJSONV0(value)
	// It seems the module version jumped from 0 to 2 when the module is rewritten in Rust, instead of C.
	// And, there is no release with the JSON module version 2, so we are skipping it as well.
	case jsonModuleV3:
		return r.readString()
	default:
		return "", errors.New("unexpected JSON module version")
	}
}

func (r *moduleReader) readEOF() error {
	opCode, _, err := r.reader.readLen()
	if err != nil {
		return err
	}

	if opCode != moduleOpCodeEOF {
		return errors.New("module not terminated with EOF")
	}

	return nil
}

// readJSONV0 reads a JSON value saved by the legacy RedisJSON module (the
// one written in C) as a tree of Go values. It follows the upstream loader
// in RedisJSON (redis_json/src/backward.rs): doubles stay floats, NaN and
// infinities are rejected, and strings must be valid UTF-8.
func (r *moduleReader) readJSONV0() (any, error) {
	node, err := r.readUint64()
	if err != nil {
		return nil, err
	}

	switch node {
	case jsonModuleV0NodeNull:
		return nil, nil
	case jsonModuleV0NodeString:
		return r.readUTF8String()
	case jsonModuleV0NodeNumber:
		number, err := r.readFloat64()
		if err != nil {
			return nil, err
		}

		if math.IsNaN(number) || math.IsInf(number, 0) {
			return nil, errors.New("unable to load JSON number as float")
		}

		return jsonV0Float(number), nil
	case jsonModuleV0NodeInteger:
		return r.readInt64()
	case jsonModuleV0NodeBoolean:
		value, err := r.readString()
		if err != nil {
			return nil, err
		}

		return len(value) == 1 && value[0] == '1', nil
	case jsonModuleV0NodeDict:
		length, err := r.readUint64()
		if err != nil {
			return nil, err
		}

		// Keys stay in the order they are in the RDB. For a duplicate key,
		// the first position and the last value are kept, like RedisJSON does.
		dict := make(jsonV0Object, 0)
		index := make(map[string]int)
		for i := 0; i < int(length); i++ {
			innerNode, err := r.readUint64()
			if err != nil {
				return nil, err
			}

			if innerNode != jsonModuleV0NodeKeyVal {
				return nil, errors.New("unexpected inner node type")
			}

			key, err := r.readUTF8String()
			if err != nil {
				return nil, err
			}

			value, err := r.readJSONV0()
			if err != nil {
				return nil, err
			}

			if at, ok := index[key]; ok {
				dict[at].value = value
				continue
			}

			index[key] = len(dict)
			dict = append(dict, jsonV0Member{key: key, value: value})
		}

		return dict, nil
	case jsonModuleV0NodeArray:
		length, err := r.readUint64()
		if err != nil {
			return nil, err
		}

		array := make([]any, 0)
		for i := 0; i < int(length); i++ {
			elem, err := r.readJSONV0()
			if err != nil {
				return nil, err
			}

			array = append(array, elem)
		}

		return array, nil
	default:
		return nil, errors.New("unexpected node type")
	}
}

// jsonV0Object is a JSON object read from the legacy JSON format. It is a
// list instead of a map, because RedisJSON keeps the keys in the order they
// are in the RDB.
type jsonV0Object []jsonV0Member

type jsonV0Member struct {
	key   string
	value any
}

// marshalJSONV0 converts a value read by readJSONV0 to a JSON string, the
// way RedisJSON does (serde_json::to_string): object keys keep their order
// and HTML characters are not escaped.
func marshalJSONV0(value any) (string, error) {
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)

	err := writeJSONV0(&buf, encoder, value)
	if err != nil {
		return "", err
	}

	return buf.String(), nil
}

// writeJSONV0 writes objects and arrays itself to keep the key order, and
// leaves the other values to the encoder, which writes to the same buffer.
func writeJSONV0(buf *bytes.Buffer, encoder *json.Encoder, value any) error {
	switch v := value.(type) {
	case jsonV0Object:
		buf.WriteByte('{')
		for i, member := range v {
			if i > 0 {
				buf.WriteByte(',')
			}

			err := writeJSONV0(buf, encoder, member.key)
			if err != nil {
				return err
			}

			buf.WriteByte(':')

			err = writeJSONV0(buf, encoder, member.value)
			if err != nil {
				return err
			}
		}

		buf.WriteByte('}')
		return nil
	case []any:
		buf.WriteByte('[')
		for i, elem := range v {
			if i > 0 {
				buf.WriteByte(',')
			}

			err := writeJSONV0(buf, encoder, elem)
			if err != nil {
				return err
			}
		}

		buf.WriteByte(']')
		return nil
	default:
		err := encoder.Encode(v)
		if err != nil {
			return err
		}

		// Encode always adds a newline at the end.
		buf.Truncate(buf.Len() - 1)
		return nil
	}
}

// jsonV0Float is a double read from the legacy JSON format. RedisJSON keeps
// it as a float, so a whole number is written as "42.0", not "42". Otherwise,
// RedisJSON would load it back as an integer.
type jsonV0Float float64

func (f jsonV0Float) MarshalJSON() ([]byte, error) {
	b, err := json.Marshal(float64(f))
	if err != nil {
		return nil, err
	}

	if !bytes.ContainsAny(b, ".eE") {
		b = append(b, ".0"...)
	}

	return b, nil
}

func (r *moduleReader) readInt64() (int64, error) {
	// There is an opcode for signed integers but
	// it seems Redis is using unsigned integer
	// opcode, even for this.
	value, err := r.readUint64()
	if err != nil {
		return 0, err
	}

	return int64(value), nil
}

func (r *moduleReader) readUint64() (uint64, error) {
	opCode, _, err := r.reader.readLen()
	if err != nil {
		return 0, err
	}

	if opCode != moduleOpCodeUInt {
		return 0, errors.New("unexpected opcode")
	}

	value, _, err := r.reader.readLen()
	if err != nil {
		return 0, err
	}

	return value, nil
}

func (r *moduleReader) readFloat64() (float64, error) {
	opCode, _, err := r.reader.readLen()
	if err != nil {
		return 0, err
	}

	if opCode != moduleOpCodeDouble {
		return 0, errors.New("unexpected opcode")
	}

	value, err := r.reader.readUint64()
	if err != nil {
		return 0, err
	}

	return math.Float64frombits(value), nil
}

func (r *moduleReader) readString() (string, error) {
	opCode, _, err := r.reader.readLen()
	if err != nil {
		return "", err
	}

	if opCode != moduleOpCodeString {
		return "", errors.New("unexpected opcode")
	}

	return r.reader.ReadString()
}

func (r *moduleReader) readUTF8String() (string, error) {
	value, err := r.readString()
	if err != nil {
		return "", err
	}

	if !utf8.ValidString(value) {
		return "", errors.New("JSON string is not valid UTF-8")
	}

	return value, nil
}
