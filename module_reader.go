package rdb

import (
	"errors"
	"math"
	"strconv"

	"github.com/ohler55/ojg/oj"
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
	json, err := r.readJSON(version)
	if err != nil {
		return "", err
	}

	err = r.readEOF()
	if err != nil {
		return "", err
	}

	return json, nil
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
		value, err := r.readJSONV0(false)
		if err != nil {
			return "", err
		}

		if v, ok := value.(string); ok {
			return v, nil
		}

		return "", errors.New("unable to read JSON module content")
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

func (r *moduleReader) readJSONV0(nested bool) (any, error) {
	// This function always returns a string when nested is false, but for the
	// sake of code reuse, I made it return any type, to be able to read nested
	// objects recursively, so that we can pass the actual values of the nested
	// keys (any key that is not at the root level) instead of their string representation
	// to oj, and it can marshall these values to the JSON string as expected.
	node, err := r.readUint64()
	if err != nil {
		return "", err
	}

	switch node {
	case jsonModuleV0NodeNull:
		if nested {
			return nil, nil
		}

		return "null", nil
	case jsonModuleV0NodeString:
		return r.readString()
	case jsonModuleV0NodeNumber:
		number, err := r.readFloat64()
		if err != nil {
			return "", err
		}

		if nested {
			return number, nil
		}

		return strconv.FormatFloat(number, 'g', -1, 64), nil
	case jsonModuleV0NodeInteger:
		integer, err := r.readInt64()
		if err != nil {
			return "", err
		}

		if nested {
			return integer, nil
		}

		return strconv.Itoa(int(integer)), nil
	case jsonModuleV0NodeBoolean:
		value, err := r.readString()
		if err != nil {
			return "", err
		}

		boolean := len(value) == 1 && value[0] == '1'

		if nested {
			return boolean, nil
		}

		if boolean {
			return "true", nil
		} else {
			return "false", nil
		}
	case jsonModuleV0NodeDict:
		length, err := r.readUint64()
		if err != nil {
			return "", err
		}

		dict := make(map[string]any)
		for i := 0; i < int(length); i++ {
			innerNode, err := r.readUint64()
			if err != nil {
				return "", err
			}

			if innerNode != jsonModuleV0NodeKeyVal {
				return "", errors.New("unexpected inner node type")
			}

			key, err := r.readString()
			if err != nil {
				return "", err
			}

			value, err := r.readJSONV0(true)
			if err != nil {
				return "", err
			}

			dict[key] = value
		}

		if nested {
			return dict, nil
		}

		return oj.JSON(dict), nil
	case jsonModuleV0NodeArray:
		length, err := r.readUint64()
		if err != nil {
			return "", err
		}

		array := make([]any, 0)
		for i := 0; i < int(length); i++ {
			elem, err := r.readJSONV0(true)
			if err != nil {
				return "", err
			}

			array = append(array, elem)
		}

		if nested {
			return array, nil
		}

		return oj.JSON(array), nil
	default:
		return "", errors.New("unexpected node type")
	}
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
