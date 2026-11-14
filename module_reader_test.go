package rdb

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// jsonV0Builder writes values in the legacy RedisJSON (version 0) module format.
type jsonV0Builder struct {
	t *testing.T
	w *Writer
}

func newJSONV0Builder(t *testing.T) *jsonV0Builder {
	return &jsonV0Builder{t: t, w: NewWriter()}
}

func (b *jsonV0Builder) uint(value uint64) *jsonV0Builder {
	require.NoError(b.t, b.w.writeLen(moduleOpCodeUInt))
	require.NoError(b.t, b.w.writeLen(value))
	return b
}

func (b *jsonV0Builder) str(value string) *jsonV0Builder {
	require.NoError(b.t, b.w.writeLen(moduleOpCodeString))
	require.NoError(b.t, b.w.WriteString(value))
	return b
}

func (b *jsonV0Builder) double(value float64) *jsonV0Builder {
	require.NoError(b.t, b.w.writeLen(moduleOpCodeDouble))
	require.NoError(b.t, b.w.writeUint64(math.Float64bits(value)))
	return b
}

func (b *jsonV0Builder) nullNode() *jsonV0Builder {
	return b.uint(jsonModuleV0NodeNull)
}

func (b *jsonV0Builder) strNode(value string) *jsonV0Builder {
	return b.uint(jsonModuleV0NodeString).str(value)
}

func (b *jsonV0Builder) numberNode(value float64) *jsonV0Builder {
	return b.uint(jsonModuleV0NodeNumber).double(value)
}

func (b *jsonV0Builder) intNode(value int64) *jsonV0Builder {
	return b.uint(jsonModuleV0NodeInteger).uint(uint64(value))
}

func (b *jsonV0Builder) boolNode(value bool) *jsonV0Builder {
	if value {
		return b.uint(jsonModuleV0NodeBoolean).str("1")
	}
	return b.uint(jsonModuleV0NodeBoolean).str("0")
}

func (b *jsonV0Builder) dictNode(length uint64) *jsonV0Builder {
	return b.uint(jsonModuleV0NodeDict).uint(length)
}

func (b *jsonV0Builder) key(key string) *jsonV0Builder {
	return b.uint(jsonModuleV0NodeKeyVal).str(key)
}

func (b *jsonV0Builder) arrayNode(length uint64) *jsonV0Builder {
	return b.uint(jsonModuleV0NodeArray).uint(length)
}

func (b *jsonV0Builder) read() (string, error) {
	require.NoError(b.t, b.w.writeLen(moduleOpCodeEOF))
	r := moduleReader{
		reader: &valueReader{buf: newMemoryBackedBuffer(b.w.GetBuffer())},
	}
	return r.ReadJSON(jsonModuleV0)
}

// The expected values below are what RedisJSON gives for the same legacy
// values (serde_json::to_string in redis_json/src/redisjson.rs).
func TestReadJSONV0(t *testing.T) {
	tests := map[string]struct {
		build    func(b *jsonV0Builder)
		expected string
	}{
		"top level null": {
			build:    func(b *jsonV0Builder) { b.nullNode() },
			expected: `null`,
		},
		"top level string is quoted": {
			build:    func(b *jsonV0Builder) { b.strNode(`say "hi"`) },
			expected: `"say \"hi\""`,
		},
		"top level integer": {
			build:    func(b *jsonV0Builder) { b.intNode(-42) },
			expected: `-42`,
		},
		"top level whole double stays float": {
			build:    func(b *jsonV0Builder) { b.numberNode(42) },
			expected: `42.0`,
		},
		"top level double": {
			build:    func(b *jsonV0Builder) { b.numberNode(-42.5) },
			expected: `-42.5`,
		},
		"top level bool": {
			build:    func(b *jsonV0Builder) { b.boolNode(true) },
			expected: `true`,
		},
		"keys keep RDB order": {
			build: func(b *jsonV0Builder) {
				b.dictNode(3).
					key("b").intNode(2).
					key("c").intNode(3).
					key("a").intNode(1)
			},
			expected: `{"b":2,"c":3,"a":1}`,
		},
		"nested keys keep RDB order": {
			build: func(b *jsonV0Builder) {
				b.arrayNode(1).
					dictNode(2).
					key("z").dictNode(2).
					key("y").nullNode().
					key("x").boolNode(false).
					key("a").strNode("s")
			},
			expected: `[{"z":{"y":null,"x":false},"a":"s"}]`,
		},
		"duplicate key keeps first position and last value": {
			build: func(b *jsonV0Builder) {
				b.dictNode(3).
					key("b").intNode(1).
					key("a").intNode(2).
					key("b").intNode(3)
			},
			expected: `{"b":3,"a":2}`,
		},
		"key is escaped": {
			build:    func(b *jsonV0Builder) { b.dictNode(1).key("a\"b\n").intNode(1) },
			expected: `{"a\"b\n":1}`,
		},
		"html characters are not escaped": {
			build:    func(b *jsonV0Builder) { b.dictNode(1).key("<a&b>").strNode("<a&b>") },
			expected: `{"<a&b>":"<a&b>"}`,
		},
		"nested doubles stay floats": {
			build: func(b *jsonV0Builder) {
				b.arrayNode(5).
					numberNode(1).
					numberNode(math.Copysign(0, -1)).
					numberNode(1e21).
					numberNode(1e-7).
					intNode(1)
			},
			expected: `[1.0,-0.0,1e+21,1e-7,1]`,
		},
		"empty containers": {
			build:    func(b *jsonV0Builder) { b.dictNode(2).key("a").arrayNode(0).key("d").dictNode(0) },
			expected: `{"a":[],"d":{}}`,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			b := newJSONV0Builder(t)
			test.build(b)
			actual, err := b.read()
			require.NoError(t, err)
			require.Equal(t, test.expected, actual)
		})
	}
}

// RedisJSON fails to load these values, so we fail as well.
func TestReadJSONV0_Invalid(t *testing.T) {
	tests := map[string]func(b *jsonV0Builder){
		"NaN":                    func(b *jsonV0Builder) { b.numberNode(math.NaN()) },
		"infinity":               func(b *jsonV0Builder) { b.arrayNode(1).numberNode(math.Inf(1)) },
		"invalid UTF-8 string":   func(b *jsonV0Builder) { b.strNode("\xff") },
		"invalid UTF-8 key":      func(b *jsonV0Builder) { b.dictNode(1).key("\xff").nullNode() },
		"missing key-value node": func(b *jsonV0Builder) { b.dictNode(1).strNode("a") },
		"unknown node type":      func(b *jsonV0Builder) { b.uint(0x100) },
	}

	for name, build := range tests {
		t.Run(name, func(t *testing.T) {
			b := newJSONV0Builder(t)
			build(b)
			_, err := b.read()
			require.Error(t, err)
		})
	}
}
