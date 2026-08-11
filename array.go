package rdb

import (
	"errors"
	"math"
	"strconv"
	"strings"

	"github.com/upstash/rdb/fpconv"
)

// errEmptyArray is returned for the arrays with no elements, which Redis
// rejects both while reading and writing them.
var errEmptyArray = errors.New("array must have at least one element")

// ArrayInsertIndexNone is the insert cursor of the arrays that were never
// written to with ARINSERT. It is the same value Redis uses internally to
// mark the missing cursors, and it is not a valid element index either.
const ArrayInsertIndexNone uint64 = 1<<64 - 1

// arrayMaxIndex is the highest index an array element can have. The maximum
// uint64 value is reserved for the missing insert cursors.
const arrayMaxIndex uint64 = 1<<64 - 2

// Type tags of the array elements. They only describe how the element is
// stored in the RDB file. All array elements are logically strings, and
// Redis picks the most compact tag for each of them.
const (
	arrayTagStr      uint64 = 0
	arrayTagInt      uint64 = 1
	arrayTagFloat    uint64 = 2
	arrayTagSmallStr uint64 = 3
)

// arraySmallStrMaxLen is the maximum length of an element stored with the
// arrayTagSmallStr tag on 64 bit platforms. 32 bit Redis instances inline at
// most 3 byte long strings, which are read back correctly with this limit
// as well.
const arraySmallStrMaxLen = 7

// arrayIntMin and arrayIntMax are the bounds of the integers Redis can inline
// into an array element on 64 bit platforms, where the lowest two bits of the
// value are reserved for the type tag.
const (
	arrayIntMin int64 = -(1 << 61)
	arrayIntMax int64 = 1<<61 - 1
)

// arrayFloatTagMask is the bit mask Redis clears from the float elements to
// make room for the type tag.
const arrayFloatTagMask uint64 = 3

// encodeArrayValue returns the tag Redis would store the given array element
// value with, along with the payload to write for the integer and float tags.
//
// The tags are tried in the same order with Redis, so that the elements we
// write are identical to the ones Redis writes for the same values.
func encodeArrayValue(value string) (uint64, int64, float64) {
	if ival, ok := parseArrayInt(value); ok {
		return arrayTagInt, ival, 0
	}

	if fval, ok := parseArrayFloat(value); ok {
		return arrayTagFloat, 0, fval
	}

	if len(value) <= arraySmallStrMaxLen {
		return arrayTagSmallStr, 0, 0
	}

	return arrayTagStr, 0, 0
}

// parseArrayInt returns the value of the element, if it can be inlined into an
// array element as an integer. Redis only inlines the canonical decimal
// representations, so values like "007" or "+7" are not integers.
func parseArrayInt(value string) (int64, bool) {
	ival, err := strconv.ParseInt(value, 10, 64)
	if err != nil || strconv.FormatInt(ival, 10) != value {
		return 0, false
	}

	if ival < arrayIntMin || ival > arrayIntMax {
		return 0, false
	}

	return ival, true
}

// parseArrayFloat returns the value of the element, if it can be inlined into
// an array element as a float.
//
// Redis clears the lowest two bits of the float to make room for the type tag,
// and only inlines the element when the truncated value is formatted back into
// exactly the same string.
func parseArrayFloat(value string) (float64, bool) {
	if !isArrayFloatCandidate(value) {
		return 0, false
	}

	fval, err := strconv.ParseFloat(value, 64)
	if err != nil || math.IsNaN(fval) || math.IsInf(fval, 0) {
		return 0, false
	}

	truncated := math.Float64frombits(math.Float64bits(fval) &^ arrayFloatTagMask)
	if formatArrayFloat(truncated) != value {
		return 0, false
	}

	return truncated, true
}

// isArrayFloatCandidate reports whether the element might be inlined as a
// float. Redis only considers the values that consist of digits and exactly
// one dot, with an optional leading minus sign.
func isArrayFloatCandidate(value string) bool {
	if len(value) == 0 {
		return false
	}

	i := 0
	if value[0] == '-' {
		if len(value) == 1 {
			return false
		}

		i = 1
	}

	dotSeen := false
	for ; i < len(value); i++ {
		c := value[i]
		if c == '.' {
			if dotSeen {
				return false
			}

			dotSeen = true
		} else if c < '0' || c > '9' {
			return false
		}
	}

	return dotSeen
}

// formatArrayFloat returns the string form of a float encoded array element.
// It is the shortest representation that can be parsed back into the same
// float, with the ".0" suffix restored for the values that look like integers,
// so that the elements Redis inlines as floats are read back exactly as they
// were written.
func formatArrayFloat(value float64) string {
	s := d2string(value)
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return s
	}

	if strings.ContainsAny(s, ".eE") {
		return s
	}

	return s + ".0"
}

// d2string returns the shortest string representation of the given float,
// mirroring the d2string function of Redis.
func d2string(value float64) string {
	switch {
	case math.IsNaN(value):
		return "nan"
	case math.IsInf(value, 1):
		return "inf"
	case math.IsInf(value, -1):
		return "-inf"
	case value == 0:
		// See: http://en.wikipedia.org/wiki/Signed_zero, "Comparisons".
		if math.Signbit(value) {
			return "-0"
		}

		return "0"
	}

	// Values in a range where casting to an int64 is safe and lossless are
	// printed as integers.
	if value >= -float64(math.MaxInt64/2) && value <= float64(math.MaxInt64/2) {
		if ival := int64(value); float64(ival) == value {
			return strconv.FormatInt(ival, 10)
		}
	}

	return fpconv.Dtoa(value)
}
