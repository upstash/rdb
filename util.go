package rdb

import "unsafe"

func maxInt(x, y int) int {
	if x > y {
		return x
	}
	return y
}

func minInt(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func absInt(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

func bytesToString(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b))
}

var emptyBytes = make([]byte, 0)

func stringToBytes(s string) []byte {
	if s == "" {
		return emptyBytes
	}
	return unsafe.Slice(unsafe.StringData(s), len(s))
}
