package util

import (
	"encoding/binary"

	"github.com/yuya-isaka/chibidb/disk"
)

func PageIDTo8Bytes(i disk.PageID) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(i))
	return b
}

func BytesToPageID(b []byte) disk.PageID {
	return disk.PageID(binary.LittleEndian.Uint64(b))
}

func Uint16To2Bytes(i uint16) []byte {
	b := make([]byte, 2)
	binary.LittleEndian.PutUint16(b, i)
	return b
}

func Uint64To8Bytes(i uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, i)
	return b
}

// ===================================================

type Ordering interface {
	orderProtexted()
}

type order int

func (o order) orderProtexted() {}

const (
	Less    order = -1
	Equal   order = 0
	Greater order = 1
)

func CompareByteSlice(a, b []byte) Ordering {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] < b[i] {
			return Less
		}
		if a[i] > b[i] {
			return Greater
		}
	}

	// ここまで来た場合、共有されている要素は等しい
	if len(a) < len(b) {
		return Less
	}
	if len(a) > len(b) {
		return Greater
	}

	return Equal
}
