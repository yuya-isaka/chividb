package util

import (
	"encoding/binary"

	"github.com/yuya-isaka/chibidb/btree/bsearch"
	"github.com/yuya-isaka/chibidb/disk"
)

// ======================================================================

func PageIDToBytes(i disk.PageID) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(i))
	return b
}

func Uint16ToBytes(i uint16) []byte {
	b := make([]byte, 2)
	binary.LittleEndian.PutUint16(b, i)
	return b
}

func Uint64ToBytes(i uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, i)
	return b
}

// ======================================================================

func CompareByteSlice(a, b []byte) bsearch.Ordering {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] < b[i] {
			return bsearch.Less
		}
		if a[i] > b[i] {
			return bsearch.Greater
		}
	}

	// ここまで来た場合、共有されている要素は等しい
	if len(a) < len(b) {
		return bsearch.Less
	}
	if len(a) > len(b) {
		return bsearch.Greater
	}

	return bsearch.Equal
}
