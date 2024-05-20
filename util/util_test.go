package util

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/yuya-isaka/chibidb/disk"
)

func TestPageIDTo8BytesAndBytesToPageID(t *testing.T) {
	tests := []struct {
		name   string
		pageID disk.PageID
	}{
		{"Zero", 0},
		{"Positive", 123456},
		{"Negative", -123456},
		{"MaxInt64", disk.PageID(math.MaxInt64)},
		{"MinInt64", disk.PageID(math.MinInt64)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bytes := PageIDTo8Bytes(test.pageID)
			result := BytesToPageID(bytes)
			assert.Equal(t, test.pageID, result)
		})
	}
}

func TestUint16To2Bytes(t *testing.T) {
	tests := []struct {
		name  string
		value uint16
	}{
		{"Zero", 0},
		{"Standard", 255},
		{"MaxUint16", ^uint16(0)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bytes := Uint16To2Bytes(test.value)
			result := binary.LittleEndian.Uint16(bytes)
			assert.Equal(t, test.value, result)
		})
	}
}

func TestUint64To8Bytes(t *testing.T) {
	tests := []struct {
		name  string
		value uint64
	}{
		{"Zero", 0},
		{"Standard", 1234567890},
		{"MaxUint64", ^uint64(0)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bytes := Uint64To8Bytes(test.value)
			result := binary.LittleEndian.Uint64(bytes)
			assert.Equal(t, test.value, result)
		})
	}
}

func TestCompareByteSlice(t *testing.T) {
	tests := []struct {
		name     string
		a, b     []byte
		expected Ordering
	}{
		{"Equal", []byte("abc"), []byte("abc"), Equal},
		{"ALess", []byte("abc"), []byte("abcd"), Less},
		{"BGreater", []byte("abcd"), []byte("abc"), Greater},
		{"NonEqualLess", []byte("abc"), []byte("abd"), Less},
		{"NonEqualGreater", []byte("abd"), []byte("abc"), Greater},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := CompareByteSlice(test.a, test.b)
			assert.Equal(t, test.expected, result)
		})
	}
}
