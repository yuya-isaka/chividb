package bsearch

import (
	"testing"
)

func TestBinarySearch(t *testing.T) {
	a := []uint16{1, 2, 3, 5, 8, 13, 21, 34, 55, 89}
	tests := []struct {
		target uint16
		expect uint16
		ok     bool
	}{
		{1, 0, true},
		{0, 0, false},
		{2, 1, true},
		{8, 4, true},
		{6, 4, false},
		{21, 6, true},
		{22, 7, false},
		{34, 7, true},
		{55, 8, true},
		{89, 9, true},
		{90, 10, false},
	}

	for _, test := range tests {
		index, ok := BinarySearch(uint16(len(a)), func(i uint16) Ordering {
			if a[i] == test.target {
				return Equal
			} else if a[i] < test.target {
				return Less
			} else {
				return Greater
			}
		})

		if test.ok {
			if !ok {
				t.Errorf("Expected an error for target %d, but got no error", test.target)
			}
		} else {
			if ok {
				t.Errorf("Expected no error for target %d, but got an error", test.target)
			}
		}

		if index != test.expect {
			t.Errorf("Expected index %d for target %d, but got %d", test.expect, test.target, index)
		}
	}
}
