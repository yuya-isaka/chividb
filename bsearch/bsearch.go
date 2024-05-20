package bsearch

import "github.com/yuya-isaka/chibidb/util"

func BinarySearch(size uint16, f func(uint16) util.Ordering) (uint16, bool) {
	left := uint16(0)
	right := size
	for left < right {
		mid := left + (right-left)/2
		cmp := f(mid)
		if cmp == util.Less {
			left = mid + 1
		} else if cmp == util.Greater {
			right = mid
		} else {
			return mid, true
		}
	}
	return left, false
}
