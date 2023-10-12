package bsearch

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

func BinarySearch(size uint16, f func(uint16) Ordering) (uint16, bool) {
	left := uint16(0)
	right := size
	for left < right {
		mid := left + (right-left)/2
		cmp := f(mid)
		if cmp == Less {
			left = mid + 1
		} else if cmp == Greater {
			right = mid
		} else {
			return mid, true
		}
	}
	return left, false
}
