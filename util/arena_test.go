package util

import (
	"testing"
	"unsafe"
)

type pair struct {
	s uint
	r unsafe.Pointer
}

func TestSimple(t *testing.T) {
	allocated := make([]pair, 0)
	arena := NewArena()
	const n = 100000
	bytes := uint(0)
	rnd := NewRandom(301)
	var s uint
	for i := 0; i < n; i++ {
		if i%(n/10) == 0 {
			s = uint(i)
		} else {
			if rnd.OneIn(4000) {
				s = uint(rnd.Uniform(6000))
			} else {
				if rnd.OneIn(10) {
					s = uint(rnd.Uniform(100))
				} else {
					s = uint(rnd.Uniform(20))
				}
			}
		}
		if s == 0 {
			s = 1
		}
		var r unsafe.Pointer
		if rnd.OneIn(10) {
			r = arena.AllocateAligned(s)
		} else {
			r = arena.Allocate(s)
		}

		var x *byte
		for b := uint(0); b < s; b++ {
			x = (*byte)(unsafe.Pointer(uintptr(r) + uintptr(b)))
			*x = byte(i % 256)
		}
		bytes += s
		allocated = append(allocated, pair{s, r})
		AssertTrue(arena.MemoryUsage() >= uint64(bytes), "MemoryUsage < bytes", t)
		if i > n/10 {
			AssertTrue(arena.MemoryUsage() <= uint64(float64(bytes)*1.10), "MemoryUsage > bytes * 1.10", t)
		}
	}

	var x byte
	for i := 0; i < len(allocated); i++ {
		for b := uint(0); b < allocated[i].s; b++ {
			x = *(*byte)(unsafe.Pointer(uintptr(allocated[i].r) + uintptr(b)))
			AssertTrue(int(x)&0xff == i%256, "x&0xff == i%256", t)
		}
	}
}
