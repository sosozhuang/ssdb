package util

import (
	"sync/atomic"
	"unsafe"
)

const (
	arenaBlockSize = 4096
)

var align uint

func init() {
	a := uint(unsafe.Sizeof(interface{}(nil)))
	if a > 8 {
		align = a
	} else {
		align = 8
	}
	if align&(align-1) != 0 {
		panic("pointer size should be a power of 2")
	}
}

type Arena struct {
	allocPtr            unsafe.Pointer
	allocBytesRemaining uint
	blocks              [][]byte
	memoryUsage         uint64
}

func (a *Arena) Allocate(bytes uint) unsafe.Pointer {
	if bytes <= 0 {
		panic("bytes <= 0")
	}
	if bytes <= a.allocBytesRemaining {
		result := a.allocPtr
		a.allocPtr = unsafe.Pointer(uintptr(a.allocPtr) + uintptr(bytes))
		a.allocBytesRemaining -= bytes
		return result
	}
	return a.allocateFallback(bytes)
}

func (a *Arena) AllocateAligned(bytes uint) unsafe.Pointer {
	currentMod := uint(uintptr(a.allocPtr)) & (align - 1)
	var slop uint
	if currentMod == 0 {
		slop = 0
	} else {
		slop = align - currentMod
	}
	needed := bytes + slop
	var result unsafe.Pointer
	if needed <= a.allocBytesRemaining {
		result = unsafe.Pointer(uintptr(a.allocPtr) + uintptr(slop))
		a.allocPtr = unsafe.Pointer(uintptr(a.allocPtr) + uintptr(needed))
		a.allocBytesRemaining -= needed
	} else {
		result = a.allocateFallback(bytes)
	}
	if uint(uintptr(result))&(align-1) != 0 {
		panic("result & (align - 1) != 0")
	}
	return result
}

func (a *Arena) allocateFallback(bytes uint) unsafe.Pointer {
	if bytes > arenaBlockSize/4 {
		return a.allocateNewBlock(bytes)
	}
	a.allocPtr = a.allocateNewBlock(arenaBlockSize)
	a.allocBytesRemaining = arenaBlockSize

	result := a.allocPtr
	a.allocPtr = unsafe.Pointer(uintptr(a.allocPtr) + uintptr(bytes))
	a.allocBytesRemaining -= bytes
	return result
}

func (a *Arena) allocateNewBlock(blockBytes uint) (result unsafe.Pointer) {
	buf := make([]byte, blockBytes)
	result = unsafe.Pointer(&buf[0])
	a.blocks = append(a.blocks, buf)
	atomic.AddUint64(&a.memoryUsage, uint64(blockBytes))
	return
}

func (a *Arena) MemoryUsage() uint64 {
	return atomic.LoadUint64(&a.memoryUsage)
}

func NewArena() *Arena {
	return &Arena{
		allocPtr:            nil,
		allocBytesRemaining: 0,
		blocks:              make([][]byte, 0, 512),
		memoryUsage:         0,
	}
}
