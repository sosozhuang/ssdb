package table

import (
	"reflect"
	"ssdb"
	"ssdb/util"
	"unsafe"
)

const (
	uint32Size = uint(unsafe.Sizeof(uint32(0)))
)

type block struct {
	data          []byte
	size          uint
	restartOffset uint32
	owned         bool
}

func (b *block) getSize() uint {
	return b.size
}

func (b *block) newIterator(comparator ssdb.Comparator) ssdb.Iterator {
	if b.size < uint32Size {
		return NewErrorIterator(util.CorruptionError1("bad block contents"))
	}
	numRestarts := b.numRestarts()
	if numRestarts == 0 {
		return NewEmptyIterator()
	}
	return newBlockIterator(comparator, b.data, b.restartOffset, numRestarts)
}

func (b *block) numRestarts() uint32 {
	if b.size < uint32Size {
		panic("block: size < uint32Size")
	}
	return util.DecodeFixed32(b.data[b.size-uint32Size:])
}

func (b *block) release() {
	if b.owned {
		b.data = nil
	}
}

func newBlock(contents *blockContents) *block {
	b := &block{
		data:          contents.data,
		size:          uint(len(contents.data)),
		restartOffset: 0,
		owned:         contents.heapAllocated,
	}
	if b.size < uint32Size {
		b.size = 0
	} else {
		maxRestartsAllowed := (b.size - uint32Size) / uint32Size
		numRestarts := uint(b.numRestarts())
		if numRestarts > maxRestartsAllowed {
			b.size = 0
		} else {
			b.restartOffset = uint32(b.size - (1+numRestarts)*uint32Size)
		}
	}
	return b
}

func decodeEntry(data []byte, shared *uint32, nonShared *uint32, valueLength *uint32) (i int32) {
	n := len(data)
	if n < 3 {
		i = -1
		return
	}
	*shared = uint32(data[0])
	*nonShared = uint32(data[1])
	*valueLength = uint32(data[2])
	if *shared|*nonShared|*valueLength < 128 {
		i = 3
	} else {
		var p int
		if p = util.GetVarInt32Ptr(data, shared); p == -1 {
			i = -1
			return
		}
		i += int32(p)
		data = data[p:]
		if p = util.GetVarInt32Ptr(data, nonShared); p == -1 {
			i = -1
			return
		}
		i += int32(p)
		data = data[p:]
		if p = util.GetVarInt32Ptr(data, valueLength); p == -1 {
			i = -1
			return
		}
		i += int32(p)
	}

	if n-int(i) < int(*nonShared+*valueLength) {
		i = -1
		return
	}
	return
}

type blockIterator struct {
	CleanUpIterator
	comparator   ssdb.Comparator
	data         []byte
	restarts     uint32
	numRestarts  uint32
	current      uint32
	restartIndex uint32
	key          []byte
	value        []byte
	err          error
}

func newBlockIterator(comparator ssdb.Comparator, data []byte, restarts uint32, numRestarts uint32) *blockIterator {
	if numRestarts <= 0 {
		panic("blockIterator: numRestarts <= 0")
	}
	return &blockIterator{
		comparator:   comparator,
		data:         data,
		restarts:     restarts,
		numRestarts:  numRestarts,
		current:      restarts,
		restartIndex: numRestarts,
		//key:          make([]byte, 0),
	}
}

func (i *blockIterator) compare(a, b []byte) int {
	return i.comparator.Compare(a, b)
}

func (i *blockIterator) nextEntryOffset() uint32 {
	return uint32(uint(((*reflect.SliceHeader)(unsafe.Pointer(&i.value))).Data) + uint(len(i.value)) - uint(((*reflect.SliceHeader)(unsafe.Pointer(&i.data))).Data))
}

func (i *blockIterator) getRestartPoint(index uint32) uint32 {
	if index > i.numRestarts {
		panic("blockIterator: index > i.numRestarts")
	}
	return util.DecodeFixed32(i.data[i.restarts+index*uint32(uint32Size):])
}

func (i *blockIterator) seekToRestartPoint(index uint32) {
	i.key = make([]byte, 0)
	i.restartIndex = index
	offset := i.getRestartPoint(index)
	i.value = i.data[offset:offset]
}

func (i *blockIterator) Valid() bool {
	return i.current < i.restarts
}

func (i *blockIterator) SeekToFirst() {
	i.seekToRestartPoint(0)
	i.parseNextKey()
}

func (i *blockIterator) SeekToLast() {
	i.seekToRestartPoint(i.numRestarts - 1)
	for i.parseNextKey() && i.nextEntryOffset() < i.restarts {
	}
}

func (i *blockIterator) Seek(target []byte) {
	left, right := uint32(0), i.numRestarts-1
	var (
		mid          uint32
		regionOffset uint32
		shared       uint32
		nonShared    uint32
		valueLength  uint32
		index        int32
	)
	for left < right {
		mid = (left + right + 1) / 2
		regionOffset = i.getRestartPoint(mid)
		index = decodeEntry(i.data[regionOffset:i.restarts], &shared, &nonShared, &valueLength)
		if index == -1 || shared != 0 {
			i.corruptionError()
			return
		}
		start := regionOffset + uint32(index)
		if i.compare(i.data[start:start+nonShared], target) < 0 {
			left = mid
		} else {
			right = mid - 1
		}
	}

	i.seekToRestartPoint(left)
	for {
		if !i.parseNextKey() {
			return
		}
		if i.compare(i.key, target) >= 0 {
			return
		}
	}
}

func (i *blockIterator) Next() {
	if !i.Valid() {
		panic("blockIterator: not valid")
	}
	i.parseNextKey()
}

func (i *blockIterator) Prev() {
	if !i.Valid() {
		panic("blockIterator: not valid")
	}
	original := i.current
	for i.getRestartPoint(i.restartIndex) >= original {
		if i.restartIndex == 0 {
			i.current = i.restarts
			i.restartIndex = i.numRestarts
			return
		}
		i.restartIndex--
	}

	i.seekToRestartPoint(i.restartIndex)
	for i.parseNextKey() && i.nextEntryOffset() < original {
	}
}

func (i *blockIterator) Key() []byte {
	if !i.Valid() {
		panic("blockIterator: not valid")
	}
	return i.key
}

func (i *blockIterator) Value() []byte {
	if !i.Valid() {
		panic("blockIterator: not valid")
	}
	return i.value
}

func (i *blockIterator) Status() error {
	return i.err
}

func (i *blockIterator) corruptionError() {
	i.current = i.restarts
	i.restartIndex = i.numRestarts
	i.err = util.CorruptionError1("bad entry in block")
	i.key = nil
	i.value = nil
}

func (i *blockIterator) parseNextKey() bool {
	i.current = i.nextEntryOffset()
	if i.current >= i.restarts {
		i.current = i.restarts
		i.restartIndex = i.numRestarts
		return false
	}
	var (
		shared, nonShared, valueLength uint32
		index                          int32
	)
	index = decodeEntry(i.data[i.current:i.restarts], &shared, &nonShared, &valueLength)
	if index == -1 || len(i.key) < int(shared) {
		i.corruptionError()
		return false
	}
	i.key = i.key[:shared]
	start := i.current + uint32(index)
	stop := start + nonShared
	i.key = append(i.key, i.data[start:stop]...)
	start = stop
	stop = start + valueLength
	i.value = i.data[start:stop]
	for i.restartIndex+1 < i.numRestarts && i.getRestartPoint(i.restartIndex+1) < i.current {
		i.restartIndex++
	}
	return true
}
