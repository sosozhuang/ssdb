package table

import (
	"reflect"
	"ssdb"
	"ssdb/util"
	"unsafe"
)

type block struct {
	data          []byte
	size          uint
	restartOffset uint32
	owned         bool
	uint32Size    uint
}

func (b *block) getSize() uint {
	return b.size
}

func (b *block) newIterator(comparator ssdb.Comparator) ssdb.Iterator {
	if b.size < b.uint32Size {
		return NewErrorIterator(util.CorruptionError1("bad block contents"))
	}
	numRestarts := b.numRestarts()
	if numRestarts == 0 {
		return NewEmptyIterator()
	}
	return newBlockIterator(comparator, b.data, b.restartOffset, numRestarts)
}

func (b *block) numRestarts() uint32 {
	const n = uint(unsafe.Sizeof(uint32(0)))
	if b.size < n {
		panic("block: size < sizeof(uint32)")
	}
	return util.DecodeFixed32(b.data[b.size-n:])
}

func (b *block) finalize() {
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
		uint32Size:    uint(unsafe.Sizeof(uint32(0))),
	}
	if b.size < b.uint32Size {
		b.size = 0
	} else {
		maxRestartsAllowed := (b.size - b.uint32Size) / b.uint32Size
		if uint(b.numRestarts()) > maxRestartsAllowed {
			b.size = 0
		} else {
			b.restartOffset = uint32(b.size - uint(1+b.numRestarts())*b.uint32Size)
		}
	}
	return b
}

func decodeEntry(data []byte) (shared uint32, nonShared uint32, valueLength uint32, i int) {
	n := len(data)
	if n < 3 {
		i = -1
		return
	}
	shared = uint32(data[0])
	nonShared = uint32(data[1])
	valueLength = uint32(data[2])
	if shared|nonShared|valueLength < 128 {
		i = 3
	} else {
		var p int
		if p = util.GetVarInt32Ptr(data, &shared); p == -1 {
			i = -1
			return
		}
		data = data[p:]
		if p = util.GetVarInt32Ptr(data, &nonShared); p == -1 {
			i = -1
			return
		}
		data = data[p:]
		if p = util.GetVarInt32Ptr(data, &valueLength); p == -1 {
			i = -1
			return
		}
	}

	if n-i < int(nonShared+valueLength) {
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
	return util.DecodeFixed32(i.data[i.restarts+index*uint32(unsafe.Sizeof(uint32(0))):])
}

func (i *blockIterator) seekToRestartPoint(index uint32) {
	i.key = nil
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
		index        int
	)
	for left < right {
		mid = (left + right + 1) / 2
		regionOffset = i.getRestartPoint(mid)
		shared, nonShared, _, index = decodeEntry(i.data[regionOffset:i.restarts])
		if index == -1 || shared != 0 {
			i.corruptionError()
			return
		}
		if i.compare(i.data[index:index+int(nonShared)], target) < 0 {
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
	var shared, nonShared, valueLength uint32
	var index int
	shared, nonShared, valueLength, index = decodeEntry(i.data[i.current : i.current+i.restarts])
	if index == -1 || len(i.key) < int(shared) {
		i.corruptionError()
		return false
	} else {
		i.key = i.key[shared:]
		i.key = append(i.key, i.data[index:index+int(nonShared)]...)
		i.value = i.data[index+int(nonShared) : index+int(nonShared+valueLength)]
		for i.restartIndex+1 < i.numRestarts && i.getRestartPoint(i.restartIndex+1) < i.current {
			i.restartIndex++
		}
	}
	return true
}
