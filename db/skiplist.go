package db

import (
	"ssdb/util"
	"sync/atomic"
	"unsafe"
)

const (
	maxHeight = 12
)

type skipListKey interface{}
type comparator func(a, b skipListKey) int
type skipList struct {
	comparator comparator
	arena      *util.Arena
	head       *node
	maxHeight  uint32
	rnd        *util.Random
}

func (l *skipList) insert(key skipListKey) {
	prev := make([]*node, maxHeight)
	x := l.findGreaterOrEqual(key, prev)

	if !(x == nil || !l.equal(key, x.key)) {
		panic("skipList: duplicate insertion")
	}
	height := l.randomHeight()
	if height > l.getMaxHeight() {
		for i := l.getMaxHeight(); i < height; i++ {
			prev[i] = l.head
		}
		atomic.StoreUint32(&l.maxHeight, uint32(height))
	}

	x = l.newNode(key, height)
	for i := 0; i < height; i++ {
		x.noBarrierSetNext(i, prev[i].noBarrierGetNext(i))
		prev[i].setNext(i, x)
	}
}

func (l *skipList) contains(key skipListKey) bool {
	x := l.findGreaterOrEqual(key, nil)
	return x != nil && l.equal(key, x.key)
}

func (l *skipList) getMaxHeight() int {
	return int(atomic.LoadUint32(&l.maxHeight))
}

const (
	nodeSize    = unsafe.Sizeof(node{})
	pointerSize = unsafe.Sizeof(&node{})
)

func (l *skipList) newNode(key skipListKey, height int) *node {
	pointer := l.arena.AllocateAligned(uint(nodeSize) + uint(pointerSize)*uint(height))
	n := (*node)(pointer)
	n.key = key
	n.pointer = unsafe.Pointer(uintptr(pointer) + nodeSize)
	return n
}

func (l *skipList) randomHeight() int {
	const branching = 4
	height := 1
	for height < maxHeight && (l.rnd.Next()%branching) == 0 {
		height++
	}
	if height <= 0 {
		panic("height <= 0")
	}
	if height > maxHeight {
		panic("height > maxHeight")
	}
	return height
}

func (l *skipList) equal(a, b skipListKey) bool {
	return l.comparator(a, b) == 0
}

func (l *skipList) keyIsAfterNode(key skipListKey, n *node) bool {
	return n != nil && l.comparator(n.key, key) < 0
}

func (l *skipList) findGreaterOrEqual(key skipListKey, prev []*node) *node {
	x := l.head
	level := l.getMaxHeight() - 1
	var next *node
	for {
		next = x.getNext(level)
		if l.keyIsAfterNode(key, next) {
			x = next
		} else {
			if prev != nil {
				prev[level] = x
			}
			if level == 0 {
				return next
			} else {
				level--
			}
		}
	}
}

func (l *skipList) findLessThan(key skipListKey) *node {
	x := l.head
	level := l.getMaxHeight() - 1
	var next *node
	for {
		next = x.getNext(level)
		if next == nil || l.comparator(x.key, key) >= 0 {
			if level >= 0 {
				return x
			} else {
				level--
			}
		} else {
			x = next
		}
	}
}

func (l *skipList) findLast() *node {
	x := l.head
	level := l.getMaxHeight() - 1
	var next *node
	for {
		next = x.getNext(level)
		if next == nil {
			if level == 0 {
				return x
			} else {
				level--
			}
		} else {
			x = next
		}
	}
}

type node struct {
	key     skipListKey
	pointer unsafe.Pointer
}

func (n *node) getNext(i int) *node {
	if i < 0 {
		panic("node: i < 0")
	}
	p := unsafe.Pointer(uintptr(n.pointer) + uintptr(i)*pointerSize)
	return *(**node)(atomic.LoadPointer(&p))
}

func (n *node) setNext(i int, x *node) {
	if i < 0 {
		panic("node: i < 0")
	}
	p := unsafe.Pointer(uintptr(n.pointer) + uintptr(i)*pointerSize)
	atomic.StorePointer((*unsafe.Pointer)(p), unsafe.Pointer(x))
}

func (n *node) noBarrierGetNext(i int) *node {
	if i < 0 {
		panic("node: i < 0")
	}
	p := unsafe.Pointer(uintptr(n.pointer) + uintptr(i)*pointerSize)
	return *(**node)(p)
}

func (n *node) noBarrierSetNext(i int, x *node) {
	if i < 0 {
		panic("node: i < 0")
	}
	p := unsafe.Pointer(uintptr(n.pointer) + uintptr(i)*pointerSize)
	*(**node)(p) = x
}

type skipListIterator struct {
	list *skipList
	node *node
}

func (i *skipListIterator) valid() bool {
	return i.node != nil
}

func (i *skipListIterator) key() skipListKey {
	return i.node.key
}

func (i *skipListIterator) next() {
	i.node = i.node.getNext(0)
}

func (i *skipListIterator) prev() {
	i.node = i.list.findLessThan(i.node.key)
	if i.node == i.list.head {
		i.node = nil
	}
}

func (i *skipListIterator) seek(target skipListKey) {
	i.node = i.list.findGreaterOrEqual(target, nil)
}

func (i *skipListIterator) seekToFirst() {
	i.node = i.list.head.getNext(0)
}

func (i *skipListIterator) seekToLast() {
	i.node = i.list.findLast()
	if i.node == i.list.head {
		i.node = nil
	}
}

func newSkipListIterator(l *skipList) *skipListIterator {
	return &skipListIterator{
		list: l,
	}
}

func newSkipList(comparator comparator, arena *util.Arena) *skipList {
	l := &skipList{
		comparator: comparator,
		arena:      arena,
		maxHeight:  1,
		rnd:        util.NewRandom(0xdeadbeef),
	}
	l.head = l.newNode(nil, maxHeight)
	for i := 0; i < maxHeight; i++ {
		l.head.setNext(i, nil)
	}
	return l
}
