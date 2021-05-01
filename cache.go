package ssdb

import (
	"bytes"
	"ssdb/util"
	"sync"
)

type Deleter func([]byte, interface{})
type Handle interface{}
type Cache interface {
	Insert(key []byte, value interface{}, charge int, deleter Deleter) Handle
	Lookup(key []byte) Handle
	Release(handle Handle)
	Value(handle Handle) interface{}
	Erase(key []byte)
	NewId() uint64
	Prune()
	TotalCharge() int
	Finalizer
}

type lruHandle struct {
	value    interface{}
	deleter  Deleter
	nextHash *lruHandle
	next     *lruHandle
	prev     *lruHandle
	charge   int
	inCache  bool
	refs     uint32
	hash     uint32
	key      []byte
}

type handleTable struct {
	length uint32
	elems  uint32
	list   []*lruHandle
}

func (t *handleTable) lookup(key []byte, hash uint32) *lruHandle {
	return *t.findPointer(key, hash)
}

func (t *handleTable) insert(h *lruHandle) *lruHandle {
	ptr := t.findPointer(h.key, h.hash)
	old := *ptr
	if old == nil {
		h.nextHash = nil
	} else {
		h.nextHash = old.nextHash
	}
	*ptr = h
	if old == nil {
		t.elems++
		if t.elems > t.length {
			t.resize()
		}
	}
	return old
}

func (t *handleTable) remove(key []byte, hash uint32) *lruHandle {
	ptr := t.findPointer(key, hash)
	result := *ptr
	if result != nil {
		*ptr = result.nextHash
		t.elems--
	}
	return result
}

func (t *handleTable) findPointer(key []byte, hash uint32) **lruHandle {
	ptr := &t.list[hash&(t.length-1)]

	for *ptr != nil && ((*ptr).hash != hash || bytes.Compare((*ptr).key, key) != 0) {
		ptr = &(*ptr).nextHash
	}
	return ptr
}

func (t *handleTable) resize() {
	newLength := uint32(4)
	for newLength < t.elems {
		newLength *= 2
	}
	newList := make([]*lruHandle, newLength)
	count := uint32(0)
	var h *lruHandle
	var hash uint32
	var next, ptr *lruHandle
	for i := uint32(0); i < t.length; i++ {
		h = t.list[i]
		for h != nil {
			next = h.nextHash
			hash = h.hash
			ptr = newList[hash&(newLength-1)]
			h.nextHash = ptr
			newList[hash&(newLength-1)] = h
			h = next
			count++
		}
	}
	if t.elems != count {
		panic("t.elems != count")
	}
	t.list = newList
	t.length = newLength
}

func newHandleTable() *handleTable {
	ht := &handleTable{
		length: 0,
		elems:  0,
	}
	ht.resize()
	return ht
}

type lruCache struct {
	capacity int
	mutex    sync.Mutex
	usage    int
	lru      lruHandle
	inUse    lruHandle
	table    handleTable
}

func (c *lruCache) setCapacity(capacity int) {
	c.capacity = capacity
}

func (c *lruCache) totalCharge() int {
	return c.usage
}

func newLRUCache() *lruCache {
	c := &lruCache{
		capacity: 0,
		usage:    0,
		lru:      lruHandle{},
		inUse:    lruHandle{},
		table:    *newHandleTable(),
	}
	c.lru.next = &c.lru
	c.lru.prev = &c.lru
	c.inUse.next = &c.inUse
	c.inUse.prev = &c.inUse
	return c
}

func (c *lruCache) finalize() {
	if c.inUse.next != &c.inUse {
		panic("lruCache: inUse.next != &inUse")
	}
	var next *lruHandle
	for h := c.lru.next; h != &c.lru; h = next {
		next = h.next
		if !h.inCache {
			panic("lruHandle: inCache not true")
		}
		h.inCache = false
		if h.refs != 1 {
			panic("lruHandle: refs != 1")
		}
		c.unref(h)
	}
}

func (c *lruCache) ref(h *lruHandle) {
	if h.refs == 1 && h.inCache {
		c.lruRemove(h)
		c.lruAppend(&c.inUse, h)
	}
	h.refs++
}

func (c *lruCache) unref(h *lruHandle) {
	if h.refs <= 0 {
		panic("lruCache: refs <= 0")
	}
	h.refs--
	if h.refs == 0 {
		if h.inCache {
			panic("lruCache: inCache is true")
		}
		h.deleter(h.key, h.value)
	} else if h.inCache && h.refs == 1 {
		c.lruRemove(h)
		c.lruAppend(&c.lru, h)
	}
}

func (c *lruCache) lruRemove(h *lruHandle) {
	h.next.prev = h.prev
	h.prev.next = h.next
}

func (c *lruCache) lruAppend(list *lruHandle, h *lruHandle) {
	h.next = list
	h.prev = list.prev
	h.prev.next = h
	h.next.prev = h
}

func (c *lruCache) lookup(key []byte, hash uint32) Handle {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	h := c.table.lookup(key, hash)
	if h != nil {
		c.ref(h)
		return h
	}
	return nil
}

func (c *lruCache) release(h *lruHandle) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.unref(h)
}

func (c *lruCache) insert(key []byte, hash uint32, value interface{}, charge int, deleter Deleter) Handle {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	h := &lruHandle{
		value:   value,
		deleter: deleter,
		charge:  charge,
		inCache: false,
		refs:    1,
		hash:    hash,
		key:     key,
	}
	if c.capacity > 0 {
		h.refs++
		h.inCache = true
		c.lruAppend(&c.inUse, h)
		c.usage += charge
		c.finishErase(c.table.insert(h))
	} else {
		h.next = nil
	}
	var old *lruHandle
	var erased bool
	for c.usage > c.capacity && c.lru.next != &c.lru {
		old = c.lru.next
		if old.refs != 1 {
			panic("lruHandle.refs != 1")
		}
		erased = c.finishErase(c.table.remove(old.key, old.hash))
		if !erased {
			panic("erased not true")
		}
	}
	return h
}

func (c *lruCache) finishErase(h *lruHandle) bool {
	if h != nil {
		if !h.inCache {
			panic("lruHandle.inCache not true")
		}
		c.lruRemove(h)
		h.inCache = false
		c.usage = h.charge
		c.unref(h)
	}
	return h != nil
}

func (c *lruCache) erase(key []byte, hash uint32) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.finishErase(c.table.remove(key, hash))
}

func (c *lruCache) prune() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	var h *lruHandle
	var erased bool
	for c.lru.next != &c.lru {
		h = c.lru.next
		if h.refs != 1 {
			panic("lruHandle.refs != 1")
		}
		erased = c.finishErase(c.table.remove(h.key, h.hash))
		if !erased {
			panic("erased not true")
		}
	}
}

const (
	numShardBits = 4
	numShards    = 1 << numShardBits
)

type shardedLRUCache struct {
	shard   []*lruCache
	idMutex sync.Mutex
	lastID  uint64
}

func (c *shardedLRUCache) Insert(key []byte, value interface{}, charge int, deleter Deleter) Handle {
	hash := hashSlice(key)
	return c.shard[shard(hash)].insert(key, hash, value, charge, deleter)
}

func (c *shardedLRUCache) Lookup(key []byte) Handle {
	hash := hashSlice(key)
	return c.shard[shard(hash)].lookup(key, hash)
}

func (c *shardedLRUCache) Release(handle Handle) {
	h, ok := handle.(*lruHandle)
	if !ok {
		panic("Handle not a lruHandle")
	}
	c.shard[shard(h.hash)].release(h)
}

func (c *shardedLRUCache) Value(handle Handle) interface{} {
	h, ok := handle.(*lruHandle)
	if !ok {
		panic("Handle not a lruHandle")
	}
	return h.value
}

func (c *shardedLRUCache) Erase(key []byte) {
	hash := hashSlice(key)
	c.shard[shard(hash)].erase(key, hash)
}

func (c *shardedLRUCache) NewId() uint64 {
	c.idMutex.Lock()
	defer c.idMutex.Unlock()
	c.lastID++
	return c.lastID
}

func (c *shardedLRUCache) Prune() {
	for s := 0; s < numShards; s++ {
		c.shard[s].prune()
	}
}

func (c *shardedLRUCache) TotalCharge() int {
	total := 0
	for s := 0; s < numShards; s++ {
		total += c.shard[s].totalCharge()
	}
	return total
}

func (c *shardedLRUCache) Finalize() {
	for s := 0; s < numShards; s++ {
		c.shard[s].finalize()
	}
}

func hashSlice(b []byte) uint32 {
	return util.Hash(b, 0)
}

func shard(hash uint32) uint32 {
	return hash >> (32 - numShardBits)
}

func NewLRUCache(capacity int) Cache {
	c := &shardedLRUCache{
		shard:  make([]*lruCache, numShards),
		lastID: 0,
	}
	perShard := (capacity + (numShards - 1)) / numShards
	for i := range c.shard {
		c.shard[i] = newLRUCache()
		c.shard[i].setCapacity(perShard)
	}
	return c
}
