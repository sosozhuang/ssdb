package ssdb

import (
	"runtime"
	"ssdb/util"
	"testing"
)

func encodeCacheKey(k int) []byte {
	var result [4]byte
	util.EncodeFixed32(&result, uint32(k))
	return result[:]
}

func decodeCacheKey(k []byte) int {
	if len(k) != 4 {
		panic("slice length != 4")
	}
	return int(util.DecodeFixed32(k))
}

func encodeValue(v int) interface{} {
	return v
}

func decodeValue(v interface{}) int {
	i, _ := v.(int)
	return i
}

const cacheSize = 1000

var current = &cacheTest{
	deletedKeys:   make([]int, 0),
	deletedValues: make([]int, 0),
	cache:         NewLRUCache(cacheSize),
}

type cacheTest struct {
	deletedKeys   []int
	deletedValues []int
	cache         Cache
}

func (t *cacheTest) lookup(key int) int {
	handle := t.cache.Lookup(encodeCacheKey(key))
	var r int
	if handle == nil {
		r = -1
	} else {
		r = decodeValue(t.cache.Value(handle))
	}
	if handle != nil {
		t.cache.Release(handle)
	}
	return r
}

func (t *cacheTest) insert1(key, value int) {
	t.insert2(key, value, 1)
}

func (t *cacheTest) insert2(key, value, charge int) {
	t.cache.Release(t.cache.Insert(encodeCacheKey(key), encodeValue(value), charge, deleter))
}

func (t *cacheTest) insertAndReturnHandle1(key, value int) Handle {
	return t.insertAndReturnHandle2(key, value, 1)
}

func (t *cacheTest) insertAndReturnHandle2(key, value, charge int) Handle {
	return t.cache.Insert(encodeCacheKey(key), encodeValue(value), charge, deleter)
}

func (t *cacheTest) erase(key int) {
	t.cache.Erase(encodeCacheKey(key))
}

func newCacheTest() {
	if current == nil {
		current = &cacheTest{
			deletedKeys:   make([]int, 0),
			deletedValues: make([]int, 0),
			cache:         NewLRUCache(cacheSize),
		}
	}
}

func deleter(key []byte, value interface{}) {
	current.deletedKeys = append(current.deletedKeys, decodeCacheKey(key))
	current.deletedValues = append(current.deletedValues, decodeValue(value))
}

func TestHitAndMiss(t *testing.T) {
	if -1 != current.lookup(100) {
		t.Error("100")
	}

	current.insert1(100, 101)
	if 101 != current.lookup(100) {
		t.Error("100")
	}
	if -1 != current.lookup(200) {
		t.Error("200")
	}
	if -1 != current.lookup(300) {
		t.Error("300")
	}

	current.insert1(200, 201)
	if 101 != current.lookup(100) {
		t.Error("100")
	}
	if 201 != current.lookup(200) {
		t.Error("200")
	}
	if -1 != current.lookup(300) {
		t.Error("300")
	}

	current.insert1(100, 102)
	if 102 != current.lookup(100) {
		t.Error("100")
	}
	if 201 != current.lookup(200) {
		t.Error("200")
	}
	if -1 != current.lookup(300) {
		t.Error("300")
	}

	if 1 != len(current.deletedKeys) {
		t.Error("1")
	}
	if 100 != current.deletedKeys[0] {
		t.Error("100")
	}
	if 101 != current.deletedValues[0] {
		t.Error("101")
	}
}

func TestErase(t *testing.T) {
	current.erase(200)
	if 0 != len(current.deletedKeys) {
		t.Error("0")
	}

	current.insert1(100, 101)
	current.insert1(200, 201)
	current.erase(100)
	if -1 != current.lookup(100) {
		t.Error("-1")
	}
	if 201 != current.lookup(200) {
		t.Error("201")
	}
	if 1 != len(current.deletedKeys) {
		t.Error("1")
	}
	if 100 != current.deletedKeys[0] {
		t.Error("100")
	}
	if 101 != current.deletedValues[0] {
		t.Error("101")
	}

	current.erase(100)
	if -1 != current.lookup(100) {
		t.Error("-1")
	}
	if 201 != current.lookup(200) {
		t.Error("201")
	}
	if 1 != len(current.deletedKeys) {
		t.Error("1")
	}
}

func TestEntriesArePinned(t *testing.T) {
	current.insert1(100, 101)
	h1 := current.cache.Lookup(encodeCacheKey(100))
	if 101 != decodeValue(current.cache.Value(h1)) {
		t.Error("101")
	}

	current.insert1(100, 102)
	h2 := current.cache.Lookup(encodeCacheKey(100))
	if 102 != decodeValue(current.cache.Value(h2)) {
		t.Error("201")
	}
	if 0 != len(current.deletedKeys) {
		t.Error("0")
	}

	current.cache.Release(h1)
	if 1 != len(current.deletedKeys) {
		t.Error("1")
	}
	if 100 != current.deletedKeys[0] {
		t.Error("100")
	}
	if 101 != current.deletedValues[0] {
		t.Error("101")
	}

	current.erase(100)
	if -1 != current.lookup(100) {
		t.Error("-1")
	}
	if 1 != len(current.deletedKeys) {
		t.Error("1")
	}

	current.cache.Release(h2)
	if 2 != len(current.deletedKeys) {
		t.Error("1")
	}
	if 100 != current.deletedKeys[1] {
		t.Error("100")
	}
	if 102 != current.deletedValues[1] {
		t.Error("102")
	}
}

func TestEvictionPolicy(t *testing.T) {
	current.insert1(100, 101)
	current.insert1(200, 201)
	current.insert1(300, 301)
	h := current.cache.Lookup(encodeCacheKey(300))

	for i := 0; i < cacheSize+100; i++ {
		current.insert1(1000+i, 2000+i)
		if 2000+i != current.lookup(1000+i) {
			t.Errorf("2000\n")
		}
		if 101 != current.lookup(100) {
			t.Errorf("101\n")
		}
	}

	if 101 != current.lookup(100) {
		t.Errorf("101")
	}
	if -1 != current.lookup(200) {
		t.Errorf("-1")
	}
	if 301 != current.lookup(300) {
		t.Errorf("301")
	}
	current.cache.Release(h)
}

func TestUseExceedsCacheSize(t *testing.T) {
	h := make([]Handle, cacheSize+100)
	for i := range h {
		h[i] = current.insertAndReturnHandle1(1000+i, 2000+i)
	}

	for i := range h {
		if 2000+i != current.lookup(1000+i) {
			t.Errorf("%d, %d\n", 2000+i, 1000+i)
		}
	}

	for i := range h {
		current.cache.Release(h[i])
	}
}

func TestHeavyEntries(t *testing.T) {
	const light, heavy = 1, 10
	added, index := 0, 0
	var weight int
	for added < 2*cacheSize {
		if (index & 1) != 0 {
			weight = light
		} else {
			weight = heavy
		}
		current.insert2(index, 1000+index, weight)
		added += weight
		index++
	}
	var r int
	cachedWeight := 0
	for i := 0; i < index; i++ {
		if (i & 1) != 0 {
			weight = light
		} else {
			weight = heavy
		}
		r = current.lookup(i)
		if r >= 0 {
			cachedWeight += weight
			if 1000+i != r {
				t.Errorf("1000 + i(%d) != r(%d)\n", i, r)
			}
		}
	}
	if cachedWeight > cacheSize+cacheSize/10 {
		t.Errorf("cachedWeight(%d) > cacheSize(%d) + cacheSize(%d) / 10\n", cachedWeight, cacheSize, cacheSize)
	}
}

func TestNewID(t *testing.T) {
	a, b := current.cache.NewId(), current.cache.NewId()
	if a == b {
		t.Errorf("a(%d) == b(%d)\n", a, b)
	}
}

func TestPrune(t *testing.T) {
	current.insert1(1, 100)
	current.insert1(2, 200)

	handle := current.cache.Lookup(encodeCacheKey(1))
	if handle == nil {
		t.Error("nil")
	}

	current.cache.Prune()
	current.cache.Release(handle)

	if 100 != current.lookup(1) {
		t.Error("100")
	}
	if -1 != current.lookup(2) {
		t.Error("-1")
	}
}

func TestZeroSizeCache(t *testing.T) {
	current.cache = NewLRUCache(0)
	current.insert1(1, 100)
	if -1 != current.lookup(1) {
		t.Error("-1")
	}
}

func TestFinalizer(t *testing.T) {
	current = nil
	runtime.GC()
}
