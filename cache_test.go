package ssdb

import (
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
		t.cache.Release(handle)
	}
	return r
}

func (t *cacheTest) insert1(key, value int) {
	t.insert2(key, value, 1)
}

func (t *cacheTest) insert2(key, value, charge int) {
	t.cache.Release(t.cache.Insert(encodeCacheKey(key), encodeValue(value), charge, t.deleter))
}

func (t *cacheTest) insertAndReturnHandle1(key, value int) Handle {
	return t.insertAndReturnHandle2(key, value, 1)
}

func (t *cacheTest) insertAndReturnHandle2(key, value, charge int) Handle {
	return t.cache.Insert(encodeCacheKey(key), encodeValue(value), charge, t.deleter)
}

func (t *cacheTest) erase(key int) {
	t.cache.Erase(encodeCacheKey(key))
}

func newCacheTest() *cacheTest {
	return &cacheTest{
		deletedKeys:   make([]int, 0),
		deletedValues: make([]int, 0),
		cache:         NewLRUCache(cacheSize),
	}
}

func (t *cacheTest) deleter(key []byte, value interface{}) {
	t.deletedKeys = append(t.deletedKeys, decodeCacheKey(key))
	t.deletedValues = append(t.deletedValues, decodeValue(value))
}

func TestHitAndMiss(t *testing.T) {
	test := newCacheTest()
	defer test.cache.Clear()
	util.AssertEqual(-1, test.lookup(100), "lookup", t)

	test.insert1(100, 101)
	util.AssertEqual(101, test.lookup(100), "lookup", t)

	util.AssertEqual(-1, test.lookup(200), "lookup", t)
	util.AssertEqual(-1, test.lookup(300), "lookup", t)

	test.insert1(200, 201)
	util.AssertEqual(101, test.lookup(100), "lookup", t)
	util.AssertEqual(201, test.lookup(200), "lookup", t)
	util.AssertEqual(-1, test.lookup(300), "lookup", t)

	test.insert1(100, 102)
	util.AssertEqual(102, test.lookup(100), "lookup", t)
	util.AssertEqual(201, test.lookup(200), "lookup", t)
	util.AssertEqual(-1, test.lookup(300), "lookup", t)

	util.AssertEqual(1, len(test.deletedKeys), "length of deletedKeys", t)
	util.AssertEqual(100, test.deletedKeys[0], "deletedKeys[0]", t)
	util.AssertEqual(101, test.deletedValues[0], "deletedValues[0]", t)
}

func TestErase(t *testing.T) {
	test := newCacheTest()
	defer test.cache.Clear()
	test.erase(200)
	util.AssertEqual(0, len(test.deletedKeys), "length of deletedKeys", t)

	test.insert1(100, 101)
	test.insert1(200, 201)
	test.erase(100)
	util.AssertEqual(-1, test.lookup(100), "lookup", t)
	util.AssertEqual(201, test.lookup(200), "lookup", t)
	util.AssertEqual(1, len(test.deletedKeys), "length of deletedKeys", t)
	util.AssertEqual(100, test.deletedKeys[0], "deletedKeys[0]", t)
	util.AssertEqual(101, test.deletedValues[0], "deletedValues[0]", t)

	test.erase(100)
	util.AssertEqual(-1, test.lookup(100), "lookup", t)
	util.AssertEqual(201, test.lookup(200), "lookup", t)
	util.AssertEqual(1, len(test.deletedKeys), "length of deletedKeys", t)
}

func TestEntriesArePinned(t *testing.T) {
	test := newCacheTest()
	defer test.cache.Clear()
	test.insert1(100, 101)
	h1 := test.cache.Lookup(encodeCacheKey(100))
	util.AssertEqual(101, decodeValue(test.cache.Value(h1)), "cache value", t)

	test.insert1(100, 102)
	h2 := test.cache.Lookup(encodeCacheKey(100))
	util.AssertEqual(102, decodeValue(test.cache.Value(h2)), "cache value", t)
	util.AssertEqual(0, len(test.deletedKeys), "length of deletedKeys", t)

	test.cache.Release(h1)
	util.AssertEqual(1, len(test.deletedKeys), "length of deletedKeys", t)
	util.AssertEqual(100, test.deletedKeys[0], "deletedKeys[0]", t)
	util.AssertEqual(101, test.deletedValues[0], "deletedValues[0]", t)

	test.erase(100)
	util.AssertEqual(-1, test.lookup(100), "lookup", t)
	util.AssertEqual(1, len(test.deletedKeys), "length of deletedKeys", t)

	test.cache.Release(h2)
	util.AssertEqual(2, len(test.deletedKeys), "length of deletedKeys", t)
	util.AssertEqual(100, test.deletedKeys[1], "deletedKeys[1]", t)
	util.AssertEqual(102, test.deletedValues[1], "deletedValues[1]", t)
}

func TestEvictionPolicy(t *testing.T) {
	test := newCacheTest()
	defer test.cache.Clear()
	test.insert1(100, 101)
	test.insert1(200, 201)
	test.insert1(300, 301)
	h := test.cache.Lookup(encodeCacheKey(300))

	for i := 0; i < cacheSize+100; i++ {
		test.insert1(1000+i, 2000+i)
		util.AssertEqual(2000+i, test.lookup(1000+i), "lookup", t)
		util.AssertEqual(101, test.lookup(100), "lookup", t)
	}

	util.AssertEqual(101, test.lookup(100), "lookup", t)
	util.AssertEqual(-1, test.lookup(200), "lookup", t)
	util.AssertEqual(301, test.lookup(300), "lookup", t)
	test.cache.Release(h)
}

func TestUseExceedsCacheSize(t *testing.T) {
	test := newCacheTest()
	defer test.cache.Clear()
	h := make([]Handle, cacheSize+100)
	for i := range h {
		h[i] = test.insertAndReturnHandle1(1000+i, 2000+i)
	}

	for i := range h {
		util.AssertEqual(2000+i, test.lookup(1000+i), "lookup", t)
	}

	for i := range h {
		test.cache.Release(h[i])
	}
}

func TestHeavyEntries(t *testing.T) {
	test := newCacheTest()
	defer test.cache.Clear()
	const light, heavy = 1, 10
	added, index := 0, 0
	var weight int
	for added < 2*cacheSize {
		if (index & 1) != 0 {
			weight = light
		} else {
			weight = heavy
		}
		test.insert2(index, 1000+index, weight)
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
		r = test.lookup(i)
		if r >= 0 {
			cachedWeight += weight
			util.AssertEqual(1000+i, r, "1000+i", t)
		}
	}
	util.AssertLessThanOrEqual(cachedWeight, cacheSize+cacheSize/10, "cachedWeight", t)
}

func TestNewId(t *testing.T) {
	test := newCacheTest()
	defer test.cache.Clear()
	a, b := test.cache.NewId(), test.cache.NewId()
	util.AssertNotEqual(a, b, "newId", t)
}

func TestPrune(t *testing.T) {
	test := newCacheTest()
	defer test.cache.Clear()
	test.insert1(1, 100)
	test.insert1(2, 200)

	handle := test.cache.Lookup(encodeCacheKey(1))
	util.AssertTrue(handle != nil, "handle not nil", t)

	test.cache.Prune()
	test.cache.Release(handle)

	util.AssertEqual(100, test.lookup(1), "lookup", t)
	util.AssertEqual(-1, test.lookup(2), "lookup", t)
}

func TestZeroSizeCache(t *testing.T) {
	test := newCacheTest()
	defer test.cache.Clear()
	test.cache = NewLRUCache(0)
	test.insert1(1, 100)
	util.AssertEqual(-1, test.lookup(1), "lookup", t)
}
