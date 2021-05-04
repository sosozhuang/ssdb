package table

import (
	"ssdb/util"
	"testing"
)

type testHashFilter struct {
}

func (f *testHashFilter) Name() string {
	return "TestHashFilter"
}

func (f *testHashFilter) CreateFilter(keys [][]byte, dst *[]byte) {
	var h uint32
	for _, key := range keys {
		h = util.Hash(key, 1)
		util.PutFixed32(dst, h)
	}
}

func (f *testHashFilter) KeyMayMatch(key []byte, filter []byte) bool {
	h := util.Hash(key, 1)
	for i := 0; i+4 <= len(filter); i += 4 {
		if h == util.DecodeFixed32(filter[i:]) {
			return true
		}
	}
	return false
}

func TestEmptyBuilder(t *testing.T) {
	policy := new(testHashFilter)
	builder := newFilterBlockBuilder(policy)
	block := builder.finish()
	util.AssertEqual("\\x00\\x00\\x00\\x00\\x0b", util.EscapeString(block), "empty builder", t)
	reader := newFilterBlockReader(policy, block)
	util.AssertTrue(reader.keyMayMatch(0, []byte("foo")), "empty builder", t)
	util.AssertTrue(reader.keyMayMatch(100000, []byte("foo")), "empty builder", t)
}

func TestSingleChunk(t *testing.T) {
	policy := new(testHashFilter)
	builder := newFilterBlockBuilder(policy)
	builder.startBlock(100)
	builder.addKey([]byte("foo"))
	builder.addKey([]byte("bar"))
	builder.addKey([]byte("box"))
	builder.startBlock(200)
	builder.addKey([]byte("box"))
	builder.startBlock(300)
	builder.addKey([]byte("hello"))
	block := builder.finish()
	reader := newFilterBlockReader(policy, block)
	util.AssertTrue(reader.keyMayMatch(100, []byte("foo")), "single chunk", t)
	util.AssertTrue(reader.keyMayMatch(100, []byte("bar")), "single chunk", t)
	util.AssertTrue(reader.keyMayMatch(100, []byte("box")), "single chunk", t)
	util.AssertTrue(reader.keyMayMatch(100, []byte("hello")), "single chunk", t)
	util.AssertTrue(reader.keyMayMatch(100, []byte("foo")), "single chunk", t)
	util.AssertFalse(reader.keyMayMatch(100, []byte("missing")), "single chunk", t)
	util.AssertFalse(reader.keyMayMatch(100, []byte("other")), "single chunk", t)
}

func TestMultiChunk(t *testing.T) {
	policy := new(testHashFilter)
	builder := newFilterBlockBuilder(policy)
	builder.startBlock(0)
	builder.addKey([]byte("foo"))
	builder.startBlock(2000)
	builder.addKey([]byte("bar"))

	builder.startBlock(3100)
	builder.addKey([]byte("box"))

	builder.startBlock(9000)
	builder.addKey([]byte("box"))
	builder.addKey([]byte("hello"))

	block := builder.finish()
	reader := newFilterBlockReader(policy, block)

	util.AssertTrue(reader.keyMayMatch(0, []byte("foo")), "multi chunk", t)
	util.AssertTrue(reader.keyMayMatch(2000, []byte("bar")), "multi chunk", t)
	util.AssertFalse(reader.keyMayMatch(0, []byte("box")), "multi chunk", t)
	util.AssertFalse(reader.keyMayMatch(0, []byte("hello")), "multi chunk", t)

	util.AssertTrue(reader.keyMayMatch(3100, []byte("box")), "multi chunk", t)
	util.AssertFalse(reader.keyMayMatch(3100, []byte("foo")), "multi chunk", t)
	util.AssertFalse(reader.keyMayMatch(3100, []byte("bar")), "multi chunk", t)
	util.AssertFalse(reader.keyMayMatch(3100, []byte("hello")), "multi chunk", t)

	util.AssertFalse(reader.keyMayMatch(4100, []byte("foo")), "multi chunk", t)
	util.AssertFalse(reader.keyMayMatch(4100, []byte("bar")), "multi chunk", t)
	util.AssertFalse(reader.keyMayMatch(4100, []byte("box")), "multi chunk", t)
	util.AssertFalse(reader.keyMayMatch(4100, []byte("hello")), "multi chunk", t)

	util.AssertTrue(reader.keyMayMatch(9000, []byte("box")), "multi chunk", t)
	util.AssertTrue(reader.keyMayMatch(9000, []byte("hello")), "multi chunk", t)
	util.AssertFalse(reader.keyMayMatch(9000, []byte("foo")), "multi chunk", t)
	util.AssertFalse(reader.keyMayMatch(9000, []byte("bar")), "multi chunk", t)
}
