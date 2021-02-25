package table

import (
	"ssdb"
	"ssdb/util"
)

const (
	filterBaseLg = 11
	filterBase   = 1 << filterBaseLg
)

type filterBlockBuilder struct {
	policy        ssdb.FilterPolicy
	keys          []byte   // Flattened key contents
	start         []int    // Starting index in keys of each key
	result        []byte   // Filter data computed so far
	tmpKeys       [][]byte // policy->CreateFilter() argument
	filterOffsets []uint32
}

func (b *filterBlockBuilder) startBlock(blockOffset uint64) {
	filterIndex := blockOffset / filterBase
	if filterIndex < uint64(len(b.filterOffsets)) {
		panic("filterBlockBuilder: filterIndex < uint64(len(b.filterOffsets))")
	}
	for filterIndex > uint64(len(b.filterOffsets)) {
		b.generateFilter()
	}
}

func (b *filterBlockBuilder) addKey(key []byte) {
	b.start = append(b.start, len(b.keys))
	b.keys = append(b.keys, key...)
}

func (b *filterBlockBuilder) finish() []byte {
	if len(b.start) != 0 {
		b.generateFilter()
	}
	arrayOffset := uint32(len(b.result))
	for i := 0; i < len(b.filterOffsets); i++ {
		util.PutFixed32(&b.result, b.filterOffsets[i])
	}
	util.PutFixed32(&b.result, arrayOffset)
	b.result = append(b.result, filterBaseLg)
	return b.result
}

func (b *filterBlockBuilder) generateFilter() {
	numKeys := len(b.start)
	if numKeys == 0 {
		b.filterOffsets = append(b.filterOffsets, uint32(len(b.result)))
		return
	}
	b.start = append(b.start, len(b.keys))
	b.tmpKeys = make([][]byte, numKeys)
	for i := 0; i < numKeys; i++ {
		b.tmpKeys[i] = b.keys[b.start[i]:b.start[i+1]]
	}

	b.filterOffsets = append(b.filterOffsets, uint32(len(b.result)))
	b.policy.CreateFilter(b.tmpKeys, &b.result)
	b.tmpKeys = make([][]byte, 0)
	b.keys = make([]byte, 0)
	b.start = make([]int, 0)
}

func newFilterBlockBuilder(policy ssdb.FilterPolicy) *filterBlockBuilder {
	return &filterBlockBuilder{
		policy:        policy,
		keys:          make([]byte, 0),
		start:         make([]int, 0),
		result:        make([]byte, 0),
		tmpKeys:       make([][]byte, 0),
		filterOffsets: make([]uint32, 0),
	}
}

type filterBlockReader struct {
	policy ssdb.FilterPolicy
	data   []byte // Pointer to filter data (at block-start)
	offset uint64 // Pointer to beginning of offset array (at block-end)
	num    int    // Number of entries in offset array
	baseLg uint   // Encoding parameter (see kFilterBaseLg)
}

func (r *filterBlockReader) keyMayMatch(blockOffset uint64, key []byte) bool {
	index := blockOffset >> r.baseLg
	if index < uint64(r.num) {
		start := util.DecodeFixed32(r.data[r.offset+index*4:])
		limit := util.DecodeFixed32(r.data[r.offset+index*4+4:])
		if start <= limit && uint64(limit) <= r.offset {
			return r.policy.KeyMayMatch(key, r.data[start:limit])
		} else if start == limit {
			return false
		}
	}
	return true
}

func newFilterBlockReader(policy ssdb.FilterPolicy, contents []byte) (r *filterBlockReader) {
	r = &filterBlockReader{
		policy: policy,
		data:   nil,
		offset: 0,
		num:    0,
		baseLg: 0,
	}
	n := len(contents)
	if n < 5 {
		return
	}
	r.baseLg = uint(contents[n-1])
	r.offset = uint64(util.DecodeFixed32(contents[n-5:]))
	if int(r.offset) > (n - 5) {
		return
	}
	r.data = contents
	r.num = (n - 5 - int(r.offset)) / 4
	return
}
