package ssdb

import (
	"ssdb/util"
)

type FilterPolicy interface {
	Name() string
	CreateFilter(keys [][]byte, dst *[]byte)
	KeyMayMatch(key []byte, filter []byte) bool
}

type bloomFilterPolicy struct {
	bitsPerKey int
	k          uint
}

func (b *bloomFilterPolicy) Name() string {
	return "ssdb.BuiltinBloomFilter2"
}

func (b *bloomFilterPolicy) CreateFilter(keys [][]byte, dst *[]byte) {
	n := len(keys)
	bits := n * b.bitsPerKey
	if bits < 64 {
		bits = 64
	}
	bs := (bits + 7) / 8
	bits = bs * 8
	initSize := len(*dst)
	*dst = append(*dst, make([]byte, bs)...)
	*dst = append(*dst, byte(b.k))
	array := (*dst)[initSize:]
	var h, delta, bitpos uint32
	for i := 0; i < n; i++ {
		h = bloomHash(keys[i])
		delta = h>>17 | h<<15
		for j := uint(0); j < b.k; j++ {
			bitpos = h % uint32(bits)
			array[bitpos/8] |= 1 << (bitpos % 8)
			h += delta
		}
	}
}

func (b *bloomFilterPolicy) KeyMayMatch(key []byte, filter []byte) bool {
	l := len(filter)
	if l < 2 {
		return false
	}
	bits := uint((l - 1) * 8)
	k := uint(filter[l-1])
	if k > 30 {
		return true
	}
	h := bloomHash(key)
	delta := h>>17 | h<<15
	var bitpos uint32
	for j := uint(0); j < k; j++ {
		bitpos = h % uint32(bits)
		if (filter[bitpos/8] & (1 << (bitpos % 8))) == 0 {
			return false
		}
		h += delta
	}
	return true
}

func bloomHash(key []byte) uint32 {
	return util.Hash(key, 0xbc9f1d34)
}

func NewBloomFilterPolicy(bitsPerKey int) FilterPolicy {
	k := uint(float64(bitsPerKey) * 0.69)
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}
	return &bloomFilterPolicy{
		bitsPerKey: bitsPerKey,
		k:          k,
	}
}
