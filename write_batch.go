package ssdb

import (
	"ssdb/util"
	"unsafe"
)

// WriteBatch header has an 8-byte sequence number followed by a 4-byte count.
const writeBatchHeader = 12

// WriteBatch::rep_ :=
//    sequence: fixed64
//    count: fixed32
//    data: record[count]
// record :=
//    kTypeValue varstring varstring         |
//    kTypeDeletion varstring
// varstring :=
//    len: varint32
//    data: uint8[len]
type WriteBatch struct {
	rep []byte
}

func (b *WriteBatch) Put(key, value []byte) {
	b.setCount(b.count() + 1)
	b.rep = append(b.rep, byte(TypeValue))
	util.PutLengthPrefixedSlice(&b.rep, key)
	util.PutLengthPrefixedSlice(&b.rep, value)
}

func (b *WriteBatch) Delete(key []byte) {
	b.setCount(b.count() + 1)
	b.rep = append(b.rep, byte(TypeDeletion))
	util.PutLengthPrefixedSlice(&b.rep, key)
}

func (b *WriteBatch) Clear() {
	b.rep = make([]byte, writeBatchHeader)
}

func (b *WriteBatch) ApproximateSize() int {
	return len(b.rep)
}

func (b *WriteBatch) Append(source *WriteBatch) {
	b.setCount(b.count() + source.count())
	if len(source.rep) < writeBatchHeader {
		panic("WriteBatch: rep < writeBatchHeader")
	}
	b.rep = append(b.rep, source.rep[writeBatchHeader:]...)
}

func (b *WriteBatch) Iterate(handler WriteBatchHandler) error {
	input := b.rep
	if len(input) < writeBatchHeader {
		return util.CorruptionError1("malformed WriteBatch (too small)")
	}
	input = input[writeBatchHeader:]
	var key, value []byte
	var found = 0
	var tag ValueType
	for len(input) > 0 {
		found++
		tag = ValueType(input[0])
		input = input[1:]
		switch tag {
		case TypeValue:
			if util.GetLengthPrefixedSlice1(input, &key) != -1 && util.GetLengthPrefixedSlice1(input, &value) != -1 {
				handler.Put(key, value)
			} else {
				return util.CorruptionError1("bad WriteBatch Put")
			}
		case TypeDeletion:
			if util.GetLengthPrefixedSlice1(input, &key) != -1 {
				handler.Delete(key)
			} else {
				return util.CorruptionError1("bad WriteBatch Delete")
			}
		default:
			return util.CorruptionError1("unknown WriteBatch tag")
		}
	}
	if found != b.count() {
		return util.CorruptionError1("WriteBatch has wrong count")
	}
	return nil
}

func (b *WriteBatch) count() int {
	return int(util.DecodeFixed32(b.rep[8:]))
}

func (b *WriteBatch) setCount(n int) {
	dst := (*[4]byte)(unsafe.Pointer(&b.rep[8]))
	util.EncodeFixed32(dst, uint32(n))
}

func NewWriteBatch() *WriteBatch {
	wb := new(WriteBatch)
	wb.Clear()
	return wb
}

type WriteBatchHandler interface {
	Put(key, value []byte)
	Delete(key []byte)
}
