package ssdb

import (
	"ssdb/util"
	"unsafe"
)

// WriteBatch header has an 8-byte sequence number followed by a 4-byte count.
const writeBatchHeader = 12

type WriteBatch interface {
	Put(key, value []byte)
	Delete(key []byte)
	Clear()
	ApproximateSize() int
	Append(source WriteBatch)
	Iterate(handle WriteBatchHandler) error
}

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
type writeBatch struct {
	rep []byte
}

func (b *writeBatch) Put(key, value []byte) {
	b.SetCount(b.Count() + 1)
	b.rep = append(b.rep, byte(TypeValue))
	util.PutLengthPrefixedSlice(&b.rep, key)
	util.PutLengthPrefixedSlice(&b.rep, value)
}

func (b *writeBatch) Delete(key []byte) {
	b.SetCount(b.Count() + 1)
	b.rep = append(b.rep, byte(TypeDeletion))
	util.PutLengthPrefixedSlice(&b.rep, key)
}

func (b *writeBatch) Clear() {
	b.rep = make([]byte, writeBatchHeader)
}

func (b *writeBatch) ApproximateSize() int {
	return len(b.rep)
}

func (b *writeBatch) Append(source WriteBatch) {
	src := (source).(*writeBatch)
	b.SetCount(b.Count() + src.Count())
	if len(src.rep) < writeBatchHeader {
		panic("writeBatch: rep < writeBatchHeader")
	}
	b.rep = append(b.rep, src.rep[writeBatchHeader:]...)
}

func (b *writeBatch) Iterate(handler WriteBatchHandler) error {
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
			if util.GetLengthPrefixedSlice(&input, &key) && util.GetLengthPrefixedSlice(&input, &value) {
				handler.Put(key, value)
			} else {
				return util.CorruptionError1("bad WriteBatch Put")
			}
		case TypeDeletion:
			if util.GetLengthPrefixedSlice(&input, &key) {
				handler.Delete(key)
			} else {
				return util.CorruptionError1("bad WriteBatch Delete")
			}
		default:
			return util.CorruptionError1("unknown WriteBatch tag")
		}
	}
	if found != b.Count() {
		return util.CorruptionError1("WriteBatch has wrong count")
	}
	return nil
}

func (b *writeBatch) Count() int {
	return int(util.DecodeFixed32(b.rep[8:]))
}

func (b *writeBatch) SetCount(n int) {
	dst := (*[4]byte)(unsafe.Pointer(&b.rep[8]))
	util.EncodeFixed32(dst, uint32(n))
}

func (b *writeBatch) Sequence() uint64 {
	return util.DecodeFixed64(b.rep)
}

func (b *writeBatch) SetSequence(seq uint64) {
	dst := (*[8]byte)(unsafe.Pointer(&b.rep[0]))
	util.EncodeFixed64(dst, seq)
}

//func (b *writeBatch) InsertInto(mem *memTable) error {
//	return b.Iterate(&memTableInserter{
//		seq: b.sequence(),
//		mem: mem,
//	})
//}

func (b *writeBatch) Contents() []byte {
	return b.rep
}

func (b *writeBatch) SetContents(contents []byte) {
	if len(contents) < writeBatchHeader {
		panic("writeBatch: len(contents) < writeBatchHeader")
	}
	b.rep = make([]byte, len(contents))
	copy(b.rep, contents)
}

func (b *writeBatch) ByteSize() int {
	return len(b.rep)
}

func NewWriteBatch() WriteBatch {
	wb := new(writeBatch)
	wb.Clear()
	return wb
}

type WriteBatchHandler interface {
	Put(key, value []byte)
	Delete(key []byte)
}
