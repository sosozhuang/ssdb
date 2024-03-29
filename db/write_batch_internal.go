package db

import "ssdb"

type writeBatchInternal interface {
	Count() uint32
	SetCount(n uint32)
	Sequence() uint64
	SetSequence(seq uint64)
	Contents() []byte
	SetContents(contents []byte)
	ByteSize() int
	//InsertInto(handler ssdb.WriteBatchHandler) error
	Append(src ssdb.WriteBatch)
}

func insertInto(b ssdb.WriteBatch, mem *memTable) error {
	seq := sequenceNumber(b.(writeBatchInternal).Sequence())
	return b.Iterate(&memTableInserter{
		seq: seq,
		mem: mem,
	})
}
