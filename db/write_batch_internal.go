package db

import "ssdb"

type writeBatchInternal interface {
	Count() int
	SetCount(n int)
	Sequence() uint64
	SetSequence(seq uint64)
	Contents() []byte
	SetContents(contents []byte)
	ByteSize() int
	//InsertInto(handler ssdb.WriteBatchHandler) error
	Append(src ssdb.WriteBatch)
}

func insertInto(b ssdb.WriteBatch, seq sequenceNumber, mem *MemTable) error {
	return b.Iterate(&memTableInserter{
		seq: seq,
		mem: mem,
	})
}
