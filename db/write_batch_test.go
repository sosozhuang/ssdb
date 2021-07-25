package db

import (
	"ssdb"
	"ssdb/util"
	"strings"
	"testing"
)

func printContents(b ssdb.WriteBatch, t *testing.T) string {
	cmp := newInternalKeyComparator(ssdb.BytewiseComparator)
	mem := newMemTable(cmp)
	mem.ref()

	var builder strings.Builder
	bi := b.(writeBatchInternal)
	err := insertInto(b, mem)
	//err := b.insertInto(mem)

	count := 0
	iter := mem.newIterator()
	var ikey parsedInternalKey
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		util.AssertTrue(parseInternalKey(iter.Key(), &ikey), "parseInternalKey", t)
		switch ikey.valueType {
		case ssdb.TypeValue:
			builder.WriteString("Put(")
			builder.Write(ikey.userKey)
			builder.WriteString(", ")
			builder.Write(iter.Value())
			builder.WriteByte(')')
			count++
		case ssdb.TypeDeletion:
			builder.WriteString("Delete(")
			builder.Write(ikey.userKey)
			builder.WriteByte(')')
			count++
		}
		builder.WriteByte('@')
		builder.WriteString(util.NumberToString(uint64(ikey.sequence)))
	}
	iter = nil
	if err != nil {
		builder.WriteString("ParseError()")
	} else if count != bi.Count() {
		builder.WriteString("CountMismatch()")
	}
	mem.unref()
	return builder.String()
}

func TestEmptyWriteBatch(t *testing.T) {
	batch := ssdb.NewWriteBatch()
	bi := batch.(writeBatchInternal)
	util.AssertEqual("", printContents(batch, t), "empty content", t)
	util.AssertEqual(0, bi.Count(), "zero count", t)
}

func TestMulti(t *testing.T) {
	batch := ssdb.NewWriteBatch()
	batch.Put([]byte("foo"), []byte("bar"))
	batch.Delete([]byte("box"))
	batch.Put([]byte("baz"), []byte("boo"))
	bi := batch.(writeBatchInternal)
	bi.SetSequence(100)
	util.AssertEqual(uint64(100), bi.Sequence(), "Sequence", t)
	util.AssertEqual(3, bi.Count(), "Count", t)
	util.AssertEqual("Put(baz, boo)@102"+
		"Delete(box)@101"+
		"Put(foo, bar)@100", printContents(batch, t), "contents", t)
}

func TestCorruption(t *testing.T) {
	batch := ssdb.NewWriteBatch()
	batch.Put([]byte("foo"), []byte("bar"))
	batch.Delete([]byte("box"))
	bi := batch.(writeBatchInternal)
	bi.SetSequence(200)
	contents := bi.Contents()
	bi.SetContents(contents[:len(contents)-1])
	util.AssertEqual("Put(foo, bar)@200"+"ParseError()", printContents(batch, t), "contents", t)
}

func TestAppend(t *testing.T) {
	b1, b2 := ssdb.NewWriteBatch(), ssdb.NewWriteBatch()
	bi1, bi2 := b1.(writeBatchInternal), b2.(writeBatchInternal)
	bi1.SetSequence(200)
	bi2.SetSequence(300)
	b1.Append(b2)
	util.AssertEqual("", printContents(b1, t), "contents", t)
	b2.Put([]byte("a"), []byte("va"))
	b1.Append(b2)
	util.AssertEqual("Put(a, va)@200", printContents(b1, t), "contents", t)
	b2.Clear()
	b2.Put([]byte("b"), []byte("vb"))
	b1.Append(b2)
	util.AssertEqual("Put(a, va)@200"+"Put(b, vb)@201", printContents(b1, t), "contents", t)
	b2.Delete([]byte("foo"))
	b1.Append(b2)
	util.AssertEqual("Put(a, va)@200"+
		"Put(b, vb)@202"+
		"Put(b, vb)@201"+
		"Delete(foo)@203", printContents(b1, t), "contents", t)
}

func TestApproximateSize(t *testing.T) {
	batch := ssdb.NewWriteBatch()
	emptySize := batch.ApproximateSize()
	batch.Put([]byte("foo"), []byte(" bar"))
	oneKeySize := batch.ApproximateSize()
	util.AssertLessThan(emptySize, oneKeySize, "ApproximateSize", t)

	batch.Put([]byte("baz"), []byte("boo"))
	twoKeySize := batch.ApproximateSize()
	util.AssertLessThan(oneKeySize, twoKeySize, "ApproximateSize", t)

	batch.Delete([]byte("box"))
	postDeleteSize := batch.ApproximateSize()
	util.AssertLessThan(twoKeySize, postDeleteSize, "ApproximateSize", t)
}
