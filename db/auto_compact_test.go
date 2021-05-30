package db

import (
	"bytes"
	"fmt"
	"os"
	"ssdb"
	"ssdb/util"
	"testing"
)

const (
	valueSize = 200 * 1024
	totalSize = 100 * 1024 * 1024
	count     = totalSize / valueSize
)

type autoCompactTest struct {
	dbName    string
	tinyCache ssdb.Cache
	options   *ssdb.Options
	db        ssdb.DB
	t         *testing.T
}

func newAutoCompactTest(t *testing.T) *autoCompactTest {
	test := &autoCompactTest{
		dbName:    tmpDir() + "/autocompact_test",
		tinyCache: ssdb.NewLRUCache(100),
		options:   ssdb.NewOptions(),
		t:         t,
	}
	test.options.BlockCache = test.tinyCache
	_ = Destroy(test.dbName, test.options)
	test.options.CreateIfMissing = true
	test.options.CompressionType = ssdb.NoCompression
	test.db, _ = Open(test.options, test.dbName)
	return test
}

func (t *autoCompactTest) finalize() {
	_ = Destroy(t.dbName, ssdb.NewOptions())
	t.tinyCache.Finalize()
}

func (t *autoCompactTest) key(i int) []byte {
	buf := bytes.NewBufferString("")
	fmt.Fprintf(buf, "key%06d", i)
	return buf.Bytes()
}

func (t *autoCompactTest) size(start, limit []byte) uint64 {
	rs := make([]ssdb.Range, 1, 1)
	rs[0] = ssdb.Range{
		Start: start,
		Limit: limit,
	}
	return t.db.GetApproximateSizes(rs)[0]
}

func (t *autoCompactTest) doReads(n int) {
	value := bytes.Repeat([]byte("x"), valueSize)
	dbi := t.db.(*db)
	for i := 0; i < count; i++ {
		util.AssertNotError(t.db.Put(ssdb.NewWriteOptions(), t.key(i), value), "put", t.t)
	}
	util.AssertNotError(dbi.testCompactMemTable(), "testCompactMemTable", t.t)

	for i := 0; i < count; i++ {
		util.AssertNotError(t.db.Delete(ssdb.NewWriteOptions(), t.key(i)), "put", t.t)
	}
	util.AssertNotError(dbi.testCompactMemTable(), "testCompactMemTable", t.t)

	initialSize := int64(t.size(t.key(0), t.key(n)))
	initialOtherSize := int64(t.size(t.key(n), t.key(count)))

	limitKey := t.key(n)
	for read := 0; true; read++ {
		util.AssertLessThan(read, 100, "read", t.t)
		iter := t.db.NewIterator(ssdb.NewReadOptions())
		for iter.SeekToFirst(); iter.Valid() && bytes.Compare(iter.Key(), limitKey) < 0; iter.Next() {
		}
		iter.Finalize()
		ssdb.DefaultEnv().SleepForMicroseconds(1000000)
		size := t.size(t.key(0), t.key(n))
		fmt.Fprintf(os.Stderr, "iter %3d => %7.3f MB [other %7.3f MB]\n", read+1, float64(size)/1048576.0, float64(t.size(t.key(n), t.key(count)))/1048576.0)
		if size <= uint64(initialSize/10) {
			break
		}
	}

	finalOtherSize := int64(t.size(t.key(n), t.key(count)))
	util.AssertLessThanOrEqual(finalOtherSize, initialOtherSize+1048576, "finalOtherSize", t.t)
	util.AssertGreaterThanOrEqual(finalOtherSize, initialOtherSize/5-1048576, "finalOtherSize", t.t)
}

func TestReadAll(t *testing.T) {
	test := newAutoCompactTest(t)
	test.doReads(count)
}

func TestReadHalf(t *testing.T) {
	test := newAutoCompactTest(t)
	test.doReads(count / 2)
}
