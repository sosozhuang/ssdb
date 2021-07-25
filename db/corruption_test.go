package db

import (
	"bytes"
	"fmt"
	"os"
	"ssdb"
	"ssdb/util"
	"testing"
)

const corruptionValueSize = 1000

type errorEnv struct {
	ssdb.EnvWrapper
	writableFileError     bool
	numWritableFileErrors int
}

func newErrorEnv() *errorEnv {
	return &errorEnv{
		EnvWrapper:            ssdb.EnvWrapper{Env: newMemEnv(ssdb.DefaultEnv())},
		writableFileError:     false,
		numWritableFileErrors: 0,
	}
}

func (e *errorEnv) NewWritableFile(name string) (result ssdb.WritableFile, err error) {
	if e.writableFileError {
		e.numWritableFileErrors++
		err = util.IOError1("fake error")
		return
	}
	return e.Target().NewWritableFile(name)
}

func (e *errorEnv) NewAppendableFile(name string) (result ssdb.WritableFile, err error) {
	if e.writableFileError {
		e.numWritableFileErrors++
		err = util.IOError1("fake error")
		return
	}
	return e.Target().NewAppendableFile(name)
}

type corruptionTest struct {
	env       *errorEnv
	options   *ssdb.Options
	db        ssdb.DB
	dbName    string
	tinyCache ssdb.Cache
	t         *testing.T
}

func newCorruptionTest(t *testing.T) *corruptionTest {
	test := &corruptionTest{
		env:       newErrorEnv(),
		options:   ssdb.NewOptions(),
		db:        nil,
		dbName:    "/memenv/corruption_test",
		tinyCache: ssdb.NewLRUCache(100),
		t:         t,
	}
	test.options.Env = test.env
	test.options.BlockCache = test.tinyCache
	_ = Destroy(test.dbName, test.options)

	test.options.CreateIfMissing = true
	test.reopen()
	test.options.CreateIfMissing = false
	return test
}

func (t *corruptionTest) finish() {
	if t.db != nil {
		t.db.Close()
	}
	if t.tinyCache != nil {
		t.tinyCache.Clear()
	}
}

func (t *corruptionTest) tryReopen() (err error) {
	if t.db != nil {
		t.db.Close()
		t.db = nil
	}
	t.db, err = Open(t.options, t.dbName)
	return
}

func (t *corruptionTest) reopen() {
	util.AssertNotError(t.tryReopen(), "tryReopen", t.t)
}

func (t *corruptionTest) repairDB() {
	if t.db != nil {
		t.db.Close()
		t.db = nil
	}
	util.AssertNotError(Repair(t.dbName, t.options), "repair", t.t)
}

func (t *corruptionTest) build(n int) {
	batch := ssdb.NewWriteBatch()
	var options *ssdb.WriteOptions
	for i := 0; i < n; i++ {
		batch.Clear()
		batch.Put(t.key(i), t.value(i))
		options = ssdb.NewWriteOptions()
		if i == n-1 {
			options.Sync = true
		}
		util.AssertNotError(t.db.Write(options, batch), "Write", t.t)
	}
}

func (t *corruptionTest) check(minExcepted, maxExcepted int) {
	var (
		nextExcepted int
		missed       int
		badKeys      int
		badValues    int
		correct      int
	)
	iter := t.db.NewIterator(ssdb.NewReadOptions())
	var key uint64
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		in := string(iter.Key())
		if len(in) == 0 || in == "~" {
			continue
		}
		if !util.ConsumeDecimalNumber(&in, &key) || len(in) != 0 || key < uint64(nextExcepted) {
			badKeys++
			continue
		}
		missed += int(key) - nextExcepted
		nextExcepted = int(key + 1)
		if bytes.Compare(iter.Value(), t.value(int(key))) != 0 {
			badValues++
		} else {
			correct++
		}
	}
	iter.Close()

	fmt.Fprintf(os.Stderr, "expected=%d..%d; got=%d; bad_keys=%d; bad_values=%d; missed=%d\n", minExcepted, maxExcepted, correct, badKeys, badValues, missed)
	util.AssertLessThanOrEqual(minExcepted, correct, "correct >= minExcepted", t.t)
	util.AssertGreaterThanOrEqual(maxExcepted, correct, "correct <= maxExcepted", t.t)
}

func (t *corruptionTest) corrupt(ft fileType, offset, bytesToCorrupt int) {
	filenames, err := t.env.Target().GetChildren(t.dbName)
	util.AssertNotError(err, "GetChildren", t.t)
	var (
		number uint64
		typ    fileType
		fname  string
	)
	pickedNumber := -1
	for _, filename := range filenames {
		if parseFileName(filename, &number, &typ) && typ == ft && int(number) > pickedNumber {
			fname = t.dbName + "/" + filename
			pickedNumber = int(number)
		}
	}
	util.AssertTrue(len(fname) > 0, "fname", t.t)

	fileSize, err := t.env.Target().GetFileSize(fname)
	util.AssertNotError(err, "GetFileSize", t.t)
	if offset < 0 {
		if -offset > fileSize {
			offset = 0
		} else {
			offset += fileSize
		}
	}
	if offset > fileSize {
		offset = fileSize
	}
	if offset+bytesToCorrupt > fileSize {
		bytesToCorrupt = fileSize - offset
	}

	contents, err := ssdb.ReadFileToString(t.env.Target(), fname)
	util.AssertNotError(err, "ReadFileToString", t.t)
	newContents := []byte(contents)
	for i := 0; i < bytesToCorrupt; i++ {
		newContents[i+offset] ^= 0x80
	}
	err = ssdb.WriteStringToFile(t.env.Target(), string(newContents), fname)
	util.AssertNotError(err, "WriteStringToFile", t.t)
}

func (t *corruptionTest) property(name string) int {
	if property, ok := t.db.GetProperty(name); ok {
		var result int
		if _, err := fmt.Sscanf(property, "%d", &result); err == nil {
			return result
		}
	}
	return -1
}

func (t *corruptionTest) key(i int) []byte {
	return []byte(fmt.Sprintf("%016d", i))
}

func (t *corruptionTest) value(k int) []byte {
	r := util.NewRandom(uint32(k))
	return []byte(util.RandomString(r, corruptionValueSize))
}

func TestRecovery(t *testing.T) {
	test := newCorruptionTest(t)
	defer test.finish()
	test.build(100)
	test.check(100, 100)
	test.corrupt(logFile, 19, 1)
	test.corrupt(logFile, blockSize+1000, 1)
	test.reopen()

	test.check(36, 36)
}

func TestRecoverWriteError(t *testing.T) {
	test := newCorruptionTest(t)
	defer test.finish()
	test.env.writableFileError = true
	util.AssertError(test.tryReopen(), "tryReopen", t)
}

func TestNewFileErrorDuringWrite(t *testing.T) {
	test := newCorruptionTest(t)
	defer test.finish()
	test.env.writableFileError = true
	num := 3 + ssdb.NewOptions().WriteBufferSize/corruptionValueSize
	var (
		err   error
		batch ssdb.WriteBatch
	)
	for i := 0; err == nil && i < num; i++ {
		batch = ssdb.NewWriteBatch()
		batch.Put([]byte("a"), test.value(100))
		err = test.db.Write(ssdb.NewWriteOptions(), batch)
	}
	util.AssertError(err, "Write", t)
	util.AssertGreaterThanOrEqual(test.env.numWritableFileErrors, 1, "numWritableFileErrors", t)
	test.env.writableFileError = false
	test.reopen()
}

func TestTableFile(t *testing.T) {
	test := newCorruptionTest(t)
	defer test.finish()
	test.build(100)
	dbi := test.db.(*db)
	_ = dbi.testCompactMemTable()
	dbi.testCompactRange(0, nil, nil)
	dbi.testCompactRange(1, nil, nil)

	test.corrupt(tableFile, 100, 1)
	test.check(90, 99)
}

func TestFileRepair(t *testing.T) {
	test := newCorruptionTest(t)
	defer test.finish()
	test.options.BlockSize = 2 * corruptionValueSize
	test.options.ParanoidChecks = true
	test.reopen()
	test.build(100)
	dbi := test.db.(*db)
	_ = dbi.testCompactMemTable()
	dbi.testCompactRange(0, nil, nil)
	dbi.testCompactRange(1, nil, nil)

	test.corrupt(tableFile, 100, 1)
	test.repairDB()
	test.reopen()
	test.check(95, 99)
}

func TestTableFileIndexData(t *testing.T) {
	test := newCorruptionTest(t)
	defer test.finish()
	test.build(10000)
	dbi := test.db.(*db)
	_ = dbi.testCompactMemTable()

	test.corrupt(tableFile, -2000, 500)
	test.reopen()
	test.check(5000, 9999)
}

func TestMissingDescriptor(t *testing.T) {
	test := newCorruptionTest(t)
	defer test.finish()
	test.build(1000)
	test.repairDB()
	test.reopen()
	test.check(1000, 1000)
}

func TestSequenceNumberRecovery(t *testing.T) {
	test := newCorruptionTest(t)
	defer test.finish()
	util.AssertNotError(test.db.Put(ssdb.NewWriteOptions(), []byte("foo"), []byte("v1")), "Write", t)
	util.AssertNotError(test.db.Put(ssdb.NewWriteOptions(), []byte("foo"), []byte("v2")), "Write", t)
	util.AssertNotError(test.db.Put(ssdb.NewWriteOptions(), []byte("foo"), []byte("v3")), "Write", t)
	util.AssertNotError(test.db.Put(ssdb.NewWriteOptions(), []byte("foo"), []byte("v4")), "Write", t)
	util.AssertNotError(test.db.Put(ssdb.NewWriteOptions(), []byte("foo"), []byte("v5")), "Write", t)
	test.repairDB()
	test.reopen()
	v, err := test.db.Get(ssdb.NewReadOptions(), []byte("foo"))
	util.AssertNotError(err, "Get", t)
	util.AssertEqual("v5", string(v), "value", t)

	util.AssertNotError(test.db.Put(ssdb.NewWriteOptions(), []byte("foo"), []byte("v6")), "Write", t)
	v, err = test.db.Get(ssdb.NewReadOptions(), []byte("foo"))
	util.AssertNotError(err, "Get", t)
	util.AssertEqual("v6", string(v), "value", t)
	test.reopen()
	v, err = test.db.Get(ssdb.NewReadOptions(), []byte("foo"))
	util.AssertNotError(err, "Get", t)
	util.AssertEqual("v6", string(v), "value", t)
}

func TestCorruptedDescriptor(t *testing.T) {
	test := newCorruptionTest(t)
	defer test.finish()
	util.AssertNotError(test.db.Put(ssdb.NewWriteOptions(), []byte("foo"), []byte("hello")), "Put", t)
	dbi := test.db.(*db)
	_ = dbi.testCompactMemTable()
	dbi.testCompactRange(0, nil, nil)

	test.corrupt(descriptorFile, 0, 1000)
	err := test.tryReopen()
	util.AssertError(err, "tryReopen", t)

	test.repairDB()
	test.reopen()
	v, err := test.db.Get(ssdb.NewReadOptions(), []byte("foo"))
	util.AssertNotError(err, "Get", t)
	util.AssertEqual("hello", string(v), "value", t)
}

func TestCompactionInputError(t *testing.T) {
	test := newCorruptionTest(t)
	defer test.finish()
	test.build(10)
	dbi := test.db.(*db)
	_ = dbi.testCompactMemTable()
	last := maxMemCompactLevel
	util.AssertEqual(1, test.property("ssdb.num-files-at-level"+util.NumberToString(uint64(last))), "property", t)

	test.corrupt(tableFile, 100, 1)
	test.check(5, 9)

	test.build(10000)
	test.check(10000, 10000)
}

func TestCompactionInputErrorParanoid(t *testing.T) {
	test := newCorruptionTest(t)
	defer test.finish()
	test.options.ParanoidChecks = true
	test.options.WriteBufferSize = 512 << 10
	test.reopen()
	dbi := test.db.(*db)

	for i := 0; i < 2; i++ {
		test.build(10)
		_ = dbi.testCompactMemTable()
		test.corrupt(tableFile, 100, 1)
		test.env.SleepForMicroseconds(100000)
	}
	dbi.CompactRange(nil, nil)
	err := test.db.Put(ssdb.NewWriteOptions(), test.key(5), test.value(5))
	util.AssertError(err, "Put", t)
}

func TestUnrelatedKeys(t *testing.T) {
	test := newCorruptionTest(t)
	defer test.finish()
	test.build(10)
	dbi := test.db.(*db)
	_ = dbi.testCompactMemTable()
	test.corrupt(tableFile, 100, 1)

	util.AssertNotError(test.db.Put(ssdb.NewWriteOptions(), test.key(1000), test.value(1000)), "Put", t)
	v, err := test.db.Get(ssdb.NewReadOptions(), test.key(1000))
	util.AssertNotError(err, "Get", t)
	util.AssertEqual(test.value(1000), v, "value", t)
	_ = dbi.testCompactMemTable()
	v, err = test.db.Get(ssdb.NewReadOptions(), test.key(1000))
	util.AssertNotError(err, "Get", t)
	util.AssertEqual(test.value(1000), v, "value", t)
}
