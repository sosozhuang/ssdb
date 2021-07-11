package db

import (
	"bytes"
	"fmt"
	"os"
	"ssdb"
	"ssdb/util"
	"testing"
)

type recoveryTest struct {
	dbName string
	env    ssdb.Env
	db     ssdb.DB
	t      *testing.T
}

func newRecoveryTest(t *testing.T) *recoveryTest {
	test := &recoveryTest{
		dbName: tmpDir() + "/recovery_test",
		env:    ssdb.DefaultEnv(),
		db:     nil,
		t:      t,
	}
	_ = Destroy(test.dbName, ssdb.NewOptions())
	test.open(nil)
	return test
}

func (t *recoveryTest) dbFull() *db {
	return t.db.(*db)
}

func (t *recoveryTest) canAppend() bool {
	tmp, err := t.env.NewAppendableFile(currentFileName(t.dbName))
	if err == nil {
		tmp.Finalize()
	}
	if ssdb.IsNotSupportedError(err) {
		return false
	}
	return true
}

func (t *recoveryTest) close() {
	if t.db != nil {
		t.db.Close()
		t.db = nil
	}
}

func (t *recoveryTest) openWithStatus(options *ssdb.Options) error {
	t.close()
	var opts *ssdb.Options
	if options != nil {
		opts = options
	} else {
		opts = ssdb.NewOptions()
		opts.ReuseLogs = true
		opts.CreateIfMissing = true
	}
	if opts.Env == nil {
		opts.Env = t.env
	}
	var err error
	t.db, err = Open(opts, t.dbName)
	return err
}

func (t *recoveryTest) open(options *ssdb.Options) {
	util.AssertNotError(t.openWithStatus(options), "openWithStatus", t.t)
	util.AssertEqual(1, t.numLogs(), "numLogs", t.t)
}

func (t *recoveryTest) put(k, v string) error {
	return t.db.Put(ssdb.NewWriteOptions(), []byte(k), []byte(v))
}

func (t *recoveryTest) get(k string, snapshot ssdb.Snapshot) string {
	result, err := t.db.Get(ssdb.NewReadOptions(), []byte(k))
	if ssdb.IsNotFound(err) {
		return "NOT_FOUND"
	} else if err != nil {
		return err.Error()
	}
	return string(result)
}

func (t *recoveryTest) manifestFileName() string {
	current, err := ssdb.ReadFileToBytes(t.env, currentFileName(t.dbName))
	util.AssertNotError(err, "ReadFileToBytes", t.t)
	l := len(current)
	if l > 0 && current[l-1] == '\n' {
		current = current[:l-1]
	}
	return t.dbName + string(os.PathSeparator) + string(current)
}

func (t *recoveryTest) logName(number uint64) string {
	return logFileName(t.dbName, number)
}

func (t *recoveryTest) deleteLogFiles() int {
	t.close()
	logs := t.getFiles(logFile)
	for _, log := range logs {
		util.AssertNotError(t.env.DeleteFile(t.logName(log)), "", t.t)
	}
	return len(logs)
}

func (t *recoveryTest) deleteManifestFile() {
	util.AssertNotError(t.env.DeleteFile(t.manifestFileName()), "deleteManifestFile", t.t)
}

func (t *recoveryTest) firstLogFile() uint64 {
	return t.getFiles(logFile)[0]
}

func (t *recoveryTest) getFiles(ft fileType) []uint64 {
	filenames, err := t.env.GetChildren(t.dbName)
	util.AssertNotError(err, "GetChildren", t.t)
	var (
		number   uint64
		fileType fileType
	)
	result := make([]uint64, 0)
	for _, filename := range filenames {
		if parseFileName(filename, &number, &fileType) && fileType == ft {
			result = append(result, number)
		}
	}
	return result
}

func (t *recoveryTest) numLogs() int {
	return len(t.getFiles(logFile))
}

func (t *recoveryTest) numTables() int {
	return len(t.getFiles(tableFile))
}

func (t *recoveryTest) fileSize(fname string) int {
	result, err := t.env.GetFileSize(fname)
	util.AssertNotError(err, "GetFileSize", t.t)
	return result
}

func (t *recoveryTest) compactMemTable() {
	_ = t.dbFull().testCompactMemTable()
}

func (t *recoveryTest) makeLogFile(lognum uint64, seq sequenceNumber, key, val string) {
	fname := logFileName(t.dbName, lognum)
	file, err := t.env.NewWritableFile(fname)
	util.AssertNotError(err, "NewWritableFile", t.t)
	writer := newLogWriter(file)
	batch := ssdb.NewWriteBatch()
	batch.Put([]byte(key), []byte(val))
	batch.(writeBatchInternal).SetSequence(uint64(seq))
	util.AssertNotError(writer.addRecord(batch.(writeBatchInternal).Contents()), "addRecord", t.t)
	util.AssertNotError(file.Flush(), "Flush", t.t)
	file.Finalize()
}

func TestManifestReused(t *testing.T) {
	test := newRecoveryTest(t)
	if !test.canAppend() {
		fmt.Fprintf(os.Stderr, "skipping test because env does not support appending\n")
		return
	}
	util.AssertNotError(test.put("foo", "bar"), "put", t)
	test.close()
	oldManifest := test.manifestFileName()
	test.open(nil)
	util.AssertEqual(oldManifest, test.manifestFileName(), "manifestFileName", t)
	util.AssertEqual("bar", test.get("foo", nil), "get", t)
	test.open(nil)
	util.AssertEqual(oldManifest, test.manifestFileName(), "manifestFileName", t)
	util.AssertEqual("bar", test.get("foo", nil), "get", t)
}

func TestLargeManifestCompacted(t *testing.T) {
	test := newRecoveryTest(t)
	if !test.canAppend() {
		fmt.Fprintf(os.Stderr, "skipping test because env does not support appending\n")
		return
	}
	util.AssertNotError(test.put("foo", "bar"), "put", t)
	test.close()
	oldManifest := test.manifestFileName()

	l := test.fileSize(oldManifest)
	file, err := test.env.NewAppendableFile(oldManifest)
	util.AssertNotError(err, "NewAppendableFile", t)
	zeroes := bytes.Repeat([]byte{'\000'}, 3*1048576-l)
	util.AssertNotError(file.Append(zeroes), "Append", t)
	util.AssertNotError(file.Flush(), "Flush", t)
	file.Finalize()
	file = nil

	test.open(nil)
	newManifest := test.manifestFileName()
	util.AssertNotEqual(oldManifest, newManifest, "manifestFileName", t)
	util.AssertGreaterThan(10000, test.fileSize(newManifest), "fileSize", t)
	util.AssertEqual("bar", test.get("foo", nil), "get", t)

	test.open(nil)
	util.AssertEqual(newManifest, test.manifestFileName(), "manifestFileName", t)
	util.AssertEqual("bar", test.get("foo", nil), "get", t)
}

func TestNoLogFiles(t *testing.T) {
	test := newRecoveryTest(t)
	util.AssertNotError(test.put("foo", "bar"), "put", t)
	util.AssertEqual(1, test.deleteLogFiles(), "deleteLogFiles", t)
	test.open(nil)
	util.AssertEqual("NOT_FOUND", test.get("foo", nil), "get", t)
	test.open(nil)
	util.AssertEqual("NOT_FOUND", test.get("foo", nil), "get", t)
}

func TestLogFileReuse(t *testing.T) {
	test := newRecoveryTest(t)
	if !test.canAppend() {
		fmt.Fprintf(os.Stderr, "skipping test because env does not support appending\n")
		return
	}
	for i := 0; i < 2; i++ {
		util.AssertNotError(test.put("foo", "bar"), "put", t)
		if i == 0 {
			test.compactMemTable()
		}
		test.close()
		util.AssertEqual(1, test.numLogs(), "numLogs", t)
		number := test.firstLogFile()
		if i == 0 {
			util.AssertEqual(0, test.fileSize(test.logName(number)), "fileSize", t)
		} else {
			util.AssertLessThan(0, test.fileSize(test.logName(number)), "fileSize", t)
		}
		test.open(nil)
		util.AssertEqual(1, test.numLogs(), "numLogs", t)
		util.AssertEqual(number, test.firstLogFile(), "firstLogFile", t)
		util.AssertEqual("bar", test.get("foo", nil), "get", t)
		test.open(nil)
		util.AssertEqual(1, test.numLogs(), "numLogs", t)
		util.AssertEqual(number, test.firstLogFile(), "firstLogFile", t)
		util.AssertEqual("bar", test.get("foo", nil), "get", t)
	}
}

func TestMultipleMemTables(t *testing.T) {
	const num = 1000
	test := newRecoveryTest(t)
	for i := 0; i < num; i++ {
		buf := fmt.Sprintf("%050d", i)
		util.AssertNotError(test.put(buf, buf), "put", t)
	}
	util.AssertEqual(0, test.numTables(), "numTables", t)
	test.close()
	util.AssertEqual(0, test.numTables(), "numTables", t)
	util.AssertEqual(1, test.numLogs(), "numLogs", t)
	oldLogFile := test.firstLogFile()

	opt := ssdb.NewOptions()
	opt.ReuseLogs = true
	opt.WriteBufferSize = (num * 100) / 2
	test.open(opt)
	util.AssertLessThanOrEqual(2, test.numTables(), "numTables", t)
	util.AssertEqual(1, test.numLogs(), "numLogs", t)
	util.AssertNotEqual(oldLogFile, test.firstLogFile(), "firstLogFile", t)
	for i := 0; i < num; i++ {
		buf := fmt.Sprintf("%050d", i)
		util.AssertEqual(buf, test.get(buf, nil), "put", t)
	}
}

func TestMultipleLogFiles(t *testing.T) {
	test := newRecoveryTest(t)
	util.AssertNotError(test.put("foo", "bar"), "put", t)
	test.close()
	util.AssertEqual(1, test.numLogs(), "numLogs", t)

	oldLog := test.firstLogFile()
	test.makeLogFile(oldLog+1, 1000, "hello", "world")
	test.makeLogFile(oldLog+2, 1001, "hi", "there")
	test.makeLogFile(oldLog+3, 1002, "foo", "bar2")

	test.open(nil)
	util.AssertLessThanOrEqual(1, test.numTables(), "numTables", t)
	util.AssertLessThanOrEqual(1, test.numLogs(), "numLogs", t)
	newLog := test.firstLogFile()
	util.AssertLessThanOrEqual(oldLog+3, newLog, "newLog", t)
	util.AssertEqual("bar2", test.get("foo", nil), "get", t)
	util.AssertEqual("world", test.get("hello", nil), "get", t)
	util.AssertEqual("there", test.get("hi", nil), "get", t)

	test.open(nil)
	util.AssertLessThanOrEqual(1, test.numTables(), "numTables", t)
	util.AssertLessThanOrEqual(1, test.numLogs(), "numLogs", t)
	if test.canAppend() {
		util.AssertEqual(newLog, test.firstLogFile(), "firstLogFile", t)
	}
	util.AssertEqual("bar2", test.get("foo", nil), "get", t)
	util.AssertEqual("world", test.get("hello", nil), "get", t)
	util.AssertEqual("there", test.get("hi", nil), "get", t)

	test.close()
	test.makeLogFile(oldLog+1, 2000, "hello", "stale write")
	test.open(nil)
	util.AssertLessThanOrEqual(1, test.numTables(), "numTables", t)
	util.AssertEqual(1, test.numLogs(), "numLogs", t)
	if test.canAppend() {
		util.AssertEqual(newLog, test.firstLogFile(), "firstLogFile", t)
	}
	util.AssertEqual("bar2", test.get("foo", nil), "get", t)
	util.AssertEqual("world", test.get("hello", nil), "get", t)
	util.AssertEqual("there", test.get("hi", nil), "get", t)
}

func TestManifestMissing(t *testing.T) {
	test := newRecoveryTest(t)
	util.AssertNotError(test.put("foo", "bar"), "put", t)
	test.close()
	test.deleteManifestFile()
	err := test.openWithStatus(nil)
	util.AssertTrue(ssdb.IsCorruption(err), "IsCorruption", t)
}
