package db

import (
	"bytes"
	"ssdb"
	"ssdb/util"
	"testing"
)

type memEnvTest struct {
	env ssdb.Env
}

func newMemEnvTest() *memEnvTest {
	return &memEnvTest{env: newMemEnv(ssdb.DefaultEnv())}
}

func (t *memEnvTest) finish() {
	t.env.(*inMemoryEnv).finalize()
}

func TestBasics(t *testing.T) {
	test := newMemEnvTest()
	util.AssertNotError(test.env.CreateDir("/dir"), "CreateDir", t)

	util.AssertFalse(test.env.FileExists("/dir/non_existent"), "FileExists", t)
	_, err := test.env.GetFileSize("/dir/non_existent")
	util.AssertError(err, "GetFileSize", t)
	children, err := test.env.GetChildren("/dir")
	util.AssertNotError(err, "GetChildren", t)
	util.AssertEqual(0, len(children), "children", t)

	writableFile, err := test.env.NewWritableFile("/dir/f")
	util.AssertNotError(err, "NewWritableFile", t)
	fileSize, err := test.env.GetFileSize("/dir/f")
	util.AssertNotError(err, "GetFileSize", t)
	util.AssertEqual(0, fileSize, "fileSize", t)
	_ = writableFile.Close()

	util.AssertTrue(test.env.FileExists("/dir/f"), "FileExists", t)
	fileSize, err = test.env.GetFileSize("/dir/f")
	util.AssertEqual(0, fileSize, "GetFileSize", t)
	children, err = test.env.GetChildren("/dir")
	util.AssertEqual(1, len(children), "children", t)
	util.AssertEqual("f", children[0], "children", t)

	writableFile, err = test.env.NewWritableFile("/dir/f")
	util.AssertNotError(err, "NewWritableFile", t)
	util.AssertNotError(writableFile.Append([]byte("abc")), "Append", t)
	_ = writableFile.Close()

	writableFile, err = test.env.NewAppendableFile("/dir/f")
	util.AssertNotError(err, "NewAppendableFile", t)
	fileSize, err = test.env.GetFileSize("/dir/f")
	util.AssertNotError(err, "GetFileSize", t)
	util.AssertEqual(3, fileSize, "fileSize", t)
	util.AssertNotError(writableFile.Append([]byte("hello")), "Append", t)
	_ = writableFile.Close()

	fileSize, err = test.env.GetFileSize("/dir/f")
	util.AssertNotError(err, "GetFileSize", t)
	util.AssertEqual(8, fileSize, "fileSize", t)

	util.AssertError(test.env.RenameFile("/dir/non_existent", "/dir/g"), "RenameFile", t)
	util.AssertNotError(test.env.RenameFile("/dir/f", "/dir/g"), "RenameFile", t)
	util.AssertFalse(test.env.FileExists("/dir/f"), "FileExists", t)
	util.AssertTrue(test.env.FileExists("/dir/g"), "FileExists", t)
	fileSize, err = test.env.GetFileSize("/dir/g")
	util.AssertNotError(err, "GetFileSize", t)
	util.AssertEqual(8, fileSize, "fileSize", t)

	seqFile, err := test.env.NewSequentialFile("/dir/non_existent")
	util.AssertError(err, "NewSequentialFile", t)
	util.AssertTrue(seqFile == nil, "seqFile", t)
	randFile, err := test.env.NewRandomAccessFile("/dir/non_existent")
	util.AssertError(err, "NewRandomAccessFile", t)
	util.AssertTrue(randFile == nil, "randFile", t)

	util.AssertError(test.env.DeleteFile("/dir/non_existent"), "DeleteFile", t)
	util.AssertNotError(test.env.DeleteFile("/dir/g"), "DeleteFile", t)
	util.AssertFalse(test.env.FileExists("/dir/g"), "FileExists", t)
	children, err = test.env.GetChildren("/dir")
	util.AssertNotError(err, "GetChildren", t)
	util.AssertEqual(0, len(children), "children", t)
	util.AssertNotError(test.env.DeleteDir("/dir"), "DeleteDir", t)
}

func TestMemEnvReadWrite(t *testing.T) {
	test := newMemEnvTest()

	util.AssertNotError(test.env.CreateDir("/dir"), "CreateDir", t)

	writableFile, err := test.env.NewWritableFile("/dir/f")
	util.AssertNotError(err, "NewWritableFile", t)
	util.AssertNotError(writableFile.Append([]byte("hello ")), "Append", t)
	util.AssertNotError(writableFile.Append([]byte("world")), "Append", t)
	_ = writableFile.Close()

	seqFile, err := test.env.NewSequentialFile("/dir/f")
	util.AssertNotError(err, "NewSequentialFile", t)
	b := make([]byte, 5)
	result, _, err := seqFile.Read(b)
	util.AssertNotError(err, "Read", t)
	util.AssertEqual(0, bytes.Compare(result, []byte("hello")), "Compare", t)
	util.AssertNotError(seqFile.Skip(1), "Skip", t)
	b = make([]byte, 1000)
	result, _, err = seqFile.Read(b)
	util.AssertNotError(err, "Read", t)
	util.AssertEqual(0, bytes.Compare(result, []byte("world")), "Compare", t)
	result, _, err = seqFile.Read(b)
	util.AssertEqual(0, len(result), "result", t)
	util.AssertNotError(seqFile.Skip(100), "Skip", t)
	result, _, err = seqFile.Read(b)
	util.AssertNotError(err, "Read", t)
	util.AssertEqual(0, len(result), "result", t)
	seqFile.Close()

	randFile, err := test.env.NewRandomAccessFile("/dir/f")
	util.AssertNotError(err, "NewRandomAccessFile", t)
	b = make([]byte, 5)
	result, _, err = randFile.Read(b, 6)
	util.AssertNotError(err, "Read", t)
	util.AssertEqual(0, bytes.Compare(result, []byte("world")), "Compare", t)
	result, _, err = randFile.Read(b, 0)
	util.AssertNotError(err, "Read", t)
	util.AssertEqual(0, bytes.Compare(result, []byte("hello")), "Compare", t)
	b = make([]byte, 100)
	result, _, err = randFile.Read(b, 10)
	util.AssertNotError(err, "Read", t)
	util.AssertEqual(0, bytes.Compare(result, []byte("d")), "Compare", t)

	b = make([]byte, 5)
	_, _, err = randFile.Read(b, 1000)
	util.AssertError(err, "Read", t)
	randFile.Close()
}

func TestLocks(t *testing.T) {
	test := newMemEnvTest()
	lock, err := test.env.LockFile("some file")
	util.AssertNotError(err, "LockFile", t)
	err = test.env.UnlockFile(lock)
	util.AssertNotError(err, "UnlockFile", t)
}

func TestMisc(t *testing.T) {
	test := newMemEnvTest()
	testDir, err := test.env.GetTestDirectory()
	util.AssertNotError(err, "GetTestDirectory", t)
	util.AssertTrue(len(testDir) != 0, "testDir", t)

	writableFile, err := test.env.NewWritableFile("/a/b")
	util.AssertNotError(err, "NewWritableFile", t)

	util.AssertNotError(writableFile.Sync(), "Sync", t)
	util.AssertNotError(writableFile.Flush(), "Flush", t)
	util.AssertNotError(writableFile.Close(), "Close", t)
	_ = writableFile.Close()
}

func TestLargeWrite(t *testing.T) {
	const writeSize = 300 * 1024
	test := newMemEnvTest()

	buf := bytes.NewBufferString("")
	for i := 0; i < writeSize; i++ {
		buf.WriteByte(byte(i))
	}
	writeData := buf.Bytes()

	writableFile, err := test.env.NewWritableFile("/dir/f")
	util.AssertNotError(err, "NewWritableFile", t)
	util.AssertNotError(writableFile.Append([]byte("foo")), "Append", t)
	util.AssertNotError(writableFile.Append(writeData), "Append", t)
	_ = writableFile.Close()

	seqFile, err := test.env.NewSequentialFile("/dir/f")
	util.AssertNotError(err, "NewSequentialFile", t)
	b := make([]byte, 3)
	result, _, err := seqFile.Read(b)
	util.AssertNotError(err, "Read", t)
	util.AssertEqual(0, bytes.Compare(result, []byte("foo")), "Compare", t)

	read := 0
	readData := make([]byte, 0, len(writeData))
	b = make([]byte, writeSize)
	for read < writeSize {
		result, _, err = seqFile.Read(b)
		readData = append(readData, result...)
		read += len(result)
	}
	util.AssertEqual(readData, writeData, "read write data", t)
	seqFile.Close()
}

func TestOverwriteOpenFile(t *testing.T) {
	test := newMemEnvTest()

	write1Data := "Write #1 data"
	fileDataLen := len(write1Data)
	testFileName := tmpDir() + "/ssdb-TestFile.dat"

	err := ssdb.WriteStringToFile(test.env, write1Data, testFileName)
	util.AssertNotError(err, "WriteStringToFile", t)

	randFile, err := test.env.NewRandomAccessFile(testFileName)
	util.AssertNotError(err, "NewRandomAccessFile", t)

	write2Data := "Write #2 data"
	err = ssdb.WriteStringToFile(test.env, write2Data, testFileName)
	util.AssertNotError(err, "WriteStringToFile", t)

	b := make([]byte, fileDataLen)
	result, _, err := randFile.Read(b, 0)
	util.AssertNotError(err, "Read", t)
	util.AssertEqual(0, bytes.Compare(result, []byte(write2Data)), "Compare", t)

	randFile.Close()
}

func TestDBTest(t *testing.T) {
	test := newMemEnvTest()
	options := ssdb.NewOptions()
	options.CreateIfMissing = true
	options.Env = test.env

	keys := []string{"aaa", "bbb", "ccc"}
	vals := []string{"foo", "bar", "baz"}

	d, err := Open(options, "/dir/db")
	util.AssertNotError(err, "Open", t)
	for i := 0; i < 3; i++ {
		err = d.Put(ssdb.NewWriteOptions(), []byte(keys[i]), []byte(vals[i]))
		util.AssertNotError(err, "Put", t)
	}

	for i := 0; i < 3; i++ {
		res, err := d.Get(ssdb.NewReadOptions(), []byte(keys[i]))
		util.AssertNotError(err, "Get", t)
		util.AssertEqual(string(res), vals[i], "value", t)
	}

	iterator := d.NewIterator(ssdb.NewReadOptions())
	iterator.SeekToFirst()
	for i := 0; i < 3; i++ {
		util.AssertTrue(iterator.Valid(), "Valid", t)
		util.AssertEqual([]byte(keys[i]), iterator.Key(), "Key", t)
		util.AssertEqual([]byte(vals[i]), iterator.Value(), "Value", t)
		iterator.Next()
	}
	util.AssertFalse(iterator.Valid(), "Valid", t)
	iterator.Close()

	dbi := d.(*db)
	util.AssertNotError(dbi.testCompactMemTable(), "testCompactMemTable", t)

	for i := 0; i < 3; i++ {
		res, err := d.Get(ssdb.NewReadOptions(), []byte(keys[i]))
		util.AssertNotError(err, "Get", t)
		util.AssertEqual(string(res), vals[i], "value", t)
	}
	d.Close()
}
