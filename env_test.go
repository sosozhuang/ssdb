package ssdb

import (
	"ssdb/util"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

const delayMicros = 100000

func setAtomicBool(i interface{}) {
	b := i.(*uint32)
	atomic.StoreUint32(b, 1)
}

func TestReadWrite(t *testing.T) {
	rnd := util.NewRandom(uint32(util.RandomSeed()))
	env := DefaultEnv()
	testDir, err := env.GetTestDirectory()
	util.TestNotError(err, "GetTestDirectory", t)
	testFileName := testDir + "/open_on_read.txt"
	writableFile, err := env.NewWritableFile(testFileName)
	util.TestNotError(err, "NewWritableFile", t)

	const dataSize = 10 * 1048576
	var (
		b strings.Builder
		l int
		r []byte
	)
	for b.Len() < dataSize {
		l = int(rnd.Skewed(18))
		r = []byte(util.RandomString(rnd, l))
		util.TestNotError(writableFile.Append(r), "WritableFile.Append", t)
		b.Write(r)
		if rnd.OneIn(10) {
			util.TestNotError(writableFile.Flush(), "WritableFile.Flush", t)
		}
	}
	data := b.String()
	util.TestNotError(writableFile.Sync(), "Writable.Sync", t)
	util.TestNotError(writableFile.Close(), "Writable.Close", t)
	writableFile = nil

	sequentialFile, err := env.NewSequentialFile(testFileName)
	util.TestNotError(err, "Env.NewSequentialFile", t)
	var (
		readResult strings.Builder
		d          int
		read       []byte
		scratch    []byte
	)
	for readResult.Len() < len(data) {
		l = int(rnd.Skewed(18))
		if d = len(data) - readResult.Len(); l > d {
			l = d
		}
		if l > 1 {
			scratch = make([]byte, l)
		} else {
			scratch = make([]byte, 1)
		}
		read, _, err = sequentialFile.Read(scratch)
		if l > 0 {
			util.TestTrue(len(read) > 0, "", t)
		}
		util.TestTrue(len(read) <= l, "", t)
		readResult.Write(read)
	}
	util.TestEqual(readResult.String(), data, "", t)
}

func TestRunImmediately(t *testing.T) {
	called := uint32(0)
	env := DefaultEnv()
	env.Schedule(setAtomicBool, &called)
	env.SleepForMicroseconds(delayMicros)
	util.TestTrue(atomic.LoadUint32(&called) == 1, "Env.Schedule", t)
}

type callback struct {
	lastId *int32
	id     int32
	t      *testing.T
}

func run(arg interface{}) {
	c := arg.(*callback)
	currentId := atomic.LoadInt32(c.lastId)
	util.TestEqual(c.id-1, currentId, "callback id", c.t)
	atomic.StoreInt32(c.lastId, c.id)
}

func TestRunMany(t *testing.T) {
	lastId := int32(0)
	callback1 := callback{&lastId, 1, t}
	callback2 := callback{&lastId, 2, t}
	callback3 := callback{&lastId, 3, t}
	callback4 := callback{&lastId, 4, t}
	env := DefaultEnv()
	env.Schedule(run, &callback1)
	env.Schedule(run, &callback2)
	env.Schedule(run, &callback3)
	env.Schedule(run, &callback4)

	env.SleepForMicroseconds(delayMicros)
	util.TestEqual(int32(4), atomic.LoadInt32(&lastId), "callback.lastId", t)
}

type state struct {
	mu         sync.Mutex
	val        int
	numRunning int
}

func ThreadBody(arg interface{}) {
	s := arg.(*state)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.val += 1
	s.numRunning -= 1
}

func TestStartThread(t *testing.T) {
	state := &state{
		val:        0,
		numRunning: 3,
	}
	env := DefaultEnv()
	for i := 0; i < 3; i++ {
		env.StartThread(ThreadBody, state)
	}
	var num int
	for {
		state.mu.Lock()
		num = state.numRunning
		state.mu.Unlock()
		if num == 0 {
			break
		}
		env.SleepForMicroseconds(delayMicros)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	util.TestEqual(3, state.val, "state.val", t)
}

func TestOpenNonExistentFile(t *testing.T) {
	env := DefaultEnv()
	testDir, err := env.GetTestDirectory()
	util.TestNotError(err, "Env.GetTestDirectory", t)
	nonExistentFile := testDir + "/non_existent_file"
	util.TestFalse(env.FileExists(nonExistentFile), "Env.FileExists", t)

	_, err = env.NewRandomAccessFile(nonExistentFile)
	util.TestTrue(IsNotFound(err), "Env.NewRandomAccessFile", t)
	_, err = env.NewSequentialFile(nonExistentFile)
	util.TestTrue(IsNotFound(err), "Env.NewSequentialFile", t)
}

func TestReopenWritableFile(t *testing.T) {
	env := DefaultEnv()
	testDir, err := env.GetTestDirectory()
	util.TestNotError(err, "Env.GetTestDirectory", t)
	testFileName := testDir + "/reopen_writable_file.txt"
	_ = env.DeleteFile(testFileName)

	writableFile, err := env.NewWritableFile(testFileName)
	util.TestNotError(err, "Env.NewWritableFile", t)
	err = writableFile.Append([]byte("hello world!"))
	util.TestNotError(err, "WritableFile.Append", t)
	err = writableFile.Close()
	util.TestNotError(err, "WritableFile.Close", t)
	writableFile = nil

	writableFile, err = env.NewWritableFile(testFileName)
	util.TestNotError(err, "Env.NewWritableFile", t)
	err = writableFile.Append([]byte("42"))
	util.TestNotError(err, "WritableFile.Append", t)
	err = writableFile.Close()
	util.TestNotError(err, "WritableFile.Close", t)

	b, err := ReadFileToString(env, testFileName)
	util.TestEqual("42", string(b), "ReadFileToString", t)
	_ = env.DeleteFile(testFileName)
}

func TestReopenAppendableFile(t *testing.T) {
	env := DefaultEnv()
	testDir, err := env.GetTestDirectory()
	util.TestNotError(err, "Env.GetTestDirectory", t)
	testFileName := testDir + "/reopen_appendable_file.txt"
	_ = env.DeleteFile(testFileName)

	appendableFile, err := env.NewAppendableFile(testFileName)
	util.TestNotError(err, "Env.NewAppendableFile", t)
	err = appendableFile.Append([]byte("hello world!"))
	util.TestNotError(err, "AppendableFile.Append", t)
	err = appendableFile.Close()
	util.TestNotError(err, "AppendableFile.Close", t)
	appendableFile = nil

	appendableFile, err = env.NewAppendableFile(testFileName)
	util.TestNotError(err, "Env.NewAppendableFile", t)
	err = appendableFile.Append([]byte("42"))
	util.TestNotError(err, "AppendableFile.Append", t)
	err = appendableFile.Close()
	util.TestNotError(err, "AppendableFile.Close", t)

	b, err := ReadFileToString(env, testFileName)
	util.TestEqual("hello world!42", string(b), "ReadFileToString", t)
	_ = env.DeleteFile(testFileName)
}
