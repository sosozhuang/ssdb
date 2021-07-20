package ssdb

import (
	"io"
	"log"
	"os"
	"path"
	"runtime"
	"ssdb/util"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"
)

type Env interface {
	NewSequentialFile(name string) (SequentialFile, error)
	NewRandomAccessFile(name string) (RandomAccessFile, error)
	NewWritableFile(name string) (WritableFile, error)
	NewAppendableFile(name string) (WritableFile, error)
	FileExists(name string) bool
	GetChildren(dir string) ([]string, error)
	DeleteFile(name string) error
	CreateDir(name string) error
	DeleteDir(name string) error
	GetFileSize(name string) (int, error)
	RenameFile(from, to string) error
	LockFile(name string) (FileLock, error)
	UnlockFile(lock FileLock) error
	Schedule(function func(interface{}), arg interface{})
	StartThread(function func(interface{}), arg interface{})
	GetTestDirectory() (string, error)
	NewLogger(name string) (*log.Logger, func(), error)
	NowMicros() uint64
	SleepForMicroseconds(micros int)
}

type SequentialFile interface {
	// Read up to "n" bytes from the file.  "scratch[0..n-1]" may be
	// written by this routine.  Sets "*result" to the data that was
	// read (including if fewer than "n" bytes were successfully read).
	// May set "*result" to point at data in "scratch[0..n-1]", so
	// "scratch[0..n-1]" must be live when "*result" is used.
	// If an error was encountered, returns a non-OK status.
	//
	// REQUIRES: External synchronization
	Read(b []byte) ([]byte, int, error)
	Skip(n uint64) error
	Closer
}

type RandomAccessFile interface {
	// Read up to "n" bytes from the file starting at "offset".
	// "scratch[0..n-1]" may be written by this routine.  Sets "*result"
	// to the data that was read (including if fewer than "n" bytes were
	// successfully read).  May set "*result" to point at data in
	// "scratch[0..n-1]", so "scratch[0..n-1]" must be live when
	// "*result" is used.  If an error was encountered, returns a non-OK
	// status.
	//
	// Safe for concurrent use by multiple threads.
	Read(b []byte, offset int64) ([]byte, int, error)
	Closer
}

type WritableFile interface {
	Append(data []byte) error
	Close() error
	Flush() error
	Sync() error
}

type FileLock interface {
}

func WriteStringToFile(env Env, data string, name string) error {
	return doWriteBytesToFile(env, data, name, false)
}

func WriteStringToFileSync(env Env, data string, name string) error {
	return doWriteBytesToFile(env, data, name, true)
}

func doWriteBytesToFile(env Env, data string, name string, shouldSync bool) error {
	file, err := env.NewWritableFile(name)
	if err != nil {
		return err
	}
	err = file.Append([]byte(data))
	if err == nil && shouldSync {
		err = file.Sync()
	}
	if err == nil {
		err = file.Close()
	} else {
		_ = file.Close()
	}
	if err != nil {
		_ = env.DeleteFile(name)
	}
	return err
}

func ReadFileToString(env Env, name string) (string, error) {
	file, err := env.NewSequentialFile(name)
	if err != nil {
		return "", err
	}
	const bufferSize = 8192
	space := make([]byte, bufferSize)
	var data strings.Builder
	var r []byte
	var n int
	for {
		r, n, err = file.Read(space)
		if err != nil || n == 0 {
			break
		}
		data.Write(r)
	}
	file.Close()
	return data.String(), err
}

type EnvWrapper struct {
	Env
}

func (w *EnvWrapper) Target() Env {
	return w.Env
}

const (
	writableFileBufferSize = 65536
)

var openReadOnlyFileLimit = -1
var defaultMmapLimit int
var mmapLimit int

type limiter struct {
	acquiresAllowed int64
}

func newLimiter(maxAcquires int) limiter {
	return limiter{acquiresAllowed: int64(maxAcquires)}
}

func (l *limiter) acquire() bool {
	n := atomic.AddInt64(&l.acquiresAllowed, -1)
	if n >= 0 {
		return true
	}
	atomic.AddInt64(&l.acquiresAllowed, 1)
	return false
}

func (l *limiter) release() {
	atomic.AddInt64(&l.acquiresAllowed, 1)
}

type env struct {
	bgStarted   int32
	workItemCh  chan *backgroundWorkItem
	stopped     chan struct{}
	wg          sync.WaitGroup
	mmapLimiter limiter
}

func (e *env) NewSequentialFile(name string) (SequentialFile, error) {
	return newSequentialFile(name)
}

func (e *env) NewWritableFile(name string) (f WritableFile, err error) {
	var file *os.File
	if file, err = os.OpenFile(name, os.O_TRUNC|os.O_WRONLY|os.O_CREATE, 0644); err != nil {
		err = envError(name, err)
		return
	}
	f = newWritableFile(file)
	return
}

func (e *env) NewAppendableFile(name string) (f WritableFile, err error) {
	var file *os.File
	if file, err = os.OpenFile(name, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644); err != nil {
		err = envError(name, err)
		return
	}
	f = newWritableFile(file)
	return
}

func (e *env) FileExists(name string) bool {
	_, err := os.Stat(name)
	return err == nil
}

func (e *env) GetChildren(dir string) (result []string, err error) {
	var file *os.File
	if file, err = os.Open(dir); err != nil {
		err = envError(dir, err)
		return
	}
	if result, err = file.Readdirnames(-1); err != nil {
		err = envError(dir, err)
	}
	return
}

func (e *env) DeleteFile(name string) (err error) {
	if err = os.Remove(name); err != nil {
		err = envError(name, err)
	}
	return
}

func (e *env) CreateDir(name string) (err error) {
	if err = os.Mkdir(name, 0755); err != nil {
		err = envError(name, err)
	}
	return
}

func (e *env) DeleteDir(name string) (err error) {
	if err = os.Remove(name); err != nil {
		err = envError(name, err)
	}
	return
}

func (e *env) GetFileSize(name string) (size int, err error) {
	var info os.FileInfo
	if info, err = os.Stat(name); err != nil {
		err = envError(name, err)
		return
	}
	size = int(info.Size())
	return
}

func (e *env) RenameFile(from, to string) (err error) {
	if err = os.Rename(from, to); err != nil {
		err = envError(from, err)
	}
	return
}

func (e *env) Schedule(function func(interface{}), arg interface{}) {
	if atomic.CompareAndSwapInt32(&e.bgStarted, 0, 1) {
		go e.backgroundRoutine()
		runtime.SetFinalizer(e, func(e *env) {
			e.stopped <- struct{}{}
			e.wg.Wait()
		})
	}
	i := &backgroundWorkItem{
		function: function,
		arg:      arg,
	}
	e.workItemCh <- i
}

func (e *env) backgroundRoutine() {
	e.wg.Add(1)
	defer e.wg.Done()
	for {
		select {
		case <-e.stopped:
			return
		case i := <-e.workItemCh:
			i.function(i.arg)
		}
	}
}

func (e *env) StartThread(function func(interface{}), arg interface{}) {
	go function(arg)
}

func (e *env) NewLogger(name string) (l *log.Logger, close func(), err error) {
	var f *os.File
	if f, err = os.OpenFile(name, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644); err != nil {
		close = func() {}
		return
	}
	l = log.New(f, "", log.LstdFlags)
	close = func() {
		_ = f.Close()
	}
	return
}

func (e *env) NowMicros() uint64 {
	return uint64(time.Now().UnixNano() / int64(time.Microsecond/time.Nanosecond))
}

func (e *env) SleepForMicroseconds(micros int) {
	time.Sleep(time.Microsecond * time.Duration(micros))
}

type backgroundWorkItem struct {
	function func(interface{})
	arg      interface{}
}

var e Env
var once sync.Once

func init() {
	if unsafe.Sizeof(uintptr(0)) >= 8 {
		defaultMmapLimit = 1000
	} else {
		defaultMmapLimit = 0
	}
	mmapLimit = defaultMmapLimit
}

func maxMmap() int {
	return mmapLimit
}

type sequentialFile struct {
	filename string
	file     *os.File
}

func (f *sequentialFile) Read(b []byte) (result []byte, n int, err error) {
	for {
		n, err = f.file.Read(b)
		if n < 0 {
			if err == syscall.EINTR {
				continue
			}
			err = envError(f.filename, err)
			break
		}
		result = b[:n]
		break
	}
	if n == 0 && err == io.EOF {
		err = nil
	}
	return
}

func (f *sequentialFile) Skip(n uint64) error {
	if ret, err := f.file.Seek(int64(n), io.SeekCurrent); err != nil {
		return envError(f.filename, err)
	} else if ret == -1 {
		return envError(f.filename, nil)
	}
	return nil
}

func (f *sequentialFile) Close() {
	_ = f.file.Close()
}

func envError(filename string, err error) error {
	if err == nil {
		return util.IOError1(filename)
	} else if os.IsNotExist(err) {
		return util.NotFoundError2(filename, err.Error())
	} else {
		return util.IOError2(filename, err.Error())
	}
}

func newSequentialFile(filename string) (SequentialFile, error) {
	file, err := os.OpenFile(filename, os.O_RDONLY, 0)
	if err != nil {
		return nil, envError(filename, err)
	}
	f := &sequentialFile{
		filename: filename,
		file:     file,
	}
	return f, nil
}

func isManifest(filename string) bool {
	return strings.HasPrefix(path.Base(filename), "MANIFEST")
}

type writableFile struct {
	buf        [writableFileBufferSize]byte
	pos        int
	file       *os.File
	isManifest bool
	filename   string
	dirname    string
}

func newWritableFile(file *os.File) *writableFile {
	filename := file.Name()
	f := &writableFile{
		buf:        [65536]byte{},
		pos:        0,
		file:       file,
		isManifest: isManifest(filename),
		filename:   filename,
		dirname:    path.Dir(filename),
	}
	return f
}

func (f *writableFile) Append(data []byte) (err error) {
	writeSize := len(data)
	copySize := writableFileBufferSize - f.pos
	if copySize > writeSize {
		copySize = writeSize
	}
	copy(f.buf[f.pos:], data[:copySize])
	writeSize -= copySize
	f.pos += copySize
	if writeSize == 0 {
		return
	}
	if err = f.flushBuffer(); err != nil {
		return
	}
	if writeSize < writableFileBufferSize {
		copy(f.buf[:], data[copySize:])
		f.pos = writeSize
		return
	}
	err = f.writeUnbuffered(data[copySize:])
	return
}

func (f *writableFile) Close() (err error) {
	if f.file != nil {
		err = f.flushBuffer()
		if e := f.file.Close(); e != nil && err == nil {
			err = envError(f.filename, e)
		}
		f.file = nil
	}
	return
}

func (f *writableFile) Flush() error {
	return f.flushBuffer()
}

//func (f *writableFile) Finalize() {
//	if f.file != nil {
//		_ = f.Close()
//	}
//}

func (f *writableFile) flushBuffer() error {
	err := f.writeUnbuffered(f.buf[:f.pos])
	f.pos = 0
	return err
}

func (f *writableFile) writeUnbuffered(data []byte) error {
	size := len(data)
	start := 0
	for size > 0 {
		n, err := f.file.Write(data[start:])
		if n <= 0 {
			if err == syscall.EINTR {
				continue
			}
			return envError(f.filename, err)
		}
		start += n
		size -= n
	}
	return nil
}

func (f *writableFile) syncDirIfManifest() error {
	if !f.isManifest {
		return nil
	}
	file, err := os.OpenFile(f.dirname, os.O_RDONLY, 0)
	if err != nil {
		return envError(f.filename, err)
	}
	defer file.Close()
	return syncFD(file, f.dirname)
}

func syncFD(file *os.File, path string) (err error) {
	//todo: call fdatasync
	if err = file.Sync(); err != nil {
		err = envError(path, err)
	}
	return
}
