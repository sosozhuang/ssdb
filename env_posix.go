// +build darwin dragonfly freebsd linux netbsd openbsd solaris

package ssdb

import (
	"math"
	"os"
	"runtime"
	"ssdb/util"
	"sync"
	"syscall"
)

func DefaultEnv() Env {
	once.Do(func() {
		e = &posixEnv{
			env: &env{
				bgStarted:   0,
				workItemCh:  make(chan *backgroundWorkItem, 2*runtime.NumCPU()),
				stopped:     make(chan struct{}, 1),
				mmapLimiter: newLimiter(maxMmap()),
			},
			fileLimiter: newLimiter(maxOpenFiles()),
			locks:       newLockTable(),
		}
	})
	return e
}

func maxOpenFiles() int {
	if openReadOnlyFileLimit > 0 {
		return openReadOnlyFileLimit
	}
	rlimit := new(syscall.Rlimit)
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, rlimit); err != nil {
		openReadOnlyFileLimit = 50
	} else if rlimit.Cur == math.MaxUint64 {
		openReadOnlyFileLimit = math.MaxInt64
	} else {
		openReadOnlyFileLimit = int(rlimit.Cur / 5)
	}
	return openReadOnlyFileLimit
}

type posixEnv struct {
	*env
	fileLimiter limiter
	locks       lockTable
}

type posixFileLock struct {
	file *os.File
}

func (e *posixEnv) LockFile(name string) (lock FileLock, err error) {
	var file *os.File
	if file, err = os.OpenFile(name, os.O_RDWR|os.O_CREATE, 0644); err != nil {
		err = envError(name, err)
		return
	}
	if !e.locks.insert(name) {
		_ = file.Close()
		err = util.IOError2("lock "+name, "already held by process")
		return
	}
	if err = lockOrUnlock(file, true); err != nil {
		_ = file.Close()
		e.locks.remove(name)
		err = envError("lock "+name, err)
		return
	}
	lock = &posixFileLock{file}
	return
}

func (e *posixEnv) UnlockFile(lock FileLock) (err error) {
	l := lock.(*posixFileLock)
	if err = lockOrUnlock(l.file, false); err != nil {
		err = envError("unlock "+l.file.Name(), err)
		return
	}
	e.locks.remove(l.file.Name())
	_ = l.file.Close()
	return
}

type lockTable struct {
	mu          sync.Mutex
	lockedFiles map[string]bool
}

func newLockTable() lockTable {
	return lockTable{lockedFiles: make(map[string]bool)}
}

func (t *lockTable) insert(name string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.lockedFiles[name]; ok {
		return false
	}
	t.lockedFiles[name] = true
	return true
}

func (t *lockTable) remove(name string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.lockedFiles, name)
}

func lockOrUnlock(file *os.File, lock bool) error {
	lk := &syscall.Flock_t{
		Whence: 0,
		Start:  0,
		Len:    0,
	}
	if lock {
		lk.Type = syscall.F_WRLCK
	} else {
		lk.Type = syscall.F_UNLCK
	}
	return syscall.FcntlFlock(file.Fd(), syscall.F_SETLK, lk)
}

func (e *posixEnv) NewRandomAccessFile(name string) (f RandomAccessFile, err error) {
	var file *os.File
	if file, err = os.OpenFile(name, os.O_RDONLY, 0); err != nil {
		err = envError(name, err)
		return
	}
	if !e.mmapLimiter.acquire() {
		f = newPosixRandomAccessFile(file, &e.fileLimiter)
		return
	}
	var fileSize int
	if fileSize, err = e.GetFileSize(name); err == nil {
		var data []byte
		data, err = syscall.Mmap(int(file.Fd()), 0, fileSize, syscall.PROT_READ, syscall.MAP_SHARED)
		if err != nil {
			err = envError(name, err)
		} else {
			f = newPosixMmapReadableFile(name, data, fileSize, &e.mmapLimiter)
		}
	}
	_ = file.Close()
	if err != nil {
		e.mmapLimiter.release()
	}
	return
}

type posixRandomAccessFile struct {
	file           *os.File
	hasPermanentFD bool
	limiter        *limiter
	filename       string
}

func newPosixRandomAccessFile(file *os.File, limiter *limiter) RandomAccessFile {
	f := &posixRandomAccessFile{
		hasPermanentFD: limiter.acquire(),
		limiter:        limiter,
		filename:       file.Name(),
	}
	if !f.hasPermanentFD {
		_ = file.Close()
	} else {
		f.file = file
	}
	return f
}

func (f *posixRandomAccessFile) Read(b []byte, offset int64) (result []byte, n int, err error) {
	var file *os.File
	if !f.hasPermanentFD {
		if file, err = os.OpenFile(f.filename, os.O_RDONLY, 0); err != nil {
			err = envError(f.filename, err)
			return
		}
	}
	if file == nil {
		panic("randomAccessFile: file == nil")
	}
	n, err = file.ReadAt(b, offset)
	if n < 0 || err != nil {
		err = envError(f.filename, err)
		return
	}
	if !f.hasPermanentFD {
		_ = file.Close()
		file = nil
	}
	result = b
	return
}

func (f *posixRandomAccessFile) Finalize() {
	if f.hasPermanentFD {
		if f.file == nil {
			panic("posixRandomAccessFile: file == nil")
		}
		_ = f.file.Close()
		f.limiter.release()
	}
}

type posixMmapReadableFile struct {
	data     []byte
	length   int
	limiter  *limiter
	filename string
}

func newPosixMmapReadableFile(filename string, data []byte, length int, limiter *limiter) RandomAccessFile {
	f := &posixMmapReadableFile{
		data:     data,
		length:   length,
		limiter:  limiter,
		filename: filename,
	}
	return f
}

func (f *posixMmapReadableFile) Read(b []byte, offset int64) (result []byte, n int, err error) {
	n = len(b)
	if offset+int64(n) > int64(f.length) {
		err = envError(f.filename, syscall.EINVAL)
		return
	}
	result = f.data[offset : offset+int64(n)]
	return
}

func (f *posixMmapReadableFile) Finalize() {
	syscall.Munmap(f.data)
	f.limiter.release()
}
