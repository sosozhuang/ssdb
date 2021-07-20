package ssdb

import (
	"errors"
	"os"
	"path"
	"reflect"
	"runtime"
	"strconv"
	"syscall"
	"unsafe"
)

var (
	modKernel32      = syscall.NewLazyDLL("kernel32.dll")
	procLockFileEx   = modKernel32.NewProc("LockFileEx")
	procCreateEventW = modKernel32.NewProc("CreateEventW")
)

const (
	lockFileExclusiveLock = 2
)

func DefaultEnv() Env {
	once.Do(func() {
		e = &windowsEnv{
			env: &env{
				bgStarted:   0,
				workItemCh:  make(chan *backgroundWorkItem, 2*runtime.NumCPU()),
				stopped:     make(chan struct{}, 1),
				mmapLimiter: newLimiter(maxMmap()),
			},
		}
	})
	return e
}

func (f *writableFile) Sync() (err error) {
	if err = f.flushBuffer(); err != nil {
		return
	}
	err = syncFD(f.file, f.filename)
	return
}

type windowsEnv struct {
	*env
}

type windowsFileLock struct {
	handle syscall.Handle
	name   string
}

func (e *windowsEnv) LockFile(filename string) (lock FileLock, err error) {
	var name *uint16
	if name, err = syscall.UTF16PtrFromString(filename); err != nil {
		return
	}
	var handle syscall.Handle
	if handle, err = syscall.CreateFile(name, syscall.GENERIC_READ|syscall.GENERIC_WRITE, syscall.FILE_SHARE_READ, nil, syscall.OPEN_ALWAYS, syscall.FILE_ATTRIBUTE_NORMAL, 0); err != nil {
		err = envError(filename, err)
		return
	}
	defer func() {
		if err != nil {
			_ = syscall.Close(handle)
		}
	}()
	if handle == syscall.InvalidHandle {
		err = envError(filename, err)
		return
	}
	if err = lockOrUnlock(handle, true); err != nil {
		err = envError("lock "+filename, err)
		return
	}
	lock = &windowsFileLock{
		handle: handle,
		name:   filename,
	}
	return
}

func (e *windowsEnv) UnlockFile(lock FileLock) (err error) {
	l := lock.(*windowsFileLock)
	if err = lockOrUnlock(l.handle, false); err != nil {
		err = envError("unlock", err)
	}
	return
}

func lockOrUnlock(handle syscall.Handle, lock bool) error {
	if lock {
		ol, err := newOverlapped()
		if err != nil {
			return err
		}
		defer syscall.CloseHandle(ol.HEvent)
		if err = lockFileEx(handle, lockFileExclusiveLock, 0, 1, 0, ol); err == nil {
			return nil
		}
		if err != syscall.ERROR_IO_PENDING {
			return err
		}
		s, err := syscall.WaitForSingleObject(ol.HEvent, syscall.INFINITE)
		switch s {
		case syscall.WAIT_OBJECT_0:
			return nil
		case syscall.WAIT_TIMEOUT:
			return errors.New("lock timeout")
		default:
			return err
		}
	} else {
		return syscall.Close(handle)
	}
}

func newOverlapped() (*syscall.Overlapped, error) {
	event, err := createEvent(nil, true, false, nil)
	if err != nil {
		return nil, err
	}
	return &syscall.Overlapped{HEvent: event}, nil
}

func createEvent(sa *syscall.SecurityAttributes, manualReset bool, initialState bool, name *uint16) (handle syscall.Handle, err error) {
	var p0 uint32
	if manualReset {
		p0 = 1
	}
	var p1 uint32
	if initialState {
		p1 = 2
	}
	r0, _, e1 := syscall.Syscall6(procCreateEventW.Addr(), 4, uintptr(unsafe.Pointer(sa)), uintptr(p0), uintptr(p1), uintptr(unsafe.Pointer(name)), 0, 0)
	handle = syscall.Handle(r0)
	if handle == syscall.InvalidHandle {
		if e1 != 0 {
			err = e1
		} else {
			err = syscall.EINVAL
		}
	}
	return
}

func lockFileEx(h syscall.Handle, flags, reserved, low, high uint32, ol *syscall.Overlapped) (err error) {
	r1, _, e1 := syscall.Syscall6(procLockFileEx.Addr(), 6, uintptr(h), uintptr(flags), uintptr(reserved), uintptr(low), uintptr(high), uintptr(unsafe.Pointer(ol)))
	if r1 == 0 {
		if e1 != 0 {
			err = e1
		} else {
			err = syscall.EINVAL
		}
	}
	return
}

func (e *windowsEnv) GetTestDirectory() (string, error) {
	result, ok := os.LookupEnv("TEST_TMPDIR")
	if result == "" || !ok {
		result = path.Join(os.TempDir(), "ssdbtest-"+strconv.Itoa(os.Getuid()))
	}
	// The CreateDir status is ignored because the directory may already exist.
	_ = e.CreateDir(result)
	return result, nil
}

func (e *windowsEnv) NewRandomAccessFile(name string) (f RandomAccessFile, err error) {
	var file *os.File
	if file, err = os.OpenFile(name, os.O_RDONLY, 0); err != nil {
		err = envError(name, err)
		return
	}
	if !e.mmapLimiter.acquire() {
		f = newWindowsRandomAccessFile(file)
		return
	}
	defer func() {
		if err != nil {
			e.mmapLimiter.release()
		}
	}()
	var info os.FileInfo
	if info, err = os.Stat(name); err != nil {
		err = envError(name, err)
		return
	}
	var mapping syscall.Handle
	if mapping, err = syscall.CreateFileMapping(syscall.Handle(file.Fd()), nil, syscall.PAGE_READONLY, 0, 0, nil); err != nil {
		err = envError(name, err)
		return
	}
	if syscall.InvalidHandle == mapping {
		err = envError(name, errors.New("invalid handle"))
		return
	}
	defer syscall.CloseHandle(mapping)
	var base uintptr
	if base, err = syscall.MapViewOfFile(mapping, syscall.FILE_MAP_READ, 0, 0, 0); err != nil {
		err = envError(name, err)
		return
	}
	f = newWindowsMmapReadableFile(name, unsafe.Pointer(base), int(info.Size()), &e.mmapLimiter)
	return
}

type windowsRandomAccessFile struct {
	file *os.File
}

func newWindowsRandomAccessFile(file *os.File) RandomAccessFile {
	f := &windowsRandomAccessFile{
		file: file,
	}
	return f
}

func (f *windowsRandomAccessFile) Read(b []byte, offset int64) (result []byte, n int, err error) {
	n, err = f.file.ReadAt(b, offset)
	if n < 0 || err != nil {
		err = envError(f.file.Name(), err)
		return
	}
	result = b
	return
}

func (f *windowsRandomAccessFile) Close() {
	_ = f.file.Close()
}

type windowsMmapReadableFile struct {
	pointer  unsafe.Pointer
	data     []byte
	length   int
	limiter  *limiter
	filename string
}

func newWindowsMmapReadableFile(filename string, pointer unsafe.Pointer, length int, limiter *limiter) RandomAccessFile {
	f := &windowsMmapReadableFile{
		pointer: pointer,
		data: *(*[]byte)(unsafe.Pointer(&reflect.SliceHeader{
			Data: uintptr(pointer),
			Len:  length,
			Cap:  length,
		})),
		length:   length,
		limiter:  limiter,
		filename: filename,
	}
	return f
}

func (f *windowsMmapReadableFile) Read(b []byte, offset int64) (result []byte, n int, err error) {
	n = len(b)
	if offset+int64(n) > int64(f.length) {
		err = envError(f.filename, syscall.EINVAL)
		return
	}
	result = f.data[offset : offset+int64(n)]
	return
}

func (f *windowsMmapReadableFile) Close() {
	_ = syscall.UnmapViewOfFile(uintptr(f.pointer))
	f.limiter.release()
}
