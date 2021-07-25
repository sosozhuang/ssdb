package db

import (
	"io/ioutil"
	"log"
	"ssdb"
	"ssdb/util"
	"strings"
	"sync"
)

type envFileState struct {
	refsMutex   sync.Mutex
	refs        int
	blocksMutex sync.Mutex
	blocks      []byte
}

func newEnvFileState() *envFileState {
	return &envFileState{
		refs:   0,
		blocks: make([]byte, 0),
	}
}

func (s *envFileState) ref() {
	s.refsMutex.Lock()
	defer s.refsMutex.Unlock()
	s.refs++
}

func (s *envFileState) unref() {
	doDelete := false
	s.refsMutex.Lock()
	s.refs--
	if s.refs < 0 {
		panic("refs < 0")
	}
	if s.refs <= 0 {
		doDelete = true
	}
	s.refsMutex.Unlock()
	if doDelete {
		s.finalize()
	}
}

func (s *envFileState) size() int {
	s.blocksMutex.Lock()
	defer s.blocksMutex.Unlock()
	return len(s.blocks)
}

func (s *envFileState) truncate() {
	s.blocksMutex.Lock()
	defer s.blocksMutex.Unlock()
	s.blocks = make([]byte, 0)
}

func (s *envFileState) read(offset uint64, b []byte) (result []byte, n int, err error) {
	s.blocksMutex.Lock()
	defer s.blocksMutex.Unlock()
	size := uint64(len(s.blocks))
	if offset > size {
		err = util.IOError1("Offset greater than file size.")
		return
	}
	available := size - offset
	n = len(b)
	if uint64(n) > available {
		n = int(available)
	}
	if n == 0 {
		return
	}
	copy(b, s.blocks[offset:offset+uint64(n)])
	result = b[:n]
	return
}

func (s *envFileState) append(data []byte) error {
	s.blocksMutex.Lock()
	defer s.blocksMutex.Unlock()
	s.blocks = append(s.blocks, data...)
	return nil
}

func (s *envFileState) finalize() {
	s.truncate()
}

type sequentialFileImpl struct {
	file *envFileState
	pos  uint64
}

func newSequentialFileImpl(file *envFileState) *sequentialFileImpl {
	f := &sequentialFileImpl{
		file: file,
		pos:  0,
	}
	f.file.ref()
	return f
}

func (f *sequentialFileImpl) Read(b []byte) (result []byte, n int, err error) {
	result, n, err = f.file.read(f.pos, b)
	if err == nil {
		f.pos += uint64(n)
	}
	return
}

func (f *sequentialFileImpl) Skip(n uint64) error {
	size := uint64(f.file.size())
	if f.pos > size {
		return util.IOError1("pos_ > file_->Size()")
	}
	available := size - f.pos
	if n > available {
		n = available
	}
	f.pos += n
	return nil
}

func (f *sequentialFileImpl) Close() {
	f.file.unref()
}

type randomAccessFileImpl struct {
	file *envFileState
}

func newRandomAccessFileImpl(file *envFileState) *randomAccessFileImpl {
	f := &randomAccessFileImpl{file: file}
	f.file.ref()
	return f
}

func (f *randomAccessFileImpl) Read(b []byte, offset int64) ([]byte, int, error) {
	return f.file.read(uint64(offset), b)
}

func (f *randomAccessFileImpl) Close() {
	f.file.unref()
}

type writableFileImpl struct {
	file *envFileState
}

func newWritableFileImpl(file *envFileState) *writableFileImpl {
	f := &writableFileImpl{file: file}
	f.file.ref()
	return f
}

func (f *writableFileImpl) Append(data []byte) error {
	return f.file.append(data)
}

func (f *writableFileImpl) Close() error {
	f.file.unref()
	return nil
}

func (f *writableFileImpl) Flush() error {
	return nil
}

func (f *writableFileImpl) Sync() error {
	return nil
}

//func (f *writableFileImpl) Finalize() {
//	f.file.unref()
//}

type fileSystem map[string]*envFileState
type inMemoryEnv struct {
	ssdb.EnvWrapper
	mutex   sync.Mutex
	fileMap fileSystem
}

func newInMemoryEnv(baseEnv ssdb.Env) *inMemoryEnv {
	e := &inMemoryEnv{
		EnvWrapper: ssdb.EnvWrapper{Env: baseEnv},
		fileMap:    make(map[string]*envFileState),
	}
	return e
}

func (e *inMemoryEnv) finalize() {
	for _, f := range e.fileMap {
		f.unref()
	}
}

func (e *inMemoryEnv) NewSequentialFile(fname string) (ssdb.SequentialFile, error) {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	var (
		f  *envFileState
		ok bool
	)
	if f, ok = e.fileMap[fname]; !ok {
		return nil, util.IOError2(fname, "File not found")
	}
	return newSequentialFileImpl(f), nil
}

func (e *inMemoryEnv) NewRandomAccessFile(fname string) (ssdb.RandomAccessFile, error) {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	var (
		f  *envFileState
		ok bool
	)
	if f, ok = e.fileMap[fname]; !ok {
		return nil, util.IOError2(fname, "File not found")
	}
	return newRandomAccessFileImpl(f), nil
}

func (e *inMemoryEnv) NewWritableFile(fname string) (ssdb.WritableFile, error) {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	var (
		file *envFileState
		ok   bool
	)
	if file, ok = e.fileMap[fname]; !ok {
		file = newEnvFileState()
		file.ref()
		e.fileMap[fname] = file
	} else {
		file.truncate()
	}
	return newWritableFileImpl(file), nil
}

func (e *inMemoryEnv) NewAppendableFile(fname string) (ssdb.WritableFile, error) {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	file := e.fileMap[fname]
	if file == nil {
		file = newEnvFileState()
		file.ref()
		e.fileMap[fname] = file
	}
	return newWritableFileImpl(file), nil
}

func (e *inMemoryEnv) FileExists(fname string) (ok bool) {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	_, ok = e.fileMap[fname]
	return
}

func (e *inMemoryEnv) GetChildren(dir string) ([]string, error) {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	result := make([]string, 0)
	for filename := range e.fileMap {
		if len(filename) >= len(dir)+1 && filename[len(dir)] == '/' && strings.HasPrefix(filename, dir) {
			result = append(result, filename[len(dir)+1:])
		}
	}
	return result, nil
}

func (e *inMemoryEnv) deleteFileInternal(fname string) {
	fs, ok := e.fileMap[fname]
	if !ok {
		return
	}
	fs.unref()
	delete(e.fileMap, fname)
}

func (e *inMemoryEnv) DeleteFile(fname string) error {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	if _, ok := e.fileMap[fname]; !ok {
		return util.IOError2(fname, "File not found")
	}
	e.deleteFileInternal(fname)
	return nil
}

func (e *inMemoryEnv) CreateDir(_ string) error {
	return nil
}

func (e *inMemoryEnv) DeleteDir(_ string) error {
	return nil
}

func (e *inMemoryEnv) GetFileSize(fname string) (int, error) {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	fs, ok := e.fileMap[fname]
	if !ok {
		return 0, util.IOError2(fname, "File not found")
	}
	return fs.size(), nil
}

func (e *inMemoryEnv) RenameFile(src, target string) error {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	if _, ok := e.fileMap[src]; !ok {
		return util.IOError2(src, "File not found")
	}
	e.deleteFileInternal(target)
	e.fileMap[target] = e.fileMap[src]
	delete(e.fileMap, src)
	return nil
}

func (e *inMemoryEnv) LockFile(_ string) (ssdb.FileLock, error) {
	return struct{}{}, nil
}

func (e *inMemoryEnv) UnlockFile(_ ssdb.FileLock) error {
	return nil
}

func (e *inMemoryEnv) GetTestDirectory() (string, error) {
	return "/test", nil
}

func (e *inMemoryEnv) NewLogger(_ string) (*log.Logger, func(), error) {
	return log.New(ioutil.Discard, "", log.LstdFlags), func() {}, nil
}

func newMemEnv(baseEnv ssdb.Env) ssdb.Env {
	return newInMemoryEnv(baseEnv)
}
