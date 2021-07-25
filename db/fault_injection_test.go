package db

import (
	"fmt"
	"os"
	"ssdb"
	"ssdb/util"
	"strings"
	"sync"
	"testing"
)

func getDirName(filename string) string {
	found := strings.LastIndex(filename, "/")
	if found == -1 {
		return ""
	}
	return filename[0:found]
}

func syncDir(_ string) error {
	return nil
}

func truncate(filename string, length uint64) error {
	env := ssdb.DefaultEnv()
	origFile, err := env.NewSequentialFile(filename)
	if err != nil {
		return err
	}
	b := make([]byte, length)
	b, _, err = origFile.Read(b)
	origFile.Close()
	if err == nil {
		tmpName := getDirName(filename) + "/truncate.tmp"
		var tmpFile ssdb.WritableFile
		if tmpFile, err = env.NewWritableFile(tmpName); err == nil {
			err = tmpFile.Append(b)
			_ = tmpFile.Close()
			if err == nil {
				err = env.RenameFile(tmpName, filename)
			} else {
				_ = env.DeleteFile(tmpName)
			}
		}
	}
	return err
}

type fileState struct {
	filename       string
	pos            int64
	posAtLastSync  int64
	posAtLastFlush int64
}

func newFileState(filename string) *fileState {
	return &fileState{
		filename:       filename,
		pos:            -1,
		posAtLastSync:  -1,
		posAtLastFlush: -1,
	}
}

func (s *fileState) isFullySynced() bool {
	return s.pos <= 0 || s.pos == s.posAtLastSync
}

func (s *fileState) dropUnsyncedData() error {
	var syncPos uint64
	if s.posAtLastSync == -1 {
		syncPos = 0
	} else {
		syncPos = uint64(s.posAtLastSync)
	}
	return truncate(s.filename, syncPos)
}

type testWritableFile struct {
	state              *fileState
	target             ssdb.WritableFile
	writableFileOpened bool
	env                *faultInjectionTestEnv
}

func (f *testWritableFile) Append(data []byte) error {
	err := f.target.Append(data)
	if err == nil && f.env.isFilesystemActive() {
		f.state.pos += int64(len(data))
	}
	return err
}

func (f *testWritableFile) Close() (err error) {
	if f.writableFileOpened {
		f.writableFileOpened = false
		if err = f.target.Close(); err == nil {
			f.env.writableFileClosed(f.state)
		}
	}
	return
}

func (f *testWritableFile) Flush() error {
	err := f.target.Flush()
	if err == nil && f.env.isFilesystemActive() {
		f.state.posAtLastFlush = f.state.pos
	}
	return err
}

func (f *testWritableFile) Sync() error {
	if !f.env.isFilesystemActive() {
		return nil
	}
	err := f.target.Sync()
	if err == nil {
		f.state.posAtLastSync = f.state.pos
	}
	if f.env.isFileCreatedSinceLastDirSync(f.state.filename) {
		if perr := f.syncParent(); err == nil && perr != nil {
			err = perr
		}
	}
	return err
}

//func (f *testWritableFile) Finalize() {
//	if f.writableFileOpened {
//		_ = f.Close()
//	}
//	f.target.Finalize()
//}

func (f *testWritableFile) syncParent() error {
	err := syncDir(getDirName(f.state.filename))
	if err == nil {
		f.env.dirWasSynced()
	}
	return err
}

func newTestWritableFile(state *fileState, f ssdb.WritableFile, env *faultInjectionTestEnv) *testWritableFile {
	if f == nil {
		panic("f == nil")
	}
	file := &testWritableFile{
		state:              state,
		target:             f,
		writableFileOpened: true,
		env:                env,
	}
	return file
}

type faultInjectionTestEnv struct {
	ssdb.EnvWrapper
	mutex                    sync.Mutex
	dbFileState              map[string]*fileState
	newFilesSinceLastDirSync map[string]struct{}
	filesystemActive         bool
}

func newFaultInjectionTestEnv() *faultInjectionTestEnv {
	return &faultInjectionTestEnv{
		EnvWrapper:               ssdb.EnvWrapper{Env: ssdb.DefaultEnv()},
		dbFileState:              make(map[string]*fileState),
		newFilesSinceLastDirSync: make(map[string]struct{}),
		filesystemActive:         true,
	}
}

func (e *faultInjectionTestEnv) NewWritableFile(fname string) (ssdb.WritableFile, error) {
	actualWritableFile, err := e.Target().NewWritableFile(fname)
	if err == nil {
		state := newFileState(fname)
		state.pos = 0
		result := newTestWritableFile(state, actualWritableFile, e)
		e.untrackFile(fname)
		e.mutex.Lock()
		defer e.mutex.Unlock()
		e.newFilesSinceLastDirSync[fname] = struct{}{}
		return result, nil
	}
	return nil, err
}

func (e *faultInjectionTestEnv) NewAppendableFile(fname string) (ssdb.WritableFile, error) {
	actualWritableFile, err := e.Target().NewAppendableFile(fname)
	if err == nil {
		state := newFileState(fname)
		state.pos = 0
		e.mutex.Lock()
		if _, ok := e.dbFileState[fname]; !ok {
			e.newFilesSinceLastDirSync[fname] = struct{}{}
		} else {
			state = e.dbFileState[fname]
		}
		e.mutex.Unlock()
		result := newTestWritableFile(state, actualWritableFile, e)
		return result, err
	}
	return nil, err
}

func (e *faultInjectionTestEnv) dropUnsyncedFileData() error {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	var err error
	for _, state := range e.dbFileState {
		if !state.isFullySynced() {
			if err = state.dropUnsyncedData(); err != nil {
				break
			}
		}
	}
	return err
}

func (e *faultInjectionTestEnv) dirWasSynced() {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	e.newFilesSinceLastDirSync = make(map[string]struct{})
}

func (e *faultInjectionTestEnv) isFileCreatedSinceLastDirSync(filename string) (ok bool) {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	_, ok = e.newFilesSinceLastDirSync[filename]
	return
}

func (e *faultInjectionTestEnv) untrackFile(f string) {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	delete(e.dbFileState, f)
	delete(e.newFilesSinceLastDirSync, f)
}

func (e *faultInjectionTestEnv) DeleteFile(f string) error {
	err := e.EnvWrapper.DeleteFile(f)
	if err == nil {
		e.untrackFile(f)
	}
	return err
}

func (e *faultInjectionTestEnv) RenameFile(s, t string) error {
	ret := e.EnvWrapper.RenameFile(s, t)
	if ret == nil {
		e.mutex.Lock()
		defer e.mutex.Unlock()
		if state, ok := e.dbFileState[s]; ok {
			e.dbFileState[t] = state
			delete(e.dbFileState, s)
		}

		if _, ok := e.newFilesSinceLastDirSync[s]; ok {
			delete(e.newFilesSinceLastDirSync, s)
			e.newFilesSinceLastDirSync[t] = struct{}{}
		}
	}
	return ret
}

func (e *faultInjectionTestEnv) resetState() {
	e.setFilesystemActive(true)
}

func (e *faultInjectionTestEnv) deleteFilesCreatedAfterLastDirSync() error {
	e.mutex.Lock()
	newFiles := make(map[string]struct{}, len(e.newFilesSinceLastDirSync))
	for k := range e.newFilesSinceLastDirSync {
		newFiles[k] = struct{}{}
	}
	e.mutex.Unlock()
	var err error
	for file := range newFiles {
		err = e.DeleteFile(file)
	}
	return err
}

func (e *faultInjectionTestEnv) writableFileClosed(state *fileState) {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	e.dbFileState[state.filename] = state
}

func (e *faultInjectionTestEnv) isFilesystemActive() bool {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	return e.filesystemActive
}

func (e *faultInjectionTestEnv) setFilesystemActive(active bool) {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	e.filesystemActive = active
}

type faultInjectionTest struct {
	env       *faultInjectionTestEnv
	dbName    string
	tinyCache ssdb.Cache
	options   *ssdb.Options
	db        ssdb.DB
	t         *testing.T
}

type ExpectedVerifyResult int8

const (
	valExpectNoError ExpectedVerifyResult = iota
	valExpectError
)

type ResetMethod int8

const (
	resetDropUnsyncedData ResetMethod = iota
	resetDeleteUnsyncedFiles
)

func newFaultInjectionTest(t *testing.T) *faultInjectionTest {
	test := &faultInjectionTest{
		env:       newFaultInjectionTestEnv(),
		tinyCache: ssdb.NewLRUCache(100),
		options:   ssdb.NewOptions(),
		t:         t,
	}
	test.dbName = tmpDir() + "/fault_test"
	_ = Destroy(test.dbName, ssdb.NewOptions())
	test.options.ReuseLogs = true
	test.options.Env = test.env
	test.options.ParanoidChecks = true
	test.options.BlockCache = test.tinyCache
	test.options.CreateIfMissing = true
	return test
}

func (t *faultInjectionTest) reuseLogs(reuse bool) {
	t.options.ReuseLogs = reuse
}

func (t *faultInjectionTest) build(startIdx, numVals int) {
	batch := ssdb.NewWriteBatch()
	var options *ssdb.WriteOptions
	for i := startIdx; i < startIdx+numVals; i++ {
		key := key(i)
		value := value(i)
		batch.Clear()
		batch.Put(key, value)
		options = ssdb.NewWriteOptions()
		util.AssertNotError(t.db.Write(options, batch), "Write", t.t)
	}
}

func (t *faultInjectionTest) readValue(i int) ([]byte, error) {
	key := key(i)
	_ = value(i)
	options := ssdb.NewReadOptions()
	return t.db.Get(options, key)
}

func (t *faultInjectionTest) verify(startIdx, numVals int, expected ExpectedVerifyResult) error {
	var (
		err        error
		val        []byte
		valueSpace []byte
	)
	for i := startIdx; i < startIdx+numVals; i++ {
		valueSpace = value(i)
		val, err = t.readValue(i)
		if expected == valExpectNoError {
			if err == nil {
				util.AssertEqual(valueSpace, val, "value", t.t)
			}
		} else if err == nil {
			fmt.Fprintf(os.Stderr, "Expected an error at %d, but was OK\n", i)
			err = util.IOError2(t.dbName, "Expected value error:")
		} else {
			err = nil
		}
	}
	return err
}

func (t *faultInjectionTest) finish() {
	_ = Destroy(t.dbName, ssdb.NewOptions())
	t.closeDB()
	t.tinyCache.Clear()
	t.env = nil
}

func key(i int) []byte {
	return []byte(fmt.Sprintf("%016d", i))
}

func value(k int) []byte {
	r := util.NewRandom(uint32(k))
	return []byte(util.RandomString(r, faultValueSize))
}

func (t *faultInjectionTest) openDB() (err error) {
	if t.db != nil {
		t.db.Close()
	}
	t.db = nil
	t.env.resetState()
	t.db, err = Open(t.options, t.dbName)
	return
}

func (t *faultInjectionTest) closeDB() {
	t.db.Close()
	t.db = nil
}

func (t *faultInjectionTest) deleteAllData() {
	iter := t.db.NewIterator(ssdb.NewReadOptions())
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		util.AssertNotError(t.db.Delete(ssdb.NewWriteOptions(), iter.Key()), "Delete", t.t)
	}
	iter.Close()
}

func (t *faultInjectionTest) resetDBState(resetMethod ResetMethod) {
	switch resetMethod {
	case resetDropUnsyncedData:
		util.AssertNotError(t.env.dropUnsyncedFileData(), "dropUnsyncedFileData", t.t)
	case resetDeleteUnsyncedFiles:
		util.AssertNotError(t.env.deleteFilesCreatedAfterLastDirSync(), "deleteFilesCreatedAfterLastDirSync", t.t)
	default:
		panic("false")
	}
}

func (t *faultInjectionTest) partialCompactTestPreFault(numPreSync, numPostSync int) {
	t.deleteAllData()
	t.build(0, numPreSync)
	t.db.CompactRange(nil, nil)
	t.build(numPreSync, numPostSync)
}

func (t *faultInjectionTest) partialCompactTestReopenWithFault(resetMethod ResetMethod, numPreSync, numPostSync int) {
	t.env.setFilesystemActive(false)
	t.closeDB()
	t.resetDBState(resetMethod)
	util.AssertNotError(t.openDB(), "openDB", t.t)
	util.AssertNotError(t.verify(0, numPreSync, valExpectNoError), "verify", t.t)
	util.AssertNotError(t.verify(numPreSync, numPostSync, valExpectError), "verify", t.t)
}

func (t *faultInjectionTest) noWriteTestPreFault() {
}

func (t *faultInjectionTest) noWriteTestReopenWithFault(resetMethod ResetMethod) {
	t.closeDB()
	t.resetDBState(resetMethod)
	util.AssertNotError(t.openDB(), "openDB", t.t)
}

const (
	faultValueSize = 1000
	maxNumValues   = 2000
	numIterations  = 3
)

func (t *faultInjectionTest) doTest() {
	rnd := util.NewRandom(0)
	util.AssertNotError(t.openDB(), "openDB", t.t)
	var (
		numPreSync  int
		numPostSync int
	)
	for idx := 0; idx < numIterations; idx++ {
		numPreSync = int(rnd.Uniform(maxNumValues))
		numPostSync = int(rnd.Uniform(maxNumValues))

		t.partialCompactTestPreFault(numPreSync, numPostSync)
		t.partialCompactTestReopenWithFault(resetDropUnsyncedData, numPreSync, numPostSync)

		t.noWriteTestPreFault()
		t.noWriteTestReopenWithFault(resetDropUnsyncedData)

		t.partialCompactTestPreFault(numPreSync, numPostSync)

		t.partialCompactTestReopenWithFault(resetDeleteUnsyncedFiles, numPreSync+numPostSync, 0)

		t.noWriteTestPreFault()
		t.noWriteTestReopenWithFault(resetDeleteUnsyncedFiles)
	}
}

func TestNoLogReuse(t *testing.T) {
	test := newFaultInjectionTest(t)
	defer test.finish()
	test.reuseLogs(false)
	test.doTest()
}

func TestWithLogReuse(t *testing.T) {
	test := newFaultInjectionTest(t)
	defer test.finish()
	test.reuseLogs(true)
	test.doTest()
}
