package db

import (
	"bytes"
	"fmt"
	"os"
	"sort"
	"ssdb"
	"ssdb/util"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func randomString(rnd *util.Random, l int) string {
	return util.RandomString(rnd, l)
}

func randomKey(rnd *util.Random) string {
	var l int
	if rnd.OneIn(3) {
		l = 1
	} else if rnd.OneIn(100) {
		l = int(rnd.Skewed(10))
	} else {
		l = int(rnd.Uniform(10))
	}
	return util.RandomKey(rnd, l)
}

type atomicCounter struct {
	count int64
}

func (c *atomicCounter) increment() {
	c.incrementBy(1)
}

func (c *atomicCounter) incrementBy(count int) {
	atomic.AddInt64(&c.count, int64(count))
}

func (c *atomicCounter) read() int64 {
	return atomic.LoadInt64(&c.count)
}

func (c *atomicCounter) reset() {
	atomic.StoreInt64(&c.count, 0)
}

func delayMilliseconds(millis int) {
	ssdb.DefaultEnv().SleepForMicroseconds(millis * 1000)
}

type testEnv struct {
	ssdb.EnvWrapper
	ignoreDotFiles bool
}

func newTestEnv(base ssdb.Env) *testEnv {
	return &testEnv{
		EnvWrapper:     ssdb.EnvWrapper{Env: base},
		ignoreDotFiles: false,
	}
}

func (e *testEnv) setIgnoreDotFiles(ignored bool) {
	e.ignoreDotFiles = ignored
}

func (e *testEnv) GetChildren(dir string) ([]string, error) {
	result, err := e.Target().GetChildren(dir)
	if err != nil || !e.ignoreDotFiles {
		return result, err
	}
	filtered := make([]string, 0, len(result))
	for _, child := range result {
		if child == "." || child == ".." {
		} else {
			filtered = append(filtered, child)
		}
	}
	return filtered, err
}

type specialEnv struct {
	ssdb.EnvWrapper
	delayDataSync      util.AtomicBool
	dataSyncError      util.AtomicBool
	noSpace            util.AtomicBool
	nonWritable        util.AtomicBool
	manifestSyncError  util.AtomicBool
	manifestWriteError util.AtomicBool
	countRandomReads   bool
	randomReadCounter  atomicCounter
}

func newSpecialEnv(base ssdb.Env) *specialEnv {
	return &specialEnv{
		EnvWrapper: ssdb.EnvWrapper{Env: base},
	}
}

func (e *specialEnv) NewWritableFile(f string) (ssdb.WritableFile, error) {
	if e.nonWritable.IsTrue() {
		return nil, util.IOError1("simulated write error")
	}
	r, err := e.Target().NewWritableFile(f)
	if err == nil {
		if strings.HasSuffix(f, ".ssdb") || strings.HasSuffix(f, ".log") {
			r = &dataFile{
				env:  e,
				base: r,
			}
		} else if strings.HasSuffix(f, "MANIFEST") {
			r = &manifestFile{
				env:  e,
				base: r,
			}
		}
	}
	return r, err
}

func (e *specialEnv) NewRandomAccessFile(f string) (ssdb.RandomAccessFile, error) {
	r, err := e.Target().NewRandomAccessFile(f)
	if err == nil && e.countRandomReads {
		r = &countingFile{
			target:  r,
			counter: &e.randomReadCounter,
		}
	}
	return r, err
}

type dataFile struct {
	env  *specialEnv
	base ssdb.WritableFile
}

func (f *dataFile) Append(data []byte) error {
	if f.env.noSpace.IsTrue() {
		return nil
	}
	return f.base.Append(data)
}

func (f *dataFile) Close() error {
	return f.base.Close()
}

func (f *dataFile) Flush() error {
	return f.base.Flush()
}

func (f *dataFile) Sync() error {
	if f.env.dataSyncError.IsTrue() {
		return util.IOError1("simulated data sync error")
	}
	for f.env.delayDataSync.IsTrue() {
		delayMilliseconds(100)
	}
	return f.base.Sync()
}

func (f *dataFile) Finalize() {
	f.base.Finalize()
}

type manifestFile struct {
	env  *specialEnv
	base ssdb.WritableFile
}

func (f *manifestFile) Append(data []byte) error {
	if f.env.manifestWriteError.IsTrue() {
		return util.IOError1("simulated writer error")
	}
	return f.base.Append(data)
}

func (f *manifestFile) Close() error {
	return f.base.Close()
}

func (f *manifestFile) Flush() error {
	return f.base.Flush()
}

func (f *manifestFile) Sync() error {
	if f.env.manifestSyncError.IsTrue() {
		return util.IOError1("simulated sync error")
	}
	return f.base.Sync()
}

func (f *manifestFile) Finalize() {
	f.base.Finalize()
}

type countingFile struct {
	target  ssdb.RandomAccessFile
	counter *atomicCounter
}

func (f *countingFile) Read(b []byte, offset int64) ([]byte, int, error) {
	f.counter.increment()
	return f.target.Read(b, offset)
}

func (f *countingFile) Finalize() {
	f.target.Finalize()
}

type optionConfig int8

const (
	defaultCfg optionConfig = iota
	reuseCfg
	filterCfg
	uncompressedCfg
	endCfg
)

type dbTest struct {
	dbName       string
	env          *specialEnv
	db           ssdb.DB
	lastOptions  *ssdb.Options
	filterPolicy ssdb.FilterPolicy
	optionConfig optionConfig
	t            *testing.T
}

func newDBTest(t *testing.T) *dbTest {
	test := &dbTest{
		dbName:       tmpDir() + "/db_test",
		env:          newSpecialEnv(ssdb.DefaultEnv()),
		lastOptions:  ssdb.NewOptions(),
		filterPolicy: ssdb.NewBloomFilterPolicy(10),
		optionConfig: defaultCfg,
		t:            t,
	}
	_ = Destroy(test.dbName, ssdb.NewOptions())
	test.reopen(nil)
	return test
}

func (t *dbTest) finalize() {
	if t.db != nil {
		t.db.(*db).finalize()
	}
	_ = Destroy(t.dbName, ssdb.NewOptions())
}

func (t *dbTest) changeOptions() bool {
	t.optionConfig++
	if t.optionConfig >= endCfg {
		return false
	}
	t.destroyAndReopen(nil)
	return true
}

func (t *dbTest) currentOptions() *ssdb.Options {
	options := ssdb.NewOptions()
	options.ReuseLogs = false
	switch t.optionConfig {
	case reuseCfg:
		options.ReuseLogs = true
	case filterCfg:
		options.FilterPolicy = t.filterPolicy
	case uncompressedCfg:
		options.CompressionType = ssdb.NoCompression
	}
	return options
}

func (t *dbTest) dbFull() *db {
	return t.db.(*db)
}

func (t *dbTest) reopen(options *ssdb.Options) {
	util.AssertNotError(t.tryOpen(options), "tryOpen", t.t)
}

func (t *dbTest) close() {
	if t.db != nil {
		t.dbFull().finalize()
		t.db = nil
	}
}

func (t *dbTest) destroyAndReopen(options *ssdb.Options) {
	if t.db != nil {
		t.dbFull().finalize()
		t.db = nil
	}
	_ = Destroy(t.dbName, ssdb.NewOptions())
	util.AssertNotError(t.tryOpen(options), "tryOpen", t.t)

}

func (t *dbTest) tryOpen(options *ssdb.Options) (err error) {
	if t.db != nil {
		t.dbFull().finalize()
		t.db = nil
	}
	var opts *ssdb.Options
	if options != nil {
		opts = options
	} else {
		opts = t.currentOptions()
		opts.CreateIfMissing = true
	}
	t.lastOptions = opts
	t.db, err = Open(opts, t.dbName)
	return
}

func (t *dbTest) put(k, v string) error {
	return t.db.Put(ssdb.NewWriteOptions(), []byte(k), []byte(v))
}

func (t *dbTest) delete(k string) error {
	return t.db.Delete(ssdb.NewWriteOptions(), []byte(k))
}

func (t *dbTest) get(k string) string {
	return t.getWithSnapshot(k, nil)
}

func (t *dbTest) getWithSnapshot(k string, snapshot ssdb.Snapshot) string {
	options := ssdb.NewReadOptions()
	options.Snapshot = snapshot
	v, err := t.db.Get(options, []byte(k))
	result := string(v)
	if ssdb.IsNotFound(err) {
		result = "NOT_FOUND"
	} else if err != nil {
		result = err.Error()
	}
	return result
}

func (t *dbTest) contents() string {
	forward := make([]string, 0)
	var b strings.Builder
	iter := t.db.NewIterator(ssdb.NewReadOptions())
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		s := t.iterStatus(iter)
		b.WriteByte('(')
		b.WriteString(s)
		b.WriteByte(')')
		forward = append(forward, s)
	}
	matched := 0
	for iter.SeekToLast(); iter.Valid(); iter.Prev() {
		util.AssertLessThan(matched, len(forward), "matched < len(forward)", t.t)
		util.AssertEqual(t.iterStatus(iter), forward[len(forward)-matched-1], "iterStatus", t.t)
		matched++
	}
	util.AssertEqual(matched, len(forward), "matched", t.t)
	iter.Finalize()
	return b.String()
}

func (t *dbTest) allEntriesFor(userKey string) string {
	iter := t.dbFull().testNewInternalIterator()
	target := newInternalKey([]byte(userKey), maxSequenceNumber, ssdb.TypeValue)
	iter.Seek(target.encode())
	var result string
	if iter.Status() != nil {
		result = iter.Status().Error()
	} else {
		var b strings.Builder
		b.WriteString("[ ")
		first := true
		for iter.Valid() {
			var ikey parsedInternalKey
			if !parseInternalKey(iter.Key(), &ikey) {
				b.WriteString("CORRUPTED")
			} else {
				if t.lastOptions.Comparator.Compare(ikey.userKey, []byte(userKey)) != 0 {
					break
				}
				if !first {
					b.WriteString(", ")
				}
				first = false
				switch ikey.valueType {
				case ssdb.TypeValue:
					b.Write(iter.Value())
				case ssdb.TypeDeletion:
					b.WriteString("DEL")
				}
			}
			iter.Next()
		}
		if !first {
			b.WriteByte(' ')
		}
		b.WriteByte(']')
		result = b.String()
	}
	iter.Finalize()
	return result
}

func (t *dbTest) numTableFilesAtLevel(level int) (i int) {
	property, ok := t.db.GetProperty("ssdb.num-files-at-level" + util.NumberToString(uint64(level)))
	util.AssertTrue(ok, "GetProperty", t.t)
	i, _ = strconv.Atoi(property)
	return
}

func (t *dbTest) totalTableFiles() int {
	result := 0
	for level := 0; level < numLevels; level++ {
		result += t.numTableFilesAtLevel(level)
	}
	return result
}

func (t *dbTest) filesPerLevel() string {
	var b strings.Builder
	lastNonZeroOffset := 0
	for level := 0; level < numLevels; level++ {
		f := t.numTableFilesAtLevel(level)
		if level == 0 {
			fmt.Fprintf(&b, "%s%d", "", f)
		} else {
			fmt.Fprintf(&b, "%s%d", ",", f)
		}
		if f > 0 {
			lastNonZeroOffset = b.Len()
		}
	}
	return b.String()[:lastNonZeroOffset]
}

func (t *dbTest) countFiles() int {
	files, _ := t.env.GetChildren(t.dbName)
	return len(files)
}

func (t *dbTest) size(start, limit string) uint64 {
	r := ssdb.Range{
		Start: []byte(start),
		Limit: []byte(limit),
	}
	sizes := t.db.GetApproximateSizes([]ssdb.Range{r})
	return sizes[0]
}

func (t *dbTest) compact(start, limit string) {
	t.db.CompactRange([]byte(start), []byte(limit))
}

func (t *dbTest) makeTables(n int, smallKey, largeKey string) {
	for i := 0; i < n; i++ {
		_ = t.put(smallKey, "begin")
		_ = t.put(largeKey, "end")
		_ = t.dbFull().testCompactMemTable()
	}
}

func (t *dbTest) fillLevels(smallest, largest string) {
	t.makeTables(numLevels, smallest, largest)
}

func (t *dbTest) dumpFileCounts(label string) {
	fmt.Fprintf(os.Stderr, "---\n%s:\n", label)
	fmt.Fprintf(os.Stderr, "maxoverlap: %d\n", t.dbFull().testMaxNextLevelOverlappingBytes())
	for level := 0; level < numLevels; level++ {
		if num := t.numTableFilesAtLevel(level); num > 0 {
			fmt.Fprintf(os.Stderr, "  level %3d : %d files\n", level, num)
		}
	}
}

func (t *dbTest) dumpSSTableList() string {
	property, _ := t.db.GetProperty("ssdb.sstables")
	return property
}

func (t *dbTest) iterStatus(iter ssdb.Iterator) string {
	var result string
	if iter.Valid() {
		result = string(iter.Key()) + "->" + string(iter.Value())
	} else {
		result = "(invalid)"
	}
	return result
}

func (t *dbTest) deleteAnSSTFile() bool {
	filenames, err := t.env.GetChildren(t.dbName)
	util.AssertNotError(err, "GetChildren", t.t)
	var (
		number uint64
		ft     fileType
	)
	for _, filename := range filenames {
		if parseFileName(filename, &number, &ft) && ft == tableFile {
			util.AssertNotError(t.env.DeleteFile(tableFileName(t.dbName, number)), "DeleteFile", t.t)
			return true
		}
	}
	return false
}

func (t *dbTest) renameSSDBToSST() int {
	filenames, err := t.env.GetChildren(t.dbName)
	util.AssertNotError(err, "GetChildren", t.t)
	var (
		number uint64
		ft     fileType
	)
	filesRenamed := 0
	for _, filename := range filenames {
		if parseFileName(filename, &number, &ft) && ft == tableFile {
			from := tableFileName(t.dbName, number)
			to := sstTableFileName(t.dbName, number)
			util.AssertNotError(t.env.RenameFile(from, to), "RenameFile", t.t)
			filesRenamed++
		}
	}
	return filesRenamed
}

func TestDBEmpty(t *testing.T) {
	test := newDBTest(t)
	defer test.finalize()
	for {
		util.AssertTrue(test.db != nil, "db", t)
		util.AssertEqual("NOT_FOUND", test.get("foo"), "get", t)
		if !test.changeOptions() {
			break
		}
	}
}

func TestDBEmptyKey(t *testing.T) {
	test := newDBTest(t)
	defer test.finalize()
	for {
		util.AssertNotError(test.put("", "v1"), "put", t)
		util.AssertEqual("v1", test.get(""), "get", t)
		util.AssertNotError(test.put("", "v2"), "put", t)
		util.AssertEqual("v2", test.get(""), "get", t)
		if !test.changeOptions() {
			break
		}
	}
}

func TestDBEmptyValue(t *testing.T) {
	test := newDBTest(t)
	defer test.finalize()
	for {
		util.AssertNotError(test.put("key", "v1"), "put", t)
		util.AssertEqual("v1", test.get("key"), "get", t)
		util.AssertNotError(test.put("key", ""), "put", t)
		util.AssertEqual("", test.get("key"), "get", t)
		util.AssertNotError(test.put("key", "v2"), "put", t)
		util.AssertEqual("v2", test.get("key"), "get", t)
		if !test.changeOptions() {
			break
		}
	}
}

func TestDBReadWrite(t *testing.T) {
	test := newDBTest(t)
	defer test.finalize()
	for {
		util.AssertNotError(test.put("foo", "v1"), "put", t)
		util.AssertEqual("v1", test.get("foo"), "get", t)
		util.AssertNotError(test.put("bar", "v2"), "put", t)
		util.AssertNotError(test.put("foo", "v3"), "put", t)
		util.AssertEqual("v3", test.get("foo"), "get", t)
		util.AssertEqual("v2", test.get("bar"), "get", t)
		if !test.changeOptions() {
			break
		}
	}
}

func TestDBPutDeleteGet(t *testing.T) {
	test := newDBTest(t)
	defer test.finalize()
	for {
		util.AssertNotError(test.db.Put(ssdb.NewWriteOptions(), []byte("foo"), []byte("v1")), "Put", t)
		util.AssertEqual("v1", test.get("foo"), "get", t)
		util.AssertNotError(test.db.Put(ssdb.NewWriteOptions(), []byte("foo"), []byte("v2")), "Put", t)
		util.AssertEqual("v2", test.get("foo"), "get", t)
		util.AssertNotError(test.db.Delete(ssdb.NewWriteOptions(), []byte("foo")), "Delete", t)
		util.AssertEqual("NOT_FOUND", test.get("foo"), "get", t)
		if !test.changeOptions() {
			break
		}
	}
}

func TestDBGetFromImmutableLayer(t *testing.T) {
	test := newDBTest(t)
	defer test.finalize()
	for {
		options := test.currentOptions()
		options.Env = test.env
		options.WriteBufferSize = 100000
		test.reopen(options)

		util.AssertNotError(test.put("foo", "v1"), "put", t)
		util.AssertEqual("v1", test.get("foo"), "get", t)

		test.env.delayDataSync.SetTrue()
		_ = test.put("k1", strings.Repeat("x", 100000))
		_ = test.put("k2", strings.Repeat("y", 100000))
		util.AssertEqual("v1", test.get("foo"), "get", t)

		test.env.delayDataSync.SetFalse()
		if !test.changeOptions() {
			break
		}
	}
}

func TestDBGetFromVersions(t *testing.T) {
	test := newDBTest(t)
	defer test.finalize()
	for {
		util.AssertNotError(test.put("foo", "v1"), "put", t)
		_ = test.dbFull().testCompactMemTable()
		util.AssertEqual("v1", test.get("foo"), "get", t)
		if !test.changeOptions() {
			break
		}
	}
}

func TestDBGetMemUsage(t *testing.T) {
	test := newDBTest(t)
	defer test.finalize()
	for {
		util.AssertNotError(test.put("foo", "v1"), "put", t)
		val, ok := test.db.GetProperty("ssdb.approximate-memory-usage")
		util.AssertTrue(ok, "GetProperty", t)
		memUsage, _ := strconv.Atoi(val)
		util.AssertGreaterThan(memUsage, 0, "memUsage > 0", t)
		util.AssertLessThan(memUsage, 5*1024*1024, "memUsage < 5M", t)
		if !test.changeOptions() {
			break
		}
	}
}

func TestDBGetSnapshot(t *testing.T) {
	test := newDBTest(t)
	defer test.finalize()
	for {
		for i := 0; i < 2; i++ {
			var key string
			if i == 0 {
				key = "foo"
			} else {
				key = strings.Repeat("x", 200)
			}
			util.AssertNotError(test.put(key, "v1"), "put", t)
			s1 := test.db.GetSnapshot()
			util.AssertNotError(test.put(key, "v2"), "put", t)
			util.AssertEqual("v2", test.get(key), "get", t)
			util.AssertEqual("v1", test.getWithSnapshot(key, s1), "get", t)
			_ = test.dbFull().testCompactMemTable()
			util.AssertEqual("v2", test.get(key), "get", t)
			util.AssertEqual("v1", test.getWithSnapshot(key, s1), "get", t)
			test.db.ReleaseSnapshot(s1)
		}
		if !test.changeOptions() {
			break
		}
	}
}

func TestDBGetIdenticalSnapshots(t *testing.T) {
	test := newDBTest(t)
	defer test.finalize()
	for {
		for i := 0; i < 2; i++ {
			var key string
			if i == 0 {
				key = "foo"
			} else {
				key = strings.Repeat("x", 200)
			}
			util.AssertNotError(test.put(key, "v1"), "put", t)
			s1 := test.db.GetSnapshot()
			s2 := test.db.GetSnapshot()
			s3 := test.db.GetSnapshot()
			util.AssertNotError(test.put(key, "v2"), "put", t)
			util.AssertEqual("v2", test.get(key), "get", t)
			util.AssertEqual("v1", test.getWithSnapshot(key, s1), "get", t)
			util.AssertEqual("v1", test.getWithSnapshot(key, s2), "get", t)
			util.AssertEqual("v1", test.getWithSnapshot(key, s3), "get", t)
			test.db.ReleaseSnapshot(s1)
			_ = test.dbFull().testCompactMemTable()
			util.AssertEqual("v2", test.get(key), "get", t)
			util.AssertEqual("v1", test.getWithSnapshot(key, s2), "get", t)
			test.db.ReleaseSnapshot(s2)
			util.AssertEqual("v1", test.getWithSnapshot(key, s3), "get", t)
			test.db.ReleaseSnapshot(s3)
		}
		if !test.changeOptions() {
			break
		}
	}
}

func TestDBIterateOverEmptySnapshot(t *testing.T) {
	test := newDBTest(t)
	defer test.finalize()
	for {
		snapshot := test.db.GetSnapshot()
		readOptions := ssdb.NewReadOptions()
		readOptions.Snapshot = snapshot
		util.AssertNotError(test.put("foo", "v1"), "put", t)
		util.AssertNotError(test.put("foo", "v2"), "put", t)

		iterator1 := test.db.NewIterator(readOptions)
		iterator1.SeekToFirst()
		util.AssertFalse(iterator1.Valid(), "Valid", t)
		iterator1.Finalize()

		_ = test.dbFull().testCompactMemTable()

		iterator2 := test.db.NewIterator(readOptions)
		iterator2.SeekToFirst()
		util.AssertFalse(iterator2.Valid(), "Valid", t)
		iterator2.Finalize()

		test.db.ReleaseSnapshot(snapshot)
		if !test.changeOptions() {
			break
		}
	}
}

func TestDBGetLevel0Ordering(t *testing.T) {
	test := newDBTest(t)
	defer test.finalize()
	for {
		util.AssertNotError(test.put("bar", "b"), "put", t)
		util.AssertNotError(test.put("foo", "v1"), "put", t)
		_ = test.dbFull().testCompactMemTable()
		util.AssertNotError(test.put("foo", "v2"), "put", t)
		_ = test.dbFull().testCompactMemTable()
		util.AssertEqual("v2", test.get("foo"), "get", t)
		if !test.changeOptions() {
			break
		}
	}
}

func TestDBGetOrderedByLevels(t *testing.T) {
	test := newDBTest(t)
	defer test.finalize()
	for {
		util.AssertNotError(test.put("foo", "v1"), "put", t)
		test.compact("a", "z")
		util.AssertEqual("v1", test.get("foo"), "get", t)
		util.AssertNotError(test.put("foo", "v2"), "put", t)
		util.AssertEqual("v2", test.get("foo"), "get", t)
		_ = test.dbFull().testCompactMemTable()
		util.AssertEqual("v2", test.get("foo"), "get", t)
		if !test.changeOptions() {
			break
		}
	}
}

func TestDBGetPicksCorrectFile(t *testing.T) {
	test := newDBTest(t)
	defer test.finalize()
	for {
		util.AssertNotError(test.put("a", "va"), "put", t)
		test.compact("a", "b")
		util.AssertNotError(test.put("x", "vx"), "put", t)
		test.compact("x", "y")
		util.AssertNotError(test.put("f", "vf"), "put", t)
		test.compact("f", "g")
		util.AssertEqual("va", test.get("a"), "get", t)
		util.AssertEqual("vf", test.get("f"), "get", t)
		util.AssertEqual("vx", test.get("x"), "get", t)

		if !test.changeOptions() {
			break
		}
	}
}

func TestDBGetEncountersEmptyLevel(t *testing.T) {
	test := newDBTest(t)
	defer test.finalize()
	for {
		compactionCount := 0
		for test.numTableFilesAtLevel(0) == 0 || test.numTableFilesAtLevel(2) == 0 {
			util.AssertLessThanOrEqual(compactionCount, 100, "compactionCount", t)
			compactionCount++
			_ = test.put("a", "begin")
			_ = test.put("z", "end")
			_ = test.dbFull().testCompactMemTable()
		}

		test.dbFull().testCompactRange(1, nil, nil)
		util.AssertEqual(test.numTableFilesAtLevel(0), 1, "numTableFilesAtLevel", t)
		util.AssertEqual(test.numTableFilesAtLevel(1), 0, "numTableFilesAtLevel", t)
		util.AssertEqual(test.numTableFilesAtLevel(2), 1, "numTableFilesAtLevel", t)

		for i := 0; i < 1000; i++ {
			util.AssertEqual("NOT_FOUND", test.get("missing"), "get", t)
		}

		delayMilliseconds(1000)

		util.AssertEqual(test.numTableFilesAtLevel(0), 0, "numTableFilesAtLevel", t)
		if !test.changeOptions() {
			break
		}
	}
}

func TestDBIterEmpty(t *testing.T) {
	test := newDBTest(t)
	defer test.finalize()
	iter := test.db.NewIterator(ssdb.NewReadOptions())
	iter.SeekToFirst()
	util.AssertEqual(test.iterStatus(iter), "(invalid)", "SeekToFirst", t)
	iter.SeekToLast()
	util.AssertEqual(test.iterStatus(iter), "(invalid)", "SeekToFirst", t)
	iter.Seek([]byte("foo"))
	util.AssertEqual(test.iterStatus(iter), "(invalid)", "SeekToFirst", t)
	iter.Finalize()
}

func TestDBIterSingle(t *testing.T) {
	test := newDBTest(t)
	defer test.finalize()
	util.AssertNotError(test.put("a", "va"), "put", t)
	iter := test.db.NewIterator(ssdb.NewReadOptions())

	iter.SeekToFirst()
	util.AssertEqual(test.iterStatus(iter), "a->va", "iterStatus", t)
	iter.Next()
	util.AssertEqual(test.iterStatus(iter), "(invalid)", "iterStatus", t)
	iter.SeekToFirst()
	util.AssertEqual(test.iterStatus(iter), "a->va", "iterStatus", t)
	iter.Prev()
	util.AssertEqual(test.iterStatus(iter), "(invalid)", "iterStatus", t)

	iter.SeekToLast()
	util.AssertEqual(test.iterStatus(iter), "a->va", "iterStatus", t)
	iter.Next()
	util.AssertEqual(test.iterStatus(iter), "(invalid)", "iterStatus", t)
	iter.SeekToLast()
	util.AssertEqual(test.iterStatus(iter), "a->va", "iterStatus", t)
	iter.Prev()
	util.AssertEqual(test.iterStatus(iter), "(invalid)", "iterStatus", t)

	iter.Seek([]byte(""))
	util.AssertEqual(test.iterStatus(iter), "a->va", "iterStatus", t)
	iter.Next()
	util.AssertEqual(test.iterStatus(iter), "(invalid)", "iterStatus", t)

	iter.Seek([]byte("a"))
	util.AssertEqual(test.iterStatus(iter), "a->va", "iterStatus", t)
	iter.Next()
	util.AssertEqual(test.iterStatus(iter), "(invalid)", "iterStatus", t)

	iter.Seek([]byte("b"))
	util.AssertEqual(test.iterStatus(iter), "(invalid)", "iterStatus", t)

	iter.Finalize()
}

func TestDBIterMulti(t *testing.T) {
	test := newDBTest(t)
	defer test.finalize()
	util.AssertNotError(test.put("a", "va"), "put", t)
	util.AssertNotError(test.put("b", "vb"), "put", t)
	util.AssertNotError(test.put("c", "vc"), "put", t)
	iter := test.db.NewIterator(ssdb.NewReadOptions())

	iter.SeekToFirst()
	util.AssertEqual(test.iterStatus(iter), "a->va", "iterStatus", t)
	iter.Next()
	util.AssertEqual(test.iterStatus(iter), "b->vb", "iterStatus", t)
	iter.Next()
	util.AssertEqual(test.iterStatus(iter), "c->vc", "iterStatus", t)
	iter.Next()
	util.AssertEqual(test.iterStatus(iter), "(invalid)", "iterStatus", t)
	iter.SeekToFirst()
	util.AssertEqual(test.iterStatus(iter), "a->va", "iterStatus", t)
	iter.Prev()
	util.AssertEqual(test.iterStatus(iter), "(invalid)", "iterStatus", t)

	iter.SeekToLast()
	util.AssertEqual(test.iterStatus(iter), "c->vc", "iterStatus", t)
	iter.Prev()
	util.AssertEqual(test.iterStatus(iter), "b->vb", "iterStatus", t)
	iter.Prev()
	util.AssertEqual(test.iterStatus(iter), "a->va", "iterStatus", t)
	iter.Prev()
	util.AssertEqual(test.iterStatus(iter), "(invalid)", "iterStatus", t)
	iter.SeekToLast()
	util.AssertEqual(test.iterStatus(iter), "c->vc", "iterStatus", t)
	iter.Next()
	util.AssertEqual(test.iterStatus(iter), "(invalid)", "iterStatus", t)

	iter.Seek([]byte(""))
	util.AssertEqual(test.iterStatus(iter), "a->va", "iterStatus", t)
	iter.Seek([]byte("a"))
	util.AssertEqual(test.iterStatus(iter), "a->va", "iterStatus", t)
	iter.Seek([]byte("ax"))
	util.AssertEqual(test.iterStatus(iter), "b->vb", "iterStatus", t)
	iter.Seek([]byte("b"))
	util.AssertEqual(test.iterStatus(iter), "b->vb", "iterStatus", t)
	iter.Seek([]byte("z"))
	util.AssertEqual(test.iterStatus(iter), "(invalid)", "iterStatus", t)

	iter.SeekToLast()
	iter.Prev()
	iter.Prev()
	iter.Next()
	util.AssertEqual(test.iterStatus(iter), "b->vb", "iterStatus", t)

	iter.SeekToFirst()
	iter.Next()
	iter.Next()
	iter.Prev()
	util.AssertEqual(test.iterStatus(iter), "b->vb", "iterStatus", t)

	util.AssertNotError(test.put("a", "va2"), "put", t)
	util.AssertNotError(test.put("a2", "va3"), "put", t)
	util.AssertNotError(test.put("b", "vb2"), "put", t)
	util.AssertNotError(test.put("c", "vc2"), "put", t)
	util.AssertNotError(test.delete("b"), "delete", t)
	iter.SeekToFirst()
	util.AssertEqual(test.iterStatus(iter), "a->va", "iterStatus", t)
	iter.Next()
	util.AssertEqual(test.iterStatus(iter), "b->vb", "iterStatus", t)
	iter.Next()
	util.AssertEqual(test.iterStatus(iter), "c->vc", "iterStatus", t)
	iter.Next()
	util.AssertEqual(test.iterStatus(iter), "(invalid)", "iterStatus", t)
	iter.SeekToLast()
	util.AssertEqual(test.iterStatus(iter), "c->vc", "iterStatus", t)
	iter.Prev()
	util.AssertEqual(test.iterStatus(iter), "b->vb", "iterStatus", t)
	iter.Prev()
	util.AssertEqual(test.iterStatus(iter), "a->va", "iterStatus", t)
	iter.Prev()
	util.AssertEqual(test.iterStatus(iter), "(invalid)", "iterStatus", t)

	iter.Finalize()
}

func TestDBIterSmallAndLargeMix(t *testing.T) {
	test := newDBTest(t)
	defer test.finalize()
	util.AssertNotError(test.put("a", "va"), "put", t)
	util.AssertNotError(test.put("b", strings.Repeat("b", 100000)), "put", t)
	util.AssertNotError(test.put("c", "vc"), "put", t)
	util.AssertNotError(test.put("d", strings.Repeat("d", 100000)), "put", t)
	util.AssertNotError(test.put("e", strings.Repeat("e", 100000)), "put", t)
	iter := test.db.NewIterator(ssdb.NewReadOptions())

	iter.SeekToFirst()
	util.AssertEqual(test.iterStatus(iter), "a->va", "iterStatus", t)
	iter.Next()
	util.AssertEqual(test.iterStatus(iter), "b->"+strings.Repeat("b", 100000), "iterStatus", t)
	iter.Next()
	util.AssertEqual(test.iterStatus(iter), "c->vc", "iterStatus", t)
	iter.Next()
	util.AssertEqual(test.iterStatus(iter), "d->"+strings.Repeat("d", 100000), "iterStatus", t)
	iter.Next()
	util.AssertEqual(test.iterStatus(iter), "e->"+strings.Repeat("e", 100000), "iterStatus", t)
	iter.Next()
	util.AssertEqual(test.iterStatus(iter), "(invalid)", "iterStatus", t)

	iter.SeekToLast()
	util.AssertEqual(test.iterStatus(iter), "e->"+strings.Repeat("e", 100000), "iterStatus", t)
	iter.Prev()
	util.AssertEqual(test.iterStatus(iter), "d->"+strings.Repeat("d", 100000), "iterStatus", t)
	iter.Prev()
	util.AssertEqual(test.iterStatus(iter), "c->vc", "iterStatus", t)
	iter.Prev()
	util.AssertEqual(test.iterStatus(iter), "b->"+strings.Repeat("b", 100000), "iterStatus", t)
	iter.Prev()
	util.AssertEqual(test.iterStatus(iter), "a->va", "iterStatus", t)
	iter.Prev()
	util.AssertEqual(test.iterStatus(iter), "(invalid)", "iterStatus", t)

	iter.Finalize()
}

func TestDBIterMultiWithDelete(t *testing.T) {
	test := newDBTest(t)
	defer test.finalize()
	for {
		util.AssertNotError(test.put("a", "va"), "put", t)
		util.AssertNotError(test.put("b", "vb"), "put", t)
		util.AssertNotError(test.put("c", "vc"), "put", t)
		util.AssertNotError(test.delete("b"), "delete", t)
		util.AssertEqual("NOT_FOUND", test.get("b"), "get", t)

		iter := test.db.NewIterator(ssdb.NewReadOptions())
		iter.Seek([]byte("c"))
		util.AssertEqual(test.iterStatus(iter), "c->vc", "iterStatus", t)
		iter.Prev()
		util.AssertEqual(test.iterStatus(iter), "a->va", "iterStatus", t)

		iter.Finalize()
		if !test.changeOptions() {
			break
		}
	}
}

func TestDBRecover(t *testing.T) {
	test := newDBTest(t)
	defer test.finalize()
	for {
		util.AssertNotError(test.put("foo", "v1"), "put", t)
		util.AssertNotError(test.put("baz", "v5"), "put", t)

		test.reopen(nil)
		util.AssertEqual("v1", test.get("foo"), "get", t)

		util.AssertEqual("v1", test.get("foo"), "get", t)
		util.AssertEqual("v5", test.get("baz"), "get", t)
		util.AssertNotError(test.put("bar", "v2"), "put", t)
		util.AssertNotError(test.put("foo", "v3"), "put", t)

		test.reopen(nil)
		util.AssertEqual("v3", test.get("foo"), "get", t)
		util.AssertNotError(test.put("foo", "v4"), "put", t)
		util.AssertEqual("v4", test.get("foo"), "get", t)
		util.AssertEqual("v2", test.get("bar"), "get", t)
		util.AssertEqual("v5", test.get("baz"), "get", t)

		if !test.changeOptions() {
			break
		}
	}
}

func TestDBRecoveryWithEmptyLog(t *testing.T) {
	test := newDBTest(t)
	defer test.finalize()
	for {
		util.AssertNotError(test.put("foo", "v1"), "put", t)
		util.AssertNotError(test.put("foo", "v2"), "put", t)
		test.reopen(nil)
		test.reopen(nil)
		util.AssertNotError(test.put("foo", "v3"), "put", t)
		test.reopen(nil)
		util.AssertEqual("v3", test.get("foo"), "get", t)

		if !test.changeOptions() {
			break
		}
	}
}

func TestDBRecoverDuringMemtableCompaction(t *testing.T) {
	test := newDBTest(t)
	defer test.finalize()
	for {
		options := test.currentOptions()
		options.Env = test.env
		options.WriteBufferSize = 1000000
		test.reopen(options)
		util.AssertNotError(test.put("foo", "v1"), "put", t)
		util.AssertNotError(test.put("big1", strings.Repeat("x", 10000000)), "put", t)
		util.AssertNotError(test.put("big2", strings.Repeat("y", 1000)), "put", t)
		util.AssertNotError(test.put("bar", "v2"), "put", t)

		test.reopen(options)
		util.AssertEqual("v1", test.get("foo"), "get", t)
		util.AssertEqual("v2", test.get("bar"), "get", t)
		util.AssertEqual(strings.Repeat("x", 10000000), test.get("big1"), "get", t)
		util.AssertEqual(strings.Repeat("y", 1000), test.get("big2"), "get", t)

		if !test.changeOptions() {
			break
		}
	}
}

func dbKey(i int) string {
	return fmt.Sprintf("key%06d", i)
}

func TestMinorCompactionsHappen(t *testing.T) {
	test := newDBTest(t)
	defer test.finalize()
	options := test.currentOptions()
	options.WriteBufferSize = 10000
	test.reopen(options)

	const n = 500

	startingNumTables := test.totalTableFiles()
	for i := 0; i < n; i++ {
		util.AssertNotError(test.put(dbKey(i), dbKey(i)+strings.Repeat("v", 1000)), "put", t)
	}
	endingNumTables := test.totalTableFiles()
	util.AssertGreaterThan(endingNumTables, startingNumTables, "totalTableFiles", t)

	for i := 0; i < n; i++ {
		util.AssertEqual(dbKey(i)+strings.Repeat("v", 1000), test.get(dbKey(i)), "get", t)
	}

	test.reopen(nil)

	for i := 0; i < n; i++ {
		util.AssertEqual(dbKey(i)+strings.Repeat("v", 1000), test.get(dbKey(i)), "get", t)
	}
}

func TestDBRecoverWithLargeLog(t *testing.T) {
	test := newDBTest(t)
	defer test.finalize()
	options := test.currentOptions()
	test.reopen(options)
	util.AssertNotError(test.put("big1", strings.Repeat("1", 200000)), "put", t)
	util.AssertNotError(test.put("big2", strings.Repeat("2", 200000)), "put", t)
	util.AssertNotError(test.put("small3", strings.Repeat("3", 10)), "put", t)
	util.AssertNotError(test.put("small4", strings.Repeat("4", 10)), "put", t)
	util.AssertEqual(test.numTableFilesAtLevel(0), 0, "numTableFilesAtLevel", t)

	options = test.currentOptions()
	options.WriteBufferSize = 100000
	test.reopen(options)
	util.AssertEqual(test.numTableFilesAtLevel(0), 3, "numTableFilesAtLevel", t)
	util.AssertEqual(strings.Repeat("1", 200000), test.get("big1"), "get", t)
	util.AssertEqual(strings.Repeat("2", 200000), test.get("big2"), "get", t)
	util.AssertEqual(strings.Repeat("3", 10), test.get("small3"), "get", t)
	util.AssertEqual(strings.Repeat("4", 10), test.get("small4"), "get", t)
	util.AssertGreaterThan(test.numTableFilesAtLevel(0), 1, "numTableFilesAtLevel", t)

}

func TestDBCompactionsGenerateMultipleFiles(t *testing.T) {
	test := newDBTest(t)
	defer test.finalize()
	options := test.currentOptions()
	options.WriteBufferSize = 100000000

	rnd := util.NewRandom(301)
	util.AssertEqual(test.numTableFilesAtLevel(0), 0, "numTableFilesAtLevel", t)
	values := make([]string, 80)
	for i := 0; i < 80; i++ {
		values[i] = randomString(rnd, 100000)
		util.AssertNotError(test.put(dbKey(i), values[i]), "put", t)
	}

	test.reopen(options)
	test.dbFull().testCompactRange(0, nil, nil)
	util.AssertEqual(test.numTableFilesAtLevel(0), 0, "numTableFilesAtLevel", t)
	util.AssertGreaterThan(test.numTableFilesAtLevel(1), 1, "numTableFilesAtLevel", t)
	for i := 0; i < 80; i++ {
		util.AssertEqual(test.get(dbKey(i)), values[i], "get", t)
	}
}

func TestDBRepeatedWritesToSameKey(t *testing.T) {
	test := newDBTest(t)
	defer test.finalize()
	options := test.currentOptions()
	options.Env = test.env
	options.WriteBufferSize = 100000
	test.reopen(options)

	const maxFiles = numLevels + l0StopWritesTrigger

	rnd := util.NewRandom(301)
	value := randomString(rnd, 2*options.WriteBufferSize)
	for i := 0; i < maxFiles; i++ {
		_ = test.put("key", value)
		util.AssertLessThanOrEqual(test.totalTableFiles(), maxFiles, "totalTableFiles", t)
		fmt.Fprintf(os.Stderr, "after %d: %d files\n", i+1, test.totalTableFiles())
	}
}

func TestDBSparseMerge(t *testing.T) {
	test := newDBTest(t)
	defer test.finalize()
	options := test.currentOptions()
	options.CompressionType = ssdb.NoCompression
	test.reopen(options)

	test.fillLevels("A", "Z")

	value := strings.Repeat("x", 1000)
	_ = test.put("A", "va")
	for i := 0; i < 100000; i++ {
		key := fmt.Sprintf("B%010d", i)
		_ = test.put(key, value)
	}
	_ = test.put("C", "vc")
	_ = test.dbFull().testCompactMemTable()
	test.dbFull().testCompactRange(0, nil, nil)

	_ = test.put("A", "va2")
	_ = test.put("B100", "bvalue2")
	_ = test.put("C", "vc2")
	_ = test.dbFull().testCompactMemTable()

	util.AssertLessThanOrEqual(test.dbFull().testMaxNextLevelOverlappingBytes(), int64(20*1048576), "testMaxNextLevelOverlappingBytes", t)
	test.dbFull().testCompactRange(0, nil, nil)
	util.AssertLessThanOrEqual(test.dbFull().testMaxNextLevelOverlappingBytes(), int64(20*1048576), "testMaxNextLevelOverlappingBytes", t)
	test.dbFull().testCompactRange(1, nil, nil)
	util.AssertLessThanOrEqual(test.dbFull().testMaxNextLevelOverlappingBytes(), int64(20*1048576), "testMaxNextLevelOverlappingBytes", t)
}

func between(val, low, high uint64) bool {
	result := val >= low && val <= high
	if !result {
		fmt.Fprintf(os.Stderr, "Value %d is not in range [%d, %d]\n", val, low, high)
	}
	return result
}

func TestDBApproximateSizes(t *testing.T) {
	test := newDBTest(t)
	defer test.finalize()
	for {
		options := test.currentOptions()
		options.WriteBufferSize = 100000000
		options.CompressionType = ssdb.NoCompression
		test.destroyAndReopen(nil)

		util.AssertTrue(between(test.size("", "xyz"), 0, 0), "size", t)
		test.reopen(options)
		util.AssertTrue(between(test.size("", "xyz"), 0, 0), "size", t)

		util.AssertEqual(test.numTableFilesAtLevel(0), 0, "numTableFilesAtLevel", t)
		const (
			n  = 80
			s1 = 100000
			s2 = 105000
		)
		rnd := util.NewRandom(301)
		for i := 0; i < n; i++ {
			util.AssertNotError(test.put(dbKey(i), randomString(rnd, s1)), "put", t)
		}

		util.AssertTrue(between(test.size("", dbKey(50)), 0, 0), "size", t)

		if options.ReuseLogs {
			test.reopen(options)
			util.AssertTrue(between(test.size("", dbKey(50)), 0, 0), "size", t)
			goto cont
		}

		for run := 0; run < 3; run++ {
			test.reopen(options)

			for compactStart := 0; compactStart < n; compactStart += 10 {
				for i := 0; i < n; i += 10 {
					util.AssertTrue(between(test.size("", dbKey(i)), uint64(s1*i), uint64(s2*i)), "size", t)
					util.AssertTrue(between(test.size("", dbKey(i)+".suffix"), uint64(s1*(i+1)), uint64(s2*(i+1))), "size", t)
					util.AssertTrue(between(test.size(dbKey(i), dbKey(i+10)), uint64(s1*10), uint64(s2*10)), "size", t)
				}
				util.AssertTrue(between(test.size("", dbKey(50)), uint64(s1*50), uint64(s2*50)), "size", t)
				util.AssertTrue(between(test.size("", dbKey(50)+".suffix"), uint64(s1*50), uint64(s2*50)), "size", t)
				cstart := dbKey(compactStart)
				cend := dbKey(compactStart + 9)
				test.dbFull().testCompactRange(0, []byte(cstart), []byte(cend))
			}
			util.AssertEqual(test.numTableFilesAtLevel(0), 0, "numTableFilesAtLevel", t)
			util.AssertGreaterThan(test.numTableFilesAtLevel(1), 0, "numTableFilesAtLevel", t)
		}
	cont:
		if !test.changeOptions() {
			break
		}
	}
}

func TestDBApproximateSizesMixOfSmallAndLarge(t *testing.T) {
	test := newDBTest(t)
	defer test.finalize()
	for {
		options := test.currentOptions()
		options.CompressionType = ssdb.NoCompression
		test.reopen(nil)

		rnd := util.NewRandom(301)
		big1 := randomString(rnd, 100000)
		util.AssertNotError(test.put(dbKey(0), randomString(rnd, 10000)), "put", t)
		util.AssertNotError(test.put(dbKey(1), randomString(rnd, 10000)), "put", t)
		util.AssertNotError(test.put(dbKey(2), big1), "put", t)
		util.AssertNotError(test.put(dbKey(3), randomString(rnd, 10000)), "put", t)
		util.AssertNotError(test.put(dbKey(4), big1), "put", t)
		util.AssertNotError(test.put(dbKey(5), randomString(rnd, 10000)), "put", t)
		util.AssertNotError(test.put(dbKey(6), randomString(rnd, 300000)), "put", t)
		util.AssertNotError(test.put(dbKey(7), randomString(rnd, 10000)), "put", t)

		if options.ReuseLogs {
			util.AssertNotError(test.dbFull().testCompactMemTable(), "testCompactMemTable", t)
		}

		test.reopen(options)
		util.AssertTrue(between(test.size("", dbKey(0)), 0, 0), "size", t)
		util.AssertTrue(between(test.size("", dbKey(1)), 10000, 11000), "size", t)
		util.AssertTrue(between(test.size("", dbKey(2)), 20000, 21000), "size", t)
		util.AssertTrue(between(test.size("", dbKey(3)), 120000, 121000), "size", t)
		util.AssertTrue(between(test.size("", dbKey(4)), 130000, 131000), "size", t)
		util.AssertTrue(between(test.size("", dbKey(5)), 230000, 231000), "size", t)
		util.AssertTrue(between(test.size("", dbKey(6)), 240000, 241000), "size", t)
		util.AssertTrue(between(test.size("", dbKey(7)), 540000, 541000), "size", t)
		util.AssertTrue(between(test.size("", dbKey(8)), 550000, 560000), "size", t)
		util.AssertTrue(between(test.size(dbKey(3), dbKey(5)), 110000, 111000), "size", t)

		test.dbFull().testCompactRange(0, nil, nil)

		if !test.changeOptions() {
			break
		}
	}
}

func TestDBIteratorPinsRef(t *testing.T) {
	test := newDBTest(t)
	defer test.finalize()
	_ = test.put("foo", "hello")

	iter := test.db.NewIterator(ssdb.NewReadOptions())
	_ = test.put("foo", "newvalue1")
	for i := 0; i < 100; i++ {
		util.AssertNotError(test.put(dbKey(i), dbKey(i)+strings.Repeat("v", 100000)), "put", t)
	}
	_ = test.put("foo", "newvalue2")
	iter.SeekToFirst()
	util.AssertTrue(iter.Valid(), "Valid", t)
	util.AssertEqual([]byte("foo"), iter.Key(), "Key", t)
	util.AssertEqual([]byte("hello"), iter.Value(), "Value", t)
	iter.Next()
	util.AssertFalse(iter.Valid(), "Valid", t)
	iter.Finalize()
}

func TestDBSnapshot(t *testing.T) {
	test := newDBTest(t)
	defer test.finalize()
	for {
		_ = test.put("foo", "v1")
		s1 := test.db.GetSnapshot()
		_ = test.put("foo", "v2")
		s2 := test.db.GetSnapshot()
		_ = test.put("foo", "v3")
		s3 := test.db.GetSnapshot()

		_ = test.put("foo", "v4")
		util.AssertEqual("v1", test.getWithSnapshot("foo", s1), "getWithSnapshot", t)
		util.AssertEqual("v2", test.getWithSnapshot("foo", s2), "getWithSnapshot", t)
		util.AssertEqual("v3", test.getWithSnapshot("foo", s3), "getWithSnapshot", t)
		util.AssertEqual("v4", test.get("foo"), "get", t)

		test.db.ReleaseSnapshot(s3)
		util.AssertEqual("v1", test.getWithSnapshot("foo", s1), "getWithSnapshot", t)
		util.AssertEqual("v2", test.getWithSnapshot("foo", s2), "getWithSnapshot", t)
		util.AssertEqual("v4", test.get("foo"), "get", t)

		test.db.ReleaseSnapshot(s1)
		util.AssertEqual("v2", test.getWithSnapshot("foo", s2), "getWithSnapshot", t)
		util.AssertEqual("v4", test.get("foo"), "get", t)

		test.db.ReleaseSnapshot(s2)
		util.AssertEqual("v4", test.get("foo"), "get", t)

		if !test.changeOptions() {
			break
		}
	}
}

func TestDBHiddenValuesAreRemoved(t *testing.T) {
	test := newDBTest(t)
	defer test.finalize()
	for {
		rnd := util.NewRandom(301)
		test.fillLevels("a", "z")

		big := randomString(rnd, 50000)
		_ = test.put("foo", big)
		_ = test.put("pastfoo", "v")
		snapshot := test.db.GetSnapshot()
		_ = test.put("foo", "tiny")
		_ = test.put("pastfoo2", "v2")

		util.AssertNotError(test.dbFull().testCompactMemTable(), "testCompactMemTable", t)
		util.AssertGreaterThan(test.numTableFilesAtLevel(0), 0, "numTableFilesAtLevel", t)

		util.AssertEqual(big, test.getWithSnapshot("foo", snapshot), "getWithSnapshot", t)
		util.AssertTrue(between(test.size("", "pastfoo"), 50000, 60000), "size", t)
		test.db.ReleaseSnapshot(snapshot)
		util.AssertEqual(test.allEntriesFor("foo"), "[ tiny, "+big+" ]", "allEntriesFor", t)
		x := []byte("x")
		test.dbFull().testCompactRange(0, nil, x)
		util.AssertEqual(test.allEntriesFor("foo"), "[ tiny ]", "allEntriesFor", t)
		util.AssertEqual(test.numTableFilesAtLevel(0), 0, "numTableFilesAtLevel", t)
		util.AssertEqual(test.numTableFilesAtLevel(1), 1, "numTableFilesAtLevel", t)
		test.dbFull().testCompactRange(1, nil, x)
		util.AssertEqual(test.allEntriesFor("foo"), "[ tiny ]", "allEntriesFor", t)

		util.AssertTrue(between(test.size("", "pastfoo"), 0, 1000), "size", t)
		if !test.changeOptions() {
			break
		}
	}
}

func TestDBDeletionMarkers1(t *testing.T) {
	test := newDBTest(t)
	defer test.finalize()
	_ = test.put("foo", "v1")
	util.AssertNotError(test.dbFull().testCompactMemTable(), "testCompactMemTable", t)
	const last = maxMemCompactLevel
	util.AssertEqual(test.numTableFilesAtLevel(last), 1, "numTableFilesAtLevel", t)

	_ = test.put("a", "begin")
	_ = test.put("z", "end")
	_ = test.dbFull().testCompactMemTable()
	util.AssertEqual(test.numTableFilesAtLevel(last), 1, "numTableFilesAtLevel", t)
	util.AssertEqual(test.numTableFilesAtLevel(last-1), 1, "numTableFilesAtLevel", t)

	_ = test.delete("foo")
	_ = test.put("foo", "v2")
	util.AssertEqual(test.allEntriesFor("foo"), "[ v2, DEL, v1 ]", "allEntriesFor", t)
	util.AssertNotError(test.dbFull().testCompactMemTable(), "testCompactMemTable", t)
	util.AssertEqual(test.allEntriesFor("foo"), "[ v2, DEL, v1 ]", "allEntriesFor", t)
	z := []byte("z")
	test.dbFull().testCompactRange(last-2, nil, z)
	util.AssertEqual(test.allEntriesFor("foo"), "[ v2, v1 ]", "allEntriesFor", t)
	test.dbFull().testCompactRange(last-1, nil, nil)
	util.AssertEqual(test.allEntriesFor("foo"), "[ v2 ]", "allEntriesFor", t)
}

func TestDBDeletionMarkers2(t *testing.T) {
	test := newDBTest(t)
	defer test.finalize()
	_ = test.put("foo", "v1")
	util.AssertNotError(test.dbFull().testCompactMemTable(), "testCompactMemTable", t)
	const last = maxMemCompactLevel
	util.AssertEqual(test.numTableFilesAtLevel(last), 1, "numTableFilesAtLevel", t)

	_ = test.put("a", "begin")
	_ = test.put("z", "end")
	_ = test.dbFull().testCompactMemTable()
	util.AssertEqual(test.numTableFilesAtLevel(last), 1, "numTableFilesAtLevel", t)
	util.AssertEqual(test.numTableFilesAtLevel(last-1), 1, "numTableFilesAtLevel", t)

	_ = test.delete("foo")
	util.AssertEqual(test.allEntriesFor("foo"), "[ DEL, v1 ]", "allEntriesFor", t)
	util.AssertNotError(test.dbFull().testCompactMemTable(), "testCompactMemTable", t)
	util.AssertEqual(test.allEntriesFor("foo"), "[ DEL, v1 ]", "allEntriesFor", t)
	test.dbFull().testCompactRange(last-2, nil, nil)
	util.AssertEqual(test.allEntriesFor("foo"), "[ DEL, v1 ]", "allEntriesFor", t)
	test.dbFull().testCompactRange(last-1, nil, nil)
	util.AssertEqual(test.allEntriesFor("foo"), "[ ]", "allEntriesFor", t)
}

func TestDBOverlapInLevel0(t *testing.T) {
	test := newDBTest(t)
	defer test.finalize()
	for {
		util.AssertNotError(test.put("100", "v100"), "put", t)
		util.AssertNotError(test.put("999", "v999"), "put", t)
		_ = test.dbFull().testCompactMemTable()
		util.AssertNotError(test.delete("100"), "delete", t)
		util.AssertNotError(test.delete("999"), "delete", t)
		_ = test.dbFull().testCompactMemTable()
		util.AssertEqual("0,1,1", test.filesPerLevel(), "filesPerLevel", t)

		util.AssertNotError(test.put("300", "v300"), "put", t)
		util.AssertNotError(test.put("500", "v500"), "put", t)
		_ = test.dbFull().testCompactMemTable()
		util.AssertNotError(test.put("200", "v200"), "put", t)
		util.AssertNotError(test.put("600", "v600"), "put", t)
		util.AssertNotError(test.put("900", "v900"), "put", t)
		_ = test.dbFull().testCompactMemTable()
		util.AssertEqual("2,1,1", test.filesPerLevel(), "filesPerLevel", t)

		test.dbFull().testCompactRange(1, nil, nil)
		test.dbFull().testCompactRange(2, nil, nil)
		util.AssertEqual("2", test.filesPerLevel(), "filesPerLevel", t)

		util.AssertNotError(test.delete("600"), "delete", t)
		_ = test.dbFull().testCompactMemTable()
		util.AssertEqual("3", test.filesPerLevel(), "filesPerLevel", t)
		util.AssertEqual("NOT_FOUND", test.get("600"), "get", t)
		if !test.changeOptions() {
			break
		}
	}
}

func TestDBL0CompactionBugIssue44A(t *testing.T) {
	test := newDBTest(t)
	defer test.finalize()
	test.reopen(nil)
	util.AssertNotError(test.put("b", "v"), "put", t)
	test.reopen(nil)
	util.AssertNotError(test.delete("b"), "delete", t)
	util.AssertNotError(test.delete("a"), "delete", t)
	test.reopen(nil)
	util.AssertNotError(test.delete("a"), "delete", t)
	test.reopen(nil)
	util.AssertNotError(test.put("a", "v"), "put", t)
	test.reopen(nil)
	test.reopen(nil)
	util.AssertEqual("(a->v)", test.contents(), "contents", t)
	delayMilliseconds(1000)
	util.AssertEqual("(a->v)", test.contents(), "contents", t)
}

func TestDBL0CompactionBugIssue44B(t *testing.T) {
	test := newDBTest(t)
	defer test.finalize()
	test.reopen(nil)
	_ = test.put("", "")
	test.reopen(nil)
	_ = test.delete("e")
	_ = test.put("", "")
	test.reopen(nil)
	_ = test.put("c", "cv")
	test.reopen(nil)
	_ = test.put("", "")
	test.reopen(nil)
	_ = test.put("", "")
	delayMilliseconds(1000)
	test.reopen(nil)
	_ = test.put("d", "dv")
	test.reopen(nil)
	_ = test.put("", "")
	test.reopen(nil)
	_ = test.delete("d")
	_ = test.delete("b")
	test.reopen(nil)
	util.AssertEqual("(->)(c->cv)", test.contents(), "contents", t)
	delayMilliseconds(1000)
	util.AssertEqual("(->)(c->cv)", test.contents(), "contents", t)
}

func TestDBFflushIssue474(t *testing.T) {
	test := newDBTest(t)
	defer test.finalize()
	const num = 100000
	rnd := util.NewRandom(uint32(randomSeed()))
	for i := 0; i < num; i++ {
		testFflush()
		util.AssertNotError(test.put(randomKey(rnd), randomString(rnd, 100)), "put", t)
	}
}

func TestDBComparatorCheck(t *testing.T) {
	test := newDBTest(t)
	defer test.finalize()
	var cmp newComparator
	newOptions := test.currentOptions()
	newOptions.Comparator = &cmp
	err := test.tryOpen(newOptions)
	util.AssertError(err, "tryOpen", t)
	util.AssertTrue(strings.Contains(err.Error(), "comparator"), "error", t)
}

type newComparator struct {
}

func (c *newComparator) Compare(a, b []byte) int {
	return ssdb.BytewiseComparator.Compare(a, b)
}

func (c *newComparator) Name() string {
	return "ssdb.NewComparator"
}

func (c *newComparator) FindShortestSeparator(start *[]byte, limit []byte) {
	ssdb.BytewiseComparator.FindShortestSeparator(start, limit)
}

func (c *newComparator) FindShortSuccessor(key *[]byte) {
	ssdb.BytewiseComparator.FindShortSuccessor(key)
}

func TestDBCustomComparator(t *testing.T) {
	test := newDBTest(t)
	defer test.finalize()
	var cmp numberComparator
	newOptions := test.currentOptions()
	newOptions.CreateIfMissing = true
	newOptions.Comparator = &cmp
	newOptions.FilterPolicy = nil
	newOptions.WriteBufferSize = 1000
	test.destroyAndReopen(newOptions)
	util.AssertNotError(test.put("[10]", "ten"), "put", t)
	util.AssertNotError(test.put("[20]", "twenty"), "put", t)
	for i := 0; i < 2; i++ {
		util.AssertEqual("ten", test.get("[10]"), "get", t)
		util.AssertEqual("ten", test.get("[0xa]"), "get", t)
		util.AssertEqual("twenty", test.get("[20]"), "get", t)
		util.AssertEqual("twenty", test.get("[0x14]"), "get", t)
		util.AssertEqual("NOT_FOUND", test.get("[15]"), "get", t)
		util.AssertEqual("NOT_FOUND", test.get("[0xf]"), "get", t)
		test.compact("[0]", "[9999]")
	}

	for run := 0; run < 2; run++ {
		for i := 0; i < 1000; i++ {
			buf := fmt.Sprintf("[%d]", i*10)
			util.AssertNotError(test.put(buf, buf), "put", t)
		}
		test.compact("[0]", "[1000000]")
	}
}

type numberComparator struct {
}

func (c *numberComparator) Compare(a, b []byte) int {
	return toNumber(a) - toNumber(b)
}

func (c *numberComparator) Name() string {
	return "test.NumberComparator"
}

func (c *numberComparator) FindShortestSeparator(start *[]byte, limit []byte) {
	toNumber(*start)
	toNumber(limit)
}

func (c *numberComparator) FindShortSuccessor(key *[]byte) {
	toNumber(*key)
}

func toNumber(x []byte) int {
	if len(x) < 2 || x[0] != '[' || x[len(x)-1] != ']' {
		panic("invalid")
	}
	var val int
	var ignored byte
	if i, err := fmt.Sscanf(string(x), "[%v]%c", &val, &ignored); i <= 0 {
		panic(err)
	}
	return val
}

func TestDBManualCompaction(t *testing.T) {
	test := newDBTest(t)
	defer test.finalize()
	test.makeTables(3, "p", "q")
	util.AssertEqual("1,1,1", test.filesPerLevel(), "filesPerLevel", t)

	test.compact("", "c")
	util.AssertEqual("1,1,1", test.filesPerLevel(), "filesPerLevel", t)

	test.compact("r", "z")
	util.AssertEqual("1,1,1", test.filesPerLevel(), "filesPerLevel", t)

	test.compact("p1", "p9")
	util.AssertEqual("0,0,1", test.filesPerLevel(), "filesPerLevel", t)

	test.makeTables(3, "c", "e")
	util.AssertEqual("1,1,2", test.filesPerLevel(), "filesPerLevel", t)

	test.compact("b", "f")
	util.AssertEqual("0,0,2", test.filesPerLevel(), "filesPerLevel", t)

	test.makeTables(1, "a", "z")
	util.AssertEqual("0,1,2", test.filesPerLevel(), "filesPerLevel", t)
	test.db.CompactRange(nil, nil)
	util.AssertEqual("0,0,1", test.filesPerLevel(), "filesPerLevel", t)
}

func TestDBOpenOptions(t *testing.T) {
	dbName := tmpDir() + "/db_options_test"
	_ = Destroy(dbName, ssdb.NewOptions())

	opts := ssdb.NewOptions()
	opts.CreateIfMissing = false
	d, err := Open(opts, dbName)
	util.AssertTrue(strings.Contains(err.Error(), "does not exist"), "Open", t)
	util.AssertTrue(d == nil, "db", t)

	opts.CreateIfMissing = true
	d, err = Open(opts, dbName)
	util.AssertNotError(err, "Open", t)
	util.AssertTrue(d != nil, "db", t)
	d.(*db).finalize()
	d = nil

	opts.CreateIfMissing = false
	opts.ErrorIfExists = true
	d, err = Open(opts, dbName)
	util.AssertTrue(strings.Contains(err.Error(), "exists"), "error", t)
	util.AssertTrue(d == nil, "db", t)

	opts.CreateIfMissing = true
	opts.ErrorIfExists = false
	d, err = Open(opts, dbName)
	util.AssertNotError(err, "error", t)
	util.AssertTrue(d != nil, "db", t)

	d.(*db).finalize()
	d = nil
}

func TestDBDestroyEmptyDir(t *testing.T) {
	dbName := tmpDir() + "/db_empty_dir"
	env := newTestEnv(ssdb.DefaultEnv())
	_ = env.DeleteDir(dbName)
	util.AssertFalse(env.FileExists(dbName), "FileExists", t)

	opts := ssdb.NewOptions()
	opts.Env = env

	util.AssertNotError(env.CreateDir(dbName), "CreateDir", t)
	util.AssertTrue(env.FileExists(dbName), "FileExists", t)
	children, err := env.GetChildren(dbName)
	util.AssertNotError(err, "GetChildren", t)
	util.AssertEqual(0, len(children), "children", t)
	util.AssertNotError(Destroy(dbName, opts), "Destroy", t)
	util.AssertFalse(env.FileExists(dbName), "FileExists", t)

	env.setIgnoreDotFiles(true)
	util.AssertNotError(env.CreateDir(dbName), "CreateDir", t)
	util.AssertTrue(env.FileExists(dbName), "FileExists", t)
	children, err = env.GetChildren(dbName)
	util.AssertEqual(0, len(children), "children", t)
	util.AssertNotError(Destroy(dbName, opts), "Destroy", t)
	util.AssertFalse(env.FileExists(dbName), "FileExists", t)
}

func TestDBDestroyOpenDB(t *testing.T) {
	test := newDBTest(t)
	defer test.finalize()
	dbName := tmpDir() + "/open_db_dir"
	_ = test.env.DeleteDir(dbName)
	util.AssertFalse(test.env.FileExists(dbName), "FileExists", t)

	opts := ssdb.NewOptions()
	opts.CreateIfMissing = true
	d, err := Open(opts, dbName)
	util.AssertNotError(err, "Open", t)
	util.AssertTrue(d != nil, "db", t)

	util.AssertTrue(test.env.FileExists(dbName), "FileExists", t)
	util.AssertError(Destroy(dbName, ssdb.NewOptions()), "Destroy", t)
	util.AssertTrue(test.env.FileExists(dbName), "FileExists", t)

	d.(*db).finalize()
	d = nil
	util.AssertNotError(Destroy(dbName, ssdb.NewOptions()), "Destroy", t)
	util.AssertFalse(test.env.FileExists(dbName), "FileExists", t)
}

func TestDBLocking(t *testing.T) {
	test := newDBTest(t)
	defer test.finalize()
	_, err := Open(test.currentOptions(), test.dbName)
	util.AssertError(err, "locking", t)
}

func TestDBNoSpace(t *testing.T) {
	test := newDBTest(t)
	defer test.finalize()
	options := test.currentOptions()
	options.Env = test.env
	test.reopen(options)

	util.AssertNotError(test.put("foo", "v1"), "put", t)
	util.AssertEqual("v1", test.get("foo"), "get", t)
	test.compact("a", "z")
	numFiles := test.countFiles()
	test.env.noSpace.SetTrue()
	for i := 0; i < 10; i++ {
		for level := 0; level < numLevels-1; level++ {
			test.dbFull().testCompactRange(level, nil, nil)
		}
	}
	test.env.noSpace.SetFalse()
	util.AssertLessThan(test.countFiles(), numFiles+3, "countFiles", t)
}

func TestDBNonWritableFileSystem(t *testing.T) {
	test := newDBTest(t)
	defer test.finalize()
	options := test.currentOptions()
	options.WriteBufferSize = 1000
	options.Env = test.env
	test.reopen(options)
	util.AssertNotError(test.put("foo", "v1"), "put", t)

	test.env.nonWritable.SetTrue()
	big := strings.Repeat("x", 100000)
	errors := 0
	for i := 0; i < 20; i++ {
		fmt.Fprintf(os.Stderr, "iter %d; errors %d\n", i, errors)
		if test.put("foo", big) != nil {
			errors++
			delayMilliseconds(100)
		}
	}
	util.AssertGreaterThan(errors, 0, "errors", t)
	test.env.nonWritable.SetFalse()
}

func TestDBWriteSyncError(t *testing.T) {
	test := newDBTest(t)
	defer test.finalize()
	options := test.currentOptions()
	options.Env = test.env
	test.reopen(options)
	test.env.dataSyncError.SetTrue()

	w := ssdb.NewWriteOptions()
	util.AssertNotError(test.db.Put(w, []byte("k1"), []byte("v1")), "Put", t)
	util.AssertEqual("v1", test.get("k1"), "get", t)

	w.Sync = true
	util.AssertError(test.db.Put(w, []byte("k2"), []byte("v2")), "Put", t)
	util.AssertEqual("v1", test.get("k1"), "get", t)
	util.AssertEqual("NOT_FOUND", test.get("k2"), "get", t)

	test.env.dataSyncError.SetFalse()

	w.Sync = false
	util.AssertError(test.db.Put(w, []byte("k3"), []byte("v3")), "Put", t)
	util.AssertEqual("v1", test.get("k1"), "get", t)
	util.AssertEqual("NOT_FOUND", test.get("k2"), "get", t)
	util.AssertEqual("NOT_FOUND", test.get("k3"), "get", t)
}

func TestDBManifestWriteError(t *testing.T) {
	test := newDBTest(t)
	defer test.finalize()
	for iter := 0; iter < 2; iter++ {
		var errorType *util.AtomicBool
		if iter == 0 {
			errorType = &test.env.manifestSyncError
		} else {
			errorType = &test.env.manifestWriteError
		}
		options := test.currentOptions()
		options.Env = test.env
		options.CreateIfMissing = true
		options.ErrorIfExists = false
		test.destroyAndReopen(options)
		util.AssertNotError(test.put("foo", "bar"), "Put", t)
		util.AssertEqual("bar", test.get("foo"), "get", t)

		_ = test.dbFull().testCompactMemTable()
		util.AssertEqual("bar", test.get("foo"), "get", t)
		const last = maxMemCompactLevel
		util.AssertEqual(test.numTableFilesAtLevel(last), 1, "numTableFilesAtLevel", t)

		errorType.SetTrue()
		test.dbFull().testCompactRange(last, nil, nil)
		util.AssertEqual("bar", test.get("foo"), "get", t)

		errorType.SetFalse()
		test.reopen(options)
		util.AssertEqual("bar", test.get("foo"), "get", t)
	}
}

func TestDBMissingSSTFile(t *testing.T) {
	test := newDBTest(t)
	defer test.finalize()

	util.AssertNotError(test.put("foo", "bar"), "put", t)
	util.AssertEqual("bar", test.get("foo"), "get", t)

	_ = test.dbFull().testCompactMemTable()
	util.AssertEqual("bar", test.get("foo"), "get", t)

	test.close()
	util.AssertTrue(test.deleteAnSSTFile(), "deleteAnSSTFile", t)
	options := test.currentOptions()
	options.ParanoidChecks = true
	err := test.tryOpen(options)
	util.AssertError(err, "tryOpen", t)
	util.AssertTrue(strings.Contains(err.Error(), "issing"), "error", t)
}

func TestDBStillReadSST(t *testing.T) {
	test := newDBTest(t)
	defer test.finalize()

	util.AssertNotError(test.put("foo", "bar"), "put", t)
	util.AssertEqual("bar", test.get("foo"), "get", t)

	_ = test.dbFull().testCompactMemTable()
	util.AssertEqual("bar", test.get("foo"), "get", t)
	test.close()
	util.AssertGreaterThan(test.renameSSDBToSST(), 0, "renameSSDBToSST", t)
	options := test.currentOptions()
	options.ParanoidChecks = true
	err := test.tryOpen(options)
	util.AssertNotError(err, "tryOpen", t)
	util.AssertEqual("bar", test.get("foo"), "get", t)
}

func TestDBFilesDeletedAfterCompaction(t *testing.T) {
	test := newDBTest(t)
	defer test.finalize()
	util.AssertNotError(test.put("foo", "v2"), "put", t)
	test.compact("a", "z")
	numFiles := test.countFiles()
	for i := 0; i < 10; i++ {
		util.AssertNotError(test.put("foo", "v2"), "put", t)
		test.compact("a", "z")
	}
	util.AssertEqual(test.countFiles(), numFiles, "countFiles", t)
}

func TestDBBloomFilter(t *testing.T) {
	test := newDBTest(t)
	defer test.finalize()
	test.env.countRandomReads = true
	options := test.currentOptions()
	options.Env = test.env
	options.BlockCache = ssdb.NewLRUCache(0)
	options.FilterPolicy = ssdb.NewBloomFilterPolicy(10)
	test.reopen(options)

	const n = 10000
	for i := 0; i < n; i++ {
		util.AssertNotError(test.put(dbKey(i), dbKey(i)), "put", t)
	}
	test.compact("a", "z")
	for i := 0; i < n; i += 100 {
		util.AssertNotError(test.put(dbKey(i), dbKey(i)), "put", t)
	}
	_ = test.dbFull().testCompactMemTable()

	test.env.delayDataSync.SetTrue()
	defer test.env.delayDataSync.SetFalse()

	test.env.randomReadCounter.reset()
	for i := 0; i < n; i++ {
		util.AssertEqual(dbKey(i), test.get(dbKey(i)), "get", t)
	}
	reads := test.env.randomReadCounter.read()
	fmt.Fprintf(os.Stderr, "%d present => %d reads\n", n, reads)
	util.AssertGreaterThanOrEqual(reads, int64(n), "reads", t)
	util.AssertLessThanOrEqual(reads, int64(n+2*n/100), "reads", t)

	test.env.randomReadCounter.reset()
	for i := 0; i < n; i++ {
		util.AssertEqual("NOT_FOUND", test.get(dbKey(i)+".missing"), "get", t)
	}
	reads = test.env.randomReadCounter.read()
	fmt.Fprintf(os.Stderr, "%d missing => %d reads\n", n, reads)
	util.AssertLessThanOrEqual(reads, int64(3*n/100), "reads", t)

	test.close()
	options.BlockCache.Finalize()
}

const (
	numThreads  = 4
	testSeconds = 10
	numKeys     = 1000
)

type mtState struct {
	test       *dbTest
	stop       util.AtomicBool
	counter    [numThreads]uint32
	threadDone [numThreads]util.AtomicBool
}

type mtThread struct {
	state *mtState
	id    int
	t     *testing.T
}

func mtThreadBody(arg interface{}) {
	t := arg.(*mtThread)
	id := t.id
	db := t.state.test.db
	counter := uint32(0)
	fmt.Fprintf(os.Stderr, "... starting thread %d\n", id)
	rnd := util.NewRandom(uint32(1000 + id))
	for t.state.stop.IsFalse() {
		atomic.StoreUint32(&t.state.counter[id], counter)

		key := rnd.Uniform(numKeys)
		keyBuf := fmt.Sprintf("%016d", key)

		if rnd.OneIn(2) {
			valBuf := fmt.Sprintf("%d.%d.%-1000d", key, id, counter)
			util.AssertNotError(db.Put(ssdb.NewWriteOptions(), []byte(keyBuf), []byte(valBuf)), "Put", t.t)
		} else {
			value, err := db.Get(ssdb.NewReadOptions(), []byte(keyBuf))
			if ssdb.IsNotFound(err) {
			} else {
				util.AssertNotError(err, "Get", t.t)
				var k, w, c int
				i, _ := fmt.Sscanf(string(value), "%d.%d.%d", &k, &w, &c)
				util.AssertEqual(3, i, "Sscanf", t.t)
				util.AssertEqual(k, int(key), "key", t.t)
				util.AssertGreaterThanOrEqual(w, 0, "w", t.t)
				util.AssertLessThan(w, numThreads, "w", t.t)
				util.AssertLessThanOrEqual(c, int(atomic.LoadUint32(&t.state.counter[w])), "counter", t.t)
			}
		}
		counter++
	}
	t.state.threadDone[id].SetTrue()
	fmt.Fprintf(os.Stderr, "... stopping thread %d after %d ops\n", id, counter)
}

func TestDBMultiThreaded(t *testing.T) {
	test := newDBTest(t)
	defer test.finalize()
	var mt mtState
	for {
		mt.test = test
		mt.stop.SetFalse()
		for id := 0; id < numThreads; id++ {
			atomic.StoreUint32(&mt.counter[id], 0)
			mt.threadDone[id].SetFalse()
		}

		var thread [numThreads]mtThread
		for id := 0; id < numThreads; id++ {
			thread[id].state = &mt
			thread[id].id = id
			thread[id].t = t
			test.env.StartThread(mtThreadBody, &thread[id])
		}

		delayMilliseconds(testSeconds * 1000)

		mt.stop.SetTrue()
		for id := 0; id < numThreads; id++ {
			for mt.threadDone[id].IsFalse() {
				delayMilliseconds(100)
			}
		}
		if !test.changeOptions() {
			break
		}
	}
}

type modelDB struct {
	options *ssdb.Options
	m       map[string]string
}

func newModelDB(options *ssdb.Options) *modelDB {
	return &modelDB{
		options: options,
		m:       make(map[string]string),
	}
}

func (d *modelDB) Put(options *ssdb.WriteOptions, key, value []byte) error {
	batch := ssdb.NewWriteBatch()
	batch.Put(key, value)
	return d.Write(options, batch)
}

func (d *modelDB) Delete(options *ssdb.WriteOptions, key []byte) error {
	batch := ssdb.NewWriteBatch()
	batch.Delete(key)
	return d.Write(options, batch)
}

func (d *modelDB) Write(_ *ssdb.WriteOptions, batch ssdb.WriteBatch) error {
	handler := modelDBHandler{m: d.m}
	return batch.Iterate(&handler)
}

type modelDBHandler struct {
	m map[string]string
}

func (h *modelDBHandler) Put(key, value []byte) {
	h.m[string(key)] = string(value)
}

func (h *modelDBHandler) Delete(key []byte) {
	delete(h.m, string(key))
}

func (d *modelDB) Get(_ *ssdb.ReadOptions, _ []byte) ([]byte, error) {
	panic("not implemented")
}

func (d *modelDB) NewIterator(options *ssdb.ReadOptions) ssdb.Iterator {
	if options.Snapshot == nil {
		return newModelIterator(d.m, true)
	} else {
		snapshotState := options.Snapshot.(*modelSnapshot).m
		return newModelIterator(snapshotState, false)
	}
}

func (d *modelDB) GetSnapshot() ssdb.Snapshot {
	s := &modelSnapshot{m: make(map[string]string, len(d.m))}
	for k, v := range d.m {
		s.m[k] = v
	}
	return s
}

func (d *modelDB) ReleaseSnapshot(_ ssdb.Snapshot) {
}

func (d *modelDB) GetProperty(_ string) (string, bool) {
	return "", false
}

func (d *modelDB) GetApproximateSizes(r []ssdb.Range) []uint64 {
	return make([]uint64, len(r))
}

func (d *modelDB) CompactRange(_, _ []byte) {
}

type modelSnapshot struct {
	m map[string]string
}

type modelIterator struct {
	m     map[string]string
	keys  []string
	owned bool
	iter  int
}

func newModelIterator(m map[string]string, owned bool) *modelIterator {
	i := &modelIterator{
		m:     m,
		owned: owned,
		iter:  -1,
	}
	i.keys = make([]string, 0, len(m))
	for k := range m {
		i.keys = append(i.keys, k)
	}
	sort.Strings(i.keys)
	return i
}

func (i *modelIterator) Valid() bool {
	return i.iter != -1
}

func (i *modelIterator) SeekToFirst() {
	if len(i.keys) == 0 {
		i.iter = -1
	} else {
		i.iter = 0
	}
}

func (i *modelIterator) SeekToLast() {
	if l := len(i.keys); l == 0 {
		i.iter = -1
	} else {
		i.iter = l
	}
}

func (i *modelIterator) Seek(target []byte) {
	i.iter = sort.Search(len(i.keys), func(x int) bool {
		return strings.Compare(i.keys[x], string(target)) >= 0
	})
}

func (i *modelIterator) Next() {
	i.iter++
	if i.iter == len(i.keys) {
		i.iter = -1
	}
}

func (i *modelIterator) Prev() {
	if i.iter >= 0 {
		i.iter--
	}
}

func (i *modelIterator) Key() []byte {
	return []byte(i.keys[i.iter])
}

func (i *modelIterator) Value() []byte {
	return []byte(i.m[i.keys[i.iter]])
}

func (i *modelIterator) Status() error {
	return nil
}

func (i *modelIterator) RegisterCleanUp(function ssdb.CleanUpFunction, arg1, arg2 interface{}) {
}

func (i *modelIterator) Finalize() {
}

func compareIterators(step int, model, db ssdb.DB, modelSnap, dbSnap ssdb.Snapshot) bool {
	options := ssdb.NewReadOptions()
	options.Snapshot = modelSnap
	mIter := model.NewIterator(options)
	options = ssdb.NewReadOptions()
	options.Snapshot = dbSnap
	dbIter := db.NewIterator(options)
	var (
		ok    = true
		count = 0
	)
	mIter.SeekToFirst()
	dbIter.SeekToFirst()
	for ok && mIter.Valid() && dbIter.Valid() {
		count++
		if bytes.Compare(mIter.Key(), dbIter.Key()) != 0 {
			fmt.Fprintf(os.Stderr, "step %d: Key mismatch: '%s' vs. '%s'\n", step, string(mIter.Key()), string(dbIter.Key()))
			ok = false
			break
		}

		if bytes.Compare(mIter.Value(), dbIter.Value()) != 0 {
			fmt.Fprintf(os.Stderr, "step %d: Value mismatch for key '%s': '%s' vs. '%s'\n", step, string(mIter.Key()), string(mIter.Value()), string(dbIter.Value()))
			ok = false
		}
		mIter.Next()
		dbIter.Next()
	}

	if ok {
		if mIter.Valid() != dbIter.Valid() {
			fmt.Fprintf(os.Stderr, "step %d: Mismatch at end of iterators: %v vs. %v\n", step, mIter.Valid(), dbIter.Valid())
			ok = false
		}
	}
	fmt.Fprintf(os.Stderr, "%d entries compared: ok=%v\n", count, ok)
	mIter.Finalize()
	dbIter.Finalize()
	return ok
}

func TestDBRandomized(t *testing.T) {
	test := newDBTest(t)
	defer test.finalize()
	rnd := util.NewRandom(uint32(util.RandomSeed()))
	const n = 10000
	for {
		model := newModelDB(test.currentOptions())
		var modelSnap, dbSnap ssdb.Snapshot
		var k, v []byte
		var l int
		for step := 0; step < n; step++ {
			if step%100 == 0 {
				fmt.Fprintf(os.Stderr, "Step %d of %d\n", step, n)
			}
			p := rnd.Uniform(100)
			if p < 45 {
				k = []byte(randomKey(rnd))
				if rnd.OneIn(20) {
					l = 100 + int(rnd.Uniform(100))
				} else {
					l = int(rnd.Uniform(8))
				}
				v = []byte(randomString(rnd, l))
				util.AssertNotError(model.Put(ssdb.NewWriteOptions(), k, v), "model Put", t)
				util.AssertNotError(test.db.Put(ssdb.NewWriteOptions(), k, v), "db Put", t)
			} else if p < 90 {
				k = []byte(randomKey(rnd))
				util.AssertNotError(model.Delete(ssdb.NewWriteOptions(), k), "model Delete", t)
				util.AssertNotError(test.db.Delete(ssdb.NewWriteOptions(), k), "db Delete", t)
			} else {
				b := ssdb.NewWriteBatch()
				num := int(rnd.Uniform(8))
				for i := 0; i < num; i++ {
					if i == 0 || !rnd.OneIn(10) {
						k = []byte(randomKey(rnd))
					} else {
					}
					if rnd.OneIn(2) {
						v = []byte(randomString(rnd, int(rnd.Uniform(10))))
						b.Put(k, v)
					} else {
						b.Delete(k)
					}
				}
				util.AssertNotError(model.Write(ssdb.NewWriteOptions(), b), "model Write", t)
				util.AssertNotError(test.db.Write(ssdb.NewWriteOptions(), b), "db Write", t)
			}

			if step%100 == 0 {
				util.AssertTrue(compareIterators(step, model, test.db, nil, nil), "compareIterators", t)
				util.AssertTrue(compareIterators(step, model, test.db, modelSnap, dbSnap), "compareIterators", t)
				if modelSnap != nil {
					model.ReleaseSnapshot(modelSnap)
				}
				if dbSnap != nil {
					test.db.ReleaseSnapshot(dbSnap)
				}
				test.reopen(nil)
				util.AssertTrue(compareIterators(step, model, test.db, nil, nil), "compareIterators", t)

				modelSnap = model.GetSnapshot()
				dbSnap = test.db.GetSnapshot()
			}
		}
		if modelSnap != nil {
			model.ReleaseSnapshot(modelSnap)
		}
		if dbSnap != nil {
			test.db.ReleaseSnapshot(dbSnap)
		}
		if !test.changeOptions() {
			break
		}
	}
}

func makeDBKey(num uint) []byte {
	buf := bytes.NewBuffer(make([]byte, 20))
	fmt.Fprintf(buf, "%016d", num)
	return buf.Bytes()
}

func bmLogAndApply(iters, numBaseFiles int, t *testing.T) {
	dbName := tmpDir() + "/leveldb_test_benchmark"
	_ = Destroy(dbName, ssdb.NewOptions())

	opts := ssdb.NewOptions()
	opts.CreateIfMissing = true
	d, err := Open(opts, dbName)
	util.AssertNotError(err, "Open", t)
	util.AssertTrue(d != nil, "db", t)

	d.(*db).finalize()
	d = nil

	env := ssdb.DefaultEnv()

	var mu sync.Mutex
	mu.Lock()
	defer mu.Unlock()

	cmp := newInternalKeyComparator(ssdb.BytewiseComparator)
	options := ssdb.NewOptions()
	vset := newVersionSet(dbName, options, nil, cmp)
	_, err = vset.recover()
	util.AssertNotError(err, "recover", t)

	vbase := newVersionEdit()
	fnum := uint64(1)
	for i := 0; i < numBaseFiles; i++ {
		start := newInternalKey(makeDBKey(uint(2*fnum)), 1, ssdb.TypeValue)
		limit := newInternalKey(makeDBKey(uint(2*fnum+1)), 1, ssdb.TypeDeletion)
		fnum++
		vbase.addFile(2, fnum, 1, *start, *limit)
	}
	util.AssertNotError(vset.logAndApply(vbase, &mu), "logAndApply", t)

	startMicros := env.NowMicros()

	for i := 0; i < iters; i++ {
		vedit := newVersionEdit()
		vedit.deletedFile(2, fnum)
		start := newInternalKey(makeDBKey(uint(2*fnum)), 1, ssdb.TypeValue)
		limit := newInternalKey(makeDBKey(uint(2*fnum+1)), 1, ssdb.TypeDeletion)
		fnum++
		vbase.addFile(2, fnum, 1, *start, *limit)
		_ = vset.logAndApply(vedit, &mu)
	}

	stopMicros := env.NowMicros()
	us := stopMicros - startMicros
	fmt.Fprintf(os.Stderr, "BM_LogAndApply/%-6d   %8d iters : %9d us (%7.0f us / iter)\n", numBaseFiles, iters, us, float64(us)/float64(iters))
}

func TestDBLogAndApply(t *testing.T) {
	bmLogAndApply(1000, 1, t)
	bmLogAndApply(1000, 100, t)
	bmLogAndApply(1000, 10000, t)
	bmLogAndApply(100, 100000, t)
}
