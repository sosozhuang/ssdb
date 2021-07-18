package db

import (
	"fmt"
	"os"
	"sort"
	"ssdb"
	"ssdb/table"
	"ssdb/util"
	"strconv"
	"strings"
	"testing"
)

func reverseString(key string) string {
	var b strings.Builder
	n := len(key)
	b.Grow(n)
	n--
	for i := range key {
		b.WriteByte(key[n-i])
	}
	return b.String()
}

func reverseBytes(key []byte) []byte {
	n := len(key)
	r := make([]byte, n)
	n--
	for i := range key {
		r[i] = key[n-i]
	}
	return r

}

type reverseKeyComparator struct{}

func (c *reverseKeyComparator) Compare(a, b []byte) int {
	return ssdb.BytewiseComparator.Compare(reverseBytes(a), reverseBytes(b))
}

func (c *reverseKeyComparator) Name() string {
	return "ssdb.ReverseBytewiseComparator"
}

func (c *reverseKeyComparator) FindShortestSeparator(start *[]byte, limit []byte) {
	s := reverseBytes(*start)
	l := reverseBytes(limit)
	ssdb.BytewiseComparator.FindShortestSeparator(&s, l)
	*start = reverseBytes(s)
}

func (c *reverseKeyComparator) FindShortSuccessor(key *[]byte) {
	s := reverseBytes(*key)
	ssdb.BytewiseComparator.FindShortSuccessor(&s)
	*key = reverseBytes(s)
}

var revKeyComparator reverseKeyComparator

func increment(cmp ssdb.Comparator, key *string) {
	if cmp == ssdb.BytewiseComparator {
		*key += "\000"
	} else {
		if cmp != &revKeyComparator {
			panic("cmp != revKeyComparator")
		}
		rev := reverseString(*key)
		rev += "\000"
		*key = reverseString(rev)
	}
}

type stlLessThan struct {
	cmp ssdb.Comparator
}

func newSTLLessThan() *stlLessThan {
	return &stlLessThan{cmp: ssdb.BytewiseComparator}
}

func newSTLLessThanWithComparator(c ssdb.Comparator) *stlLessThan {
	return &stlLessThan{cmp: c}
}

func (t *stlLessThan) less(a, b string) bool {
	return t.cmp.Compare([]byte(a), []byte(b)) < 0
}

type stringSink struct {
	contents []byte
}

func newStringSink() *stringSink {
	return &stringSink{contents: make([]byte, 0)}
}

func (s *stringSink) Append(data []byte) error {
	s.contents = append(s.contents, data...)
	return nil
}

func (s *stringSink) Close() error {
	return nil
}

func (s *stringSink) Flush() error {
	return nil
}

func (s *stringSink) Sync() error {
	return nil
}

//func (s *stringSink) Finalize() {
//}

func (s *stringSink) getContents() []byte {
	return s.contents
}

type stringSource struct {
	contents []byte
}

func newStringSource(contents []byte) *stringSource {
	s := &stringSource{
		contents: make([]byte, len(contents)),
	}
	copy(s.contents, contents)
	return s
}

func (s *stringSource) Read(b []byte, offset int64) (result []byte, n int, err error) {
	if offset >= int64(len(s.contents)) {
		err = util.InvalidArgumentError1("invalid read offset")
		return
	}
	n = len(b)
	if offset+int64(len(b)) > int64(len(s.contents)) {
		n = int(int64(len(s.contents)) - offset)
	}
	copy(b, s.contents[offset:offset+int64(n)])
	result = b[:n]
	return
}

//func (s *stringSource) Finalize() {
//}

func (s *stringSource) size() int {
	return len(s.contents)
}

type kvMap struct {
	m  map[string]string
	s  []string
	op *stlLessThan
}

func (kvm *kvMap) put(key, value string) {
	if _, ok := kvm.m[key]; !ok {
		kvm.s = append(kvm.s, key)
	}
	kvm.m[key] = value
}

func (kvm *kvMap) sort() {
	sort.Sort(kvm)
}

func (kvm *kvMap) clear() {
	kvm.m = make(map[string]string)
	kvm.s = make([]string, 0)
}

func (kvm *kvMap) Len() int {
	return len(kvm.s)
}

func (kvm *kvMap) Less(i, j int) bool {
	return kvm.op.less(kvm.s[i], kvm.s[j])
}

func (kvm *kvMap) Swap(i, j int) {
	kvm.s[i], kvm.s[j] = kvm.s[j], kvm.s[i]
}

type constructorInterface interface {
	add(key, value string)
	finish(options *ssdb.Options, keys *[]string, data *kvMap)
	newIterator() ssdb.Iterator
	getData() kvMap
	db() ssdb.DB
	finalize()
}

type constructor struct {
	data         kvMap
	finishImpl   func(*ssdb.Options, kvMap) error
	finalizeImpl func()
	t            *testing.T
}

func newConstructor(cmp ssdb.Comparator, finishImpl func(*ssdb.Options, kvMap) error, finalizeImpl func(), t *testing.T) *constructor {
	return &constructor{
		data: kvMap{
			m:  make(map[string]string),
			s:  make([]string, 0),
			op: newSTLLessThanWithComparator(cmp),
		},
		finishImpl:   finishImpl,
		finalizeImpl: finalizeImpl,
		t:            t,
	}
}

func (c *constructor) add(key string, value string) {
	c.data.put(key, value)
}

func (c *constructor) finish(options *ssdb.Options, keys *[]string, kvMap *kvMap) {
	*kvMap = c.data
	*keys = make([]string, len(c.data.s))
	c.data.sort()
	copy(*keys, c.data.s)
	c.data.clear()
	util.AssertNotError(c.finishImpl(options, *kvMap), "finishImpl", c.t)
}

func (c *constructor) getData() kvMap {
	return c.data
}

func (c *constructor) finalize() {
	c.finalizeImpl()
}

type keyConvertingIterator struct {
	table.CleanUpIterator
	err  error
	iter ssdb.Iterator
}

func (i *keyConvertingIterator) Valid() bool {
	return i.iter.Valid()
}

func (i *keyConvertingIterator) SeekToFirst() {
	i.iter.SeekToFirst()
}

func (i *keyConvertingIterator) SeekToLast() {
	i.iter.SeekToLast()
}

func (i *keyConvertingIterator) Seek(target []byte) {
	ikey := parsedInternalKey{
		userKey:   target,
		sequence:  maxSequenceNumber,
		valueType: ssdb.TypeValue,
	}
	encoded := make([]byte, 0)
	appendInternalKey(&encoded, &ikey)
	i.iter.Seek(encoded)
}

func (i *keyConvertingIterator) Next() {
	i.iter.Next()
}

func (i *keyConvertingIterator) Prev() {
	i.iter.Prev()
}

func (i *keyConvertingIterator) Key() []byte {
	var key parsedInternalKey
	if !parseInternalKey(i.iter.Key(), &key) {
		i.err = util.CorruptionError1("malformed internal key")
		return []byte("corrupted key")
	}
	return key.userKey
}

func (i *keyConvertingIterator) Value() []byte {
	return i.iter.Value()
}

func (i *keyConvertingIterator) Status() error {
	if i.err == nil {
		return i.iter.Status()
	}
	return i.err
}

func (i *keyConvertingIterator) Close() {
	i.iter.Close()
	i.CleanUpIterator.Close()
}

func newKeyConvertingIterator(iter ssdb.Iterator) *keyConvertingIterator {
	return &keyConvertingIterator{iter: iter}
}

type memTableConstructor struct {
	*constructor
	internalComparator *internalKeyComparator
	memtable           *MemTable
}

func newMemTableConstructor(cmp ssdb.Comparator, t *testing.T) *memTableConstructor {
	m := &memTableConstructor{
		internalComparator: newInternalKeyComparator(cmp),
	}
	m.constructor = newConstructor(cmp, m.finishImpl, m.finalize, t)
	m.memtable = NewMemTable(m.internalComparator)
	m.memtable.ref()
	return m
}

func (c *memTableConstructor) finishImpl(options *ssdb.Options, data kvMap) error {
	c.memtable.unref()
	c.memtable = NewMemTable(c.internalComparator)
	c.memtable.ref()
	seq := sequenceNumber(1)
	for _, key := range data.s {
		c.memtable.add(seq, ssdb.TypeValue, []byte(key), []byte(data.m[key]))
		seq++
	}
	return nil
}

func (c *memTableConstructor) newIterator() ssdb.Iterator {
	return newKeyConvertingIterator(c.memtable.newIterator())
}

func (c *memTableConstructor) db() ssdb.DB {
	return nil
}

func (c *memTableConstructor) finalize() {
	c.memtable.unref()
}

type dbConstructor struct {
	*constructor
	comparator  ssdb.Comparator
	dbInterface ssdb.DB
}

func newDBConstructor(cmp ssdb.Comparator, t *testing.T) constructorInterface {
	d := &dbConstructor{
		comparator: cmp,
	}
	d.constructor = newConstructor(cmp, d.finishImpl, d.finalize, t)
	d.newDB()
	return d
}

func (d *dbConstructor) finishImpl(options *ssdb.Options, data kvMap) error {
	if d.dbInterface != nil {
		d.dbInterface.Close()
		d.dbInterface = nil
	}
	d.newDB()
	for _, key := range data.s {
		batch := ssdb.NewWriteBatch()
		batch.Put([]byte(key), []byte(data.m[key]))
		util.AssertNotError(d.dbInterface.Write(ssdb.NewWriteOptions(), batch), "db.Write", d.t)
	}
	return nil
}

func (d *dbConstructor) newIterator() ssdb.Iterator {
	return d.dbInterface.NewIterator(ssdb.NewReadOptions())
}

func (d *dbConstructor) db() ssdb.DB {
	return d.dbInterface
}

func (d *dbConstructor) finalize() {
	d.dbInterface.Close()
}

func tmpDir() string {
	dir, _ := ssdb.DefaultEnv().GetTestDirectory()
	return dir
}

func (d *dbConstructor) newDB() {
	name := tmpDir() + "/table_testdb"
	options := ssdb.NewOptions()
	options.Comparator = d.comparator
	err := Destroy(name, options)
	util.AssertNotError(err, "Destroy", d.t)

	options.CreateIfMissing = true
	options.ErrorIfExists = true
	options.WriteBufferSize = 10000
	d.dbInterface, err = Open(options, name)
	util.AssertNotError(err, "Open", d.t)
}

type testType int8

const (
	memtableTestType testType = iota
	dbTestType
)

type testArgs struct {
	t               testType
	reverseCompare  bool
	restartInterval int
}

var testArgList = []testArgs{
	// Restart interval does not matter for memtables
	{memtableTestType, false, 16},
	{memtableTestType, true, 16},

	// Do not bother with restart interval variations for DB
	{dbTestType, false, 16},
	{dbTestType, true, 16},
}
var numTestArgs = len(testArgList)

type harness struct {
	options     *ssdb.Options
	constructor constructorInterface
	t           *testing.T
}

func newHarness(t *testing.T) *harness {
	return &harness{t: t}
}

func (h *harness) init(args *testArgs) {
	if h.constructor != nil {
		h.constructor.finalize()
	}
	h.constructor = nil
	h.options = ssdb.NewOptions()
	h.options.BlockRestartInterval = args.restartInterval
	h.options.BlockSize = 256
	if args.reverseCompare {
		h.options.Comparator = &revKeyComparator
	}
	switch args.t {
	case memtableTestType:
		h.constructor = newMemTableConstructor(h.options.Comparator, h.t)
	case dbTestType:
		h.constructor = newDBConstructor(h.options.Comparator, h.t)
	}
}

func (h *harness) add(key, value string) {
	h.constructor.add(key, value)
}

func (h *harness) test(rnd *util.Random) {
	var (
		keys []string
		data kvMap
	)
	h.constructor.finish(h.options, &keys, &data)
	h.testForwardScan(keys, data)
	h.testBackwardScan(keys, data)
	h.testRandomAccess(rnd, keys, data)
}

func (h *harness) testForwardScan(keys []string, data kvMap) {
	iter := h.constructor.newIterator()
	util.AssertFalse(iter.Valid(), "iter.Valid()", h.t)
	iter.SeekToFirst()
	for i := range data.s {
		util.AssertEqual(kvToString(data, i), iteratorToString(iter), "forwardScan", h.t)
		iter.Next()
	}
	util.AssertFalse(iter.Valid(), "iter.Valid()", h.t)
	iter.Close()
}

func (h *harness) testBackwardScan(keys []string, data kvMap) {
	iter := h.constructor.newIterator()
	util.AssertFalse(iter.Valid(), "iter.Valid()", h.t)
	iter.SeekToLast()
	for i := data.Len() - 1; i >= 0; i-- {
		util.AssertEqual(kvToString(data, i), iteratorToString(iter), "backwardScan", h.t)
		iter.Prev()
	}
	util.AssertFalse(iter.Valid(), "iter.Valid()", h.t)
	iter.Close()
}

func (h *harness) testRandomAccess(rnd *util.Random, keys []string, data kvMap) {
	const verbose = false
	iter := h.constructor.newIterator()
	util.AssertFalse(iter.Valid(), "iter.Valid()", h.t)
	index := 0
	if verbose {
		_, _ = fmt.Fprintln(os.Stderr, "---")
	}
	for i := 0; i < 200; i++ {
		toss := rnd.Uniform(5)
		switch toss {
		case 0:
			if iter.Valid() {
				if verbose {
					fmt.Fprintln(os.Stderr, "Next")
				}
				iter.Next()
				index++
				util.AssertEqual(kvToString(data, index), iteratorToString(iter), "next", h.t)
			}
		case 1:
			if verbose {
				fmt.Fprintln(os.Stderr, "SeekToFirst")
			}
			iter.SeekToFirst()
			index = 0
			util.AssertEqual(kvToString(data, index), iteratorToString(iter), "seekToFirst", h.t)
		case 2:
			key := h.pickRandomKey(rnd, keys)
			index = sort.Search(data.Len(), func(i int) bool {
				return !data.op.less(data.s[i], key)
			})
			if index < data.Len() && data.op.less(data.s[index], key) {
				index = data.Len()
			}
			if verbose {
				fmt.Fprintf(os.Stderr, "Seek '%s'\n", util.EscapeString([]byte(key)))
			}
			iter.Seek([]byte(key))
			util.AssertEqual(kvToString(data, index), iteratorToString(iter), "seek", h.t)
		case 3:
			if iter.Valid() {
				if verbose {
					fmt.Fprintln(os.Stderr, "Prev")
				}
				iter.Prev()
				if index == 0 {
					index = data.Len()
				} else {
					index--
				}
				util.AssertEqual(kvToString(data, index), iteratorToString(iter), "prev", h.t)
			}
		case 4:
			if verbose {
				fmt.Fprintln(os.Stderr, "SeekToLast")
			}
			iter.SeekToLast()
			if len(keys) == 0 {
				index = data.Len()
			} else {
				last := data.s[data.Len()-1]
				index = sort.Search(data.Len(), func(i int) bool {
					return !data.op.less(data.s[i], last)
				})
				if index < data.Len() && data.op.less(data.s[index], last) {
					index = data.Len()
				}
			}
			util.AssertEqual(kvToString(data, index), iteratorToString(iter), "seekToLast", h.t)
		}
	}
	iter.Close()
}

func kvToString(data kvMap, i int) string {
	if i >= data.Len() {
		return "END"
	}
	key := data.s[i]
	return "'" + key + "->" + data.m[key] + "'"
}

func iteratorToString(iter ssdb.Iterator) string {
	if !iter.Valid() {
		return "END"
	}
	return "'" + string(iter.Key()) + "->" + string(iter.Value()) + "'"
}

func (h *harness) pickRandomKey(rnd *util.Random, keys []string) string {
	if len(keys) == 0 {
		return "foo"
	}
	index := rnd.Uniform(len(keys))
	result := keys[index]
	switch rnd.Uniform(3) {
	case 0:
	case 1:
		if len(result) != 0 && result[len(result)-1] > '\000' {
			var b strings.Builder
			if len(result) >= 2 {
				b.WriteString(result[:len(result)-2])
			}
			b.WriteByte(result[len(result)-1] - 1)
			result = b.String()
		}
	case 2:
		increment(h.options.Comparator, &result)
	}
	return result
}

func (h *harness) db() ssdb.DB {
	return h.constructor.db()
}

func (h *harness) finish() {
	if h.constructor != nil {
		h.constructor.finalize()
	}
}

func TestEmpty(t *testing.T) {
	h := newHarness(t)
	defer h.finish()
	var rnd *util.Random
	for _, args := range testArgList {
		h.init(&args)
		rnd = util.NewRandom(uint32(util.RandomSeed() + 1))
		h.test(rnd)
	}
}

func TestSimpleEmptyKey(t *testing.T) {
	h := newHarness(t)
	defer h.finish()
	var rnd *util.Random
	for _, args := range testArgList {
		h.init(&args)
		rnd = util.NewRandom(uint32(util.RandomSeed() + 1))
		h.add("", "v")
		h.test(rnd)
	}
}

func TestSimpleSingle(t *testing.T) {
	h := newHarness(t)
	defer h.finish()
	var rnd *util.Random
	for _, args := range testArgList {
		h.init(&args)
		rnd = util.NewRandom(uint32(util.RandomSeed() + 2))
		h.add("abc", "v")
		h.test(rnd)
	}
}

func TestSimpleMulti(t *testing.T) {
	h := newHarness(t)
	defer h.finish()
	var rnd *util.Random
	for _, args := range testArgList {
		h.init(&args)
		rnd = util.NewRandom(uint32(util.RandomSeed() + 3))
		h.add("abc", "v")
		h.add("abcd", "v")
		h.add("ac", "v2")
		h.test(rnd)
	}
}

func TestSimpleSpecialKey(t *testing.T) {
	h := newHarness(t)
	defer h.finish()
	var rnd *util.Random
	for _, args := range testArgList {
		h.init(&args)
		rnd = util.NewRandom(uint32(util.RandomSeed() + 4))
		h.add("\xff\xff", "v3")
		h.test(rnd)
	}
}

func TestRandomized(t *testing.T) {
	h := newHarness(t)
	defer h.finish()
	var rnd *util.Random
	for i, args := range testArgList {
		h.init(&args)
		rnd = util.NewRandom(uint32(util.RandomSeed() + 5))
		for numEntries := 0; numEntries < 2000; {
			if numEntries%10 == 0 {
				fmt.Fprintf(os.Stderr, "case %d of %d: num_entries = %d\n", i+1, numTestArgs, numEntries)
			}
			for e := 0; e < numEntries; e++ {
				h.add(util.RandomKey(rnd, int(rnd.Skewed(4))), util.RandomString(rnd, int(rnd.Skewed(5))))
			}
			h.test(rnd)
			if numEntries < 50 {
				numEntries += 1
			} else {
				numEntries += 200
			}
		}
	}
}

func TestRandomizedLongDB(t *testing.T) {
	h := newHarness(t)
	defer h.finish()
	rnd := util.NewRandom(uint32(util.RandomSeed()))
	h.init(&testArgs{dbTestType, false, 16})
	numEntries := 100000
	for e := 0; e < numEntries; e++ {
		h.add(util.RandomKey(rnd, int(rnd.Skewed(4))), util.RandomString(rnd, int(rnd.Skewed(5))))
	}
	h.test(rnd)

	files := 0
	var (
		name  string
		value string
		ok    bool
	)
	for level := 0; level < numLevels; level++ {
		name = fmt.Sprintf("ssdb.num-files-at-level%d", level)
		value, ok = h.db().GetProperty(name)
		util.AssertTrue(ok, "db.GetProperty", t)
		i, _ := strconv.Atoi(value)
		files += i
	}
	util.AssertGreaterThan(files, 0, "files", t)
}

func TestSimple(t *testing.T) {
	memtable := NewMemTable(newInternalKeyComparator(ssdb.BytewiseComparator))
	memtable.ref()
	batch := ssdb.NewWriteBatch()
	batch.(writeBatchInternal).SetSequence(100)
	batch.Put([]byte("k1"), []byte("v1"))
	batch.Put([]byte("k2"), []byte("v2"))
	batch.Put([]byte("k3"), []byte("v3"))
	batch.Put([]byte("largekey"), []byte("vlarge"))
	util.AssertNotError(insertInto(batch, memtable), "insertInto", t)

	iter := memtable.newIterator()
	iter.SeekToFirst()
	for iter.Valid() {
		fmt.Fprintf(os.Stderr, "key: '%s' -> '%s'\n", iter.Key(), iter.Value())
		iter.Next()
	}
	iter.Close()
	memtable.unref()
	memtable.release()
}
