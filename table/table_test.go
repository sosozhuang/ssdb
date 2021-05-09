package table

import (
	"fmt"
	"os"
	"sort"
	"ssdb"
	"ssdb/util"
	"strings"
	"testing"
	"unsafe"
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

func (s *stringSink) Finalize() {
}

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

func (s *stringSource) Finalize() {
}

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
}

type constructor struct {
	data       kvMap
	finishImpl func(*ssdb.Options, kvMap) error
	t          *testing.T
}

func newConstructor(cmp ssdb.Comparator, finishImpl func(*ssdb.Options, kvMap) error, t *testing.T) *constructor {
	return &constructor{
		data: kvMap{
			m:  make(map[string]string),
			s:  make([]string, 0),
			op: newSTLLessThanWithComparator(cmp),
		},
		finishImpl: finishImpl,
		t:          t,
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

type blockConstructor struct {
	*constructor
	comparator ssdb.Comparator
	data       []byte
	block      *block
}

func newBlockConstructor(cmp ssdb.Comparator, t *testing.T) *blockConstructor {
	bc := &blockConstructor{
		comparator: cmp,
		data:       nil,
		block:      nil,
	}
	bc.constructor = newConstructor(cmp, bc.finishImpl, t)
	return bc
}

func (c *blockConstructor) finishImpl(options *ssdb.Options, data kvMap) error {
	c.block = nil
	builder := newBlockBuilder(options)
	for _, key := range data.s {
		builder.add([]byte(key), []byte(data.m[key]))
	}
	c.data = builder.finish()
	contents := blockContents{
		data:          c.data,
		cachable:      false,
		heapAllocated: false,
	}
	c.block = newBlock(&contents)
	return nil
}

func (c *blockConstructor) newIterator() ssdb.Iterator {
	return c.block.newIterator(c.comparator)
}

type tableConstructor struct {
	*constructor
	source *stringSource
	table  ssdb.Table
}

func newTableConstructor(cmp ssdb.Comparator, t *testing.T) *tableConstructor {
	tc := &tableConstructor{
		source: nil,
		table:  nil,
	}
	tc.constructor = newConstructor(cmp, tc.finishImpl, t)
	return tc
}

func (c *tableConstructor) finishImpl(options *ssdb.Options, data kvMap) error {
	c.reset()
	sink := newStringSink()
	builder := NewBuilder(options, sink)
	for _, key := range data.s {
		builder.Add([]byte(key), []byte(data.m[key]))
		util.AssertNotError(builder.Status(), "builder.Status", c.t)
	}
	err := builder.Finish()
	util.AssertNotError(err, "builder.Finish", c.t)
	util.AssertEqual(uint64(len(sink.contents)), builder.FileSize(), "builder.FileSize", c.t)

	c.source = newStringSource(sink.contents)
	tableOptions := ssdb.NewOptions()
	tableOptions.Comparator = options.Comparator
	c.table, err = Open(tableOptions, c.source, uint64(len(sink.contents)))
	return err
}

func (c *tableConstructor) newIterator() ssdb.Iterator {
	return c.table.NewIterator(ssdb.NewReadOptions())
}

func (c *tableConstructor) approximateOffset(key []byte) uint64 {
	return c.table.ApproximateOffsetOf(key)
}

func (c *tableConstructor) reset() {
	c.table = nil
	c.source = nil
}

type testType int8

const (
	tableTest testType = iota
	blockTest
)

type testArgs struct {
	t               testType
	reverseCompare  bool
	restartInterval int
}

var testArgList = []testArgs{
	{tableTest, false, 16},
	{tableTest, false, 1},
	{tableTest, false, 1024},
	{tableTest, true, 16},
	{tableTest, true, 1},
	{tableTest, true, 1024},

	{blockTest, false, 16},
	{blockTest, false, 1},
	{blockTest, false, 1024},
	{blockTest, true, 16},
	{blockTest, true, 1},
	{blockTest, true, 1024},
}
var numTestArgs = len(testArgList)

type harness struct {
	options     *ssdb.Options
	constructor constructorInterface
	t           *testing.T
}

func newHarness(t *testing.T) *harness {
	return &harness{
		t: t,
	}
}

func (h *harness) init(args *testArgs) {
	h.constructor = nil
	h.options = ssdb.NewOptions()
	h.options.BlockRestartInterval = args.restartInterval
	h.options.BlockSize = 256
	if args.reverseCompare {
		h.options.Comparator = &revKeyComparator
	}
	switch args.t {
	case tableTest:
		h.constructor = newTableConstructor(h.options.Comparator, h.t)
	case blockTest:
		h.constructor = newBlockConstructor(h.options.Comparator, h.t)
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

func TestEmpty(t *testing.T) {
	h := newHarness(t)
	var rnd *util.Random
	for _, args := range testArgList {
		h.init(&args)
		rnd = util.NewRandom(uint32(util.RandomSeed() + 1))
		h.test(rnd)
	}
}

func TestZeroRestartPointsInBlock(t *testing.T) {
	data := make([]byte, unsafe.Sizeof(uint32(0)))
	for i := range data {
		data[i] = 0
	}
	contents := blockContents{
		data:          data,
		cachable:      false,
		heapAllocated: false,
	}
	block := newBlock(&contents)
	iter := block.newIterator(ssdb.BytewiseComparator)
	iter.SeekToFirst()
	util.AssertFalse(iter.Valid(), "SeekToFirst", t)
	iter.SeekToLast()
	util.AssertFalse(iter.Valid(), "SeekToLast", t)
	iter.Seek([]byte("foo"))
	util.AssertFalse(iter.Valid(), "Seek", t)
}

func TestSimpleEmptyKey(t *testing.T) {
	h := newHarness(t)
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

func between(val, low, high uint64) bool {
	result := val >= low && val <= high
	if !result {
		fmt.Fprintf(os.Stderr, "Value %d is not in range [%d, %d]\n", val, low, high)
	}
	return result
}

func TestApproximateOffsetOfPlain(t *testing.T) {
	c := newTableConstructor(ssdb.BytewiseComparator, t)
	c.add("k01", "hello")
	c.add("k02", "hello2")
	c.add("k03", strings.Repeat("x", 10000))
	c.add("k04", strings.Repeat("x", 200000))
	c.add("k05", strings.Repeat("x", 300000))
	c.add("k06", "hello3")
	c.add("k07", strings.Repeat("x", 100000))

	var (
		keys  []string
		kvMap kvMap
	)
	options := ssdb.NewOptions()
	options.BlockSize = 1024
	options.CompressionType = ssdb.NoCompression
	c.finish(options, &keys, &kvMap)
	util.AssertTrue(between(c.approximateOffset([]byte("abc")), 0, 0), "approximateOffset", t)
	util.AssertTrue(between(c.approximateOffset([]byte("k01")), 0, 0), "approximateOffset", t)
	util.AssertTrue(between(c.approximateOffset([]byte("k01a")), 0, 0), "approximateOffset", t)
	util.AssertTrue(between(c.approximateOffset([]byte("k02")), 0, 0), "approximateOffset", t)
	util.AssertTrue(between(c.approximateOffset([]byte("k03")), 0, 0), "approximateOffset", t)
	util.AssertTrue(between(c.approximateOffset([]byte("k04")), 10000, 11000), "approximateOffset", t)
	util.AssertTrue(between(c.approximateOffset([]byte("k04a")), 210000, 211000), "approximateOffset", t)
	util.AssertTrue(between(c.approximateOffset([]byte("k05")), 210000, 211000), "approximateOffset", t)
	util.AssertTrue(between(c.approximateOffset([]byte("k06")), 510000, 511000), "approximateOffset", t)
	util.AssertTrue(between(c.approximateOffset([]byte("k07")), 510000, 511000), "approximateOffset", t)
	util.AssertTrue(between(c.approximateOffset([]byte("xyz")), 610000, 612000), "approximateOffset", t)
}

func TestApproximateOffsetOfCompressed(t *testing.T) {
	rnd := util.NewRandom(301)
	c := newTableConstructor(ssdb.BytewiseComparator, t)
	c.add("k01", "hello")
	c.add("k02", util.CompressibleString(rnd, 0.25, 10000))
	c.add("k03", "hello3")
	c.add("k04", util.CompressibleString(rnd, 0.25, 10000))
	var (
		keys  []string
		kvMap kvMap
	)
	options := ssdb.NewOptions()
	options.BlockSize = 1024
	options.CompressionType = ssdb.SnappyCompression
	c.finish(options, &keys, &kvMap)

	const (
		slop     = 1000
		expected = 2500
		minZ     = expected - slop
		maxZ     = expected + slop
	)
	util.AssertTrue(between(c.approximateOffset([]byte("abc")), 0, slop), "approximateOffset", t)
	util.AssertTrue(between(c.approximateOffset([]byte("k01")), 0, slop), "approximateOffset", t)
	util.AssertTrue(between(c.approximateOffset([]byte("k02")), 0, slop), "approximateOffset", t)
	util.AssertTrue(between(c.approximateOffset([]byte("k03")), minZ, maxZ), "approximateOffset", t)
	util.AssertTrue(between(c.approximateOffset([]byte("k04")), minZ, maxZ), "approximateOffset", t)
	util.AssertTrue(between(c.approximateOffset([]byte("xyz")), 2*minZ, 2*maxZ), "approximateOffset", t)
}
