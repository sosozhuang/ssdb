package db

import (
	"fmt"
	"os"
	"runtime"
	"sort"
	"ssdb"
	"ssdb/util"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"
)

type testKey uint64

func compare(i1, i2 skipListKey) int {
	a, ok1 := i1.(testKey)
	b, ok2 := i2.(testKey)
	if !ok1 || !ok2 {
		panic("not testKey")
	}
	if a < b {
		return -1
	} else if a > b {
		return 1
	}
	return 0
}

type testKeySlice []testKey

func (s testKeySlice) Less(i, j int) bool {
	return (s)[i] < (s)[j]
}

func (s testKeySlice) Swap(i, j int) {
	(s)[i], (s)[j] = (s)[j], (s)[i]
}

func (s testKeySlice) Len() int {
	return len(s)
}

func (s testKeySlice) element(i int) testKey {
	return s[i]
}

func TestEmptySkipList(t *testing.T) {
	list := newSkipList(compare)
	util.AssertFalse(list.contains(testKey(10)), "list contains 10", t)

	iter := newSkipListIterator(list)
	util.AssertFalse(iter.valid(), "iter is not valid", t)
	iter.seekToFirst()
	util.AssertFalse(iter.valid(), "iter is not valid", t)
	iter.seek(testKey(100))
	util.AssertFalse(iter.valid(), "iter is not valid", t)
	iter.seekToLast()
	util.AssertFalse(iter.valid(), "iter is not valid", t)
}

func TestInsertAndLookup(t *testing.T) {
	n, r := testKey(2000), testKey(5000)
	rnd := util.NewRandom(1000)
	testKeyMap := make(map[testKey]bool)
	testKeys := testKeySlice(make([]testKey, 0, n))
	list := newSkipList(compare)
	var key testKey
	for i := testKey(0); i < n; i++ {
		key = testKey(rnd.Next() % uint32(r))
		if _, ok := testKeyMap[key]; !ok {
			testKeyMap[key] = true
			testKeys = append(testKeys, key)
			list.insert(key)
		}
	}

	sort.Sort(testKeys)

	for i := testKey(0); i < n; i++ {
		if list.contains(i) {
			_, ok := testKeyMap[i]
			util.AssertTrue(ok, "list contains key", t)
		} else {
			_, ok := testKeyMap[i]
			util.AssertFalse(ok, "list not contains key", t)
		}
	}

	iter := newSkipListIterator(list)
	util.AssertFalse(iter.valid(), "iter is not valid", t)
	iter.seek(testKey(0))
	util.AssertTrue(iter.valid(), "iter is valid", t)
	util.AssertEqual(testKeys.element(0), iter.key(), "first element equals to key", t)

	iter.seekToFirst()
	util.AssertTrue(iter.valid(), "iter is valid", t)
	util.AssertEqual(testKeys.element(0), iter.key(), "first element equals to key", t)

	iter.seekToLast()
	util.AssertTrue(iter.valid(), "iter is valid", t)
	util.AssertEqual(testKeys.element(testKeys.Len()-1), iter.key(), "last element equals to key", t)

	for i := testKey(0); i < r; i++ {
		iter := newSkipListIterator(list)
		iter.seek(i)
		index := sort.Search(testKeys.Len(), func(x int) bool {
			return testKeys.element(x) >= i
		})
		for j := testKey(0); j < 3; j++ {
			if index >= testKeys.Len() {
				util.AssertFalse(iter.valid(), "iter is not valid", t)
				break
			} else {
				util.AssertTrue(iter.valid(), "iter is valid", t)
				element := testKeys.element(index)
				util.AssertEqual(element, iter.key(), "key", t)
				index++
				iter.next()
			}
		}
	}

	iter = newSkipListIterator(list)
	iter.seekToFirst()
	for i := 0; i < testKeys.Len(); i++ {
		util.AssertTrue(iter.valid(), "iter is valid", t)
		util.AssertEqual(testKeys.element(i), iter.key(), "element equals to key", t)
		//fmt.Println(testKeys.element(i), iter.key())
		iter.next()
	}
	util.AssertFalse(iter.valid(), "iter is not valid", t)
}

const k = 4

func shiftKey(key testKey) uint64 {
	return uint64(key) >> 40
}

func gen(key testKey) uint64 {
	return (uint64(key) >> 8) & 0xffffffff
}

func hash(key testKey) uint64 {
	return uint64(key) & 0xff
}

func hashNumbers(k, g uint64) uint64 {
	data := make([]byte, 0, 16)
	x := *(*[8]byte)(unsafe.Pointer(&k))
	data = append(data, x[:]...)
	x = *(*[8]byte)(unsafe.Pointer(&g))
	data = append(data, x[:]...)
	return uint64(util.Hash(data, 0))
}

func makeKey(k, g uint64) testKey {
	if k > 4 {
		panic("k > 4")
	}
	if g > 0xffffffff {
		panic("g > 0xffffffff")
	}
	return testKey((k << 40) | (g << 8) | (hashNumbers(k, g) & 0xff))
}

func isValidKey(k testKey) bool {
	return hash(k) == hashNumbers(shiftKey(k), gen(k))&0xff
}

func randomTarget(rnd *util.Random) testKey {
	switch rnd.Next() % 10 {
	case 0:
		return makeKey(0, 0)
	case 1:
		return makeKey(k, 0)
	default:
		return makeKey(uint64(rnd.Next()%k), 0)
	}
}

type state struct {
	generation [k]uint64
}

func (s *state) set(k, v int) {
	atomic.StoreUint64(&s.generation[k], uint64(v))
}

func (s *state) get(k int) int {
	return int(atomic.LoadUint64(&s.generation[k]))
}

func newState() *state {
	s := &state{
		generation: [k]uint64{},
	}
	for i := 0; i < k; i++ {
		s.set(i, 0)
	}
	return s
}

type concurrentTest struct {
	current *state
	list    *skipList
}

func (ct *concurrentTest) writeStep(rnd *util.Random) {
	i := rnd.Next() % k
	g := ct.current.get(int(i)) + 1
	key := makeKey(uint64(i), uint64(g))
	ct.list.insert(key)
	ct.current.set(int(i), g)
}

func (ct *concurrentTest) readStep(rnd *util.Random, t *testing.T) {
	initialState := newState()
	for i := 0; i < k; i++ {
		initialState.set(i, ct.current.get(i))
	}
	pos := randomTarget(rnd)
	iter := newSkipListIterator(ct.list)
	iter.seek(pos)
	var current testKey
	for {
		if !iter.valid() {
			current = makeKey(k, 0)
		} else {
			current = iter.key().(testKey)
			util.AssertTrue(isValidKey(current), "isValidKey(current)", t)
		}

		util.AssertLessThanOrEqual(uint64(pos), uint64(current), "pos <= current", t)

		for pos < current {
			util.AssertLessThan(shiftKey(pos), uint64(k), "shiftKey < k", t)

			if !(gen(pos) == 0 || gen(pos) > uint64(initialState.get(int(shiftKey(pos))))) {
				t.Fatalf("key: %d, gen: %d, initgen: %d.\n", shiftKey(pos), gen(pos), initialState.get(int(shiftKey(pos))))
			}

			if shiftKey(pos) < shiftKey(current) {
				pos = makeKey(shiftKey(pos)+1, 0)
			} else {
				pos = makeKey(shiftKey(pos), gen(pos)+1)
			}
		}

		if !iter.valid() {
			break
		}
		if rnd.Next()%2 == 1 {
			iter.next()
			pos = makeKey(shiftKey(pos), gen(pos)+1)
		} else {
			newTarget := randomTarget(rnd)
			if newTarget > pos {
				pos = newTarget
				iter.seek(newTarget)
			}
		}
	}
}

func newConcurrentTest() *concurrentTest {
	t := &concurrentTest{
		current: newState(),
	}
	t.list = newSkipList(compare)
	return t
}

type readerState uint8

const (
	starting readerState = iota
	running
	done
)

type testState struct {
	test     *concurrentTest
	t        *testing.T
	seed     int
	quitFlag uint32
	mu       sync.Mutex
	cond     *sync.Cond
	state    readerState
}

func (s *testState) wait(state readerState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for s.state != state {
		s.cond.Wait()
	}
}

func (s *testState) change(state readerState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = state
	s.cond.Signal()
}

func newTestState(seed int, t *testing.T) *testState {
	s := &testState{
		test:     newConcurrentTest(),
		t:        t,
		seed:     seed,
		quitFlag: 0,
		state:    starting,
	}
	s.cond = sync.NewCond(&s.mu)
	return s
}

func randomSeed() int {
	result := 301
	if env, ok := os.LookupEnv("TEST_RANDOM_SEED"); ok {
		result, _ = strconv.Atoi(env)
	}
	if result <= 0 {
		result = 301
	}
	return result
}

func concurrentReader(i interface{}) {
	state := i.(*testState)
	rnd := util.NewRandom(uint32(state.seed))
	reads := 0
	state.change(running)
	for atomic.LoadUint32(&state.quitFlag) == 0 {
		state.test.readStep(rnd, state.t)
		reads++
	}
	state.change(done)
}

func runConcurrent(run int, t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU() * 2)
	seed := randomSeed() + (run * 100)
	rnd := util.NewRandom(uint32(seed))
	const n, size = 1000, 1000
	var state *testState
	for i := 0; i < n; i++ {
		if i%100 == 0 {
			fmt.Fprintf(os.Stderr, "Run %d of %d\n", i, n)
		}
		state = newTestState(seed+1, t)
		ssdb.DefaultEnv().Schedule(concurrentReader, state)
		for i := 0; i < size; i++ {
			state.test.writeStep(rnd)
		}
		atomic.StoreUint32(&state.quitFlag, 1)
		state.wait(done)
	}
}

func TestConcurrentWithoutThreads(t *testing.T) {
	test := newConcurrentTest()
	rnd := util.NewRandom(uint32(randomSeed()))
	for i := 0; i < 10000; i++ {
		test.readStep(rnd, t)
		test.writeStep(rnd)
	}
}

func TestConcurrent1(t *testing.T) {
	runConcurrent(1, t)
}

func TestConcurrent2(t *testing.T) {
	runConcurrent(2, t)
}

func TestConcurrent3(t *testing.T) {
	runConcurrent(3, t)
}

func TestConcurrent4(t *testing.T) {
	runConcurrent(4, t)
}

func TestConcurrent5(t *testing.T) {
	runConcurrent(5, t)
}
