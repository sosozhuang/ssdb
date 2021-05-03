package db

import (
	"container/heap"
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
	a, _ := i1.(testKey)
	b, _ := i2.(testKey)
	if a < b {
		return -1
	} else if a > b {
		return 1
	}
	return 0
}

type myHeap []uint64

func (h *myHeap) Less(i, j int) bool {
	return (*h)[i] < (*h)[j]
}

func (h *myHeap) Swap(i, j int) {
	(*h)[i], (*h)[j] = (*h)[j], (*h)[i]
}

func (h *myHeap) Len() int {
	return len(*h)
}

func (h *myHeap) Pop() (v interface{}) {
	*h, v = (*h)[:h.Len()-1], (*h)[h.Len()-1]
	return
}

func (h *myHeap) Push(v interface{}) {
	*h = append(*h, v.(uint64))
}

func (h myHeap) element(i int) uint64 {
	return h[i]
}

func TestEmptySkipList(t *testing.T) {
	arena := util.NewArena()
	list := newSkipList(compare, arena)
	util.AssertFalse(list.contains(10), "list contains 10", t)

	iter := newSkipListIterator(list)
	util.AssertFalse(iter.valid(), "iter is not valid", t)
	iter.seekToFirst()
	util.AssertFalse(iter.valid(), "iter is not valid", t)
	iter.seek(100)
	util.AssertFalse(iter.valid(), "iter is not valid", t)
	iter.seekToLast()
	util.AssertFalse(iter.valid(), "iter is not valid", t)
}

func TestInsertAndLookup(t *testing.T) {
	n, r := uint64(2000), uint32(5000)
	rnd := util.NewRandom(1000)
	keys := make(map[uint64]bool)
	h := new(myHeap)
	arena := util.NewArena()
	list := newSkipList(compare, arena)
	var key uint64
	for i := uint64(0); i < n; i++ {
		key = uint64(rnd.Next() % r)
		if _, ok := keys[key]; !ok {
			keys[key] = true
			heap.Push(h, key)
			list.insert(key)
		}
	}

	for i := uint64(0); i < n; i++ {
		if list.contains(i) {
			_, ok := keys[i]
			util.AssertTrue(ok, "list contains key", t)
		} else {
			_, ok := keys[i]
			util.AssertFalse(ok, "list contains key", t)
		}
	}

	iter := newSkipListIterator(list)
	util.AssertFalse(iter.valid(), "iter is not valid", t)
	iter.seek(0)
	util.AssertTrue(iter.valid(), "iter is valid", t)
	util.AssertEqual(h.element(0), iter.key(), "first element equals to key", t)

	iter.seekToFirst()
	util.AssertTrue(iter.valid(), "iter is valid", t)
	util.AssertEqual(h.element(0), iter.key(), "first element equals to key", t)

	iter.seekToLast()
	util.AssertTrue(iter.valid(), "iter is valid", t)
	sort.Sort(h)
	util.AssertEqual(h.element(h.Len()-1), iter.key(), "last element equals to key", t)

	for i := uint32(0); i < r; i++ {
		iter := newSkipListIterator(list)
		iter.seek(i)

		for j := 0; j < 3; j++ {
		}
	}

	iter = newSkipListIterator(list)
	iter.seekToLast()
	for i := h.Len() - 1; i >= 0; i-- {
		util.AssertTrue(iter.valid(), "iter is valid", t)
		util.AssertEqual(h.element(i), iter.key(), "element equals to key", t)
		iter.prev()
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
	arena   *util.Arena
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
		arena:   util.NewArena(),
	}
	t.list = newSkipList(compare, t.arena)
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
