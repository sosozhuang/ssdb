package ssdb

import (
	"fmt"
	"os"
	"ssdb/util"
	"testing"
)

const verbose = 1

func nextLength(length int) int {
	if length < 10 {
		length += 1
	} else if length < 100 {
		length += 10
	} else if length < 1000 {
		length += 100
	} else {
		length += 1000
	}
	return length
}

func key(i int, buffer *[4]byte) string {
	util.EncodeFixed32(buffer, uint32(i))
	return string(buffer[:])
}

type bloomTest struct {
	policy FilterPolicy
	filter []byte
	keys   []string
}

func (bt *bloomTest) reset() {
	bt.filter = make([]byte, 0)
	bt.keys = make([]string, 0)
}

func (bt *bloomTest) add(key string) {
	bt.keys = append(bt.keys, key)
}

func (bt *bloomTest) build() {
	bt.filter = make([]byte, 0)
	keys := make([][]byte, 0)
	for _, key := range bt.keys {
		keys = append(keys, []byte(key))
	}
	bt.policy.CreateFilter(keys, &bt.filter)
	bt.keys = nil
	if verbose >= 2 {
		bt.dumpFilter()
	}
}

func (bt *bloomTest) filterSize() int {
	return len(bt.filter)
}

func (bt *bloomTest) dumpFilter() {
	fmt.Fprintf(os.Stderr, "F(")
	l := len(bt.filter)
	for i := 0; i+1 < l; i++ {
		for j := uint(0); j < 8; j++ {
			if int(bt.filter[i])&(i<<j) == 1 {
				fmt.Fprintf(os.Stderr, "%c", '1')
			} else {
				fmt.Fprintf(os.Stderr, "%c", '.')
			}
		}
	}
	fmt.Fprintf(os.Stderr, ")\n")
}

func (bt *bloomTest) matches(b []byte) bool {
	if len(bt.keys) > 0 {
		bt.build()
	}
	return bt.policy.KeyMayMatch(b, bt.filter)
}

func (bt *bloomTest) falsePositiveRate() float64 {
	var buffer [4]byte
	result := 0
	for i := 0; i < 10000; i++ {
		if bt.matches([]byte(key(i+1000000000, &buffer))) {
			result++
		}
	}
	return float64(result) / 10000.0
}

func newBloomTest() *bloomTest {
	return &bloomTest{policy: NewBloomFilterPolicy(10)}
}

func TestEmptyFilter(t *testing.T) {
	bt := newBloomTest()
	util.TestFalse(bt.matches([]byte("hello")), "empty filter", t)
	util.TestFalse(bt.matches([]byte("world")), "empty filter", t)
}

func TestSmall(t *testing.T) {
	bt := newBloomTest()
	bt.add("hello")
	bt.add("world")
	util.TestTrue(bt.matches([]byte("hello")), "small filter", t)
	util.TestTrue(bt.matches([]byte("world")), "small filter", t)
	util.TestFalse(bt.matches([]byte("x")), "small filter", t)
	util.TestFalse(bt.matches([]byte("foo")), "small filter", t)
}

func TestVaryingLengths(t *testing.T) {
	bt := newBloomTest()
	var buffer [4]byte
	mediocreFilters, goodFilters := 0, 0
	for length := 1; length <= 10000; length = nextLength(length) {
		bt.reset()
		for i := 0; i < length; i++ {
			bt.add(key(i, &buffer))
		}
		bt.build()
		if bt.filterSize() > ((length * 10 / 8) + 40) {
			t.Error("filter size")
		}
		for i := 0; i < length; i++ {
			util.TestTrue(bt.matches([]byte(key(i, &buffer))), "varying key", t)
		}

		rate := bt.falsePositiveRate()
		if verbose >= 1 {
			fmt.Fprintf(os.Stderr, "False positives: %5.2f%% @ length = %6d ; bytes = %6d\n",
				rate*100.0, length, bt.filterSize())
		}
		if rate > 0.02 {
			t.Errorf("rate: %f\n", rate)
		}
		if rate > 0.0125 {
			mediocreFilters++
		} else {
			goodFilters++
		}
	}

	if verbose >= 1 {
		fmt.Fprintf(os.Stderr, "Filters: %d good, %d mediocre\n", goodFilters, mediocreFilters)
	}
	if mediocreFilters > goodFilters/5 {
		t.Errorf("%d > %d.\n", mediocreFilters, goodFilters/5)
	}
}
