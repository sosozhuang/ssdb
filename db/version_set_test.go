package db

import (
	"ssdb"
	"ssdb/util"
	"testing"
)

type findFileTest struct {
	disjointSortedFiles bool
	files               []*fileMetaData
}

func newFindFileTest() *findFileTest {
	return &findFileTest{
		disjointSortedFiles: true,
		files:               make([]*fileMetaData, 0),
	}
}

func (t *findFileTest) add(smallest, largest []byte, smallestSeq, largestSeq sequenceNumber) {
	f := newFileMetaData()
	f.number = uint64(len(t.files) + 1)
	f.smallest = *newInternalKey(smallest, smallestSeq, ssdb.TypeValue)
	f.largest = *newInternalKey(largest, largestSeq, ssdb.TypeValue)
	t.files = append(t.files, f)
}

func (t *findFileTest) find(key []byte) int {
	target := newInternalKey(key, 100, ssdb.TypeValue)
	cmp := newInternalKeyComparator(ssdb.BytewiseComparator)
	return findFile(cmp, t.files, target.encode())
}

func (t *findFileTest) overlaps(smallest, largest []byte) bool {
	cmp := newInternalKeyComparator(ssdb.BytewiseComparator)
	return someFileOverlapsRange(cmp, t.disjointSortedFiles, t.files, smallest, largest)
}

func TestEmptyFindFile(t *testing.T) {
	test := newFindFileTest()
	util.AssertEqual(0, test.find([]byte("foo")), "find", t)
	util.AssertFalse(test.overlaps([]byte("a"), []byte("z")), "overlaps", t)
	util.AssertFalse(test.overlaps(nil, []byte("z")), "overlaps", t)
	util.AssertFalse(test.overlaps([]byte("a"), nil), "overlaps", t)
	util.AssertFalse(test.overlaps(nil, nil), "overlaps", t)
}

func TestSingleFindFile(t *testing.T) {
	test := newFindFileTest()
	test.add([]byte("p"), []byte("q"), 100, 100)
	util.AssertEqual(0, test.find([]byte("a")), "find", t)
	util.AssertEqual(0, test.find([]byte("p")), "find", t)
	util.AssertEqual(0, test.find([]byte("p1")), "find", t)
	util.AssertEqual(0, test.find([]byte("q")), "find", t)
	util.AssertEqual(1, test.find([]byte("q1")), "find", t)
	util.AssertEqual(1, test.find([]byte("z")), "find", t)

	util.AssertFalse(test.overlaps([]byte("a"), []byte("b")), "overlaps", t)
	util.AssertFalse(test.overlaps([]byte("z1"), []byte("z2")), "overlaps", t)
	util.AssertTrue(test.overlaps([]byte("a"), []byte("p")), "overlaps", t)
	util.AssertTrue(test.overlaps([]byte("a"), []byte("q")), "overlaps", t)
	util.AssertTrue(test.overlaps([]byte("a"), []byte("z")), "overlaps", t)
	util.AssertTrue(test.overlaps([]byte("p"), []byte("p1")), "overlaps", t)
	util.AssertTrue(test.overlaps([]byte("p"), []byte("q")), "overlaps", t)
	util.AssertTrue(test.overlaps([]byte("p"), []byte("z")), "overlaps", t)
	util.AssertTrue(test.overlaps([]byte("p1"), []byte("p2")), "overlaps", t)
	util.AssertTrue(test.overlaps([]byte("p1"), []byte("z")), "overlaps", t)
	util.AssertTrue(test.overlaps([]byte("q"), []byte("q")), "overlaps", t)
	util.AssertTrue(test.overlaps([]byte("q"), []byte("q1")), "overlaps", t)

	util.AssertFalse(test.overlaps(nil, []byte("j")), "overlaps", t)
	util.AssertFalse(test.overlaps([]byte("r"), nil), "overlaps", t)
	util.AssertTrue(test.overlaps(nil, []byte("p")), "overlaps", t)
	util.AssertTrue(test.overlaps(nil, []byte("p1")), "overlaps", t)
	util.AssertTrue(test.overlaps([]byte("q"), nil), "overlaps", t)
	util.AssertTrue(test.overlaps(nil, nil), "overlaps", t)
}

func TestMultiple(t *testing.T) {
	test := newFindFileTest()
	test.add([]byte("150"), []byte("200"), 100, 100)
	test.add([]byte("200"), []byte("250"), 100, 100)
	test.add([]byte("300"), []byte("350"), 100, 100)
	test.add([]byte("400"), []byte("450"), 100, 100)

	util.AssertEqual(0, test.find([]byte("100")), "find", t)
	util.AssertEqual(0, test.find([]byte("150")), "find", t)
	util.AssertEqual(0, test.find([]byte("151")), "find", t)
	util.AssertEqual(0, test.find([]byte("199")), "find", t)
	util.AssertEqual(0, test.find([]byte("200")), "find", t)
	util.AssertEqual(1, test.find([]byte("201")), "find", t)
	util.AssertEqual(1, test.find([]byte("249")), "find", t)
	util.AssertEqual(1, test.find([]byte("250")), "find", t)
	util.AssertEqual(2, test.find([]byte("251")), "find", t)
	util.AssertEqual(2, test.find([]byte("299")), "find", t)
	util.AssertEqual(2, test.find([]byte("300")), "find", t)
	util.AssertEqual(2, test.find([]byte("349")), "find", t)
	util.AssertEqual(2, test.find([]byte("350")), "find", t)
	util.AssertEqual(3, test.find([]byte("351")), "find", t)
	util.AssertEqual(3, test.find([]byte("400")), "find", t)
	util.AssertEqual(3, test.find([]byte("450")), "find", t)
	util.AssertEqual(4, test.find([]byte("451")), "find", t)

	util.AssertFalse(test.overlaps([]byte("100"), []byte("149")), "overlaps", t)
	util.AssertFalse(test.overlaps([]byte("251"), []byte("299")), "overlaps", t)
	util.AssertFalse(test.overlaps([]byte("451"), []byte("500")), "overlaps", t)
	util.AssertFalse(test.overlaps([]byte("351"), []byte("399")), "overlaps", t)

	util.AssertTrue(test.overlaps([]byte("100"), []byte("150")), "overlaps", t)
	util.AssertTrue(test.overlaps([]byte("100"), []byte("200")), "overlaps", t)
	util.AssertTrue(test.overlaps([]byte("100"), []byte("300")), "overlaps", t)
	util.AssertTrue(test.overlaps([]byte("100"), []byte("400")), "overlaps", t)
	util.AssertTrue(test.overlaps([]byte("100"), []byte("500")), "overlaps", t)
	util.AssertTrue(test.overlaps([]byte("375"), []byte("400")), "overlaps", t)
	util.AssertTrue(test.overlaps([]byte("450"), []byte("450")), "overlaps", t)
	util.AssertTrue(test.overlaps([]byte("450"), []byte("500")), "overlaps", t)
}

func TestMultipleNullBoundaries(t *testing.T) {
	test := newFindFileTest()
	test.add([]byte("150"), []byte("200"), 100, 100)
	test.add([]byte("200"), []byte("250"), 100, 100)
	test.add([]byte("300"), []byte("350"), 100, 100)
	test.add([]byte("400"), []byte("450"), 100, 100)

	util.AssertFalse(test.overlaps(nil, []byte("149")), "overlaps", t)
	util.AssertFalse(test.overlaps([]byte("451"), nil), "overlaps", t)
	util.AssertTrue(test.overlaps(nil, nil), "overlaps", t)
	util.AssertTrue(test.overlaps(nil, []byte("150")), "overlaps", t)
	util.AssertTrue(test.overlaps(nil, []byte("199")), "overlaps", t)
	util.AssertTrue(test.overlaps(nil, []byte("200")), "overlaps", t)
	util.AssertTrue(test.overlaps(nil, []byte("201")), "overlaps", t)
	util.AssertTrue(test.overlaps(nil, []byte("400")), "overlaps", t)
	util.AssertTrue(test.overlaps(nil, []byte("800")), "overlaps", t)
	util.AssertTrue(test.overlaps([]byte("100"), nil), "overlaps", t)
	util.AssertTrue(test.overlaps([]byte("200"), nil), "overlaps", t)
	util.AssertTrue(test.overlaps([]byte("449"), nil), "overlaps", t)
	util.AssertTrue(test.overlaps([]byte("450"), nil), "overlaps", t)
}

func TestOverlapSequenceChecks(t *testing.T) {
	test := newFindFileTest()
	test.add([]byte("200"), []byte("200"), 5000, 3000)

	util.AssertFalse(test.overlaps([]byte("199"), []byte("199")), "overlaps", t)
	util.AssertFalse(test.overlaps([]byte("201"), []byte("300")), "overlaps", t)
	util.AssertTrue(test.overlaps([]byte("200"), []byte("200")), "overlaps", t)
	util.AssertTrue(test.overlaps([]byte("190"), []byte("200")), "overlaps", t)
	util.AssertTrue(test.overlaps([]byte("200"), []byte("210")), "overlaps", t)
}

func TestOverlappingFiles(t *testing.T) {
	test := newFindFileTest()
	test.add([]byte("150"), []byte("600"), 100, 100)
	test.add([]byte("400"), []byte("500"), 100, 100)
	test.disjointSortedFiles = false

	util.AssertFalse(test.overlaps([]byte("199"), []byte("149")), "overlaps", t)
	util.AssertFalse(test.overlaps([]byte("601"), []byte("700")), "overlaps", t)
	util.AssertTrue(test.overlaps([]byte("100"), []byte("150")), "overlaps", t)
	util.AssertTrue(test.overlaps([]byte("100"), []byte("200")), "overlaps", t)
	util.AssertTrue(test.overlaps([]byte("100"), []byte("300")), "overlaps", t)
	util.AssertTrue(test.overlaps([]byte("100"), []byte("400")), "overlaps", t)
	util.AssertTrue(test.overlaps([]byte("100"), []byte("500")), "overlaps", t)
	util.AssertTrue(test.overlaps([]byte("375"), []byte("400")), "overlaps", t)
	util.AssertTrue(test.overlaps([]byte("450"), []byte("450")), "overlaps", t)
	util.AssertTrue(test.overlaps([]byte("450"), []byte("500")), "overlaps", t)
	util.AssertTrue(test.overlaps([]byte("450"), []byte("700")), "overlaps", t)
	util.AssertTrue(test.overlaps([]byte("600"), []byte("700")), "overlaps", t)
}

type addBoundaryInputsTest struct {
	levelFiles      []*fileMetaData
	compactionFiles []*fileMetaData
	allFiles        []*fileMetaData
	icmp            *internalKeyComparator
}

func newAddBoundaryInputsTest() *addBoundaryInputsTest {
	return &addBoundaryInputsTest{
		levelFiles:      make([]*fileMetaData, 0),
		compactionFiles: make([]*fileMetaData, 0),
		allFiles:        make([]*fileMetaData, 0),
		icmp:            newInternalKeyComparator(ssdb.BytewiseComparator),
	}
}

func (t *addBoundaryInputsTest) createFileMetaData(number uint64, smallest, largest *internalKey) *fileMetaData {
	f := newFileMetaData()
	f.number = number
	f.smallest = *smallest
	f.largest = *largest
	t.allFiles = append(t.allFiles, f)
	return f
}

func TestEmptyFileSets(t *testing.T) {
	test := newAddBoundaryInputsTest()
	addBoundaryInputs(test.icmp, test.levelFiles, &test.compactionFiles)
	util.AssertTrue(len(test.compactionFiles) == 0, "length of compactionFiles", t)
	util.AssertTrue(len(test.levelFiles) == 0, "length of levelFiles", t)
}

func TestEmptyLevelFiles(t *testing.T) {
	test := newAddBoundaryInputsTest()
	f1 := test.createFileMetaData(1, newInternalKey([]byte("100"), 2, ssdb.TypeValue), newInternalKey([]byte("100"), 1, ssdb.TypeValue))
	test.compactionFiles = append(test.compactionFiles, f1)
	addBoundaryInputs(test.icmp, test.levelFiles, &test.compactionFiles)
	util.AssertEqual(1, len(test.compactionFiles), "length of compactionFiles", t)
	util.AssertEqual(f1, test.compactionFiles[0], "f1", t)
	util.AssertTrue(len(test.levelFiles) == 0, "length of levelFiles", t)
}

func TestEmptyCompactionFiles(t *testing.T) {
	test := newAddBoundaryInputsTest()
	f1 := test.createFileMetaData(1, newInternalKey([]byte("100"), 2, ssdb.TypeValue), newInternalKey([]byte("100"), 1, ssdb.TypeValue))
	test.levelFiles = append(test.levelFiles, f1)

	addBoundaryInputs(test.icmp, test.levelFiles, &test.compactionFiles)
	util.AssertTrue(len(test.compactionFiles) == 0, "length of compactionFiles", t)
	util.AssertEqual(1, len(test.levelFiles), "length of levelFiles", t)
	util.AssertEqual(f1, test.levelFiles[0], "f1", t)
}

func TestNoBoundaryFiles(t *testing.T) {
	test := newAddBoundaryInputsTest()
	f1 := test.createFileMetaData(1, newInternalKey([]byte("100"), 2, ssdb.TypeValue), newInternalKey([]byte("100"), 1, ssdb.TypeValue))
	f2 := test.createFileMetaData(1, newInternalKey([]byte("200"), 2, ssdb.TypeValue), newInternalKey([]byte("200"), 1, ssdb.TypeValue))
	f3 := test.createFileMetaData(1, newInternalKey([]byte("300"), 2, ssdb.TypeValue), newInternalKey([]byte("300"), 1, ssdb.TypeValue))
	test.levelFiles = append(test.levelFiles, f1, f2, f3)
	test.compactionFiles = append(test.compactionFiles, f2, f3)

	addBoundaryInputs(test.icmp, test.levelFiles, &test.compactionFiles)
	util.AssertEqual(2, len(test.compactionFiles), "length of compactionFiles", t)
}

func TestOneBoundaryFiles(t *testing.T) {
	test := newAddBoundaryInputsTest()
	f1 := test.createFileMetaData(1, newInternalKey([]byte("100"), 3, ssdb.TypeValue), newInternalKey([]byte("100"), 2, ssdb.TypeValue))
	f2 := test.createFileMetaData(1, newInternalKey([]byte("100"), 1, ssdb.TypeValue), newInternalKey([]byte("200"), 3, ssdb.TypeValue))
	f3 := test.createFileMetaData(1, newInternalKey([]byte("300"), 2, ssdb.TypeValue), newInternalKey([]byte("300"), 1, ssdb.TypeValue))
	test.levelFiles = append(test.levelFiles, f1, f2, f3)
	test.compactionFiles = append(test.compactionFiles, f1)

	addBoundaryInputs(test.icmp, test.levelFiles, &test.compactionFiles)
	util.AssertEqual(2, len(test.compactionFiles), "length of compactionFiles", t)
	util.AssertEqual(f1, test.compactionFiles[0], "f1", t)
	util.AssertEqual(f2, test.compactionFiles[1], "f2", t)
}

func TestTwoBoundaryFiles(t *testing.T) {
	test := newAddBoundaryInputsTest()
	f1 := test.createFileMetaData(1, newInternalKey([]byte("100"), 6, ssdb.TypeValue), newInternalKey([]byte("100"), 5, ssdb.TypeValue))
	f2 := test.createFileMetaData(1, newInternalKey([]byte("100"), 2, ssdb.TypeValue), newInternalKey([]byte("300"), 1, ssdb.TypeValue))
	f3 := test.createFileMetaData(1, newInternalKey([]byte("100"), 4, ssdb.TypeValue), newInternalKey([]byte("100"), 3, ssdb.TypeValue))
	test.levelFiles = append(test.levelFiles, f2, f3, f1)
	test.compactionFiles = append(test.compactionFiles, f1)

	addBoundaryInputs(test.icmp, test.levelFiles, &test.compactionFiles)
	util.AssertEqual(3, len(test.compactionFiles), "length of compactionFiles", t)
	util.AssertEqual(f1, test.compactionFiles[0], "f1", t)
	util.AssertEqual(f3, test.compactionFiles[1], "f3", t)
	util.AssertEqual(f2, test.compactionFiles[2], "f2", t)
}

func TestDisjointFilePointers(t *testing.T) {
	test := newAddBoundaryInputsTest()
	f1 := test.createFileMetaData(1, newInternalKey([]byte("100"), 6, ssdb.TypeValue), newInternalKey([]byte("100"), 5, ssdb.TypeValue))
	f2 := test.createFileMetaData(1, newInternalKey([]byte("100"), 6, ssdb.TypeValue), newInternalKey([]byte("100"), 5, ssdb.TypeValue))
	f3 := test.createFileMetaData(1, newInternalKey([]byte("100"), 2, ssdb.TypeValue), newInternalKey([]byte("300"), 1, ssdb.TypeValue))
	f4 := test.createFileMetaData(1, newInternalKey([]byte("100"), 4, ssdb.TypeValue), newInternalKey([]byte("100"), 3, ssdb.TypeValue))
	test.levelFiles = append(test.levelFiles, f2, f3, f4)
	test.compactionFiles = append(test.compactionFiles, f1)

	addBoundaryInputs(test.icmp, test.levelFiles, &test.compactionFiles)
	util.AssertEqual(3, len(test.compactionFiles), "length of compactionFiles", t)
	util.AssertEqual(f1, test.compactionFiles[0], "f1", t)
	util.AssertEqual(f4, test.compactionFiles[1], "f4", t)
	util.AssertEqual(f3, test.compactionFiles[2], "f3", t)
}
