package db

import (
	"bytes"
	"fmt"
	"ssdb/util"
	"strings"
	"testing"
	"unsafe"
)

func bigString(partialString string, n int) string {
	b := bytes.NewBuffer(make([]byte, 0))
	b.Grow(n)
	for b.Len() < n {
		b.WriteString(partialString)
	}
	b.Truncate(n)
	return b.String()
}

func numberString(n int) string {
	buf := bytes.NewBufferString("")
	buf.Grow(50)
	fmt.Fprintf(buf, "%d.", n)
	return buf.String()
}

func randomSkewedString(i int, rnd *util.Random) string {
	return bigString(numberString(i), int(rnd.Skewed(17)))
}

var initialOffsetRecordSizes = []int{
	10000,
	10000,
	2*blockSize - 1000,
	1,
	13716,
	blockSize - headerSize,
}
var initialOffsetLastRecordOffsets = []uint64{
	0,
	headerSize + 10000,
	2 * (headerSize + 10000),
	2*(headerSize+10000) + (2*blockSize - 1000) + 3*headerSize,
	2*(headerSize+10000) + (2*blockSize - 1000) + 3*headerSize + headerSize + 1,
	3 * blockSize,
}

var numInitialOffsetRecords = len(initialOffsetLastRecordOffsets)

type logTest struct {
	dest    *logStringDest
	source  *logStringSource
	report  *reportCollector
	reading bool
	writer  *logWriter
	reader  *logReader
	t       *testing.T
}

func newLogTest(t *testing.T) *logTest {
	test := &logTest{
		dest:    newLogStringDest(),
		source:  newLogStringSource(),
		report:  newReportCollector(),
		reading: false,
		t:       t,
	}
	test.writer = newLogWriter(test.dest)
	test.reader = newLogReader(test.source, test.report, true, 0)
	return test
}

func (t *logTest) reopenForAppend() {
	t.writer = newLogWriter(t.dest)
}

func (t *logTest) write(msg string) {
	if t.reading {
		panic("reading")
	}
	_ = t.writer.addRecord([]byte(msg))
}

func (t *logTest) writtenBytes() int {
	return len(t.dest.contents)
}

func (t *logTest) read() string {
	if !t.reading {
		t.reading = true
		t.source.contents = t.dest.contents
	}
	if record, ok := t.reader.readRecord(); ok {
		return string(record)
	} else {
		return "EOF"
	}
}

func (t *logTest) incrementByte(offset, delta int) {
	t.dest.contents[offset] += byte(delta)
}

func (t *logTest) setByte(offset int, newByte byte) {
	t.dest.contents[offset] = newByte
}

func (t *logTest) shrinkSize(bytes int) {
	l := len(t.dest.contents)
	t.dest.contents = t.dest.contents[:l-bytes]
}

func (t *logTest) fixChecksum(headerOffset, l int) {
	start := headerOffset + 6
	stop := start + 1 + l
	crc := util.ChecksumValue(t.dest.contents[start:stop])
	crc = util.MaskChecksum(crc)
	p := unsafe.Pointer(&t.dest.contents[headerOffset])
	util.EncodeFixed32((*[4]byte)(p), crc)
}

func (t *logTest) forceError() {
	t.source.forceError = true
}

func (t *logTest) droppedBytes() int {
	return t.report.droppedBytes
}

func (t *logTest) reportMessage() string {
	return string(t.report.message)
}

func (t *logTest) matchError(msg string) string {
	if !bytes.Contains(t.report.message, []byte(msg)) {
		return string(t.report.message)
	} else {
		return "OK"
	}
}

func (t *logTest) writeInitialOffsetLog() {
	for i := 0; i < numInitialOffsetRecords; i++ {
		record := strings.Repeat(string('a'+byte(i)), initialOffsetRecordSizes[i])
		t.write(record)
	}
}

func (t *logTest) startReadingAt(initialOffset uint64) {
	t.reader = newLogReader(t.source, t.report, true, initialOffset)
}

func (t *logTest) checkOffsetPastEndReturnsNoRecords(offsetPastEnd uint64) {
	t.writeInitialOffsetLog()
	t.reading = true
	t.source.contents = t.dest.contents
	offsetReader := newLogReader(t.source, t.report, true, uint64(t.writtenBytes())+offsetPastEnd)
	_, ok := offsetReader.readRecord()
	util.AssertFalse(ok, "readRecord", t.t)
}

func (t *logTest) checkInitialOffsetRecord(initialOffset uint64, expectedRecordOffset int) {
	t.writeInitialOffsetLog()
	t.reading = true
	t.source.contents = t.dest.contents
	offsetReader := newLogReader(t.source, t.report, true, initialOffset)

	util.AssertLessThan(expectedRecordOffset, numInitialOffsetRecords, "numInitialOffsetRecords", t.t)
	for ; expectedRecordOffset < numInitialOffsetRecords; expectedRecordOffset++ {
		record, ok := offsetReader.readRecord()
		util.AssertTrue(ok, "readRecord", t.t)
		util.AssertEqual(initialOffsetRecordSizes[expectedRecordOffset], len(record), "length of record", t.t)
		util.AssertEqual(initialOffsetLastRecordOffsets[expectedRecordOffset], offsetReader.lastRecordOffset, "lastRecordOffset", t.t)
		util.AssertEqual(byte('a'+expectedRecordOffset), record[0], "record[0]", t.t)
	}
}

type logStringDest struct {
	contents []byte
}

func newLogStringDest() *logStringDest {
	return &logStringDest{contents: make([]byte, 0)}
}

func (d *logStringDest) Append(data []byte) error {
	d.contents = append(d.contents, data...)
	return nil
}

func (d *logStringDest) Close() error {
	return nil
}

func (d *logStringDest) Flush() error {
	return nil
}

func (d *logStringDest) Sync() error {
	return nil
}

func (d *logStringDest) Finalize() {
}

type logStringSource struct {
	contents        []byte
	forceError      bool
	returnedPartial bool
}

func newLogStringSource() *logStringSource {
	return &logStringSource{
		contents:        make([]byte, 0),
		forceError:      false,
		returnedPartial: false,
	}
}

func (s *logStringSource) Read(b []byte) (result []byte, n int, err error) {
	if s.returnedPartial {
		panic("returnedPartial")
	}
	if s.forceError {
		s.forceError = false
		s.returnedPartial = true
		err = util.CorruptionError1("read error")
		return
	}
	n = len(b)
	if len(s.contents) < n {
		n = len(s.contents)
		s.returnedPartial = true
	}
	result = s.contents[:n]
	s.contents = s.contents[n:]
	return
}

func (s *logStringSource) Skip(n uint64) error {
	if n > uint64(len(s.contents)) {
		s.contents = make([]byte, 0)
		return util.NotFoundError1("in-memory file skipped past end")
	}
	s.contents = s.contents[n:]
	return nil
}

func (s *logStringSource) Finalize() {
}

type reportCollector struct {
	droppedBytes int
	message      []byte
}

func newReportCollector() *reportCollector {
	return &reportCollector{
		droppedBytes: 0,
		message:      make([]byte, 0),
	}
}

func (c *reportCollector) corruption(bytes int, err error) {
	c.droppedBytes += bytes
	c.message = append(c.message, err.Error()...)
}

func TestEmptyLog(t *testing.T) {
	test := newLogTest(t)
	util.AssertEqual("EOF", test.read(), "read", t)
}

func TestReadWrite(t *testing.T) {
	test := newLogTest(t)
	test.write("foo")
	test.write("bar")
	test.write("")
	test.write("xxxx")
	util.AssertEqual("foo", test.read(), "read", t)
	util.AssertEqual("bar", test.read(), "read", t)
	util.AssertEqual("", test.read(), "read", t)
	util.AssertEqual("xxxx", test.read(), "read", t)
	util.AssertEqual("EOF", test.read(), "read", t)
	util.AssertEqual("EOF", test.read(), "read", t)
}

func TestManyBlocks(t *testing.T) {
	test := newLogTest(t)
	for i := 0; i < 100000; i++ {
		test.write(numberString(i))
	}
	for i := 0; i < 100000; i++ {
		util.AssertEqual(numberString(i), test.read(), "read", t)
	}
	util.AssertEqual("EOF", test.read(), "read", t)
}

func TestFragmentation(t *testing.T) {
	test := newLogTest(t)
	test.write("small")
	test.write(bigString("medium", 5000))
	test.write(bigString("large", 100000))
	util.AssertEqual("small", test.read(), "read small", t)
	util.AssertEqual(bigString("medium", 5000), test.read(), "read small", t)
	util.AssertEqual(bigString("large", 100000), test.read(), "read small", t)
	util.AssertEqual("EOF", test.read(), "read", t)
}

func TestMarginalTrailer(t *testing.T) {
	const n = blockSize - 2*headerSize
	test := newLogTest(t)
	test.write(bigString("foo", n))
	util.AssertEqual(blockSize-headerSize, test.writtenBytes(), "writtenBytes", t)
	test.write("")
	test.write("bar")
	util.AssertEqual(bigString("foo", n), test.read(), "read", t)
	util.AssertEqual("", test.read(), "read", t)
	util.AssertEqual("bar", test.read(), "read", t)
	util.AssertEqual("EOF", test.read(), "read", t)
}

func TestMarginalTrailer2(t *testing.T) {
	const n = blockSize - 2*headerSize
	test := newLogTest(t)
	test.write(bigString("foo", n))
	util.AssertEqual(blockSize-headerSize, test.writtenBytes(), "writtenBytes", t)
	test.write("bar")
	util.AssertEqual(bigString("foo", n), test.read(), "read", t)
	util.AssertEqual("bar", test.read(), "read", t)
	util.AssertEqual("EOF", test.read(), "read", t)
	util.AssertEqual(0, test.droppedBytes(), "droppedBytes", t)
	util.AssertEqual("", test.reportMessage(), "reportMessage", t)
}

func TestShortTrailer(t *testing.T) {
	const n = blockSize - 2*headerSize + 4
	test := newLogTest(t)
	test.write(bigString("foo", n))
	util.AssertEqual(blockSize-headerSize+4, test.writtenBytes(), "writtenBytes", t)
	test.write("")
	test.write("bar")
	util.AssertEqual(bigString("foo", n), test.read(), "read", t)
	util.AssertEqual("", test.read(), "read", t)
	util.AssertEqual("bar", test.read(), "read", t)
	util.AssertEqual("EOF", test.read(), "read", t)
}

func TestAlignedEof(t *testing.T) {
	const n = blockSize - 2*headerSize + 4
	test := newLogTest(t)
	test.write(bigString("foo", n))
	util.AssertEqual(blockSize-headerSize+4, test.writtenBytes(), "writtenBytes", t)
	util.AssertEqual(bigString("foo", n), test.read(), "read", t)
	util.AssertEqual("EOF", test.read(), "read", t)
}

func TestOpenForAppend(t *testing.T) {
	test := newLogTest(t)
	test.write("hello")
	test.reopenForAppend()
	test.write("world")
	util.AssertEqual("hello", test.read(), "read", t)
	util.AssertEqual("world", test.read(), "read", t)
	util.AssertEqual("EOF", test.read(), "read", t)
}

func TestRandomRead(t *testing.T) {
	const n = 500
	test := newLogTest(t)
	writeRnd := util.NewRandom(301)
	for i := 0; i < n; i++ {
		test.write(randomSkewedString(i, writeRnd))
	}
	readRnd := util.NewRandom(301)
	for i := 0; i < n; i++ {
		util.AssertEqual(randomSkewedString(i, readRnd), test.read(), "read", t)
	}
	util.AssertEqual("EOF", test.read(), "read", t)
}

func TestReadError(t *testing.T) {
	test := newLogTest(t)
	test.write("foo")
	test.forceError()
	util.AssertEqual("EOF", test.read(), "read", t)
	util.AssertEqual(blockSize, test.droppedBytes(), "droppedBytes", t)
	util.AssertEqual("OK", test.matchError("read error"), "matchError", t)
}

func TestBadRecordType(t *testing.T) {
	test := newLogTest(t)
	test.write("foo")
	test.incrementByte(6, 100)
	test.fixChecksum(0, 3)
	util.AssertEqual("EOF", test.read(), "read", t)
	util.AssertEqual(3, test.droppedBytes(), "droppedBytes", t)
	util.AssertEqual("OK", test.matchError("unknown record type"), "matchError", t)
}

func TestTruncatedTrailingRecordIsIgnored(t *testing.T) {
	test := newLogTest(t)
	test.write("foo")
	test.shrinkSize(4)
	util.AssertEqual("EOF", test.read(), "read", t)
	util.AssertEqual(0, test.droppedBytes(), "droppedBytes", t)
	util.AssertEqual("", test.reportMessage(), "reportMessage", t)
}

func TestBadLength(t *testing.T) {
	const payloadSize = blockSize - headerSize
	test := newLogTest(t)
	test.write(bigString("bar", payloadSize))
	test.write("foo")
	test.incrementByte(4, 1)
	util.AssertEqual("foo", test.read(), "read", t)
	util.AssertEqual(blockSize, test.droppedBytes(), "droppedBytes", t)
	util.AssertEqual("OK", test.matchError("bad record length"), "matchError", t)
}

func TestBadLengthAtEndIsIgnored(t *testing.T) {
	test := newLogTest(t)
	test.write("foo")
	test.shrinkSize(1)
	util.AssertEqual("EOF", test.read(), "read", t)
	util.AssertEqual(0, test.droppedBytes(), "droppedBytes", t)
	util.AssertEqual("", test.reportMessage(), "reportMessage", t)
}

func TestChecksumMismatch(t *testing.T) {
	test := newLogTest(t)
	test.write("foo")
	test.incrementByte(0, 10)
	util.AssertEqual("EOF", test.read(), "read", t)
	util.AssertEqual(10, test.droppedBytes(), "droppedBytes", t)
	util.AssertEqual("OK", test.matchError("checksum mismatch"), "matchError", t)
}

func TestUnexpectedMiddleType(t *testing.T) {
	test := newLogTest(t)
	test.write("foo")
	test.setByte(6, byte(middleType))
	test.fixChecksum(0, 3)
	util.AssertEqual("EOF", test.read(), "read", t)
	util.AssertEqual(3, test.droppedBytes(), "droppedBytes", t)
	util.AssertEqual("OK", test.matchError("missing start"), "matchError", t)
}

func TestUnexpectedLastType(t *testing.T) {
	test := newLogTest(t)
	test.write("foo")
	test.setByte(6, byte(lastType))
	test.fixChecksum(0, 3)
	util.AssertEqual("EOF", test.read(), "read", t)
	util.AssertEqual(3, test.droppedBytes(), "droppedBytes", t)
	util.AssertEqual("OK", test.matchError("missing start"), "matchError", t)
}

func TestUnexpectedFullType(t *testing.T) {
	test := newLogTest(t)
	test.write("foo")
	test.write("bar")
	test.setByte(6, byte(firstType))
	test.fixChecksum(0, 3)
	util.AssertEqual("bar", test.read(), "read", t)
	util.AssertEqual("EOF", test.read(), "read", t)
	util.AssertEqual(3, test.droppedBytes(), "droppedBytes", t)
	util.AssertEqual("OK", test.matchError("partial record without end"), "matchError", t)
}

func TestUnexpectedFirstType(t *testing.T) {
	test := newLogTest(t)
	test.write("foo")
	test.write(bigString("bar", 100000))
	test.setByte(6, byte(firstType))
	test.fixChecksum(0, 3)
	util.AssertEqual(bigString("bar", 100000), test.read(), "read", t)
	util.AssertEqual("EOF", test.read(), "read", t)
	util.AssertEqual(3, test.droppedBytes(), "droppedBytes", t)
	util.AssertEqual("OK", test.matchError("partial record without end"), "matchError", t)
}

func TestMissingLastIsIgnored(t *testing.T) {
	test := newLogTest(t)
	test.write(bigString("bar", blockSize))
	test.shrinkSize(14)
	util.AssertEqual("EOF", test.read(), "read", t)
	util.AssertEqual("", test.reportMessage(), "reportMessage", t)
	util.AssertEqual(0, test.droppedBytes(), "droppedBytes", t)
}

func TestPartialLastIsIgnored(t *testing.T) {
	test := newLogTest(t)
	test.write(bigString("bar", blockSize))
	test.shrinkSize(1)
	util.AssertEqual("EOF", test.read(), "read", t)
	util.AssertEqual("", test.reportMessage(), "reportMessage", t)
	util.AssertEqual(0, test.droppedBytes(), "droppedBytes", t)
}

func TestSkipIntoMultiRecord(t *testing.T) {
	test := newLogTest(t)
	test.write(bigString("foo", 3*blockSize))
	test.write("correct")
	test.startReadingAt(blockSize)

	util.AssertEqual("correct", test.read(), "read", t)
	util.AssertEqual("", test.reportMessage(), "reportMessage", t)
	util.AssertEqual(0, test.droppedBytes(), "droppedBytes", t)
	util.AssertEqual("EOF", test.read(), "read", t)
}

func TestErrorJoinsRecords(t *testing.T) {
	test := newLogTest(t)
	test.write(bigString("foo", blockSize))
	test.write(bigString("bar", blockSize))
	test.write("correct")

	for offset := blockSize; offset < 2*blockSize; offset++ {
		test.setByte(offset, 'x')
	}
	util.AssertEqual("correct", test.read(), "read", t)
	util.AssertEqual("EOF", test.read(), "read", t)
	dropped := test.droppedBytes()
	util.AssertLessThanOrEqual(dropped, 2*blockSize+100, "droppedBytes", t)
	util.AssertGreaterThanOrEqual(dropped, 2*blockSize, "droppedBytes", t)
}

func TestReadStart(t *testing.T) {
	test := newLogTest(t)
	test.checkInitialOffsetRecord(0, 0)
}

func TestReadSecondOneOff(t *testing.T) {
	test := newLogTest(t)
	test.checkInitialOffsetRecord(1, 1)
}

func TestReadSecondTenThousand(t *testing.T) {
	test := newLogTest(t)
	test.checkInitialOffsetRecord(10000, 1)
}

func TestReadSecondStart(t *testing.T) {
	test := newLogTest(t)
	test.checkInitialOffsetRecord(10007, 1)
}

func TestReadThirdOneOff(t *testing.T) {
	test := newLogTest(t)
	test.checkInitialOffsetRecord(10008, 2)
}

func TestReadThirdStart(t *testing.T) {
	test := newLogTest(t)
	test.checkInitialOffsetRecord(20014, 2)
}

func TestReadFourthOneOff(t *testing.T) {
	test := newLogTest(t)
	test.checkInitialOffsetRecord(20015, 3)
}

func TestReadFourthFirstBlockTrailer(t *testing.T) {
	test := newLogTest(t)
	test.checkInitialOffsetRecord(blockSize-4, 3)
}

func TestReadFourthMiddleBlock(t *testing.T) {
	test := newLogTest(t)
	test.checkInitialOffsetRecord(blockSize+1, 3)
}

func TestReadFourthLastBlock(t *testing.T) {
	test := newLogTest(t)
	test.checkInitialOffsetRecord(2*blockSize+1, 3)
}

func TestReadFourthStart(t *testing.T) {
	test := newLogTest(t)
	test.checkInitialOffsetRecord(2*(blockSize+10000)+(2*headerSize-1000)+3*headerSize, 3)
}

func TestReadInitialOffsetIntoBlockPadding(t *testing.T) {
	test := newLogTest(t)
	test.checkInitialOffsetRecord(3*blockSize-3, 5)
}

func TestReadEnd(t *testing.T) {
	test := newLogTest(t)
	test.checkOffsetPastEndReturnsNoRecords(0)
}

func TestReadPastEnd(t *testing.T) {
	test := newLogTest(t)
	test.checkOffsetPastEndReturnsNoRecords(5)
}
