package db

import (
	"ssdb"
	"ssdb/util"
	"testing"
)

func ikey(userKey string, seq sequenceNumber, vt ssdb.ValueType) []byte {
	encoded := make([]byte, 0)
	appendInternalKey(&encoded, &parsedInternalKey{
		userKey:   []byte(userKey),
		sequence:  seq,
		valueType: vt,
	})
	return encoded
}

func shorten(s, l []byte) []byte {
	newInternalKeyComparator(ssdb.BytewiseComparator).FindShortestSeparator(&s, l)
	return s
}

func shortSuccessor(s []byte) []byte {
	newInternalKeyComparator(ssdb.BytewiseComparator).FindShortSuccessor(&s)
	return s
}

func testInternalKey(key string, seq sequenceNumber, vt ssdb.ValueType, t *testing.T) {
	encoded := ikey(key, seq, vt)
	decoded := parsedInternalKey{
		userKey:   []byte{},
		sequence:  0,
		valueType: ssdb.TypeValue,
	}
	util.AssertTrue(parseInternalKey(encoded, &decoded), "parseInternalKey(encoded, &decoded)", t)
	util.AssertEqual(key, string(decoded.userKey), "key", t)
	util.AssertEqual(seq, decoded.sequence, "sequence", t)
	util.AssertEqual(vt, decoded.valueType, "valueType", t)
	util.AssertTrue(!parseInternalKey([]byte("bar"), &decoded), "parseInternalKey", t)
}

func TestInternalKeyEncodeDecode(t *testing.T) {
	keys := []string{"", "k", "hello", "longggggggggggggggggggggg"}
	seq := []sequenceNumber{1,
		2,
		3,
		(1 << 8) - 1,
		1 << 8,
		(1 << 8) + 1,
		(1 << 16) - 1,
		1 << 16,
		(1 << 16) + 1,
		(1 << 32) - 1,
		1 << 32,
		(1 << 32) + 1}
	for k := 0; k < len(keys); k++ {
		for s := 0; s < len(seq); s++ {
			testInternalKey(keys[k], seq[s], ssdb.TypeValue, t)
			testInternalKey("hello", 1, ssdb.TypeDeletion, t)
		}
	}
}

func TestInternalKeyShortSeparator(t *testing.T) {
	util.AssertEqual(ikey("foo", 100, ssdb.TypeValue), shorten(ikey("foo", 100, ssdb.TypeValue), ikey("foo", 99, ssdb.TypeValue)), "shorten", t)
	util.AssertEqual(ikey("foo", 100, ssdb.TypeValue), shorten(ikey("foo", 100, ssdb.TypeValue), ikey("foo", 101, ssdb.TypeValue)), "shorten", t)
	util.AssertEqual(ikey("foo", 100, ssdb.TypeValue), shorten(ikey("foo", 100, ssdb.TypeValue), ikey("foo", 100, ssdb.TypeValue)), "shorten", t)
	util.AssertEqual(ikey("foo", 100, ssdb.TypeValue), shorten(ikey("foo", 100, ssdb.TypeValue), ikey("foo", 100, ssdb.TypeDeletion)), "shorten", t)
	util.AssertEqual(ikey("foo", 100, ssdb.TypeValue), shorten(ikey("foo", 100, ssdb.TypeValue), ikey("bar", 99, ssdb.TypeValue)), "shorten", t)
	util.AssertEqual(ikey("g", maxSequenceNumber, valueTypeForSeek), shorten(ikey("foo", 100, ssdb.TypeValue), ikey("hello", 200, ssdb.TypeValue)), "shorten", t)
	util.AssertEqual(ikey("foo", 100, ssdb.TypeValue), shorten(ikey("foo", 100, ssdb.TypeValue), ikey("foobar", 200, ssdb.TypeValue)), "shorten", t)
	util.AssertEqual(ikey("foobar", 100, ssdb.TypeValue), shorten(ikey("foobar", 100, ssdb.TypeValue), ikey("foo", 200, ssdb.TypeValue)), "shorten", t)

}

func TestInternalKeyShortestSuccessor(t *testing.T) {
	util.AssertEqual(ikey("g", maxSequenceNumber, valueTypeForSeek),
		shortSuccessor(ikey("foo", 100, ssdb.TypeValue)), "shortSuccessor", t)
	util.AssertEqual(ikey("\xff\xff", 100, ssdb.TypeValue),
		shortSuccessor(ikey("\xff\xff", 100, ssdb.TypeValue)), "shortSuccessor", t)
}
