package db

import (
	"ssdb"
	"ssdb/util"
	"testing"
)

func testEncodeDecode(edit *versionEdit, t *testing.T) {
	var encoded, encoded2 []byte
	edit.encodeTo(&encoded)
	var parsed versionEdit
	err := parsed.decodeFrom(encoded)
	util.TestNotError(err, "decodeFrom", t)
	parsed.encodeTo(&encoded2)
	util.TestEqual(encoded, encoded2, "encode decode", t)
}

func TestEncodeDecode(t *testing.T) {
	big := uint64(1 << 50)
	edit := newVersionEdit()
	for i := uint64(0); i < 4; i++ {
		testEncodeDecode(edit, t)
		edit.addFile(3, big+300+i, big+400+i,
			*newInternalKey([]byte("foo"), sequenceNumber(big+500+i), ssdb.TypeValue),
			*newInternalKey([]byte("zoo"), sequenceNumber(big+600+i), ssdb.TypeDeletion))
		edit.deletedFile(4, big+700+i)
		edit.setCompactPointer(int(i), *newInternalKey([]byte("x"), sequenceNumber(big+900+i), ssdb.TypeValue))
	}
	edit.setComparatorName("foo")
	edit.setLogNumber(big + 100)
	edit.setNextFile(big + 200)
	edit.setLastSequence(sequenceNumber(big + 1000))
	testEncodeDecode(edit, t)
}
