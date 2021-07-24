package db

import (
	"ssdb"
	"ssdb/table"
	"ssdb/util"
	"unsafe"
)

type MemTable struct {
	comparator  *keyComparator
	refs        int
	table       *skipList
	memoryUsage uint64
}

func (t *MemTable) ref() {
	t.refs++
}

func (t *MemTable) unref() {
	t.refs--
	//if t.refs < 0 {
	//	panic("refs < 0")
	//}
	if t.refs <= 0 {
		t.release()
	}
}

func (t *MemTable) release() {
	//if t.refs != 0 {
	//	panic("memTable: refs != 0")
	//}
}

func (t *MemTable) approximateMemoryUsage() uint64 {
	return t.memoryUsage
}

func (t *MemTable) newIterator() ssdb.Iterator {
	return newMemTableIterator(t.table)
}

func (t *MemTable) add(seq sequenceNumber, vt ssdb.ValueType, key, value []byte) {
	// Format of an entry is concatenation of:
	//  key_size     : varint32 of internal_key.size()
	//  key bytes    : char[internal_key.size()]
	//  value_size   : varint32 of value.size()
	//  value bytes  : char[value.size()]
	keySize := len(key)
	valSize := len(value)
	internalKeySize := uint32(keySize + 8)
	encodedLen := util.VarIntLength(uint64(internalKeySize)) + int(internalKeySize) + util.VarIntLength(uint64(valSize)) + valSize
	t.memoryUsage += uint64(encodedLen)
	buf := make([]byte, encodedLen)
	i := util.EncodeVarInt32((*[5]byte)(unsafe.Pointer(&buf[0])), internalKeySize)
	copy(buf[i:], key)
	i += keySize
	util.EncodeFixed64((*[8]byte)(unsafe.Pointer(&buf[i])), uint64(seq<<8)|uint64(vt))
	i += 8
	i += util.EncodeVarInt32((*[5]byte)(unsafe.Pointer(&buf[i])), uint32(valSize))
	copy(buf[i:], value)
	//if i+valSize != encodedLen {
	//	panic("memtable: i + valSize != encodedLen")
	//}
	t.table.insert(buf)
}

func copyMemoryToPointer(pointer unsafe.Pointer, src []byte) {
	const n = 64
	size := len(src)
	var dst *[n]byte
	for start, limit := 0, n; size > 0; start += n {
		if size < n {
			limit = size
		}
		dst = (*[n]byte)(pointer)
		copy((*dst)[:], src[start:start+limit])
		pointer = unsafe.Pointer(uintptr(pointer) + n)
		size -= n
	}
}

func (t *MemTable) get(key *lookupKey, value *[]byte) (error, bool) {
	memKey := key.memtableKey()
	iter := newSkipListIterator(t.table)
	iter.seek(memKey)
	if iter.valid() {
		// entry format is:
		//    klength  varint32
		//    userkey  char[klength]
		//    tag      uint64
		//    vlength  varint32
		//    value    char[vlength]
		// Check that it belongs to same user key.  We do not check the
		// sequence number since the Seek() call above should have skipped
		// all entries with overly large sequence numbers.
		entry := iter.key().([]byte)
		//keyLenPtr := *(*[5]byte)(entry)
		var keyLen uint32
		i := util.GetVarInt32Ptr(entry, &keyLen)
		userKey := make([]byte, int(keyLen)-8)
		//copyMemoryToSlice(&userKey, unsafe.Pointer(uintptr(entry)+uintptr(i)), len(userKey))
		copy(userKey, entry[i:])
		if t.comparator.comparator.userComparator.Compare(userKey, key.userKey()) == 0 {
			//tagPtr := *(*[8]byte)(unsafe.Pointer(uintptr(entry) + uintptr(i) + uintptr(keyLen-8)))
			tag := util.DecodeFixed64(entry[i+int(keyLen)-8:])
			switch ssdb.ValueType(tag & 0xff) {
			case ssdb.TypeValue:
				v := getLengthPrefixedSlice(entry[i+int(keyLen):])
				*value = make([]byte, len(v))
				copy(*value, v)
				return nil, true
			case ssdb.TypeDeletion:
				return util.NotFoundError1(""), true
			}
		}
	}
	return nil, false
}

func NewMemTable(comparator *internalKeyComparator) *MemTable {
	t := &MemTable{
		comparator: &keyComparator{comparator},
		refs:       0,
	}
	t.table = newSkipList(t.comparator.compare)
	return t
}

type keyComparator struct {
	comparator *internalKeyComparator
}

func copyMemoryToSlice(dst *[]byte, pointer unsafe.Pointer, l int) {
	const n = 64
	var src *[n]byte
	for start, limit := 0, n; l > 0; start += n {
		if limit > l {
			limit = l
		}
		src = (*[n]byte)(pointer)
		copy((*dst)[start:start+limit], src[:limit])
		pointer = unsafe.Pointer(uintptr(pointer) + n)
		l -= n
	}
}

func getLengthPrefixedSlice(data []byte) []byte {
	var l uint32
	//data := *(*[5]byte)(pointer)
	p := util.GetVarInt32Ptr(data, &l)
	return data[p:]
	//dst := make([]byte, l)
	//copy(dst, data[p:])
	//pointer = unsafe.Pointer(uintptr(pointer) + uintptr(p))
	//copyMemoryToSlice(&dst, pointer, int(l))
	//return dst
}

func (c *keyComparator) compare(a, b skipListKey) int {
	s1 := getLengthPrefixedSlice(a.([]byte))
	s2 := getLengthPrefixedSlice(b.([]byte))
	return c.comparator.Compare(s1, s2)
}

func encodeKey(scratch *[]byte, target []byte) []byte {
	*scratch = make([]byte, 0)
	util.PutVarInt32(scratch, uint32(len(target)))
	*scratch = append(*scratch, target...)
	return *scratch
}

type memTableIterator struct {
	table.CleanUpIterator
	iter *skipListIterator
	tmp  []byte
}

func (i *memTableIterator) Valid() bool {
	return i.iter.valid()
}

func (i *memTableIterator) SeekToFirst() {
	i.iter.seekToFirst()
}

func (i *memTableIterator) SeekToLast() {
	i.iter.seekToLast()
}

func (i *memTableIterator) Seek(target []byte) {
	x := encodeKey(&i.tmp, target)
	i.iter.seek(x)
}

func (i *memTableIterator) Next() {
	i.iter.next()
}

func (i *memTableIterator) Prev() {
	i.iter.prev()
}

func (i *memTableIterator) Key() []byte {
	return getLengthPrefixedSlice(i.iter.key().([]byte))
}

func (i *memTableIterator) Value() []byte {
	key := i.iter.key().([]byte)
	keySlice := getLengthPrefixedSlice(key)
	return getLengthPrefixedSlice(keySlice)
}

func (i *memTableIterator) Status() error {
	return nil
}

func newMemTableIterator(list *skipList) *memTableIterator {
	return &memTableIterator{
		iter: newSkipListIterator(list),
		tmp:  make([]byte, 0),
	}
}

type memTableInserter struct {
	seq sequenceNumber
	mem *MemTable
}

func (i *memTableInserter) Put(key, value []byte) {
	i.mem.add(i.seq, ssdb.TypeValue, key, value)
	i.seq++
}

func (i *memTableInserter) Delete(key []byte) {
	i.mem.add(i.seq, ssdb.TypeDeletion, key, nil)
	i.seq++
}
