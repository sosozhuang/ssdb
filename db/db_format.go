package db

import (
	"bytes"
	"fmt"
	"ssdb"
	"ssdb/util"
)

const (
	numLevels               = 7
	l0CompactionTrigger     = 4
	l0SlowdownWritesTrigger = 8
	l0StopWritesTrigger     = 12
	maxMemCompactLevel      = 2
	readBytesPeriod         = 1048576
)

const valueTypeForSeek = ssdb.TypeValue

type sequenceNumber uint64

const maxSequenceNumber sequenceNumber = (1 << 56) - 1

type parsedInternalKey struct {
	userKey   []byte
	sequence  sequenceNumber
	valueType ssdb.ValueType
}

func (k *parsedInternalKey) debugString() string {
	buffer := bytes.NewBufferString("'")
	fmt.Fprintf(buffer, "' @ %d : %d", k.sequence, int(k.valueType))
	fmt.Fprintf(buffer, "%s", util.EscapeString(k.userKey))
	return buffer.String()
}

func internalKeyEncodingLength(key *parsedInternalKey) int {
	return len(key.userKey) + 8
}

func appendInternalKey(result *[]byte, key *parsedInternalKey) {
	*result = append(*result, key.userKey...)
	util.PutFixed64(result, packSequenceAndType(key.sequence, key.valueType))
}

func parseInternalKey(internalKey []byte, result *parsedInternalKey) bool {
	n := len(internalKey)
	if n < 8 {
		return false
	}
	num := util.DecodeFixed64(internalKey[n-8:])
	c := byte(num & 0xff)
	result.sequence = sequenceNumber(num >> 8)
	result.valueType = ssdb.ValueType(c)
	result.userKey = internalKey[:n-8]
	return c <= byte(ssdb.TypeValue)
}

func extractUserKey(internalKey []byte) []byte {
	if len(internalKey) < 8 {
		panic("len(internalKey) < 8")
	}
	return internalKey[:len(internalKey)-8]
}

type internalKeyComparator struct {
	userComparator ssdb.Comparator
}

func (c *internalKeyComparator) Compare(a, b []byte) int {
	r := c.userComparator.Compare(extractUserKey(a), extractUserKey(b))
	if r == 0 {
		anum := util.DecodeFixed64(a[len(a)-8:])
		bnum := util.DecodeFixed64(b[len(b)-8:])
		if anum > bnum {
			return -1
		} else if anum < bnum {
			return 1
		}
	}
	return r
}

func (c *internalKeyComparator) Name() string {
	return "ssdb.InternalKeyComparator"
}

func (c *internalKeyComparator) FindShortestSeparator(start *[]byte, limit []byte) {
	userStart := extractUserKey(*start)
	userLimit := extractUserKey(limit)
	tmp := make([]byte, len(*start))
	copy(tmp, *start)
	c.userComparator.FindShortestSeparator(&tmp, userLimit)
	if len(tmp) < len(userStart) && c.userComparator.Compare(userStart, tmp) < 0 {
		util.PutFixed64(&tmp, packSequenceAndType(maxSequenceNumber, valueTypeForSeek))
		if c.Compare(*start, tmp) >= 0 {
			panic("internalKeyComparator: Compare(*start, tmp) >= 0")
		}
		if c.Compare(tmp, limit) >= 0 {
			panic("internalKeyComparator: Compare(tmp, limit) >= 0")
		}
		*start = tmp
	}
}

func (c *internalKeyComparator) FindShortSuccessor(key *[]byte) {
	userKey := extractUserKey(*key)
	tmp := make([]byte, len(userKey))
	copy(tmp, userKey)
	c.userComparator.FindShortSuccessor(&tmp)
	if len(tmp) < len(userKey) && c.userComparator.Compare(userKey, tmp) < 0 {
		util.PutFixed64(&tmp, packSequenceAndType(maxSequenceNumber, valueTypeForSeek))
		if c.Compare(*key, tmp) >= 0 {
			panic("internalKeyComparator: Compare(*key, tmp) < 0")
		}
		*key = tmp
	}
}

func (c *internalKeyComparator) compare(a, b *internalKey) int {
	return c.Compare(a.encode(), b.encode())
}

func newInternalKeyComparator(c ssdb.Comparator) *internalKeyComparator {
	return &internalKeyComparator{userComparator: c}
}

type internalKey struct {
	rep []byte
}

func (k *internalKey) decodeFrom(b []byte) {
	k.rep = make([]byte, len(b), len(b))
	copy(k.rep, b)
}

func (k *internalKey) encode() []byte {
	return k.rep
}

func (k *internalKey) userKey() []byte {
	return extractUserKey(k.rep)
}

func (k *internalKey) setFrom(key *parsedInternalKey) {
	k.rep = make([]byte, 0)
	appendInternalKey(&k.rep, key)
}

func (k *internalKey) clear() {
	k.rep = make([]byte, 0)
}

func (k *internalKey) debugString() string {
	var result string
	parsed := new(parsedInternalKey)
	if parseInternalKey(k.rep, parsed) {
		result = parsed.debugString()
	} else {
		result = "(bad)" + util.EscapeString(k.rep)
	}
	return result
}

func newInternalKey(userKey []byte, seq sequenceNumber, t ssdb.ValueType) *internalKey {
	k := &internalKey{rep: make([]byte, 0)}
	appendInternalKey(&k.rep, &parsedInternalKey{
		userKey:   userKey,
		sequence:  seq,
		valueType: t,
	})
	return k
}

type internalFilterPolicy struct {
	userPolicy ssdb.FilterPolicy
}

func (p *internalFilterPolicy) Name() string {
	return p.userPolicy.Name()
}

func (p *internalFilterPolicy) CreateFilter(keys [][]byte, dst *[]byte) {
	for i := range keys {
		keys[i] = extractUserKey(keys[i])
	}
	p.userPolicy.CreateFilter(keys, dst)
}

func (p *internalFilterPolicy) KeyMayMatch(key []byte, filter []byte) bool {
	return p.userPolicy.KeyMayMatch(extractUserKey(key), filter)
}

func newInternalFilterPolicy(p ssdb.FilterPolicy) *internalFilterPolicy {
	return &internalFilterPolicy{userPolicy: p}
}

type lookupKey struct {
	kstart int
	end    int
	space  []byte
}

func (k *lookupKey) memtableKey() []byte {
	return k.space[:k.end]
}

func (k *lookupKey) internalKey() []byte {
	return k.space[k.kstart : k.end-k.kstart]
}

func (k *lookupKey) userKey() []byte {
	return k.space[k.kstart : k.end-k.kstart-8]
}

func newLookupKey(userKey []byte, seq sequenceNumber) *lookupKey {
	usize := len(userKey)
	needed := usize + 13
	if needed <= 200 {
		needed = 200
	}
	lk := &lookupKey{
		space: make([]byte, needed),
	}
	var buff [5]byte
	lk.kstart = util.EncodeVarInt32(&buff, uint32(usize+8))
	copy(lk.space[:lk.kstart], buff[:lk.kstart])
	copy(lk.space[lk.kstart:], userKey)
	util.EncodeFixed64(nil, packSequenceAndType(seq, valueTypeForSeek))
	lk.end = lk.kstart + usize + 8
	return lk
}

func packSequenceAndType(seq sequenceNumber, t ssdb.ValueType) uint64 {
	if seq > maxSequenceNumber {
		panic("seq > maxSequenceNumber")
	}
	if t > valueTypeForSeek {
		panic("t > valueTypeForSeek")
	}
	return (uint64(seq) << 8) | uint64(t)
}
