package db

import (
	"ssdb"
	"ssdb/table"
	"ssdb/util"
)

type direction int8

const (
	forward = direction(iota)
	reverse
)

type dbIter struct {
	table.CleanUpIterator
	db                     *db
	userComparator         ssdb.Comparator
	iter                   ssdb.Iterator
	sequence               sequenceNumber
	err                    error
	savedKey               []byte
	savedValue             []byte
	direction              direction
	valid                  bool
	rnd                    util.Random
	bytesUntilReadSampling uint
}

func (i *dbIter) Valid() bool {
	return i.valid
}

func (i *dbIter) SeekToFirst() {
	i.direction = forward
	i.clearSavedValue()
	i.iter.SeekToFirst()
	if i.iter.Valid() {
		i.findNextUserEntry(false, &i.savedKey)
	} else {
		i.valid = false
	}
}

func (i *dbIter) SeekToLast() {
	i.direction = reverse
	i.clearSavedValue()
	i.iter.SeekToLast()
	i.findPrevUserEntry()
}

func (i *dbIter) Seek(target []byte) {
	i.direction = forward
	i.clearSavedValue()
	i.savedKey = nil
	appendInternalKey(&i.savedKey, &parsedInternalKey{userKey: target, sequence: i.sequence, valueType: valueTypeForSeek})
	i.iter.Seek(i.savedKey)
	if i.iter.Valid() {
		i.findNextUserEntry(false, &i.savedKey)
	} else {
		i.valid = false
	}
}

func (i *dbIter) Next() {
	if !i.valid {
		panic("dbIter: not valid")
	}
	if i.direction == reverse {
		i.direction = forward
		if !i.iter.Valid() {
			i.iter.SeekToFirst()
		} else {
			i.iter.Next()
		}
		if !i.iter.Valid() {
			i.valid = false
			i.savedKey = nil
			return
		}
	} else {
		saveKey(extractUserKey(i.iter.Key()), &i.savedKey)
	}
	i.findNextUserEntry(true, &i.savedKey)
}

func (i *dbIter) Prev() {
	if !i.valid {
		panic("dbIter: not valid")
	}
	if i.direction == forward {
		if !i.iter.Valid() {
			panic("dbIter: iter not valid")
		}
		saveKey(extractUserKey(i.iter.Key()), &i.savedKey)
		for {
			i.iter.Prev()
			if !i.iter.Valid() {
				i.valid = false
				i.savedKey = nil
				i.clearSavedValue()
				return
			}
			if i.userComparator.Compare(extractUserKey(i.iter.Key()), i.savedKey) < 0 {
				break
			}
		}
		i.direction = reverse
	}
	i.findPrevUserEntry()
}

func (i *dbIter) Key() []byte {
	if !i.valid {
		panic("dbIter: not valid")
	}
	if i.direction == forward {
		return extractUserKey(i.iter.Key())
	}
	return i.savedKey
}

func (i *dbIter) Value() []byte {
	if !i.valid {
		panic("dbIter: not valid")
	}
	if i.direction == forward {
		return i.iter.Value()
	}
	return i.savedValue
}

func (i *dbIter) Status() error {
	if i.err == nil {
		return i.iter.Status()
	}
	return i.err
}

func (i *dbIter) Finalize() {
	i.iter.Finalize()
	i.CleanUpIterator.Finalize()
}

func (i *dbIter) findNextUserEntry(skipping bool, skip *[]byte) {
	if !i.valid {
		panic("dbIter: not valid")
	}
	if i.direction != forward {
		panic("dbIter: direction != forward")
	}
	var ikey parsedInternalKey
	for {
		if i.parseKey(&ikey) && ikey.sequence <= i.sequence {
			switch ikey.valueType {
			case ssdb.TypeDeletion:
				saveKey(ikey.userKey, skip)
				skipping = true
			case ssdb.TypeValue:
				if skipping && i.userComparator.Compare(ikey.userKey, *skip) <= 0 {
				} else {
					i.valid = true
					i.savedKey = nil
					return
				}
			}
		}
		i.iter.Next()
		if !i.iter.Valid() {
			break
		}
	}
	i.savedKey = nil
	i.valid = false
}

func (i *dbIter) findPrevUserEntry() {
	if i.direction != reverse {
		panic("dbIter: direction != reverse")
	}
	valueType := ssdb.TypeDeletion
	if i.iter.Valid() {
		var ikey parsedInternalKey
		for {
			if i.parseKey(&ikey) && ikey.sequence <= i.sequence {
				if valueType != ssdb.TypeDeletion && i.userComparator.Compare(ikey.userKey, i.savedKey) < 0 {
					break
				}
				valueType = ikey.valueType
				if valueType == ssdb.TypeDeletion {
					i.savedKey = nil
					i.clearSavedValue()
				} else {
					rawValue := i.iter.Value()
					if cap(i.savedValue) > len(rawValue)+1048576 || len(i.savedValue) < len(rawValue) {
						i.savedValue = make([]byte, len(rawValue))
					}
					saveKey(extractUserKey(i.iter.Key()), &i.savedKey)
					copy(i.savedValue, rawValue)
				}
			}
			i.iter.Prev()
			if !i.iter.Valid() {
				break
			}
		}
	}
	if valueType == ssdb.TypeDeletion {
		i.valid = false
		i.savedKey = nil
		i.clearSavedValue()
		i.direction = forward
	} else {
		i.valid = true
	}
}

func (i *dbIter) parseKey(key *parsedInternalKey) bool {
	k := i.iter.Key()
	bytesRead := len(k) + len(i.iter.Value())
	for i.bytesUntilReadSampling < uint(bytesRead) {
		i.bytesUntilReadSampling += i.randomCompactionPeriod()
		i.db.recordReadSample(k)
	}
	i.bytesUntilReadSampling -= uint(bytesRead)
	if !parseInternalKey(k, key) {
		i.err = util.CorruptionError1("corrupted internal key in DBIter")
		return false
	}
	return true
}

func saveKey(k []byte, dst *[]byte) {
	if *dst == nil {
		*dst = make([]byte, len(k))
	}
	copy(*dst, k)
}

func (i *dbIter) clearSavedValue() {
	i.savedValue = nil
}

func newDBIterator(db *db, cmp ssdb.Comparator, iter ssdb.Iterator, sequence sequenceNumber, seed uint32) ssdb.Iterator {
	i := &dbIter{
		db:             db,
		userComparator: cmp,
		iter:           iter,
		sequence:       sequence,
		direction:      forward,
		valid:          false,
		rnd:            *util.NewRandom(seed),
	}
	i.bytesUntilReadSampling = i.randomCompactionPeriod()
	return i
}

func (i *dbIter) randomCompactionPeriod() uint {
	return uint(i.rnd.Uniform(2 * readBytesPeriod))
}
