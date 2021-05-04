package table

import (
	"bytes"
	"ssdb"
)

type BlockFunction func(interface{}, *ssdb.ReadOptions, []byte) ssdb.Iterator

type twoLevelIterator struct {
	CleanUpIterator
	blockFunction   BlockFunction
	arg             interface{}
	options         *ssdb.ReadOptions
	err             error
	indexIter       *iteratorWrapper
	dataIter        *iteratorWrapper
	dataBlockHandle []byte
}

func NewTwoLevelIterator(indexIter ssdb.Iterator, blockFunction BlockFunction, arg interface{}, options *ssdb.ReadOptions) *twoLevelIterator {
	return &twoLevelIterator{
		blockFunction: blockFunction,
		arg:           arg,
		options:       options,
		indexIter:     newIteratorWrapper(indexIter),
		dataIter:      newIteratorWrapper(nil),
	}
}

func (i *twoLevelIterator) Valid() bool {
	return i.dataIter.valid
}

func (i *twoLevelIterator) SeekToFirst() {
	i.indexIter.seekToFirst()
	i.initDataBlock()
	if i.dataIter.iterator() != nil {
		i.dataIter.seekToFirst()
	}
	i.skipEmptyDataBlocksForward()
}

func (i *twoLevelIterator) SeekToLast() {
	i.indexIter.seekToLast()
	i.initDataBlock()
	if i.dataIter.iterator() != nil {
		i.dataIter.seekToLast()
	}
	i.skipEmptyDataBlocksBackward()
}

func (i *twoLevelIterator) Seek(target []byte) {
	i.indexIter.seek(target)
	i.initDataBlock()
	if i.dataIter.iterator() != nil {
		i.dataIter.seek(target)
	}
	i.skipEmptyDataBlocksForward()
}

func (i *twoLevelIterator) Next() {
	if !i.Valid() {
		panic("twoLevelIterator: not valid")
	}
	i.dataIter.next()
	i.skipEmptyDataBlocksForward()
}

func (i *twoLevelIterator) Prev() {
	if !i.Valid() {
		panic("twoLevelIterator: not valid")
	}
	i.dataIter.prev()
	i.skipEmptyDataBlocksBackward()
}

func (i *twoLevelIterator) Key() []byte {
	if !i.Valid() {
		panic("twoLevelIterator: not valid")
	}
	return i.dataIter.getKey()
}

func (i *twoLevelIterator) Value() []byte {
	if !i.Valid() {
		panic("twoLevelIterator: not valid")
	}
	return i.dataIter.getValue()
}

func (i *twoLevelIterator) Status() error {
	if i.indexIter.status() != nil {
		return i.indexIter.status()
	} else if i.dataIter.iterator() != nil && i.dataIter.iterator().Status() != nil {
		return i.dataIter.status()
	}
	return i.err
}

func (i *twoLevelIterator) saveError(err error) {
	if i.err == nil && err != nil {
		i.err = err
	}
}

func (i *twoLevelIterator) skipEmptyDataBlocksForward() {
	for i.dataIter.iterator() == nil || !i.dataIter.valid {
		if !i.indexIter.valid {
			i.setDataIterator(nil)
			return
		}
		i.indexIter.next()
		i.initDataBlock()
		if i.dataIter.iterator() != nil {
			i.dataIter.seekToFirst()
		}
	}
}

func (i *twoLevelIterator) skipEmptyDataBlocksBackward() {
	for i.dataIter.iterator() == nil || !i.dataIter.valid {
		if !i.indexIter.valid {
			i.setDataIterator(nil)
			return
		}
		i.indexIter.prev()
		i.initDataBlock()
		if i.dataIter.iter != nil {
			i.dataIter.seekToLast()
		}
	}
}

func (i *twoLevelIterator) setDataIterator(dataIter ssdb.Iterator) {
	if i.dataIter.iterator() != nil {
		i.saveError(i.dataIter.status())
	}
	i.dataIter.set(dataIter)
}

func (i *twoLevelIterator) initDataBlock() {
	if !i.indexIter.valid {
		i.setDataIterator(nil)
	} else {
		handle := i.indexIter.getValue()
		if i.dataIter.iterator() != nil && bytes.Compare(handle, i.dataBlockHandle) == 0 {
		} else {
			iter := i.blockFunction(i.arg, i.options, handle)
			i.dataBlockHandle = make([]byte, len(handle))
			copy(i.dataBlockHandle, handle)
			i.setDataIterator(iter)
		}
	}
}
