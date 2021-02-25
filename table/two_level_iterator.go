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

func newTwoLevelIterator(indexIter ssdb.Iterator, blockFunction BlockFunction, arg interface{}, options *ssdb.ReadOptions) *twoLevelIterator {
	return &twoLevelIterator{
		blockFunction: blockFunction,
		arg:           arg,
		options:       options,
		indexIter:     newIteratorWrapper(indexIter),
		dataIter:      newIteratorWrapper(nil),
	}
}

func (i *twoLevelIterator) IsValid() bool {
	return i.dataIter.isValid()
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
	if !i.IsValid() {
		panic("twoLevelIterator: not valid")
	}
	i.dataIter.next()
	i.skipEmptyDataBlocksForward()
}

func (i *twoLevelIterator) Prev() {
	if !i.IsValid() {
		panic("twoLevelIterator: not valid")
	}
	i.dataIter.prev()
	i.skipEmptyDataBlocksBackward()
}

func (i *twoLevelIterator) GetKey() []byte {
	if !i.IsValid() {
		panic("twoLevelIterator: not valid")
	}
	return i.dataIter.getKey()
}

func (i *twoLevelIterator) GetValue() []byte {
	if !i.IsValid() {
		panic("twoLevelIterator: not valid")
	}
	return i.dataIter.getValue()
}

func (i *twoLevelIterator) GetStatus() error {
	if i.indexIter.getStatus() != nil {
		return i.indexIter.getStatus()
	} else if i.dataIter.iterator() != nil && i.dataIter.iterator().GetStatus() != nil {
		return i.dataIter.getStatus()
	}
	return i.err
}

func (i *twoLevelIterator) saveError(err error) {
	if i.err == nil && err != nil {
		i.err = err
	}
}

func (i *twoLevelIterator) skipEmptyDataBlocksForward() {
	for i.dataIter.iterator() == nil || !i.dataIter.isValid() {
		if !i.indexIter.isValid() {
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
	for i.dataIter.iterator() == nil || !i.dataIter.isValid() {
		if !i.indexIter.isValid() {
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
		i.saveError(i.dataIter.getStatus())
	}
	i.dataIter.set(dataIter)
}

func (i *twoLevelIterator) initDataBlock() {
	if !i.indexIter.isValid() {
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
