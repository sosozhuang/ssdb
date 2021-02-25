package table

import "ssdb"

type cleanUpNode struct {
	function ssdb.CleanUpFunction
	arg1     interface{}
	arg2     interface{}
	next     *cleanUpNode
}

func (n *cleanUpNode) isEmpty() bool {
	return n.function == nil
}

func (n *cleanUpNode) run() {
	n.function(n.arg1, n.arg2)
}

type CleanUpIterator struct {
	cleanUpHead cleanUpNode
}

func (i *CleanUpIterator) RegisterCleanUp(function ssdb.CleanUpFunction, arg1, arg2 interface{}) {
	if function == nil {
		panic("CleanUpIterator: function cannot be nil")
	}
	var node *cleanUpNode
	if i.cleanUpHead.isEmpty() {
		node = &i.cleanUpHead
	} else {
		node = &cleanUpNode{}
		node.next = i.cleanUpHead.next
		i.cleanUpHead.next = node
	}
	node.function = function
	node.arg1 = arg1
	node.arg2 = arg2
}

func (i *CleanUpIterator) finalize() {
	//todo: run this method when finalized
	if !i.cleanUpHead.isEmpty() {
		i.cleanUpHead.run()
		for node := i.cleanUpHead.next; node != nil; node = node.next {
			node.run()
		}
	}
}

type emptyIterator struct {
	CleanUpIterator
	status error
}

func (_ *emptyIterator) IsValid() bool {
	return false
}

func (_ *emptyIterator) SeekToFirst() {
}

func (_ *emptyIterator) SeekToLast() {
}

func (_ *emptyIterator) Seek(target []byte) {
}

func (_ *emptyIterator) Next() {
	panic("empty iterator")
}

func (_ *emptyIterator) Prev() {
	panic("empty iterator")
}

func (_ *emptyIterator) GetKey() []byte {
	panic("empty iterator")
}

func (_ *emptyIterator) GetValue() []byte {
	panic("empty iterator")
}

func (i *emptyIterator) GetStatus() error {
	return i.status
}

func NewEmptyIterator() ssdb.Iterator {
	return &emptyIterator{}
}

func NewErrorIterator(err error) ssdb.Iterator {
	return &emptyIterator{status: err}
}
