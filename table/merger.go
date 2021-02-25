package table

import "ssdb"

func newMergingIterator(comparator ssdb.Comparator, children []ssdb.Iterator) ssdb.Iterator {
	i := &mergingIterator{
		comparator: comparator,
		children:   make([]*iteratorWrapper, len(children)),
		current:    nil,
		direction:  forward,
	}
	for index, iter := range children {
		i.children[index] = newIteratorWrapper(iter)
	}
	return i
}

type direction int8

const (
	forward = direction(iota)
	reverse
)

type mergingIterator struct {
	CleanUpIterator
	comparator ssdb.Comparator
	children   []*iteratorWrapper
	current    *iteratorWrapper
	direction  direction
}

func (iter *mergingIterator) IsValid() bool {
	return iter.current != nil
}

func (iter *mergingIterator) SeekToFirst() {
	for _, child := range iter.children {
		child.seekToFirst()
	}
	iter.findSmallest()
	iter.direction = forward
}

func (iter *mergingIterator) SeekToLast() {
	for _, child := range iter.children {
		child.seekToLast()
	}
	iter.findLargest()
	iter.direction = reverse
}

func (iter *mergingIterator) Seek(target []byte) {
	for _, child := range iter.children {
		child.seek(target)
	}
	iter.findSmallest()
	iter.direction = forward
}

func (iter *mergingIterator) Next() {
	if !iter.IsValid() {
		panic("mergingIterator: not valid")
	}
	if iter.direction != forward {
		for _, child := range iter.children {
			if child != iter.current {
				child.seek(iter.GetKey())
				if child.isValid() && iter.comparator.Compare(iter.GetKey(), child.getKey()) == 0 {
					child.next()
				}
			}
		}
		iter.direction = forward
	}
	iter.current.next()
	iter.findSmallest()
}

func (iter *mergingIterator) Prev() {
	if !iter.IsValid() {
		panic("mergingIterator: not valid")
	}
	if iter.direction != reverse {
		for _, child := range iter.children {
			if child != iter.current {
				child.seek(iter.GetKey())
				if child.isValid() {
					child.prev()
				} else {
					child.seekToLast()
				}
			}
		}
		iter.direction = reverse
	}
	iter.current.prev()
	iter.findLargest()
}

func (iter *mergingIterator) GetKey() []byte {
	if !iter.IsValid() {
		panic("mergingIterator: not valid")
	}
	return iter.current.getKey()
}

func (iter *mergingIterator) GetValue() []byte {
	if !iter.IsValid() {
		panic("mergingIterator: not valid")
	}
	return iter.current.getValue()
}

func (iter *mergingIterator) GetStatus() (err error) {
	for _, child := range iter.children {
		if err = child.getStatus(); err != nil {
			break
		}
	}
	return
}

func (iter *mergingIterator) findSmallest() {
	var smallest *iteratorWrapper
	for _, child := range iter.children {
		if child.isValid() {
			if smallest == nil {
				smallest = child
			} else if iter.comparator.Compare(child.getKey(), smallest.getKey()) < 0 {
				smallest = child
			}
		}
	}
	iter.current = smallest
}

func (iter *mergingIterator) findLargest() {
	var largest *iteratorWrapper
	for _, child := range iter.children {
		if child.isValid() {
			if largest == nil {
				largest = child
			} else if iter.comparator.Compare(child.getKey(), largest.getKey()) > 0 {
				largest = child
			}
		}
	}
	iter.current = largest
}
