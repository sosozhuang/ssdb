package table

import "ssdb"

type iteratorWrapper struct {
	iter  ssdb.Iterator
	valid bool
	key   []byte
}

func newIteratorWrapper(iter ssdb.Iterator) *iteratorWrapper {
	w := new(iteratorWrapper)
	w.set(iter)
	return w
}

func (w *iteratorWrapper) iterator() ssdb.Iterator {
	return w.iter
}

func (w *iteratorWrapper) set(iter ssdb.Iterator) {
	w.iter = iter
	if iter == nil {
		w.valid = false
	} else {
		w.update()
	}
}

func (w *iteratorWrapper) getKey() []byte {
	if !w.valid {
		panic("iteratorWrapper: not valid")
	}
	return w.key
}

func (w *iteratorWrapper) getValue() []byte {
	if !w.valid {
		panic("iteratorWrapper: not valid")
	}
	return w.iter.Value()
}

func (w *iteratorWrapper) status() error {
	if !w.valid {
		panic("iteratorWrapper: not valid")
	}
	return w.iter.Status()
}

func (w *iteratorWrapper) next() {
	if !w.valid {
		panic("iteratorWrapper: not valid")
	}
	w.iter.Next()
	w.update()
}

func (w *iteratorWrapper) prev() {
	if !w.valid {
		panic("iteratorWrapper: not valid")
	}
	w.iter.Prev()
	w.update()
}

func (w *iteratorWrapper) seek(k []byte) {
	if !w.valid {
		panic("iteratorWrapper: not valid")
	}
	w.iter.Seek(k)
	w.update()
}

func (w *iteratorWrapper) seekToFirst() {
	if !w.valid {
		panic("iteratorWrapper: not valid")
	}
	w.iter.SeekToFirst()
	w.update()
}

func (w *iteratorWrapper) seekToLast() {
	if !w.valid {
		panic("iteratorWrapper: not valid")
	}
	w.iter.SeekToLast()
	w.update()
}

func (w *iteratorWrapper) update() {
	w.valid = w.iter.Valid()
	if w.valid {
		w.key = w.iter.Key()
	}
}

func (w *iteratorWrapper) finalize() {
	w.iter.Finalize()
}
