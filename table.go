package ssdb

type Table interface {
	NewIterator(options *ReadOptions) Iterator
	ApproximateOffsetOf([]byte) uint64
	Closer
}
