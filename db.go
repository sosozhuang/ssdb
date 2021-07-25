package ssdb

const (
	MajorVersion = 1
	MinorVersion = 22
)

type Snapshot interface {
}

type Range struct {
	Start []byte // Included in the range
	Limit []byte // Not included in the range
}

type DB interface {
	Put(options *WriteOptions, key, value []byte) error
	Delete(options *WriteOptions, key []byte) error
	Write(options *WriteOptions, updates WriteBatch) error
	Get(options *ReadOptions, key []byte) ([]byte, error)
	NewIterator(options *ReadOptions) Iterator
	GetSnapshot() Snapshot
	ReleaseSnapshot(snapshot Snapshot)
	GetProperty(property string) (string, bool)
	GetApproximateSizes(r []Range) []uint64
	CompactRange(begin, end []byte)
	Closer
}
