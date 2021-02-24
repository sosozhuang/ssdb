package ssdb

const (
	MajorVersion = 1
	MinorVersion = 22
)

type Snapshot interface {
}

type Range struct {
	start []byte
	limit []byte
}

type DB interface {
	Put(options *WriteOptions, key []byte, value []byte) error
	Delete(options *WriteOptions, key []byte) error
	Write(options *WriteOptions, updates *WriteBatch) error
	Get(options *ReadOptions, key []byte) ([]byte, error)
	NewIterator(options *ReadOptions) Iterator
	GetSnapshot() Snapshot
	ReleaseSnapshot(snapshot Snapshot)
	GetProperty(property []byte) (string, bool)
	GetApproximateSizes(r *Range, n int, sizes uint64)
	CompactRange(begin, end []byte)
}

func Open() (DB, error) {
	return nil, nil
}

func DestroyDB(name string, options *Options) error {
	return nil
}

func RepairDB(name string, options *Options) error {
	return nil
}
