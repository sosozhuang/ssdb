package ssdb

type TableBuilder interface {
	ChangeOptions(options *Options) error
	Add(key, value []byte)
	Flush()
	Status() error
	Finish() error
	Abandon()
	NumEntries() int64
	FileSize() uint64
	Closer
}
