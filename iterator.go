package ssdb

type CleanUpFunction func(arg1, arg2 interface{})

type Iterator interface {
	Valid() bool
	SeekToFirst()
	SeekToLast()
	Seek(target []byte)
	Next()
	Prev()
	Key() []byte
	Value() []byte
	Status() error
	RegisterCleanUp(function CleanUpFunction, arg1, arg2 interface{})
	Finalizer
}
