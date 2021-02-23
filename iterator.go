package ssdb

type CleanUpFunction func(arg1, arg2 interface{})

type Iterator interface {
	IsValid() bool
	SeekToFirst()
	SeekToLast()
	Seek(target []byte)
	Next()
	Prev()
	GetKey() []byte
	GetValue() []byte
	GetStatus() error
	RegisterCleanUp(function CleanUpFunction, arg1, arg2 interface{})
}
