package ssdb

import "log"

type Env interface {
	NewSequentialFile(name string) (SequentialFile, error)
	NewRandomAccessFile(name string) (RandomAccessFile, error)
	NewWritableFile(name string) (WritableFile, error)
	NewAppendableFile(name string) (WritableFile, error)
	FileExists(name string) bool
	GetChildren(dir string) ([]string, error)
	DeleteFile(name string) error
	CreateDir(name string) error
	DeleteDir(name string) error
	GetFileSize(name string) (int, error)
	RenameFile(from, to string) error
	LockFile(name string) (FileLock, error)
	UnlockFile(lock FileLock) error
	Schedule(function func(interface{}), arg interface{})
	StartThread(function func(interface{}), arg interface{})
	GetTestDirectory() (string, error)
	NewLogger(name string) (log.Logger, error)
	NowMicros() uint64
	SleepForMicroseconds(micros int)
}

type SequentialFile interface {
	Read(b []byte) (int, error)
	Skip(n uint64) error
}

type RandomAccessFile interface {
	Read(b []byte, offset int64) ([]byte, int, error)
}

type WritableFile interface {
	Append(data []byte) error
	Close() error
	Flush() error
	Sync() error
}

type FileLock interface {
}

func NewEnv() Env {
	return nil
}

func WriteStringToFile(env Env, data []byte, name string) error {
	return nil
}

func WriteStringToFileSync(env Env, data string, name string) error {
	return nil
}

func ReadFileToString(env Env, name string, data []byte) error {
	return nil
}

type EnvWrapper struct {
	Env
}

func (w *EnvWrapper) Target() Env {
	return w.Env
}
