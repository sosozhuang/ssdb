package ssdb

import "log"

type CompressionType uint8

const (
	NoCompression CompressionType = iota
	SnappyCompression
)

type Options struct {
	Comparator           Comparator
	CreateIfMissing      bool
	ErrorIfExists        bool
	ParanoidChecks       bool
	Env                  Env
	InfoLog              *log.Logger
	WriteBufferSize      int
	MaxOpenFiles         int
	BlockSize            int
	BlockRestartInterval int
	MaxFileSize          int
	BlockCache           Cache
	CompressionType      CompressionType
	ReuseLogs            bool
	FilterPolicy         FilterPolicy
}

func NewOptions() *Options {
	return &Options{
		Comparator:           BytewiseComparator,
		CreateIfMissing:      false,
		ErrorIfExists:        false,
		ParanoidChecks:       false,
		Env:                  DefaultEnv(),
		InfoLog:              nil,
		WriteBufferSize:      4 * 1024 * 1024,
		MaxOpenFiles:         1000,
		BlockSize:            4 * 1024,
		BlockRestartInterval: 16,
		MaxFileSize:          2 * 1024 * 1024,
		CompressionType:      SnappyCompression,
		ReuseLogs:            false,
		FilterPolicy:         nil,
	}
}

type ReadOptions struct {
	VerifyChecksums bool
	FillCache       bool
	Snapshot        Snapshot
}

func NewReadOptions() *ReadOptions {
	return &ReadOptions{
		VerifyChecksums: false,
		FillCache:       true,
		Snapshot:        nil,
	}
}

type WriteOptions struct {
	Sync bool
}

func NewWriteOptions() *WriteOptions {
	return &WriteOptions{Sync: false}
}
