package db

type recordType uint8

const (
	zeroType recordType = iota
	fullType
	firstType
	middleType
	lastType
)

const maxRecordType = lastType
const blockSize = 32768
const headerSize = 4 + 2 + 1
