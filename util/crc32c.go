package util

import (
	"hash/crc32"
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

func Extend(crc uint32, data []byte) uint32 {
	return crc32.Update(crc, crcTable, data)
}

func Value(data []byte) uint32 {
	return Extend(0, data)
}
