package util

import (
	"hash/crc32"
)

const (
	maskDelta = uint32(0xa282ead8)
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

func ChecksumExtend(crc uint32, data []byte) uint32 {
	return crc32.Update(crc, crcTable, data)
}

func ChecksumValue(data []byte) uint32 {
	return ChecksumExtend(0, data)
}

func MaskChecksum(crc uint32) uint32 {
	return ((crc >> 15) | (crc << 17)) + maskDelta
}

func UnmaskChecksum(marked uint32) uint32 {
	rot := marked - maskDelta
	return (rot >> 17) | (rot << 15)
}
