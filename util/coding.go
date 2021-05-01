package util

import (
	"reflect"
	"unsafe"
)

var littleEndian bool

func init() {
	buf := [2]byte{}
	*(*uint16)(unsafe.Pointer(&buf[0])) = uint16(0xABCD)

	switch buf {
	case [2]byte{0xCD, 0xAB}:
		littleEndian = true
	case [2]byte{0xAB, 0xCD}:
		littleEndian = false
	default:
		panic("Could not determine native endianness.")
	}
}

func DecodeFixed32(b []byte) uint32 {
	if littleEndian {
		var d [4]byte
		copy(d[:], b)
		return *(*uint32)(unsafe.Pointer(&d))
	} else {
		return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
	}
}

func EncodeFixed32(dst *[4]byte, value uint32) {
	if littleEndian {
		x := *(*[4]byte)(unsafe.Pointer(&value))
		copy(dst[:], x[:])
	} else {
		dst[0] = byte(value & 0xff)
		dst[1] = byte((value >> 8) & 0xff)
		dst[2] = byte((value >> 16) & 0xff)
		dst[3] = byte((value >> 24) & 0xff)
	}
}

func DecodeFixed64(b []byte) uint64 {
	if littleEndian {
		var d [8]byte
		copy(d[:], b)
		return *(*uint64)(unsafe.Pointer(&d))
	} else {
		return uint64(DecodeFixed32(b[4:])<<32) | uint64(DecodeFixed32(b))
	}
}

func EncodeFixed64(dst *[8]byte, value uint64) {
	if littleEndian {
		x := *(*[8]byte)(unsafe.Pointer(&value))
		copy(dst[:], x[:])
	} else {
		dst[0] = byte(value & 0xff)
		dst[1] = byte((value >> 8) & 0xff)
		dst[2] = byte((value >> 16) & 0xff)
		dst[3] = byte((value >> 24) & 0xff)
		dst[4] = byte((value >> 32) & 0xff)
		dst[5] = byte((value >> 40) & 0xff)
		dst[6] = byte((value >> 48) & 0xff)
		dst[7] = byte((value >> 56) & 0xff)
	}
}

func PutFixed32(dst *[]byte, value uint32) {
	buf := new([4]byte)
	EncodeFixed32(buf, value)
	*dst = append(*dst, buf[:]...)
}

func PutFixed64(dst *[]byte, value uint64) {
	buf := new([8]byte)
	EncodeFixed64(buf, value)
	*dst = append(*dst, buf[:]...)
}

func EncodeVarInt32(buf *[5]byte, v uint32) int {
	const b = 128
	if v < 1<<7 {
		buf[0] = byte(v)
		return 1
	} else if v < 1<<14 {
		buf[0] = byte(v | b)
		buf[1] = byte(v >> 7)
		return 2
	} else if v < 1<<21 {
		buf[0] = byte(v | b)
		buf[1] = byte((v >> 7) | b)
		buf[2] = byte(v >> 14)
		return 3
	} else if v < 1<<28 {
		buf[0] = byte(v | b)
		buf[1] = byte((v >> 7) | b)
		buf[2] = byte((v >> 14) | b)
		buf[3] = byte(v >> 21)
		return 4
	} else {
		buf[0] = byte(v | b)
		buf[1] = byte((v >> 7) | b)
		buf[2] = byte((v >> 14) | b)
		buf[3] = byte((v >> 21) | b)
		buf[4] = byte(v >> 28)
		return 5
	}
}

func PutVarInt32(dst *[]byte, v uint32) {
	buf := new([5]byte)
	i := EncodeVarInt32(buf, v)
	*dst = append(*dst, (*buf)[:i]...)
}

func EncodeVarInt64(dst *[10]byte, v uint64) int {
	const b = 128
	ptr := 0
	for v >= b {
		dst[ptr] = byte(v | b)
		ptr++
		v >>= 7
	}
	dst[ptr] = byte(v)
	ptr++
	return ptr
}

func PutVarInt64(dst *[]byte, v uint64) {
	buf := new([10]byte)
	i := EncodeVarInt64(buf, v)
	*dst = append(*dst, buf[:i]...)
}

func PutLengthPrefixedSlice(dst *[]byte, value []byte) {
	PutVarInt32(dst, uint32(len(value)))
	*dst = append(*dst, value...)
}

func VarIntLength(v uint64) int {
	l := 1
	for v >= 128 {
		v >>= 7
		l++
	}
	return l
}

func getVarInt32PtrFallback(input []byte, value *uint32) int {
	result := uint32(0)
	var b uint32
	l := len(input)
	for i, shift := 0, uint32(0); shift <= 28 && i < l; shift += 7 {
		b = uint32(input[i])
		i++
		if b&128 != 0 {
			result |= (b & 127) << shift
		} else {
			result |= b << shift
			*value = result
			return i
		}
	}
	return -1
}

func GetVarInt32(input *[]byte, value *uint32) bool {
	i := GetVarInt32Ptr(*input, value)
	if i == -1 {
		return false
	} else {
		*input = (*input)[i:]
		return true
	}
}

func GetVarInt32Ptr(input []byte, value *uint32) int {
	if len(input) > 1 {
		result := uint32(input[0])
		if result&128 == 0 {
			*value = result
			return 1
		}
	}
	return getVarInt32PtrFallback(input, value)
}

func GetVarInt64(input *[]byte, value *uint64) bool {
	i := GetVarInt64Ptr(*input, value)
	if i == -1 {
		return false
	} else {
		*input = (*input)[i:]
		return true
	}
}

func GetVarInt64Ptr(input []byte, value *uint64) int {
	result := uint64(0)
	var b uint64
	l := len(input)
	for i, shift := 0, uint32(0); shift <= 63 && i < l; shift += 7 {
		b = uint64(input[i])
		i++
		if b&128 != 0 {
			result |= (b & 127) << shift
		} else {
			result |= b << shift
			*value = result
			return i
		}
	}
	return -1
}

func GetLengthPrefixedSlice1(input *[]byte, result *[]byte) bool {
	var l uint32
	if GetVarInt32(input, &l) && len(*input) >= int(l) {
		*result = make([]byte, l)
		copy(*result, (*input)[:l])
		*input = (*input)[l:]
		return true
	}
	return false
}

func GetLengthPrefixedSlice2(input, result *[]byte) bool {
	var l uint32
	if GetVarInt32(input, &l) && len(*input) >= int(l) {
		*result = make([]byte, l, l)
		copy(*result, *input)
		*input = (*input)[l:]
		return true
	} else {
		return false
	}
}

func SliceByteToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func StringToSliceByte(s string) []byte {
	sh := *(*reflect.StringHeader)(unsafe.Pointer(&s))
	bh := reflect.SliceHeader{
		Data: sh.Data,
		Len:  sh.Len,
		Cap:  sh.Len,
	}
	return *(*[]byte)(unsafe.Pointer(&bh))
}
