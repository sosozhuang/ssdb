package util

import (
	"math"
	"strings"
	"testing"
	"unsafe"
)

func TestFixed32(t *testing.T) {
	b := make([]byte, 0)
	for v := uint32(0); v < 100000; v++ {
		PutFixed32(&b, v)
	}
	var actual uint32
	size := unsafe.Sizeof(actual)
	for v := uint32(0); v < 100000; v++ {
		AssertEqual(v, DecodeFixed32(b), "DecodeFixed32", t)
		b = b[size:]
	}
}

func TestFixed64(t *testing.T) {
	b := make([]byte, 0)
	var v uint64
	for power := 0; power <= 63; power++ {
		v = 1 << uint(power)
		PutFixed64(&b, v-1)
		PutFixed64(&b, v+0)
		PutFixed64(&b, v+1)
	}

	var actual uint64
	size := unsafe.Sizeof(actual)
	for power := 0; power <= 63; power++ {
		v = 1 << uint(power)
		actual = DecodeFixed64(b)
		AssertEqual(v-1, DecodeFixed64(b), "DecodeFixed64", t)
		b = b[size:]

		AssertEqual(v, DecodeFixed64(b), "DecodeFixed64", t)
		b = b[size:]

		AssertEqual(v+1, DecodeFixed64(b), "DecodeFixed64", t)
		b = b[size:]
	}
}

func TestEncodingOutput(t *testing.T) {
	dst := make([]byte, 0)
	PutFixed32(&dst, 0x04030201)
	AssertEqual(4, len(dst), "dst length", t)
	AssertEqual(0x01, int(dst[0]), "dst[0]", t)
	AssertEqual(0x02, int(dst[1]), "dst[1]", t)
	AssertEqual(0x03, int(dst[2]), "dst[2]", t)
	AssertEqual(0x04, int(dst[3]), "dst[3]", t)

	dst = make([]byte, 0)
	PutFixed64(&dst, 0x0807060504030201)
	AssertEqual(8, len(dst), "dst length", t)
	AssertEqual(0x01, int(dst[0]), "dst[0]", t)
	AssertEqual(0x02, int(dst[1]), "dst[1]", t)
	AssertEqual(0x03, int(dst[2]), "dst[2]", t)
	AssertEqual(0x04, int(dst[3]), "dst[3]", t)
	AssertEqual(0x05, int(dst[4]), "dst[4]", t)
	AssertEqual(0x06, int(dst[5]), "dst[5]", t)
	AssertEqual(0x07, int(dst[6]), "dst[6]", t)
	AssertEqual(0x08, int(dst[7]), "dst[7]", t)
}

func TestVarInt32(t *testing.T) {
	s := make([]byte, 0)
	var v uint32
	for i := uint32(0); i < 32*32; i++ {
		v = (i / 32) << (i % 32)
		PutVarInt32(&s, v)
	}

	var expected, actual uint32
	var p, j int
	for i := uint32(0); i < 32*32; i++ {
		expected = (i / 32) << (i % 32)
		j = GetVarInt32Ptr(s[p:], &actual)
		AssertTrue(j != -1, "j != -1", t)
		p += j
		AssertEqual(expected, actual, "GetVarInt32Ptr", t)
		AssertEqual(VarIntLength(uint64(actual)), j, "VarIntLength", t)
	}
}

func TestVarInt64(t *testing.T) {
	values := make([]uint64, 0)
	values = append(values, 0, 100, math.MaxUint64, math.MaxUint64-1)
	var power uint64
	for k := uint32(0); k < 64; k++ {
		power = uint64(1) << k
		values = append(values, power, power-1, power+1)
	}

	b := make([]byte, 0)
	for i := 0; i < len(values); i++ {
		PutVarInt64(&b, values[i])
	}

	var actual uint64
	var p, j int
	for i := 0; i < len(values); i++ {
		j = GetVarInt64Ptr(b[p:], &actual)
		AssertTrue(j != -1, "j != -1", t)
		p += j
		AssertEqual(values[i], actual, "values[i] == actual", t)
		AssertEqual(VarIntLength(actual), j, "VarIntLength", t)
	}
}

func TestVarInt32Overflow(t *testing.T) {
	var result uint32
	b := []byte("\x81\x82\x83\x84\x85\x11")
	AssertEqual(GetVarInt32Ptr(b, &result), -1, "GetVarInt32Ptr", t)
}

func TestVarInt32Truncation(t *testing.T) {
	largeValue := uint32(1<<31) + 100
	b := make([]byte, 0)
	PutVarInt32(&b, largeValue)
	var result uint32
	for l := 0; l < len(b)-1; l++ {
		AssertEqual(GetVarInt32Ptr(b[:l], &result), -1, "GetVarInt32Ptr", t)
	}
	if GetVarInt32Ptr(b, &result) == -1 {
		t.Errorf(".\n")
	}
	if largeValue != result {
		t.Errorf("%d != %d.", largeValue, result)
	}
}

func TestVarInt64Overflow(t *testing.T) {
	var result uint64
	b := []byte("\x81\x82\x83\x84\x85\x81\x82\x83\x84\x85\x11")
	AssertEqual(GetVarInt64Ptr(b, &result), -1, "GetVarInt64Ptr", t)
}

func TestVarInt64Truncation(t *testing.T) {
	largeValue := uint64(1<<63) + 100
	b := make([]byte, 0)
	PutVarInt64(&b, largeValue)
	var result uint64
	for l := 0; l < len(b)-1; l++ {
		AssertEqual(GetVarInt64Ptr(b[:l], &result), -1, "GetVarInt64Ptr", t)
	}
	AssertNotEqual(GetVarInt64Ptr(b, &result), -1, "GetVarInt64Ptr", t)
	AssertEqual(largeValue, result, "largeValue", t)
}

func TestStrings(t *testing.T) {
	b := make([]byte, 0)
	PutLengthPrefixedSlice(&b, []byte(""))
	PutLengthPrefixedSlice(&b, []byte("foo"))
	PutLengthPrefixedSlice(&b, []byte("bar"))
	s := strings.Repeat("x", 200)
	PutLengthPrefixedSlice(&b, []byte(s))

	var v []byte
	AssertTrue(GetLengthPrefixedSlice2(&b, &v), "GetLengthPrefixedSlice2", t)
	AssertEqual("", string(v), "empty string", t)

	AssertTrue(GetLengthPrefixedSlice2(&b, &v), "GetLengthPrefixedSlice2", t)
	AssertEqual("foo", string(v), "foo", t)

	AssertTrue(GetLengthPrefixedSlice2(&b, &v), "GetLengthPrefixedSlice2", t)
	AssertEqual("bar", string(v), "bar", t)
	AssertTrue(GetLengthPrefixedSlice2(&b, &v), "GetLengthPrefixedSlice2", t)
	AssertEqual(strings.Repeat("x", 200), string(v), "x*200", t)
	AssertEqual(len(b), 0, "len(b)", t)
}
