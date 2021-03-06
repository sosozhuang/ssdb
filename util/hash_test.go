package util

import (
	"testing"
)

var data1 = [...]byte{0x62}
var data2 = [...]byte{0xc3, 0x97}
var data3 = [...]byte{0xe2, 0x99, 0xa5}
var data4 = [...]byte{0xe1, 0x80, 0xb9, 0x32}
var data5 = [...]byte{
	0x01, 0xc0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00,
	0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x18, 0x28, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
}

func TestHash(t *testing.T) {
	var e, a uint32
	e = 0xbc9f1d34
	if a = Hash([]byte{}, 0xbc9f1d34); a != e {
		t.Errorf("empty bytes failed, expected: %x, actual: %x", e, a)
	}
	e = 0xef1345c4
	if a = Hash(data1[:], 0xbc9f1d34); a != e {
		t.Errorf("data1 failed, expected: %x, actual: %x", e, a)
	}
	e = 0x5b663814
	if a = Hash(data2[:], 0xbc9f1d34); a != e {
		t.Errorf("data2 failed, expected: %x, actual: %x", e, a)
	}
	e = 0x323c078f
	if a = Hash(data3[:], 0xbc9f1d34); a != e {
		t.Errorf("data3 failed, expected: %x, actual: %x", e, a)
	}
	e = 0xed21633a
	if a = Hash(data4[:], 0xbc9f1d34); a != e {
		t.Errorf("data4 failed, expected: %x, actual: %x", e, a)
	}
	e = 0xf333dabb
	if a = Hash(data5[:], 0x12345678); a != e {
		t.Errorf("data5 failed, expected: %x, actual: %x", e, a)
	}
}
