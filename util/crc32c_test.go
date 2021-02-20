package util

import "testing"

func TestStandardResults(t *testing.T) {
	buf := make([]byte, 32)
	TestEqual(uint32(0x8a9136aa), Value(buf), "zero slice", t)

	for i := range buf {
		buf[i] = 0xff
	}
	TestEqual(uint32(0x62a8ab43), Value(buf), "0xff slice", t)

	for i := range buf {
		buf[i] = byte(i)
	}
	TestEqual(uint32(0x46dd794e), Value(buf), "incremental slice", t)

	for i := range buf {
		buf[i] = byte(31 - i)
	}
	TestEqual(uint32(0x113fdb5c), Value(buf), "decremental slice", t)

	data := [48]byte{
		0x01, 0xc0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00,
		0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x18, 0x28, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	}
	TestEqual(uint32(0xd9963a56), Value(data[:]), "48 length slice", t)
}

func TestValues(t *testing.T) {
	TestNotEqual(Value([]byte("a")), Value([]byte("foo")), "'a' and 'foo'", t)
}

func TestExtend(t *testing.T) {
	TestEqual(Value([]byte("hello world")), Extend(Value([]byte("hello ")), []byte("world")), "extend", t)
}
