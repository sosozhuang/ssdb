package util

import "testing"

func TestStandardResults(t *testing.T) {
	buf := make([]byte, 32)
	TestEqual(uint32(0x8a9136aa), ChecksumValue(buf), "zero slice", t)

	for i := range buf {
		buf[i] = 0xff
	}
	TestEqual(uint32(0x62a8ab43), ChecksumValue(buf), "0xff slice", t)

	for i := range buf {
		buf[i] = byte(i)
	}
	TestEqual(uint32(0x46dd794e), ChecksumValue(buf), "incremental slice", t)

	for i := range buf {
		buf[i] = byte(31 - i)
	}
	TestEqual(uint32(0x113fdb5c), ChecksumValue(buf), "decremental slice", t)

	data := [48]byte{
		0x01, 0xc0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00,
		0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x18, 0x28, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	}
	TestEqual(uint32(0xd9963a56), ChecksumValue(data[:]), "48 length slice", t)
}

func TestValues(t *testing.T) {
	TestNotEqual(ChecksumValue([]byte("a")), ChecksumValue([]byte("foo")), "'a' and 'foo'", t)
}

func TestExtend(t *testing.T) {
	TestEqual(ChecksumValue([]byte("hello world")), ChecksumExtend(ChecksumValue([]byte("hello ")), []byte("world")), "extend", t)
}
