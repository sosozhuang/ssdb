package util

import (
	"bytes"
	"strings"
)

func RandomString(rnd *Random, l int) string {
	dst := make([]byte, l)
	for i := range dst {
		dst[i] = byte(' ' + rnd.Uniform(95))
	}
	return string(dst)
}

func RandomBytes(rnd *Random, l int) []byte {
	dst := make([]byte, l)
	for i := range dst {
		dst[i] = byte(' ' + rnd.Uniform(95))
	}
	return dst
}

func RandomKey(rnd *Random, l int) string {
	testChars := [...]byte{'\000', '\001', 'a', 'b', 'c',
		'd', 'e', '\xfd', '\xfe', '\xff'}
	n := len(testChars)
	dst := make([]byte, l)
	for i := range dst {
		dst[i] = testChars[rnd.Uniform(n)]
	}
	return string(dst)
}

func CompressibleString(rnd *Random, compressedFraction float64, l int) string {
	raw := int(float64(l) * compressedFraction)
	if raw < 1 {
		raw = 1
	}
	rawData := RandomString(rnd, raw)
	var b strings.Builder
	for b.Len() < l {
		b.WriteString(rawData)
	}
	return b.String()[:l]
}

func CompressibleBytes(rnd *Random, compressedFraction float64, l int, buf *bytes.Buffer) {
	raw := int(float64(l) * compressedFraction)
	if raw < 1 {
		raw = 1
	}
	rawData := RandomBytes(rnd, raw)
	for buf.Len() < l {
		buf.Write(rawData)
	}
}
