package util

import "strings"

func RandomString(rnd *Random, l int) string {
	dst := make([]byte, l)
	for i := range dst {
		dst[i] = byte(' ' + rnd.Uniform(95))
	}
	return string(dst)
}

func RandomKey(rnd *Random, l int) string {
	testChars := [...]byte{'\000', '\001', 'a', 'b', 'c',
		'd', 'e', '\xfd', '\xfe', '\xff'}
	n := len(testChars)
	var b strings.Builder
	for i := 0; i < l; i++ {
		b.WriteByte(testChars[rnd.Uniform(n)])
	}
	return b.String()
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
