package util

func Hash(data []byte, seed uint32) uint32 {
	const m = uint32(0xc6a4a793)
	const r = uint32(24)
	start := 0
	limit := len(data)
	h := seed ^ (uint32(limit) * m)
	var w uint32
	for start+4 <= limit {
		w = DecodeFixed32(data[start:])
		start += 4
		h += w
		h *= m
		h ^= h >> 16
	}
	switch limit - start {
	case 3:
		h += uint32(data[start+2]) << 16
		fallthrough
	case 2:
		h += uint32(data[start+1]) << 8
		fallthrough
	case 1:
		h += uint32(data[start])
		h *= m
		h ^= h >> r
		break
	}
	return h
}
