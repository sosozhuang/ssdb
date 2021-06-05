package db

import (
	"ssdb"
	"ssdb/util"
	"unsafe"
)

type logWriter struct {
	dest        ssdb.WritableFile
	blockOffset int
	// crc32c values for all supported record types.  These are
	// pre-computed to reduce the overhead of computing the crc of the
	// record type stored in the header.
	typeCrc [maxRecordType + 1]uint32
}

func (w *logWriter) addRecord(data []byte) (err error) {
	start := 0
	left := len(data)
	begin := true
	var (
		leftover       int
		fragmentLength int
		avail          int
		rt             recordType
		end            bool
	)
	for {
		leftover = blockSize - w.blockOffset
		if leftover < 0 {
			panic("logWriter: leftover < 0")
		}
		if leftover < headerSize {
			if leftover > 0 {
				b := make([]byte, leftover)
				for i := range b {
					b[i] = '\x00'
				}
				_ = w.dest.Append(b)
			}
			w.blockOffset = 0
		}
		if blockSize-w.blockOffset-headerSize < 0 {
			panic("logWriter: blockSize - w.blockOffset - headerSize < 0")
		}
		avail = blockSize - w.blockOffset - headerSize
		if left < avail {
			fragmentLength = left
		} else {
			fragmentLength = avail
		}
		end = left == fragmentLength
		if begin && end {
			rt = fullType
		} else if begin {
			rt = firstType
		} else if end {
			rt = lastType
		} else {
			rt = middleType
		}
		err = w.emitPhysicalRecord(rt, data[start:start+fragmentLength])
		start += fragmentLength
		left -= fragmentLength
		begin = false
		if !(err == nil && left > 0) {
			break
		}
	}
	return
}

func (w *logWriter) emitPhysicalRecord(rt recordType, data []byte) (err error) {
	if len(data) > 0xffff {
		panic("logWriter: len(data) > 0xffff")
	}
	if w.blockOffset+headerSize+len(data) > blockSize {
		panic("logWriter: blockOffset + headerSize + len(data) > blockSize")
	}
	var buf [headerSize]byte
	buf[4] = byte(len(data) & 0xff)
	buf[5] = byte(len(data) >> 8)
	buf[6] = byte(rt)
	crc := util.ChecksumExtend(w.typeCrc[rt], data)
	crc = util.MaskChecksum(crc)
	util.EncodeFixed32((*[4]byte)(unsafe.Pointer(&buf[0])), crc)

	if err = w.dest.Append(buf[:]); err == nil {
		if err = w.dest.Append(data); err == nil {
			err = w.dest.Flush()
		}
	}
	w.blockOffset += headerSize + len(data)
	return
}

func initTypeCrc(typeCrc *[maxRecordType + 1]uint32) {
	t := make([]byte, 1)
	for i := recordType(0); i <= maxRecordType; i++ {
		t[0] = byte(i)
		typeCrc[i] = util.ChecksumValue(t)
	}
}

func newLogWriter(dest ssdb.WritableFile) *logWriter {
	return newLogWriterWithLength(dest, 0)
}

func newLogWriterWithLength(dest ssdb.WritableFile, length uint64) *logWriter {
	w := &logWriter{
		dest:        dest,
		blockOffset: int(length % uint64(blockSize)),
		typeCrc:     [maxRecordType + 1]uint32{},
	}
	initTypeCrc(&w.typeCrc)
	return w
}
