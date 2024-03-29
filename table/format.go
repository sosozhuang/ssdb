package table

import (
	"bytes"
	"github.com/golang/snappy"
	"reflect"
	"ssdb"
	"ssdb/util"
	"unsafe"
)

const maxEncodedLength = 10 + 10

type blockHandle struct {
	offset uint64
	size   uint64
}

func newBlockHandle() *blockHandle {
	return &blockHandle{
		offset: ^uint64(0),
		size:   ^uint64(0),
	}
}

func (h *blockHandle) encodeTo(dst *[]byte) {
	if h.offset == ^uint64(0) {
		panic("blockHandle: offset == ^0")
	}
	if h.size == ^uint64(0) {
		panic("blockHandle: size == ^0")
	}
	util.PutVarInt64(dst, h.offset)
	util.PutVarInt64(dst, h.size)
}

func (h *blockHandle) decodeFrom(input *[]byte) error {
	if util.GetVarInt64(input, &h.offset) && util.GetVarInt64(input, &h.size) {
		return nil
	}
	return util.CorruptionError1("bad block handle")
}

const footerEncodedLength = 2*maxEncodedLength + 8

type footer struct {
	metaIndexHandle *blockHandle
	indexHandle     *blockHandle
}

func (f *footer) getMetaIndexHandle() *blockHandle {
	return f.metaIndexHandle
}

func (f *footer) setMetaIndexHandle(h *blockHandle) {
	f.metaIndexHandle = h
}

func (f *footer) getIndexHandle() *blockHandle {
	return f.indexHandle
}

func (f *footer) setIndexHandle(h *blockHandle) {
	f.indexHandle = h
}

func (f *footer) encodeTo(dst *[]byte) {
	originalSize := len(*dst)
	f.metaIndexHandle.encodeTo(dst)
	f.indexHandle.encodeTo(dst)
	if len(*dst) < 2*maxEncodedLength {
		padding := bytes.Repeat([]byte{'\000'}, 2*maxEncodedLength-len(*dst))
		*dst = append(*dst, padding...)
	}
	util.PutFixed32(dst, uint32(tableMagicNumber&0xffffffff))
	util.PutFixed32(dst, uint32(tableMagicNumber>>32))
	if len(*dst) != originalSize+footerEncodedLength {
		panic("footer: len(*dst) != originalSize + maxEncodedLength")
	}
}

func (f *footer) decodeFrom(input *[]byte) (err error) {
	magicLo := util.DecodeFixed32((*input)[footerEncodedLength-8:])
	magicHi := util.DecodeFixed32((*input)[footerEncodedLength-8+4:])
	magic := uint64(magicHi)<<32 | uint64(magicLo)
	if magic != tableMagicNumber {
		return util.CorruptionError1("not an sstable (bad magic number)")
	}
	if err = f.metaIndexHandle.decodeFrom(input); err != nil {
		return
	}
	if err = f.indexHandle.decodeFrom(input); err != nil {
		return
	}
	//*input = (*input)[footerEncodedLength:]
	return
}

const tableMagicNumber = uint64(0xdb4775248b80fb57)
const blockTrailerSize = 5

type blockContents struct {
	data          []byte
	cachable      bool
	heapAllocated bool
}

func readBlock(file ssdb.RandomAccessFile, options *ssdb.ReadOptions, handle *blockHandle) (result *blockContents, err error) {
	n := handle.size
	buf := make([]byte, n+blockTrailerSize)
	var (
		i        int
		contents []byte
	)
	if contents, i, err = file.Read(buf, int64(handle.offset)); err != nil {
		return
	} else if i != len(contents) {
		err = util.CorruptionError1("truncated block read")
		return
	}

	if options.VerifyChecksums {
		crc := util.UnmaskChecksum(util.DecodeFixed32(contents[n+1:]))
		actual := util.ChecksumValue(contents[:n+1])
		if crc != actual {
			err = util.CorruptionError1("block checksum mismatch")
			return
		}
	}
	switch ssdb.CompressionType(contents[n]) {
	case ssdb.NoCompression:
		result = new(blockContents)
		bsh := (*reflect.SliceHeader)(unsafe.Pointer(&buf))
		csh := (*reflect.SliceHeader)(unsafe.Pointer(&contents))
		if bsh.Data != csh.Data {
			result.data = contents[:n]
			result.heapAllocated = false
			result.cachable = false
		} else {
			result.data = buf[:n]
			result.heapAllocated = true
			result.cachable = true
		}
	case ssdb.SnappyCompression:
		var l int
		if l, err = snappy.DecodedLen(contents[:n]); err != nil {
			err = util.CorruptionError2("corrupted compressed block contents", err.Error())
			return
		}
		ubuf := make([]byte, l)
		if ubuf, err = snappy.Decode(ubuf, contents[:n]); err != nil {
			err = util.CorruptionError2("corrupted compressed block contents", err.Error())
			return
		}
		result = new(blockContents)
		result.data = ubuf[:l]
		result.heapAllocated = true
		result.cachable = true
	default:
		err = util.CorruptionError1("bad block type")
		return
	}
	return
}
