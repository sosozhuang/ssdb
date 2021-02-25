package table

import (
	"github.com/golang/snappy"
	"runtime"
	"ssdb"
	"ssdb/util"
)

type tableBuilder struct {
	rep *tableBuilderRep
}

func newTableBuilder(options *ssdb.Options, file ssdb.WritableFile) *tableBuilder {
	b := &tableBuilder{rep: newTableBuilderRep(options, file)}
	if b.rep.filterBlock != nil {
		b.rep.filterBlock.startBlock(0)
	}
	runtime.SetFinalizer(b, func(b *tableBuilder) {
		if !b.rep.closed {
			panic("tableBuilder: rep not closed")
		}
	})
	return b
}

func (b *tableBuilder) changeOptions(options *ssdb.Options) error {
	if options.Comparator != b.rep.options.Comparator {
		return util.InvalidArgumentError1("changing comparator while building table")
	}
	b.rep.options = options
	b.rep.indexBlockOptions = options
	b.rep.indexBlockOptions.BlockRestartInterval = 1
	return nil
}

func (b *tableBuilder) add(key, value []byte) {
	r := b.rep
	if r.closed {
		panic("tableBuilder: rep closed")
	}
	if !b.ok() {
		return
	}
	if r.numEntries > 0 {
		if r.options.Comparator.Compare(key, r.lastKey) <= 0 {
			panic("tableBuilder: rep.options.Comparator.Compare(key, r.lastKey) <= 0")
		}
	}
	if r.pendingIndexEntry {
		if !r.dataBlock.empty() {
			panic("tableBuilder: rep.dataBlock not empty")
		}
		r.options.Comparator.FindShortestSeparator(&r.lastKey, key)
		handleEncoding := make([]byte, 0)
		r.pendingHandle.encodeTo(&handleEncoding)
		r.indexBlock.add(r.lastKey, handleEncoding)
		r.pendingIndexEntry = false
	}

	if r.filterBlock != nil {
		r.filterBlock.addKey(key)
	}

	r.lastKey = key
	r.numEntries++
	r.dataBlock.add(key, value)
	if estimatedBlockSize := r.dataBlock.currentSizeEstimate(); uint(estimatedBlockSize) >= r.options.BlockSize {
		b.flush()
	}

}

func (b *tableBuilder) flush() {
	r := b.rep
	if r.closed {
		panic("tableBuilder: rep closed")
	}
	if !b.ok() {
		return
	}
	if r.dataBlock.empty() {
		return
	}
	if r.pendingIndexEntry {
		panic("tableBuilder: rep.pendingIndexEntry is true")
	}
	b.writeBlock(r.dataBlock, r.pendingHandle)
	if b.ok() {
		r.pendingIndexEntry = true
		r.err = r.file.Flush()
	}
	if r.filterBlock != nil {
		r.filterBlock.startBlock(r.offset)
	}

}

func (b *tableBuilder) status() error {
	return b.rep.err
}

func (b *tableBuilder) finish() error {
	r := b.rep
	b.flush()
	if r.closed {
		panic("tableBuilder: rep closed")
	}
	r.closed = true
	var (
		filterBlockHandle    = newBlockHandle()
		metaIndexBlockHandle = newBlockHandle()
		indexBlockHandle     = newBlockHandle()
	)
	if b.ok() && r.filterBlock != nil {
		b.writeRawBlock(r.filterBlock.finish(), ssdb.NoCompression, filterBlockHandle)
	}

	if b.ok() {
		metaIndexblock := newBlockBuilder(r.options)
		if r.filterBlock != nil {
			key := "filter." + r.options.FilterPolicy.Name()
			handleEncoding := make([]byte, 0)
			filterBlockHandle.encodeTo(&handleEncoding)
			metaIndexblock.add([]byte(key), handleEncoding)
		}
		b.writeBlock(metaIndexblock, metaIndexBlockHandle)
	}

	if b.ok() {
		if r.pendingIndexEntry {
			r.options.Comparator.FindShortSuccessor(&r.lastKey)
			handleEncoding := make([]byte, 0)
			r.pendingHandle.encodeTo(&handleEncoding)
			r.indexBlock.add(r.lastKey, handleEncoding)
			r.pendingIndexEntry = false
		}
		b.writeBlock(r.indexBlock, indexBlockHandle)
	}
	if b.ok() {
		footer := new(footer)
		footer.setMetaIndexHandle(metaIndexBlockHandle)
		footer.setIndexHandle(indexBlockHandle)
		footerEncoding := make([]byte, 0)
		footer.encodeTo(&footerEncoding)
		r.err = r.file.Append(footerEncoding)
		if r.err == nil {
			r.offset += uint64(len(footerEncoding))
		}
	}
	return r.err
}

func (b *tableBuilder) abandon() {
	r := b.rep
	if r.closed {
		panic("tableBuilder: rep closed")
	}
	r.closed = true
}

func (b *tableBuilder) numEntries() int64 {
	return b.rep.numEntries
}

func (b *tableBuilder) fileSize() uint64 {
	return b.rep.offset
}

func (b *tableBuilder) ok() bool {
	return b.status() == nil
}

func (b *tableBuilder) writeBlock(block *blockBuilder, handle *blockHandle) {
	// File format contains a sequence of blocks where each block has:
	//    block_data: uint8[n]
	//    type: uint8
	//    crc: uint32
	if !b.ok() {
		panic("tableBuilder: not ok")
	}
	r := b.rep
	raw := block.finish()
	var blockContents []byte
	t := r.options.CompressionType
	switch t {
	case ssdb.NoCompression:
		blockContents = raw
	case ssdb.SnappyCompression:
		if true {
			//compressed := r.compressedOutput
			if compressed := snappy.Encode(nil, raw); len(compressed) < len(raw)-len(raw)/8 {
				blockContents = compressed
			}
		} else {
			blockContents = raw
			t = ssdb.NoCompression
		}
	}
	b.writeRawBlock(blockContents, t, handle)
	//r.compressedOutput = nil
	block.reset()
}

func (b *tableBuilder) writeRawBlock(blockContents []byte, t ssdb.CompressionType, handle *blockHandle) {
	r := b.rep
	handle.setOffset(r.offset)
	handle.setSize(uint64(len(blockContents)))
	r.err = r.file.Append(blockContents)
	if r.err == nil {
		var trailer [blockTrailerSize]byte
		trailer[0] = byte(t)
		crc := util.ChecksumValue(blockContents)
		crc = util.ChecksumExtend(crc, trailer[:1])
		var bs [4]byte
		util.EncodeFixed32(&bs, util.MaskChecksum(crc))
		copy(trailer[1:], bs[:])
		r.err = r.file.Append(trailer[:])
		if r.err == nil {
			r.offset += uint64(len(blockContents)) + blockTrailerSize
		}
	}
}

type tableBuilderRep struct {
	options           *ssdb.Options
	indexBlockOptions *ssdb.Options
	file              ssdb.WritableFile
	offset            uint64
	err               error
	dataBlock         *blockBuilder
	indexBlock        *blockBuilder
	lastKey           []byte
	numEntries        int64
	closed            bool
	filterBlock       *filterBlockBuilder
	pendingIndexEntry bool
	pendingHandle     *blockHandle
	//compressedOutput  []byte
}

func newTableBuilderRep(options *ssdb.Options, f ssdb.WritableFile) *tableBuilderRep {
	r := &tableBuilderRep{
		options:           options,
		indexBlockOptions: options,
		file:              f,
		offset:            0,
		dataBlock:         newBlockBuilder(options),
		indexBlock:        newBlockBuilder(options),
		numEntries:        0,
		closed:            false,
		pendingIndexEntry: false,
	}
	if options.FilterPolicy == nil {
		r.filterBlock = nil
	} else {
		r.filterBlock = newFilterBlockBuilder(options.FilterPolicy)
	}
	r.indexBlockOptions.BlockRestartInterval = 1
	return r
}
