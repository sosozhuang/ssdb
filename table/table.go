package table

import (
	"bytes"
	"ssdb"
	"ssdb/util"
)

type handleResult func(interface{}, []byte, []byte) error

type table struct {
	rep *tableRep
}

func newTable(rep *tableRep) *table {
	return &table{rep: rep}
}

func (t *table) NewIterator(options *ssdb.ReadOptions) ssdb.Iterator {
	return newTwoLevelIterator(t.rep.indexBlock.newIterator(t.rep.options.Comparator), blockReader, t, options)
}

func (t *table) ApproximateOffsetOf(key []byte) (result uint64) {
	indexIter := t.rep.indexBlock.newIterator(t.rep.options.Comparator)
	indexIter.Seek(key)
	if indexIter.IsValid() {
		handle := newBlockHandle()
		input := indexIter.GetValue()
		if err := handle.decodeFrom(&input); err == nil {
			result = handle.getOffset()
		} else {
			// Strange: we can't decode the block handle in the index block.
			// We'll just return the offset of the metaindex block, which is
			// close to the whole file size for this case.
			result = t.rep.metaIndexHandle.getOffset()
		}
	} else {
		// key is past the last key in the file.  Approximate the offset
		// by returning the offset of the metaindex block (which is
		// right near the end of the file).
		result = t.rep.metaIndexHandle.getOffset()
	}
	return
}

func blockReader(i interface{}, options *ssdb.ReadOptions, indexValue []byte) (iter ssdb.Iterator) {
	table := i.(*table)
	blockCache := table.rep.options.BlockCache
	var bb *block
	var cacheHandle ssdb.Handle
	handle := newBlockHandle()
	input := indexValue
	var err error
	if err = handle.decodeFrom(&input); err == nil {
		var contents *blockContents
		if blockCache != nil {
			key := make([]byte, 16)
			var b [8]byte
			util.EncodeFixed64(&b, table.rep.cacheID)
			copy(key, b[:])
			util.EncodeFixed64(&b, handle.getOffset())
			copy(key[8:], b[:])
			cacheHandle = blockCache.Lookup(string(key))
			if cacheHandle != nil {
				bb = (blockCache.Value(cacheHandle)).(*block)
			} else {
				if contents, err = readBlock(table.rep.file, options, handle); err == nil {
					bb = newBlock(contents)
					if contents.cachable && options.FillCache {
						cacheHandle = blockCache.Insert(string(key), bb, int(bb.getSize()), deleteCachedBlock)
					}
				}
			}
		} else {
			if contents, err = readBlock(table.rep.file, options, handle); err == nil {
				bb = newBlock(contents)
			}
		}
	}

	if bb != nil {
		iter = bb.newIterator(table.rep.options.Comparator)
		if cacheHandle == nil {
			iter.RegisterCleanUp(deleteBlock, bb, nil)
		} else {
			iter.RegisterCleanUp(releaseBlock, blockCache, cacheHandle)
		}
	} else {
		iter = NewErrorIterator(err)
	}
	return
}

func (t *table) internalGet(options *ssdb.ReadOptions, key []byte, arg interface{}, result handleResult) (err error) {
	iiter := t.rep.indexBlock.newIterator(t.rep.options.Comparator)
	iiter.Seek(key)
	if iiter.IsValid() {
		handleValue := iiter.GetValue()
		filter := t.rep.filter
		handle := newBlockHandle()
		if filter != nil && handle.decodeFrom(&handleValue) == nil && !filter.keyMayMatch(handle.getOffset(), key) {
		} else {
			blockIter := blockReader(t, options, iiter.GetValue())
			blockIter.Seek(key)
			if blockIter.IsValid() {
				_ = result(arg, blockIter.GetKey(), blockIter.GetValue())
				err = blockIter.GetStatus()
				blockIter = nil
			}
		}
	}
	if err == nil {
		err = iiter.GetStatus()
	}
	iiter = nil
	return
}

func (t *table) readMeta(footer *footer) {
	if t.rep.options.FilterPolicy == nil {
		return
	}
	opt := ssdb.NewReadOptions()
	if t.rep.options.ParanoidChecks {
		opt.VerifyChecksums = true
	}
	var contents *blockContents
	var err error
	if contents, err = readBlock(t.rep.file, opt, footer.getMetaIndexHandle()); err != nil {
		return
	}
	meta := newBlock(contents)
	iter := meta.newIterator(ssdb.BytewiseComparator)
	key := []byte("filter." + t.rep.options.FilterPolicy.Name())
	iter.Seek(key)
	if iter.IsValid() && bytes.Compare(iter.GetKey(), key) == 0 {
		t.readFilter(iter.GetValue())
	}
	iter = nil
	meta = nil
}

func (t *table) readFilter(filterHandleValue []byte) {
	v := filterHandleValue
	filterHandle := newBlockHandle()
	var err error
	if err = filterHandle.decodeFrom(&v); err != nil {
		return
	}

	opt := ssdb.NewReadOptions()
	if t.rep.options.ParanoidChecks {
		opt.VerifyChecksums = true
	}
	var block *blockContents
	if block, err = readBlock(t.rep.file, opt, filterHandle); err != nil {
		return
	}
	if block.heapAllocated {
		t.rep.filterData = block.data
	}
	t.rep.filter = newFilterBlockReader(t.rep.options.FilterPolicy, block.data)
}

func deleteBlock(arg, ignored interface{}) {
	arg.(*block).finalize()
}

func deleteCachedBlock(key string, value interface{}) {
	value.(*block).finalize()
}

func releaseBlock(arg, h interface{}) {
	cache := arg.(ssdb.Cache)
	handle := h.(ssdb.Handle)
	cache.Release(handle)
}

func Open(options *ssdb.Options, file ssdb.RandomAccessFile, size uint64) (table *table, err error) {
	if size < footerEncodedLength {
		err = util.CorruptionError1("file is too short to be an sstable")
		return
	}
	footerSpace := make([]byte, footerEncodedLength)
	var footerInput []byte
	if footerInput, _, err = file.Read(footerSpace, int64(size-uint64(footerEncodedLength))); err != nil {
		return
	}
	footer := new(footer)
	if err = footer.decodeFrom(&footerInput); err != nil {
		return
	}

	opt := ssdb.NewReadOptions()
	if options.ParanoidChecks {
		opt.VerifyChecksums = true
	}
	var indexBlockContents *blockContents
	if indexBlockContents, err = readBlock(file, opt, footer.getIndexHandle()); err != nil {
		indexBlock := newBlock(indexBlockContents)
		rep := new(tableRep)
		rep.options = options
		rep.file = file
		rep.metaIndexHandle = footer.getMetaIndexHandle()
		rep.indexBlock = indexBlock
		if options.BlockCache != nil {
			rep.cacheID = options.BlockCache.NewID()
		} else {
			rep.cacheID = 0
		}
		rep.filterData = nil
		rep.filter = nil
		table = newTable(rep)
		table.readMeta(footer)
	}

	return
}

type tableRep struct {
	options         *ssdb.Options
	err             error
	file            ssdb.RandomAccessFile
	cacheID         uint64
	filter          *filterBlockReader
	filterData      []byte
	metaIndexHandle *blockHandle
	indexBlock      *block
}
