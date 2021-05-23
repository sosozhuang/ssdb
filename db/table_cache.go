package db

import (
	"ssdb"
	"ssdb/table"
	"ssdb/util"
)

type tableCache struct {
	env     ssdb.Env
	dbName  string
	options *ssdb.Options
	cache   ssdb.Cache
}

func newTableCache(dbName string, options *ssdb.Options, entries int) *tableCache {
	return &tableCache{
		env:     options.Env,
		dbName:  dbName,
		options: options,
		cache:   ssdb.NewLRUCache(entries),
	}
}

func (c *tableCache) newIterator(options *ssdb.ReadOptions, fileNumber, fileSize uint64, tablePtr *ssdb.Table) (iter ssdb.Iterator) {
	if tablePtr != nil {
		*tablePtr = nil
	}
	var (
		handle ssdb.Handle
		err    error
	)
	if handle, err = c.findTable(fileNumber, fileSize); err != nil {
		iter = table.NewErrorIterator(err)
		return
	}
	table := c.cache.Value(handle).(tableAndFile).table
	iter = table.NewIterator(options)
	iter.RegisterCleanUp(unrefEntry, c.cache, handle)
	if tablePtr != nil {
		*tablePtr = table
	}
	return
}

func (c *tableCache) get(options *ssdb.ReadOptions, fileNumber, fileSize uint64, k []byte, arg interface{}, result table.HandleResult) (err error) {
	var handle ssdb.Handle
	if handle, err = c.findTable(fileNumber, fileSize); err != nil {
		return
	}
	t := c.cache.Value(handle).(*tableAndFile).table
	err = t.(table.InternalInterface).InternalGet(options, k, arg, result)
	c.cache.Release(handle)
	return
}

func (c *tableCache) evict(fileNumber uint64) {
	var buf [8]byte
	util.EncodeFixed64(&buf, fileNumber)
	c.cache.Erase(buf[:])
}

func (c *tableCache) findTable(fileNumber, fileSize uint64) (handle ssdb.Handle, err error) {
	var buf [8]byte
	util.EncodeFixed64(&buf, fileNumber)
	key := buf[:]
	handle = c.cache.Lookup(key)
	if handle == nil {
		fName := tableFileName(c.dbName, fileNumber)
		var file ssdb.RandomAccessFile
		var t ssdb.Table
		if file, err = c.env.NewRandomAccessFile(fName); err != nil {
			oldFName := sstTableFileName(c.dbName, fileNumber)
			var err1 error
			if file, err1 = c.env.NewRandomAccessFile(oldFName); err1 == nil {
				err = nil
			}
		}
		if err == nil {
			t, err = table.Open(c.options, file, fileSize)
		}
		if err != nil {
			if t != nil {
				panic("tableCache: t != nil")
			}
			file.Finalize()
		} else {
			tf := &tableAndFile{
				file:  file,
				table: t,
			}
			handle = c.cache.Insert(key, tf, 1, deleteEntry)
		}
	}
	return
}

func (c *tableCache) finalize() {
	c.cache.Finalize()
}

type tableAndFile struct {
	file  ssdb.RandomAccessFile
	table ssdb.Table
}

func deleteEntry(key []byte, value interface{}) {
	tf := value.(*tableAndFile)
	tf.table.Finalize()
	tf.file.Finalize()
	tf = nil
}

func unrefEntry(arg1, arg2 interface{}) {
	cache := arg1.(ssdb.Cache)
	h := arg2.(ssdb.Handle)
	cache.Release(h)
}
