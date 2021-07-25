package db

import (
	"ssdb"
	"ssdb/table"
)

func buildTable(dbName string, env ssdb.Env, options *ssdb.Options, tableCache *tableCache, iter ssdb.Iterator, meta *fileMetaData) (err error) {
	meta.fileSize = 0
	iter.SeekToFirst()
	fname := tableFileName(dbName, meta.number)
	if iter.Valid() {
		var file ssdb.WritableFile
		if file, err = env.NewWritableFile(fname); err != nil {
			return
		}
		builder := table.NewBuilder(options, file)
		key := iter.Key()
		meta.smallest.decodeFrom(key)
		for ; iter.Valid(); iter.Next() {
			key = iter.Key()
			meta.largest.decodeFrom(key)
			builder.Add(key, iter.Value())
		}

		if err = builder.Finish(); err == nil {
			meta.fileSize = builder.FileSize()
			if meta.fileSize <= 0 {
				panic("builder: meta.fileSize <= 0")
			}
		}
		builder.Close()

		if err == nil {
			err = file.Sync()
		}
		if err == nil {
			err = file.Close()
		} else {
			_ = file.Close()
		}
		if err == nil {
			it := tableCache.newIterator(ssdb.NewReadOptions(), meta.number, meta.fileSize, nil)
			err = it.Status()
			it.Close()
		}
	}

	if iter.Status() != nil {
		err = iter.Status()
	}
	if err == nil && meta.fileSize > 0 {
	} else {
		_ = env.DeleteFile(fname)
	}
	return
}
