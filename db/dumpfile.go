package db

import (
	"bytes"
	"ssdb"
	"ssdb/table"
	"ssdb/util"
	"strings"
)

func guessType(name string, ft *fileType) bool {
	var basename string
	pos := strings.LastIndex(name, "/")
	if pos == -1 {
		basename = name
	} else {
		basename = name[pos+1:]
	}
	var ignored uint64
	return parseFileName(basename, &ignored, ft)
}

type corruptionReporter struct {
	dst ssdb.WritableFile
}

func (r *corruptionReporter) corruption(n int, err error) {
	buf := bytes.NewBufferString("corruption: ")
	buf.WriteString("corruption: ")
	util.AppendNumberTo(buf, uint64(n))
	buf.WriteString(" bytes; ")
	buf.WriteString(err.Error())
	buf.WriteByte('\n')
	_ = r.dst.Append(buf.Bytes())
}

func printLogContents(env ssdb.Env, name string, f func(uint64, []byte, ssdb.WritableFile), dst ssdb.WritableFile) (err error) {
	var file ssdb.SequentialFile
	if file, err = env.NewSequentialFile(name); err != nil {
		return
	}
	reporter := corruptionReporter{dst: dst}
	reader := newLogReader(file, &reporter, true, 0)
	var record []byte
	ok := true
	for ok {
		if record, ok = reader.readRecord(); ok {
			f(reader.lastRecordOffset, record, dst)
		}
	}
	file.Finalize()
	return
}

type writeBatchItemPrinter struct {
	dst ssdb.WritableFile
}

func (p *writeBatchItemPrinter) Put(key, value []byte) {
	buf := bytes.NewBufferString("  put '")
	util.AppendEscapedStringTo(buf, key)
	buf.WriteString("' '")
	util.AppendEscapedStringTo(buf, value)
	buf.WriteString("'\n")
	_ = p.dst.Append(buf.Bytes())

}

func (p *writeBatchItemPrinter) Delete(key []byte) {
	buf := bytes.NewBufferString("  del '")
	util.AppendEscapedStringTo(buf, key)
	buf.WriteString("'\n")
	_ = p.dst.Append(buf.Bytes())
}

func writeBatchPrinter(pos uint64, record []byte, dst ssdb.WritableFile) {
	buf := bytes.NewBufferString("--- offset ")
	util.AppendNumberTo(buf, pos)
	buf.WriteString("; ")
	if len(record) < 12 {
		buf.WriteString("log record length ")
		util.AppendNumberTo(buf, uint64(len(record)))
		buf.WriteString(" is too small\n")
		_ = dst.Append(buf.Bytes())
		return
	}
	batch := ssdb.NewWriteBatch()
	batch.(writeBatchInternal).SetContents(record)
	buf.WriteString("sequence ")
	util.AppendNumberTo(buf, batch.(writeBatchInternal).Sequence())
	buf.WriteByte('\n')
	_ = dst.Append(buf.Bytes())
	batchItemPrinter := writeBatchItemPrinter{dst: dst}
	if err := batch.Iterate(&batchItemPrinter); err != nil {
		_ = dst.Append([]byte("  error: " + err.Error() + "\n"))
	}
}

func dumpLog(env ssdb.Env, name string, dst ssdb.WritableFile) error {
	return printLogContents(env, name, writeBatchPrinter, dst)
}

func versionEditPrinter(pos uint64, record []byte, dst ssdb.WritableFile) {
	buf := bytes.NewBufferString("--- offset ")
	util.AppendNumberTo(buf, pos)
	buf.WriteString("; ")
	edit := newVersionEdit()
	if err := edit.decodeFrom(record); err != nil {
		buf.WriteString(err.Error())
		buf.WriteByte('\n')
	} else {
		buf.WriteString(edit.debugString())
	}
	_ = dst.Append(buf.Bytes())
}

func dumpDescriptor(env ssdb.Env, name string, dst ssdb.WritableFile) error {
	return printLogContents(env, name, versionEditPrinter, dst)
}

func dumpTable(env ssdb.Env, fname string, dst ssdb.WritableFile) error {
	var file ssdb.RandomAccessFile
	fileSize, err := env.GetFileSize(fname)
	if err == nil {
		file, err = env.NewRandomAccessFile(fname)
	}
	var t ssdb.Table
	if err == nil {
		t, err = table.Open(ssdb.NewOptions(), file, uint64(fileSize))
	}
	if err != nil {
		if t != nil {
			t.Finalize()
		}
		if file != nil {
			file.Finalize()
		}
		return err
	}
	ro := ssdb.NewReadOptions()
	ro.FillCache = false
	iter := t.NewIterator(ro)
	buf := bytes.NewBufferString("")
	var key parsedInternalKey
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		buf.Reset()
		if !parseInternalKey(iter.Key(), &key) {
			buf.WriteString("badkey '")
			util.AppendEscapedStringTo(buf, iter.Key())
			buf.WriteString("' => '")
			util.AppendEscapedStringTo(buf, iter.Value())
			buf.WriteString("'\n")
			_ = dst.Append(buf.Bytes())
		} else {
			buf.WriteByte('\'')
			util.AppendEscapedStringTo(buf, key.userKey)
			buf.WriteString("' @ ")
			util.AppendNumberTo(buf, uint64(key.sequence))
			buf.WriteString(" : ")
			if key.valueType == ssdb.TypeDeletion {
				buf.WriteString("del")
			} else if key.valueType == ssdb.TypeValue {
				buf.WriteString("val")
			} else {
				util.AppendNumberTo(buf, uint64(key.valueType))
			}
			buf.WriteString(" => '")
			util.AppendEscapedStringTo(buf, iter.Value())
			buf.WriteString("'\n")
			_ = dst.Append(buf.Bytes())
		}
	}
	if err = iter.Status(); err != nil {
		_ = dst.Append([]byte("iterator error: " + err.Error() + "\n"))
	}
	iter.Finalize()
	t.Finalize()
	file.Finalize()
	return nil
}

func DumpFile(env ssdb.Env, name string, dst ssdb.WritableFile) error {
	var ft fileType
	if !guessType(name, &ft) {
		return util.InvalidArgumentError1(name + ": unknown file type")
	}
	switch ft {
	case logFile:
		return dumpLog(env, name, dst)
	case descriptorFile:
		return dumpDescriptor(env, name, dst)
	case tableFile:
		return dumpTable(env, name, dst)
	}
	return util.InvalidArgumentError1(name + ": not a dump-able file type")
}
