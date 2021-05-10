package db

import (
	"log"
	"ssdb"
	"ssdb/table"
	"ssdb/util"
	"strings"
)

// We recover the contents of the descriptor from the other files we find.
// (1) Any log files are first converted to tables
// (2) We scan every table to compute
//     (a) smallest/largest for the table
//     (b) largest sequence number in the table
// (3) We generate descriptor contents:
//      - log number is set to zero
//      - next-file-number is set to 1 + largest file number we found
//      - last-sequence-number is set to largest sequence# found across
//        all tables (see 2c)
//      - compaction pointers are cleared
//      - every table file is added at level 0
//
// Possible optimization 1:
//   (a) Compute total size and use to pick appropriate max-level M
//   (b) Sort tables by largest sequence# in the table
//   (c) For each table: if it overlaps earlier table, place in level-0,
//       else place in level-M.
// Possible optimization 2:
//   Store per-table metadata (smallest, largest, largest-seq#, ...)
//   in the table's meta section to speed up ScanTable.

type repairer struct {
	dbName         string
	env            ssdb.Env
	icmp           internalKeyComparator
	iPolicy        internalFilterPolicy
	options        ssdb.Options
	ownsInfoLog    bool
	ownsCache      bool
	tableCache     *tableCache
	edit           versionEdit
	manifests      []string
	tableNumbers   []uint64
	logs           []uint64
	tables         []tableInfo
	nextFileNumber uint64
}

func newRepairer(dbName string, options *ssdb.Options) *repairer {
	r := &repairer{
		dbName:         dbName,
		env:            options.Env,
		icmp:           *newInternalKeyComparator(options.Comparator),
		iPolicy:        *newInternalFilterPolicy(options.FilterPolicy),
		manifests:      nil,
		tableNumbers:   nil,
		tables:         nil,
		nextFileNumber: 1,
	}
	r.options = sanitizeOptions(dbName, &r.icmp, &r.iPolicy, *options)
	r.ownsInfoLog = r.options.InfoLog != options.InfoLog
	r.ownsCache = r.options.BlockCache != options.BlockCache

	return r
}

func (r *repairer) finalize() {
	r.tableCache.finalize()
	if r.ownsInfoLog {
		r.options.InfoLog = nil
	}
	if r.ownsCache {
		r.options.BlockCache.Finalize()
	}
}

func (r *repairer) run() error {
	err := r.findFiles()
	if err == nil {
		r.convertLogFilesToTables()
		r.extractMetaData()
		err = r.writeDescriptor()
	}
	if err == nil {
		bytes := uint64(0)
		for _, t := range r.tables {
			bytes += t.meta.fileSize
		}
		ssdb.Log(r.options.InfoLog, "**** Repaired leveldb %s; "+
			"recovered %d files; %d bytes. "+
			"Some data may have been lost. "+
			"****", r.dbName, len(r.tables), bytes)
	}
	return err
}

type tableInfo struct {
	meta        fileMetaData
	maxSequence sequenceNumber
}

func (r *repairer) findFiles() error {
	fileNames, err := r.env.GetChildren(r.dbName)
	if err != nil {
		return err
	}
	if len(fileNames) == 0 {
		return util.IOError2(r.dbName, "repair found no files")
	}
	var (
		number uint64
		ft     fileType
	)
	for _, fileName := range fileNames {
		if parseFileName(fileName, &number, &ft) {
			if ft == descriptorFile {
				r.manifests = append(r.manifests, fileName)
			} else {
				if number+1 > r.nextFileNumber {
					r.nextFileNumber = number + 1
				}
				if ft == logFile {
					r.logs = append(r.logs, number)
				} else if ft == tableFile {
					r.tableNumbers = append(r.tableNumbers, number)
				} else {
				}
			}
		}
	}
	return err
}

func (r *repairer) convertLogFilesToTables() {
	for _, l := range r.logs {
		logName := logFileName(r.dbName, l)
		if err := r.convertLogToTable(l); err != nil {
			ssdb.Log(r.options.InfoLog, "Log #%d: ignoring conversion error: %v.\n", l, err)
		}
		r.archiveFile(logName)
	}
}

type repairLogReporter struct {
	env     ssdb.Env
	infoLog *log.Logger
	logNum  uint64
}

func (r *repairLogReporter) corruption(bytes int, err error) {
	ssdb.Log(r.infoLog, "Log #%d: dropping %d bytes; %v.\n", r.logNum, bytes, err)
}

func (r *repairer) convertLogToTable(log uint64) error {
	logName := logFileName(r.dbName, log)
	lfile, err := r.env.NewSequentialFile(logName)
	if err != nil {
		return err
	}
	reporter := repairLogReporter{
		env:     r.env,
		infoLog: r.options.InfoLog,
		logNum:  log,
	}
	reader := newLogReader(lfile, &reporter, false, 0)
	var record []byte
	batch := ssdb.NewWriteBatch()
	mem := NewMemTable(&r.icmp)
	mem.ref()
	counter := 0
	ok := true
	for record, ok = reader.readRecord(); ok; record, ok = reader.readRecord() {
		if len(record) < 12 {
			reporter.corruption(len(record), util.CorruptionError1("log record too small"))
			continue
		}
		batch.(writeBatchInternal).SetContents(record)
		if err = insertInto(batch, mem); err == nil {
			counter += batch.(writeBatchInternal).Count()
		} else {
			ssdb.Log(r.options.InfoLog, "Log #%d: ignoring %v.\n", log, err)
			err = nil
		}
	}
	lfile.Finalize()
	meta := newFileMetaData()
	meta.number = r.nextFileNumber
	r.nextFileNumber++
	iter := mem.newIterator()
	err = buildTable(r.dbName, r.env, &r.options, r.tableCache, iter, meta)
	iter.Finalize()
	mem.unref()
	mem.finalize()
	if err == nil {
		if meta.fileSize > 0 {
			r.tableNumbers = append(r.tableNumbers, meta.number)
		}
	}
	ssdb.Log(r.options.InfoLog, "Log #%d: %d ops saved to Table #%d %v.\n", log, counter, meta.number, err)
	return err
}

func (r *repairer) extractMetaData() {
	for _, tableNumber := range r.tableNumbers {
		r.scanTable(tableNumber)
	}
}

func (r *repairer) newTableIterator(meta *fileMetaData) ssdb.Iterator {
	options := ssdb.NewReadOptions()
	options.VerifyChecksums = r.options.ParanoidChecks
	return r.tableCache.newIterator(options, meta.number, meta.fileSize, nil)
}

func (r *repairer) scanTable(number uint64) {
	var t tableInfo
	t.meta.number = number
	fname := tableFileName(r.dbName, number)
	fileSize, err := r.env.GetFileSize(fname)
	t.meta.fileSize = uint64(fileSize)
	if err != nil {
		fname = sstTableFileName(r.dbName, number)
		var err2 error
		if fileSize, err2 = r.env.GetFileSize(fname); err2 == nil {
			t.meta.fileSize = uint64(fileSize)
			err = nil
		}
	}
	if err != nil {
		r.archiveFile(tableFileName(r.dbName, number))
		r.archiveFile(sstTableFileName(r.dbName, number))
		ssdb.Log(r.options.InfoLog, "Table #%d: dropped: %v.\n", t.meta.number, err)
		return
	}
	counter := 0
	iter := r.newTableIterator(&t.meta)
	empty := true
	var parsed parsedInternalKey
	t.maxSequence = 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		key := iter.Key()
		if !parseInternalKey(key, &parsed) {
			ssdb.Log(r.options.InfoLog, "Table #%d: unparsable key %s.\n", t.meta.number, util.EscapeString(key))
			continue
		}
		counter++
		if empty {
			empty = false
			t.meta.smallest.decodeFrom(key)
		}
		t.meta.largest.decodeFrom(key)
		if parsed.sequence > t.maxSequence {
			t.maxSequence = parsed.sequence
		}
	}
	if iter.Status() != nil {
		err = iter.Status()
	}
	iter.Finalize()
	ssdb.Log(r.options.InfoLog, "Table #%d: %d entries %v.\n", t.meta.number, counter, err)
	if err == nil {
		r.tables = append(r.tables, t)
	} else {
		r.repairTable(fname, t)
	}
}

func (r *repairer) repairTable(src string, t tableInfo) {
	copyFileName := tableFileName(r.dbName, r.nextFileNumber)
	r.nextFileNumber++
	file, err := r.env.NewWritableFile(copyFileName)
	if err != nil {
		return
	}
	builder := table.NewBuilder(&r.options, file)
	iter := r.newTableIterator(&t.meta)
	counter := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		builder.Add(iter.Key(), iter.Value())
		counter++
	}
	iter.Finalize()
	r.archiveFile(src)
	if counter == 0 {
		builder.Abandon()
	} else {
		if err = builder.Finish(); err == nil {
			t.meta.fileSize = builder.FileSize()
		}
	}
	builder.Finalize()
	if err == nil {
		err = file.Close()
	}
	file.Finalize()
	file = nil

	if counter > 0 && err == nil {
		orig := tableFileName(r.dbName, t.meta.number)
		if err = r.env.RenameFile(copyFileName, orig); err == nil {
			ssdb.Log(r.options.InfoLog, "Table #%d: %d entries repaired.\n", t.meta.number, counter)
			r.tables = append(r.tables, t)
		}
	}
	if err != nil {
		_ = r.env.DeleteFile(copyFileName)
	}
}

func (r *repairer) writeDescriptor() error {
	tmp := tempFileName(r.dbName, 1)
	file, err := r.env.NewWritableFile(tmp)
	if err != nil {
		return err
	}
	maxSequence := sequenceNumber(0)
	for _, info := range r.tables {
		if maxSequence < info.maxSequence {
			maxSequence = info.maxSequence
		}
	}
	r.edit.setComparatorName(r.icmp.userComparator.Name())
	r.edit.setLogNumber(0)
	r.edit.setNextFile(r.nextFileNumber)
	r.edit.setLastSequence(maxSequence)

	for _, t := range r.tables {
		r.edit.addFile(0, t.meta.number, t.meta.fileSize, t.meta.smallest, t.meta.largest)
	}
	l := newLogWriter(file)
	record := make([]byte, 0)
	r.edit.encodeTo(&record)
	err = l.addRecord(record)
	if err == nil {
		err = file.Close()
	}
	file.Finalize()
	file = nil
	if err != nil {
		_ = r.env.DeleteFile(tmp)
	} else {
		for _, m := range r.manifests {
			r.archiveFile(r.dbName + "/" + m)
		}

		if err = r.env.RenameFile(tmp, descriptorFileName(r.dbName, 1)); err == nil {
			err = setCurrentFile(r.env, r.dbName, 1)
		} else {
			_ = r.env.DeleteFile(tmp)
		}
	}
	return err
}

func (r *repairer) archiveFile(fname string) {
	slash := strings.LastIndexByte(fname, '/')
	newDir := ""
	if slash != -1 {
		newDir = fname[:slash]
	}
	newDir += "/lost"
	_ = r.env.CreateDir(newDir)
	var b strings.Builder
	b.WriteString(newDir)
	b.WriteByte('/')
	if slash == -1 {
		b.WriteString(fname)
	} else {
		b.WriteString(fname[slash+1:])
	}
	newFile := b.String()
	err := r.env.RenameFile(fname, newFile)
	ssdb.Log(r.options.InfoLog, "Archiving %s: %v.\n", fname, err)
}

func Repair(dbName string, options *ssdb.Options) error {
	return newRepairer(dbName, options).run()
}
