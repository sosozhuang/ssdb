package db

import (
	"fmt"
	"log"
	"sort"
	"ssdb"
	"ssdb/table"
	"ssdb/util"
	"strings"
	"sync"
)

const numNonTableCacheFiles = 10

type manualCompaction struct {
	level      int
	done       bool
	begin      *internalKey
	end        *internalKey
	tmpStorage internalKey
}

type compactionStats struct {
	micros       int64
	bytesRead    int64
	bytesWritten int64
}

func (s *compactionStats) add(other *compactionStats) {
	s.micros += other.micros
	s.bytesRead += other.bytesRead
	s.bytesWritten += other.bytesWritten
}

type writer struct {
	err   error
	batch ssdb.WriteBatch
	sync  bool
	done  bool
	cond  *sync.Cond
}

func newWriter(mu *sync.Mutex) *writer {
	return &writer{
		sync: false,
		done: false,
		cond: sync.NewCond(mu),
	}
}

type compactionStateOutput struct {
	number   uint64
	fileSize uint64
	smallest internalKey
	largest  internalKey
}

type compactionState struct {
	compaction       *compaction
	smallestSnapshot sequenceNumber
	outputs          []compactionStateOutput
	outfile          ssdb.WritableFile
	builder          ssdb.TableBuilder
	totalBytes       uint64
}

func newCompactionState(c *compaction) *compactionState {
	return &compactionState{
		compaction:       c,
		smallestSnapshot: 0,
		outfile:          nil,
		builder:          nil,
		totalBytes:       0,
	}
}

func (s *compactionState) currentOutput() *compactionStateOutput {
	return &s.outputs[len(s.outputs)-1]
}

func clipToRangeInt(ptr *int, minValue, maxValue int) {
	if *ptr > maxValue {
		*ptr = maxValue
	}
	if *ptr < minValue {
		*ptr = minValue
	}
}

func clipToRangeUint(ptr *uint, minValue, maxValue uint) {
	if *ptr > maxValue {
		*ptr = maxValue
	}
	if *ptr < minValue {
		*ptr = minValue
	}
}

func sanitizeOptions(dbName string, iCmp *internalKeyComparator, iPolicy *internalFilterPolicy, src ssdb.Options) ssdb.Options {
	result := src
	result.Comparator = iCmp
	if src.FilterPolicy != nil {
		result.FilterPolicy = iPolicy
	} else {
		result.FilterPolicy = nil
	}
	clipToRangeInt(&result.MaxOpenFiles, 64+numNonTableCacheFiles, 50000)
	clipToRangeInt(&result.WriteBufferSize, 64<<10, 1<<30)
	clipToRangeInt(&result.MaxFileSize, 1<<20, 1<<30)
	clipToRangeInt(&result.BlockSize, 1<<10, 4<<20)
	if result.InfoLog == nil {
		_ = src.Env.CreateDir(dbName)
		_ = src.Env.RenameFile(infoLogFileName(dbName), oldInfoLogFileName(dbName))
		var err error
		if result.InfoLog, err = src.Env.NewLogger(infoLogFileName(dbName)); err != nil {
			result.InfoLog = nil
		}
	}
	if result.BlockCache == nil {
		result.BlockCache = ssdb.NewLRUCache(8 << 20)
	}
	return result
}

func tableCacheSize(sanitizedOptions *ssdb.Options) int {
	return sanitizedOptions.MaxOpenFiles - numNonTableCacheFiles
}

type db struct {
	env                           ssdb.Env
	internalComparator            internalKeyComparator
	internalFilterPolicy          internalFilterPolicy
	options                       ssdb.Options
	ownsInfoLog                   bool
	ownsCache                     bool
	dbName                        string
	tableCache                    *tableCache
	dbLock                        ssdb.FileLock
	mutex                         sync.Mutex
	shuttingDown                  util.AtomicBool
	backgroundWorkFinishedSignal  *sync.Cond
	mem                           *MemTable
	imm                           *MemTable
	hasImm                        util.AtomicBool
	logFile                       ssdb.WritableFile
	logFileNumber                 uint64
	log                           *logWriter
	seed                          uint32
	writers                       []*writer
	tmpBatch                      ssdb.WriteBatch
	snapshots                     snapshotList
	pendingOutputs                map[uint64]struct{}
	backgroundCompactionScheduled bool
	manualCompaction              *manualCompaction
	versions                      *versionSet
	bgError                       error
	stats                         [numLevels]compactionStats
}

func newDB(rawOptions *ssdb.Options, dbName string) *db {
	result := &db{
		env:                           rawOptions.Env,
		internalComparator:            *newInternalKeyComparator(rawOptions.Comparator),
		internalFilterPolicy:          *newInternalFilterPolicy(rawOptions.FilterPolicy),
		dbName:                        dbName,
		dbLock:                        nil,
		shuttingDown:                  0,
		mem:                           nil,
		imm:                           nil,
		hasImm:                        0,
		logFile:                       nil,
		logFileNumber:                 0,
		log:                           nil,
		seed:                          0,
		writers:                       make([]*writer, 0),
		tmpBatch:                      ssdb.NewWriteBatch(),
		snapshots:                     *newSnapshotList(),
		pendingOutputs:                make(map[uint64]struct{}),
		backgroundCompactionScheduled: false,
		manualCompaction:              nil,
	}
	result.options = sanitizeOptions(dbName, &result.internalComparator, &result.internalFilterPolicy, *rawOptions)
	result.ownsInfoLog = result.options.InfoLog != rawOptions.InfoLog
	result.ownsCache = result.options.BlockCache != rawOptions.BlockCache
	result.tableCache = newTableCache(result.dbName, &result.options, tableCacheSize(&result.options))
	result.backgroundWorkFinishedSignal = sync.NewCond(&result.mutex)
	result.versions = newVersionSet(result.dbName, &result.options, result.tableCache, &result.internalComparator)

	return result
}

func (d *db) newDB() error {
	edit := newVersionEdit()
	edit.setComparatorName(d.userComparator().Name())
	edit.setLogNumber(0)
	edit.setNextFile(2)
	edit.setLastSequence(0)

	manifest := descriptorFileName(d.dbName, 1)
	file, err := d.env.NewWritableFile(manifest)
	if err != nil {
		return err
	}
	log := newLogWriter(file)
	record := make([]byte, 0)
	edit.encodeTo(&record)
	if err = log.addRecord(record); err == nil {
		err = file.Close()
	}
	file.Finalize()
	if err == nil {
		err = setCurrentFile(d.env, d.dbName, 1)
	} else {
		_ = d.env.DeleteFile(manifest)
	}
	return err
}

func (d *db) maybeIgnoreError(err *error) {
	if err == nil || d.options.ParanoidChecks {
	} else {
		ssdb.Log(d.options.InfoLog, "Ignoring error %v.\n", err)
		*err = nil
	}
}

func (d *db) deleteObsoleteFiles() {
	if d.bgError != nil {
		return
	}
	live := d.pendingOutputs
	d.versions.addLiveFiles(live)

	fileNames, _ := d.env.GetChildren(d.dbName)
	var (
		number uint64
		ft     fileType
	)
	for _, fileName := range fileNames {
		if parseFileName(fileName, &number, &ft) {
			keep := true
			switch ft {
			case logFile:
				keep = number >= d.versions.logNumber || number == d.versions.prevLogNumber
			case descriptorFile:
				keep = number >= d.versions.manifestFileNumber
			case tableFile, tempFile:
				_, keep = live[number]
			case currentFile, dbLockFile, infoLogFile:
				keep = true
			}
			if !keep {
				if ft == tableFile {
					d.tableCache.evict(number)
				}
				ssdb.Log(d.options.InfoLog, "Delete type=%d #%d\n", ft, number)
				_ = d.env.DeleteFile(d.dbName + "/" + fileName)
			}
		}
	}
}

func (d *db) finalize() {
	d.mutex.Lock()
	d.shuttingDown.SetTrue()
	for d.backgroundCompactionScheduled {
		d.backgroundWorkFinishedSignal.Wait()
	}
	d.mutex.Unlock()
	if d.dbLock != nil {
		_ = d.env.UnlockFile(d.dbLock)
	}
	d.versions.finalize()
	if d.mem != nil {
		d.mem.unref()
	}
	if d.imm != nil {
		d.imm.unref()
	}
	d.tmpBatch = nil
	d.log = nil
	d.logFile.Finalize()
	d.tableCache.finalize()
	if d.ownsInfoLog {
		d.options.InfoLog = nil
	}
	if d.ownsCache {
		d.options.BlockCache.Finalize()
	}
}

type Uint64Slice []uint64

func (p Uint64Slice) Len() int           { return len(p) }
func (p Uint64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p Uint64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func (d *db) recover(edit *versionEdit) (saveManifest bool, err error) {
	_ = d.env.CreateDir(d.dbName)
	if d.dbLock != nil {
		panic("db: dbLock != nil")
	}
	if d.dbLock, err = d.env.LockFile(lockFileName(d.dbName)); err != nil {
		return
	}

	if !d.env.FileExists(currentFileName(d.dbName)) {
		if d.options.CreateIfMissing {
			if err = d.newDB(); err != nil {
				return
			}
		} else {
			err = util.InvalidArgumentError2(d.dbName, "does not exist (create_if_missing is false)")
			return
		}
	} else {
		if d.options.ErrorIfExists {
			err = util.InvalidArgumentError2(d.dbName, "exists (error_if_exists is true)")
			return
		}
	}

	if saveManifest, err = d.versions.recover(); err != nil {
		return
	}
	maxSequence := sequenceNumber(0)
	minLog, prevLog := d.versions.logNumber, d.versions.prevLogNumber
	var fileNames []string
	if fileNames, err = d.env.GetChildren(d.dbName); err != nil {
		return
	}
	expected := make(map[uint64]struct{})
	d.versions.addLiveFiles(expected)
	var (
		number uint64
		ft     fileType
	)
	logs := make([]uint64, 0)
	for _, fileName := range fileNames {
		if parseFileName(fileName, &number, &ft) {
			delete(expected, number)
			if ft == logFile && ((number >= minLog) || (number == prevLog)) {
				logs = append(logs, number)
			}
		}
	}
	if len(expected) != 0 {
		var b strings.Builder
		fmt.Fprintf(&b, "%d missing files; e.g.", len(expected))
		for k := range expected {
			err = util.CorruptionError2(b.String(), tableFileName(d.dbName, k))
			break
		}
		return
	}
	sort.Sort(Uint64Slice(logs))
	for i, log := range logs {
		if err = d.recoverLogFile(log, i == len(logs)-1, &saveManifest, edit, &maxSequence); err != nil {
			return
		}
		d.versions.markFileNumberUsed(log)
	}

	if d.versions.lastSequence < maxSequence {
		d.versions.setLastSequence(maxSequence)
	}
	return
}

type dbLogReporter struct {
	env     ssdb.Env
	infoLog *log.Logger
	fname   string
	err     error
}

func (r *dbLogReporter) corruption(bytes int, err error) {
	if r.err == nil {
		ssdb.Log(r.infoLog, "(ignoring error)%s: dropping %d bytes; %v.\n", r.fname, bytes, err)
	} else {
		ssdb.Log(r.infoLog, "%s: dropping %d bytes; %v.\n", r.fname, bytes, err)
	}
	if r.err == nil {
		r.err = err
	}
}

func (d *db) recoverLogFile(logNumber uint64, lastLog bool, saveManifest *bool, edit *versionEdit, maxSequence *sequenceNumber) error {
	fname := logFileName(d.dbName, logNumber)
	file, err := d.env.NewSequentialFile(fname)
	if err != nil {
		d.maybeIgnoreError(&err)
		return err
	}
	reporter := dbLogReporter{
		env:     d.env,
		infoLog: d.options.InfoLog,
		fname:   fname,
	}
	if d.options.ParanoidChecks {
		reporter.err = err
	} else {
		reporter.err = nil
	}
	reader := newLogReader(file, &reporter, true, 0)
	ssdb.Log(d.options.InfoLog, "Recovering log #%d.\n", logNumber)

	var (
		record []byte
		mem    *MemTable
	)
	batch := ssdb.NewWriteBatch()
	compactions := 0
	ok := true
	for ok && err == nil {
		record, ok = reader.readRecord()
		if len(record) < 12 {
			reporter.corruption(len(record), util.CorruptionError1("log record too small"))
			continue
		}
		wbi := batch.(writeBatchInternal)
		wbi.SetContents(record)

		if mem == nil {
			mem = NewMemTable(&d.internalComparator)
			mem.ref()
		}
		err = insertInto(batch, mem)
		d.maybeIgnoreError(&err)
		if err != nil {
			break
		}
		lastSeq := sequenceNumber(wbi.Sequence() + uint64(wbi.Count()) - 1)
		if lastSeq > *maxSequence {
			*maxSequence = lastSeq
		}
		if mem.approximateMemoryUsage() > uint64(d.options.WriteBufferSize) {
			compactions++
			*saveManifest = true
			err = d.writeLevel0Table(mem, edit, nil)
			mem.unref()
			mem = nil
			if err != nil {
				break
			}
		}
	}
	file = nil
	if err == nil && d.options.ReuseLogs && lastLog && compactions == 0 {
		if d.logFile != nil {
			panic("db: logFile != nil")
		}
		if d.log != nil {
			panic("db: log != nil")
		}
		if d.mem != nil {
			panic("db: mem != nil")
		}
		var (
			lFileSize int
			e         error
		)
		if lFileSize, e = d.env.GetFileSize(fname); e == nil {
			if d.logFile, e = d.env.NewAppendableFile(fname); e == nil {
				ssdb.Log(d.options.InfoLog, "Reusing old log %s.\n", fname)
				d.log = newLogWriterWithLength(d.logFile, uint64(lFileSize))
				d.logFileNumber = logNumber
				if mem != nil {
					d.mem = mem
					mem = nil
				} else {
					d.mem = NewMemTable(&d.internalComparator)
					d.mem.ref()
				}
			}
		}
	}

	if mem != nil {
		if err == nil {
			*saveManifest = true
			err = d.writeLevel0Table(mem, edit, nil)
		}
		mem.unref()
	}
	return err
}

func (d *db) writeLevel0Table(mem *MemTable, edit *versionEdit, base *version) (err error) {
	startMicros := d.env.NowMicros()
	meta := newFileMetaData()
	meta.number = d.versions.newFileNumber()
	d.pendingOutputs[meta.number] = struct{}{}
	iter := mem.newIterator()
	ssdb.Log(d.options.InfoLog, "Level-0 table #%d: started.\n", meta.number)

	d.mutex.Unlock()
	err = buildTable(d.dbName, d.env, &d.options, d.tableCache, iter, meta)
	d.mutex.Lock()

	ssdb.Log(d.options.InfoLog, "Level-0 table #%d: %d bytes %v.\n", meta.number, meta.fileSize, err)
	iter.Finalize()
	delete(d.pendingOutputs, meta.number)

	level := 0
	if err == nil && meta.fileSize > 0 {
		minUserKey, maxUserKey := meta.smallest.userKey(), meta.largest.userKey()
		if base != nil {
			level = base.pickLevelForMemTableOutput(minUserKey, maxUserKey)
		}
		edit.addFile(level, meta.number, meta.fileSize, meta.smallest, meta.largest)
	}
	stats := &compactionStats{
		micros:       int64(d.env.NowMicros() - startMicros),
		bytesWritten: int64(meta.fileSize),
	}
	d.stats[level].add(stats)
	return
}

func (d *db) compactMemTable() {
	if d.imm == nil {
		panic("db: imm == nil")
	}
	edit := newVersionEdit()
	base := d.versions.current
	base.ref()
	err := d.writeLevel0Table(d.imm, edit, base)
	base.unref()

	if err == nil && d.shuttingDown.IsTrue() {
		err = util.IOError1("Deleting DB during memtable compaction")
	}

	if err == nil {
		edit.setPrevLogNumber(0)
		edit.setLogNumber(d.logFileNumber)
		err = d.versions.logAndApply(edit, &d.mutex)
	}

	if err == nil {
		d.imm.unref()
		d.imm = nil
		d.hasImm.SetFalse()
		d.deleteObsoleteFiles()
	} else {
		d.recordBackgroundError(err)
	}
}

func (d *db) CompactRange(begin, end []byte) {
	maxLevelWithFiles := 1
	d.mutex.Lock()
	base := d.versions.current
	for level := 1; level < numLevels; level++ {
		if base.overlapInLevel(level, begin, end) {
			maxLevelWithFiles = level
		}
	}
	d.mutex.Unlock()

	_ = d.testCompactMemTable()
	for level := 0; level < maxLevelWithFiles; level++ {
		d.testCompactRange(level, begin, end)
	}
}

func (d *db) testCompactRange(level int, begin, end []byte) {
	if level < 0 {
		panic("db: level < 0")
	}
	if level+1 >= numLevels {
		panic("db: level + 1 >= numLevels")
	}

	var beginStorage, endStorage *internalKey
	manual := manualCompaction{level: level, done: false}
	if begin == nil {
		manual.begin = nil
	} else {
		beginStorage = newInternalKey(begin, maxSequenceNumber, valueTypeForSeek)
		manual.begin = beginStorage
	}
	if end == nil {
		manual.end = nil
	} else {
		endStorage = newInternalKey(end, 0, 0)
		manual.end = endStorage
	}

	d.mutex.Lock()
	defer d.mutex.Unlock()
	for !manual.done && d.shuttingDown.IsFalse() && d.bgError == nil {
		if d.manualCompaction == nil {
			d.manualCompaction = &manual
			d.maybeScheduleCompaction()
		} else {
			d.backgroundWorkFinishedSignal.Wait()
		}
	}
	if d.manualCompaction == &manual {
		d.manualCompaction = nil
	}
}

func (d *db) testCompactMemTable() (err error) {
	if err = d.Write(ssdb.NewWriteOptions(), nil); err == nil {
		d.mutex.Lock()
		defer d.mutex.Unlock()
		for d.imm != nil && d.bgError == nil {
			d.backgroundWorkFinishedSignal.Wait()
		}
		if d.imm != nil {
			err = d.bgError
		}
	}
	return
}

func (d *db) recordBackgroundError(err error) {
	if d.bgError == nil {
		d.bgError = err
		d.backgroundWorkFinishedSignal.Broadcast()
	}
}

func (d *db) maybeScheduleCompaction() {
	if d.backgroundCompactionScheduled {
	} else if d.shuttingDown.IsTrue() {
	} else if d.bgError != nil {
	} else if d.imm == nil && d.manualCompaction == nil && !d.versions.needsCompaction() {
	} else {
		d.backgroundCompactionScheduled = true
		d.env.Schedule(bgWork, d)
	}
}

func bgWork(arg interface{}) {
	arg.(*db).backgroundCall()
}

func (d *db) backgroundCall() {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if d.shuttingDown.IsTrue() {
	} else if d.bgError != nil {
	} else {
		d.backgroundCompaction()
	}
	d.backgroundCompactionScheduled = false
	d.maybeScheduleCompaction()
	d.backgroundWorkFinishedSignal.Broadcast()
}

func (d *db) backgroundCompaction() {
	if d.imm != nil {
		d.compactMemTable()
		return
	}

	var c *compaction
	isManual := d.manualCompaction != nil
	var manualEnd internalKey
	if isManual {
		m := d.manualCompaction
		c = d.versions.compactRange(m.level, m.begin, m.end)
		m.done = c == nil
		if c != nil {
			manualEnd = c.input(0, c.numInputFiles(0)-1).largest
		}
		beginStr, endStr, doneStr := "(begin)", "(end)", "(end)"
		if m.begin != nil {
			beginStr = m.begin.debugString()
		}
		if m.end != nil {
			endStr = m.end.debugString()
		}
		if !m.done {
			doneStr = manualEnd.debugString()
		}
		ssdb.Log(d.options.InfoLog, "Manual compaction at level-%d from %s .. %s; will stop at %s.\n", m.level, beginStr, endStr, doneStr)
	} else {
		c = d.versions.pickCompaction()
	}

	var err error
	if c == nil {
	} else if !isManual && c.isTrivialMove() {
		if c.numInputFiles(0) != 1 {
			panic("db: c.numInputFiles(0) != 1")
		}
		f := c.input(0, 0)
		c.edit.deletedFile(c.level, f.number)
		c.edit.addFile(c.level+1, f.number, f.fileSize, f.smallest, f.largest)
		if err = d.versions.logAndApply(&c.edit, &d.mutex); err != nil {
			d.recordBackgroundError(err)
		}
		ssdb.Log(d.options.InfoLog, "Moved #%d to level-%d %d bytes %s: %s.\n", f.number, c.level+1, f.fileSize, err, d.versions.levelSummary())
	} else {
		compact := newCompactionState(c)
		if err = d.doCompactionWork(compact); err != nil {
			d.recordBackgroundError(err)
		}
		d.cleanupCompaction(compact)
		c.releaseInputs()
		d.deleteObsoleteFiles()
	}
	c = nil
	if err == nil {
	} else if d.shuttingDown.IsTrue() {
	} else {
		ssdb.Log(d.options.InfoLog, "Compaction error: %v.\n", err)
	}

	if isManual {
		m := d.manualCompaction
		if err != nil {
			m.done = true
		}
		if !m.done {
			m.tmpStorage = manualEnd
			m.begin = &m.tmpStorage
		}
		d.manualCompaction = nil
	}
}

func (d *db) cleanupCompaction(compact *compactionState) {
	if compact.builder != nil {
		compact.builder.Abandon()
		compact.builder = nil
	} else {
		if compact.outfile != nil {
			panic("db: compact.outfile != nil")
		}
	}
	compact.outfile = nil
	for _, output := range compact.outputs {
		delete(d.pendingOutputs, output.number)
	}
	compact = nil
}

func (d *db) openCompactionOutputFile(compact *compactionState) (err error) {
	if compact == nil {
		panic("db: compact == nil")
	}
	if compact.builder != nil {
		panic("db: compact.builder != nil")
	}
	d.mutex.Lock()
	fileNumber := d.versions.newFileNumber()
	d.pendingOutputs[fileNumber] = struct{}{}
	out := compactionStateOutput{
		number: fileNumber,
		//smallest: internalKey{},
		//largest:  internalKey{},
	}
	out.smallest.clear()
	out.largest.clear()
	compact.outputs = append(compact.outputs, out)
	d.mutex.Unlock()

	fname := tableFileName(d.dbName, fileNumber)
	if compact.outfile, err = d.env.NewWritableFile(fname); err == nil {
		compact.builder = table.NewBuilder(&d.options, compact.outfile)
	}
	return err
}

func (d *db) finishCompactionOutputFile(compact *compactionState, input ssdb.Iterator) (err error) {
	if compact == nil {
		panic("db: compact == nil")
	}
	if compact.outfile == nil {
		panic("db: compact.outfile == nil")
	}
	if compact.builder == nil {
		panic("db: compact.builder == nil")
	}
	outputNumber := compact.currentOutput().number
	if outputNumber == 0 {
		panic("db: outputNumber == 0")
	}
	err = input.Status()
	currentEntries := compact.builder.NumEntries()
	if err == nil {
		err = compact.builder.Finish()
	} else {
		compact.builder.Abandon()
	}
	currentBytes := compact.builder.FileSize()
	compact.currentOutput().fileSize = currentBytes
	compact.totalBytes += currentBytes
	compact.builder.(ssdb.Finalizer).Finalize()
	compact.builder = nil

	if err == nil {
		err = compact.outfile.Sync()
	}
	if err == nil {
		err = compact.outfile.Close()
	}
	compact.outfile.Finalize()
	compact.outfile = nil
	if err == nil && currentEntries > 0 {
		iter := d.tableCache.newIterator(ssdb.NewReadOptions(), outputNumber, currentBytes, nil)
		err = iter.Status()
		iter.Finalize()
		if err == nil {
			ssdb.Log(d.options.InfoLog, "Generated table #%d@%d: %d keys, %d bytes", outputNumber, compact.compaction.level, currentEntries, currentBytes)
		}
	}
	return
}

func (d *db) installCompactionResults(compact *compactionState) error {
	ssdb.Log(d.options.InfoLog, "Compacted %d@%d + %d@%d files => %d bytes", compact.compaction.numInputFiles(0), compact.compaction.level,
		compact.compaction.numInputFiles(1), compact.compaction.level+1, compact.totalBytes)
	compact.compaction.addInputDeletions(&compact.compaction.edit)
	level := compact.compaction.level
	for _, output := range compact.outputs {
		compact.compaction.edit.addFile(level+1, output.number, output.fileSize, output.smallest, output.largest)
	}
	return d.versions.logAndApply(&compact.compaction.edit, &d.mutex)
}

func (d *db) userComparator() ssdb.Comparator {
	return d.internalComparator.userComparator
}

func (d *db) doCompactionWork(compact *compactionState) error {
	startMicros := d.env.NowMicros()
	immMicros := uint64(0)

	ssdb.Log(d.options.InfoLog, "Compacting %d@%d + %d@%d files", compact.compaction.numInputFiles(0), compact.compaction.level,
		compact.compaction.numInputFiles(1), compact.compaction.level+1)
	if d.versions.numLevelFiles(compact.compaction.level) <= 0 {
		panic("db: versions.numLevelFiles(compact.compaction.level) <= 0")
	}
	if compact.builder != nil {
		panic("db: compact.builder != nil")
	}
	if compact.outfile != nil {
		panic("db: compact.outfile != nil")
	}
	if d.snapshots.empty() {
		compact.smallestSnapshot = d.versions.lastSequence
	} else {
		compact.smallestSnapshot = d.snapshots.oldest().sequenceNumber
	}

	d.mutex.Unlock()
	input := d.versions.makeInputIterator(compact.compaction)
	input.SeekToFirst()
	var (
		err            error
		ikey           parsedInternalKey
		currentUserKey []byte
	)
	hasCurrentUserKey := false
	lastSequenceForKey := maxSequenceNumber
	for input.Valid() && d.shuttingDown.IsFalse() {
		if d.hasImm.IsTrue() {
			immStart := d.env.NowMicros()
			d.mutex.Lock()
			if d.imm != nil {
				d.compactMemTable()
				d.backgroundWorkFinishedSignal.Broadcast()
			}
			d.mutex.Unlock()
			immMicros += d.env.NowMicros() - immStart
		}

		key := input.Key()
		if compact.compaction.shouldStopBefore(key) && compact.builder != nil {
			if err = d.finishCompactionOutputFile(compact, input); err != nil {
				break
			}
		}

		drop := false
		if !parseInternalKey(key, &ikey) {
			currentUserKey = nil
			hasCurrentUserKey = false
			lastSequenceForKey = maxSequenceNumber
		} else {
			if !hasCurrentUserKey || d.userComparator().Compare(ikey.userKey, currentUserKey) != 0 {
				currentUserKey = make([]byte, len(ikey.userKey))
				copy(currentUserKey, ikey.userKey)
				hasCurrentUserKey = true
				lastSequenceForKey = maxSequenceNumber
			}
			if lastSequenceForKey <= compact.smallestSnapshot {
				drop = true
			} else if ikey.valueType == ssdb.TypeDeletion && ikey.sequence <= compact.smallestSnapshot && compact.compaction.isBaseLevelForKey(ikey.userKey) {
				drop = true
			}
			lastSequenceForKey = ikey.sequence
		}

		if !drop {
			if compact.builder == nil {
				if err = d.openCompactionOutputFile(compact); err != nil {
					break
				}
			}
			if compact.builder.NumEntries() == 0 {
				compact.currentOutput().smallest.decodeFrom(key)
			}
			compact.currentOutput().largest.decodeFrom(key)
			compact.builder.Add(key, input.Value())

			if compact.builder.FileSize() >= compact.compaction.maxOutputFileSize {
				if err = d.finishCompactionOutputFile(compact, input); err != nil {
					break
				}
			}
		}
		input.Next()
	}

	if err == nil && d.shuttingDown.IsTrue() {
		err = util.IOError1("Deleting DB during compaction")
	}
	if err == nil && compact.builder != nil {
		err = d.finishCompactionOutputFile(compact, input)
	}
	if err == nil {
		err = input.Status()
	}
	input.Finalize()
	input = nil

	var stats compactionStats
	stats.micros = int64(d.env.NowMicros() - startMicros - immMicros)
	for which := 0; which < 2; which++ {
		for i := 0; i < compact.compaction.numInputFiles(which); i++ {
			stats.bytesRead += int64(compact.compaction.input(which, i).fileSize)
		}
	}
	for _, output := range compact.outputs {
		stats.bytesWritten += int64(output.fileSize)
	}
	d.mutex.Lock()
	d.stats[compact.compaction.level+1].add(&stats)
	if err == nil {
		err = d.installCompactionResults(compact)
	}

	if err != nil {
		d.recordBackgroundError(err)
	}

	ssdb.Log(d.options.InfoLog, "compacted to: %s.\n", d.versions.levelSummary())
	return err
}

type iterState struct {
	mu      *sync.Mutex
	version *version
	mem     *MemTable
	imm     *MemTable
}

func cleanupIteratorState(arg1, arg2 interface{}) {
	state := arg1.(*iterState)
	state.mu.Lock()
	defer state.mu.Unlock()
	state.mem.unref()
	if state.imm != nil {
		state.imm.unref()
	}
	state.version.unref()
	state = nil
}

func (d *db) newInternalIterator(options *ssdb.ReadOptions) (internalIter ssdb.Iterator, latestSnapshot sequenceNumber, seed uint32) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	latestSnapshot = d.versions.lastSequence
	list := make([]ssdb.Iterator, 1, 2)
	list[0] = d.mem.newIterator()
	d.mem.ref()
	if d.imm != nil {
		list = append(list, d.imm.newIterator())
		d.imm.ref()
	}
	d.versions.current.addIterators(options, &list)
	internalIter = table.NewMergingIterator(&d.internalComparator, list)
	d.versions.current.ref()

	cleanup := &iterState{
		mu:      &d.mutex,
		version: d.versions.current,
		mem:     d.mem,
		imm:     d.imm,
	}
	internalIter.RegisterCleanUp(cleanupIteratorState, cleanup, nil)
	d.seed++
	seed = d.seed
	return
}

func (d *db) testNewInternalIterator() (iterator ssdb.Iterator) {
	iterator, _, _ = d.newInternalIterator(ssdb.NewReadOptions())
	return
}

func (d *db) testMaxNextLevelOverlappingBytes() int64 {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	return d.versions.maxNextLevelOverlappingBytes()
}

func (d *db) Get(options *ssdb.ReadOptions, key []byte) (value []byte, err error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	var snapshotSeq sequenceNumber
	if options.Snapshot != nil {
		snapshotSeq = (options.Snapshot.(*snapshot)).sequenceNumber
	} else {
		snapshotSeq = d.versions.lastSequence
	}

	mem := d.mem
	imm := d.imm
	current := d.versions.current
	mem.ref()
	if imm != nil {
		imm.ref()
	}
	current.ref()

	haveStatUpdate := false
	stats := new(getStats)
	d.mutex.Unlock()
	lkey := newLookupKey(key, snapshotSeq)
	var ok bool
	if err, ok = mem.get(lkey, &value); ok {
	} else if imm != nil {
		if err, ok = imm.get(lkey, &value); ok {
		} else {
			stats, err = current.get(options, lkey, &value)
			haveStatUpdate = true
		}
	} else {
		stats, err = current.get(options, lkey, &value)
		haveStatUpdate = true
	}
	d.mutex.Lock()

	if haveStatUpdate && current.updateStats(stats) {
		d.maybeScheduleCompaction()
	}
	mem.unref()
	if imm != nil {
		imm.unref()
	}
	current.unref()
	return
}

func (d *db) NewIterator(options *ssdb.ReadOptions) ssdb.Iterator {
	iter, latestSnapshot, seed := d.newInternalIterator(options)
	if options.Snapshot != nil {
		return newDBIterator(d, d.userComparator(), iter, options.Snapshot.(*snapshot).sequenceNumber, seed)
	} else {
		return newDBIterator(d, d.userComparator(), iter, latestSnapshot, seed)
	}
}

func (d *db) recordReadSample(key []byte) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	if d.versions.current.recordReadSample(key) {
		d.maybeScheduleCompaction()
	}
}

func (d *db) GetSnapshot() ssdb.Snapshot {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	return d.snapshots.newSnapshot(d.versions.lastSequence)
}

func (d *db) ReleaseSnapshot(snap ssdb.Snapshot) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	d.snapshots.deleteSnapshot(snap.(*snapshot))
}

func (d *db) Write(options *ssdb.WriteOptions, updates ssdb.WriteBatch) (err error) {
	w := newWriter(&d.mutex)
	w.batch = updates
	w.sync = options.Sync
	w.done = false

	d.mutex.Lock()
	defer d.mutex.Unlock()
	d.writers = append(d.writers, w)
	for !w.done && w != d.writers[0] {
		w.cond.Wait()
	}
	if w.done {
		err = w.err
		return
	}

	err = d.makeRoomForWrite(updates == nil)
	lastSequence := d.versions.lastSequence
	lastWriter := w
	if err == nil && updates != nil {
		updates = d.buildBatchGroup(&lastWriter)
		wbi := updates.(writeBatchInternal)
		wbi.SetSequence(uint64(lastSequence + 1))
		lastSequence += sequenceNumber(wbi.Count())

		// Add to log and apply to memtable.  We can release the lock
		// during this phase since &w is currently responsible for logging
		// and protects against concurrent loggers and concurrent writes
		// into mem_.
		d.mutex.Unlock()
		err = d.log.addRecord(wbi.Contents())
		syncError := false
		if err == nil && options.Sync {
			if err = d.logFile.Sync(); err != nil {
				syncError = true
			}
		}
		if err == nil {
			err = insertInto(updates, d.mem)
		}
		d.mutex.Lock()
		if syncError {
			d.recordBackgroundError(err)
		}
		if updates == d.tmpBatch {
			d.tmpBatch.Clear()
		}
		d.versions.setLastSequence(lastSequence)
	}

	for {
		ready := d.writers[0]
		d.writers = d.writers[1:]
		if ready == w {
			ready.err = err
			ready.done = true
			ready.cond.Signal()
		}
		if ready == lastWriter {
			break
		}
	}

	if len(d.writers) != 0 {
		d.writers[0].cond.Signal()
	}

	return
}

func (d *db) buildBatchGroup(lastWriter **writer) ssdb.WriteBatch {
	if len(d.writers) == 0 {
		panic("db: len(writers) == 0")
	}
	first := d.writers[0]
	result := first.batch
	if result == nil {
		panic("db: result == nil")
	}

	size := result.(writeBatchInternal).ByteSize()

	// Allow the group to grow up to a maximum size, but if the
	// original write is small, limit the growth so we do not slow
	// down the small write too much.
	maxSize := 1 << 20
	if size <= (128 << 10) {
		maxSize = size + (128 << 10)
	}

	*lastWriter = first
	for _, w := range d.writers[1:] {
		if w.sync && !first.sync {
			break
		}
		if w.batch != nil {
			size += w.batch.(writeBatchInternal).ByteSize()
			if size > maxSize {
				break
			}

			if result == first.batch {
				result = d.tmpBatch
				result.(writeBatchInternal).Append(first.batch)
			}
			result.(writeBatchInternal).Append(w.batch)
			*lastWriter = w
		}
	}

	return result
}

func (d *db) makeRoomForWrite(force bool) (err error) {
	if len(d.writers) == 0 {
		panic("db: len(writers) == 0")
	}
	allowDelay := !force
	for {
		if d.bgError != nil {
			err = d.bgError
			break
		} else if allowDelay && d.versions.numLevelFiles(0) >= l0SlowdownWritesTrigger {
			// We are getting close to hitting a hard limit on the number of
			// L0 files.  Rather than delaying a single write by several
			// seconds when we hit the hard limit, start delaying each
			// individual write by 1ms to reduce latency variance.  Also,
			// this delay hands over some CPU to the compaction thread in
			// case it is sharing the same core as the writer.
			d.mutex.Unlock()
			d.env.SleepForMicroseconds(1000)
			allowDelay = false
			d.mutex.Lock()
		} else if !force && d.mem.approximateMemoryUsage() <= uint64(d.options.WriteBufferSize) {
			break
		} else if d.imm != nil {
			ssdb.Log(d.options.InfoLog, "Current memtable full; waiting...\n")
			d.backgroundWorkFinishedSignal.Wait()
		} else if d.versions.numLevelFiles(0) >= l0StopWritesTrigger {
			ssdb.Log(d.options.InfoLog, "Too many L0 files; waiting...\n")
			d.backgroundWorkFinishedSignal.Wait()
		} else {
			if d.versions.prevLogNumber != 0 {
				panic("db: versions.prevLogNumber != 0")
			}
			newLogNumber := d.versions.newFileNumber()
			var lfile ssdb.WritableFile
			if lfile, err = d.env.NewWritableFile(logFileName(d.dbName, newLogNumber)); err != nil {
				d.versions.reuseFileNumber(newLogNumber)
				break
			}
			d.logFile = lfile
			d.logFileNumber = newLogNumber
			d.log = newLogWriter(lfile)
			d.imm = d.mem
			d.hasImm.SetTrue()
			d.mem = NewMemTable(&d.internalComparator)
			d.mem.ref()
			force = false
			d.maybeScheduleCompaction()
		}
	}
	return
}

func (d *db) GetProperty(property string) (value string, ok bool) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	in := property
	prefix := "ssdb."
	if !strings.HasPrefix(in, prefix) {
		ok = false
		return
	}
	in = strings.TrimPrefix(in, prefix)
	if strings.HasPrefix(in, "num-files-at-level") {
		in = strings.TrimPrefix(in, "num-files-at-level")
		var level uint64
		ok = util.ConsumeDecimalNumber(&in, &level) && len(in) == 0
		if !ok || level >= numLevels {
			ok = false
			return
		} else {
			value = fmt.Sprintf("%d", d.versions.numLevelFiles(int(level)))
			ok = true
			return
		}
	} else if in == "stats" {
		var b strings.Builder
		b.Grow(200)
		b.WriteString("                               Compactions\n")
		b.WriteString("Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n")
		b.WriteString("--------------------------------------------------\n")
		for level := 0; level < numLevels; level++ {
			files := d.versions.numLevelFiles(level)
			if d.stats[level].micros > 0 || files > 0 {
				fmt.Fprintf(&b, "%3d %8d %8.0f %9.0f %8.0f %9.0f\n", level,
					files, float64(d.versions.numLevelBytes(level))/1048576.0,
					float64(d.stats[level].micros)/1e6,
					float64(d.stats[level].bytesRead)/1048576.0,
					float64(d.stats[level].bytesWritten)/1048576.0)
			}
		}
		value = b.String()
		ok = true
		return
	} else if in == "sstables" {
		value = d.versions.current.debugString()
		ok = true
		return
	} else if in == "approximate-memory-usage" {
		totalUsage := uint64(d.options.BlockCache.TotalCharge())
		if d.mem != nil {
			totalUsage += d.mem.approximateMemoryUsage()
		}
		if d.imm != nil {
			totalUsage += d.imm.approximateMemoryUsage()
		}
		value = fmt.Sprintf("%d", totalUsage)
		ok = true
		return
	}
	ok = false
	return
}

func (d *db) GetApproximateSizes(r []ssdb.Range) (sizes []uint64) {
	sizes = make([]uint64, len(r))
	d.mutex.Lock()
	d.versions.current.ref()
	v := d.versions.current
	d.mutex.Unlock()

	for i := 0; i < len(r); i++ {
		k1 := newInternalKey(r[i].Start, maxSequenceNumber, valueTypeForSeek)
		k2 := newInternalKey(r[i].Limit, maxSequenceNumber, valueTypeForSeek)
		start := d.versions.approximateOffsetOf(v, k1)
		limit := d.versions.approximateOffsetOf(v, k2)
		if limit >= start {
			sizes[i] = limit - start
		} else {
			sizes[i] = 0
		}
	}
	d.mutex.Lock()
	defer d.mutex.Unlock()
	v.unref()
	return
}

func (d *db) Put(options *ssdb.WriteOptions, key, value []byte) error {
	batch := ssdb.NewWriteBatch()
	batch.Put(key, value)
	return d.Write(options, batch)
}

func (d *db) Delete(options *ssdb.WriteOptions, key []byte) error {
	batch := ssdb.NewWriteBatch()
	batch.Delete(key)
	return d.Write(options, batch)
}

func Open(options *ssdb.Options, dbName string) (db ssdb.DB, err error) {
	impl := newDB(options, dbName)
	impl.mutex.Lock()
	edit := newVersionEdit()
	var saveManifest bool
	if saveManifest, err = impl.recover(edit); err == nil && impl.mem == nil {
		newLogNumber := impl.versions.newFileNumber()
		var lfile ssdb.WritableFile
		if lfile, err = options.Env.NewWritableFile(logFileName(dbName, newLogNumber)); err == nil {
			edit.setLogNumber(newLogNumber)
			impl.logFile = lfile
			impl.logFileNumber = newLogNumber
			impl.log = newLogWriter(lfile)
			impl.mem = NewMemTable(&impl.internalComparator)
			impl.mem.ref()
		}
	}
	if err == nil && saveManifest {
		edit.setPrevLogNumber(0)
		edit.setLogNumber(impl.logFileNumber)
		err = impl.versions.logAndApply(edit, &impl.mutex)
	}
	if err == nil {
		impl.deleteObsoleteFiles()
		impl.maybeScheduleCompaction()
	}
	impl.mutex.Unlock()
	if err == nil {
		if impl.mem == nil {
			panic("db: mem == nil")
		}
		db = impl
	} else {
		impl = nil
	}
	return
}

func Destroy(dbName string, options *ssdb.Options) error {
	env := options.Env
	fileNames, err := env.GetChildren(dbName)
	if err != nil {
		return nil
	}
	lockName := lockFileName(dbName)
	lock, err := env.LockFile(lockName)
	if err == nil {
		var (
			number uint64
			ft     fileType
		)
		for _, fileName := range fileNames {
			if parseFileName(fileName, &number, &ft) && ft != dbLockFile {
				if del := env.DeleteFile(dbName + "/" + fileName); err == nil && del != nil {
					err = del
				}
			}
		}
		_ = env.UnlockFile(lock)
		_ = env.DeleteFile(lockName)
		_ = env.DeleteDir(dbName)
	}
	return err
}
