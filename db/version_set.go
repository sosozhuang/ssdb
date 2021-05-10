package db

import (
	"bytes"
	"fmt"
	"runtime"
	"sort"
	"ssdb"
	"ssdb/table"
	"ssdb/util"
	"strings"
	"sync"
	"unsafe"
)

func targetFileSize(options *ssdb.Options) int {
	return options.MaxFileSize
}

func maxGrandParentOverlapBytes(options *ssdb.Options) int64 {
	return int64(10 * targetFileSize(options))
}

func expandedCompactionByteSizeLimit(options *ssdb.Options) int64 {
	return int64(25 * targetFileSize(options))
}

func maxBytesForLevel(options *ssdb.Options, level int) float64 {
	result := 10. * 1048576.0
	for level > 1 {
		result *= 10
		level--
	}
	return result
}

func maxFileSizeForLevel(options *ssdb.Options, level int) uint64 {
	return uint64(targetFileSize(options))
}

func totalFileSize(files []*fileMetaData) (sum int64) {
	for _, file := range files {
		sum += int64(file.fileSize)
	}
	return
}

type version struct {
	vset               *versionSet
	next               *version
	prev               *version
	refs               int
	files              [numLevels][]*fileMetaData
	fileToCompact      *fileMetaData
	fileToCompactLevel int
	compactionScore    float64
	compactionLevel    int
}

func newVersion(vset *versionSet) *version {
	v := &version{
		vset:               vset,
		refs:               0,
		fileToCompact:      nil,
		fileToCompactLevel: -1,
		compactionScore:    -1,
		compactionLevel:    -1,
	}
	v.next = v
	v.prev = v
	return v
}

func (v *version) finalize() {
	if v.refs != 0 {
		panic("version: refs != 0")
	}
	v.prev.next = v.next
	v.next.prev = v.prev
	for level := 0; level < numLevels; level++ {
		for _, f := range v.files[level] {
			if f.refs <= 0 {
				panic("version: refs <= 0")
			}
			f.refs--
			if f.refs <= 0 {
				f = nil
			}
		}
	}
}

func (v *version) newConcatenatingIterator(options *ssdb.ReadOptions, level int) ssdb.Iterator {
	return table.NewTwoLevelIterator(newLevelFileNumIterator(v.vset.icmp, v.files[level]), getFileIterator, v.vset.tableCache, options)
}

func (v *version) addIterators(options *ssdb.ReadOptions, iters *[]ssdb.Iterator) {
	for _, f := range v.files[0] {
		*iters = append(*iters, v.vset.tableCache.newIterator(options, f.number, f.fileSize, nil))
	}
	for level := 1; level < numLevels; level++ {
		if len(v.files[level]) != 0 {
			*iters = append(*iters, v.newConcatenatingIterator(options, level))
		}
	}
}

type fileMetaDataSlice []*fileMetaData

func (s fileMetaDataSlice) Len() int {
	return len(s)
}

func (s fileMetaDataSlice) Less(i, j int) bool {
	return newestFirst(s[i], s[j])
}

func (s fileMetaDataSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (v *version) foreachOverlapping(userKey, internalKey []byte, arg interface{}, function func(interface{}, int, *fileMetaData) bool) {
	ucmp := v.vset.icmp.userComparator
	tmp := make([]*fileMetaData, 0, len(v.files[0]))
	for _, f := range v.files[0] {
		if ucmp.Compare(userKey, f.smallest.userKey()) >= 0 &&
			ucmp.Compare(userKey, f.largest.userKey()) <= 0 {
			tmp = append(tmp, f)
		}
	}
	if len(tmp) != 0 {
		sort.Sort(fileMetaDataSlice(tmp))
		for _, f := range tmp {
			if !function(arg, 0, f) {
				return
			}
		}
	}
	var (
		numFiles int
		index    int
		f        *fileMetaData
	)
	for level := 1; level < numLevels; level++ {
		if numFiles = len(v.files[level]); numFiles == 0 {
			continue
		}
		index = findFile(v.vset.icmp, v.files[level], internalKey)
		if index < numFiles {
			f = v.files[level][index]
			if ucmp.Compare(userKey, f.smallest.userKey()) < 0 {
			} else {
				if !function(arg, level, f) {
					return
				}
			}
		}
	}
}

func (v *version) get(options *ssdb.ReadOptions, k *lookupKey, value []byte) (stats *getStats, err error) {
	ikey := k.internalKey()
	userKey := k.userKey()
	ucmp := v.vset.icmp.userComparator
	stats = &getStats{
		seekFile:      nil,
		seekFileLevel: -1,
	}

	var (
		lastFileRead      *fileMetaData
		lastFileReadLevel = -1
		tmp               []*fileMetaData
		tmp2              *fileMetaData
		numFiles          int
	)
	for level := 0; level < numLevels; level++ {
		if numFiles = len(v.files[level]); numFiles == 0 {
			continue
		}
		files := v.files[level]
		if level == 0 {
			tmp = make([]*fileMetaData, 0, numFiles)
			for _, f := range files {
				if ucmp.Compare(userKey, f.smallest.userKey()) >= 0 &&
					ucmp.Compare(userKey, f.largest.userKey()) <= 0 {
					tmp = append(tmp, f)
				}
			}
			if len(tmp) == 0 {
				continue
			}
			sort.Sort(fileMetaDataSlice(tmp))
			files = tmp
			numFiles = len(tmp)
		} else {
			if index := findFile(v.vset.icmp, v.files[level], ikey); index >= numFiles {
				files = nil
				numFiles = 0
			} else {
				tmp2 = files[index]
				if ucmp.Compare(userKey, tmp2.smallest.userKey()) < 0 {
					files = nil
					numFiles = 0
				} else {
					files = []*fileMetaData{tmp2}
					numFiles = 1
				}
			}
		}

		for i := 0; i < numFiles; i++ {
			if lastFileRead != nil && stats.seekFile == nil {
				stats.seekFile = lastFileRead
				stats.seekFileLevel = lastFileReadLevel
			}
			f := files[i]
			lastFileRead = f
			lastFileReadLevel = level

			saver := saver{
				state:   notFound,
				ucmp:    ucmp,
				userKey: userKey,
				value:   value,
			}
			if err = v.vset.tableCache.get(options, f.number, f.fileSize, ikey, &saver, saveValue); err != nil {
				return
			}
			switch saver.state {
			case notFound:
			case found:
				return
			case deleted:
				err = util.NotFoundError1("")
				return
			case corrupt:
				err = util.CorruptionError2("corrupted key for ", string(userKey))
				return
			}
		}
	}
	err = util.NotFoundError1("")
	return
}

func (v *version) updateStats(stats *getStats) bool {
	if f := stats.seekFile; f != nil {
		f.allowedSeeks--
		if f.allowedSeeks <= 0 && v.fileToCompact == nil {
			v.fileToCompact = f
			v.fileToCompactLevel = stats.seekFileLevel
			return true
		}
	}
	return false
}

func (v *version) recordReadSample(internalKey []byte) bool {
	ikey := new(parsedInternalKey)
	if !parseInternalKey(internalKey, ikey) {
		return false
	}
	state := versionSetState{matches: 0}
	v.foreachOverlapping(ikey.userKey, internalKey, &state, match)
	if state.matches >= 2 {
		return v.updateStats(&state.stats)
	}
	return false
}

type versionSetState struct {
	stats   getStats
	matches int
}

func match(arg interface{}, level int, f *fileMetaData) bool {
	state := arg.(*versionSetState)
	state.matches++
	if state.matches == 1 {
		state.stats.seekFile = f
		state.stats.seekFileLevel = level
	}
	return state.matches < 2
}

func (v *version) ref() {
	v.refs++
}

func (v *version) unref() {
	if v == &v.vset.dummyVersions {
		panic("version: v == &v.vset.dummyVersions")
	}
	if v.refs < 1 {
		panic("version: refs < 1")
	}
	v.refs--
	if v.refs == 0 {
		v.finalize()
	}
}

func (v *version) overlapInLevel(level int, smallestUserKey, largestUserKey []byte) bool {
	return someFileOverlapsRange(v.vset.icmp, level > 0, v.files[level], smallestUserKey, largestUserKey)
}

func (v *version) pickLevelForMemTableOutput(smallestUserKey, largestUserKey []byte) int {
	level := 0
	if !v.overlapInLevel(0, smallestUserKey, largestUserKey) {
		start := newInternalKey(smallestUserKey, maxSequenceNumber, valueTypeForSeek)
		limit := newInternalKey(largestUserKey, 0, 0)
		var overlaps []*fileMetaData
		for level < maxMemCompactLevel {
			if v.overlapInLevel(level+1, smallestUserKey, largestUserKey) {
				break
			}
			if level+2 < numLevels {
				v.getOverlappingInputs(level+2, start, limit, &overlaps)
				if sum := totalFileSize(overlaps); sum > maxGrandParentOverlapBytes(v.vset.options) {
					break
				}
			}
			level++
		}
	}
	return level
}

func (v *version) getOverlappingInputs(level int, begin, end *internalKey, inputs *[]*fileMetaData) {
	if level < 0 {
		panic("version: level < 0")
	}
	if level >= numLevels {
		panic("version: level >= numLevels")
	}
	*inputs = make([]*fileMetaData, 0)
	var userBegin, userEnd []byte
	if begin != nil {
		userBegin = begin.userKey()
	}
	if end != nil {
		userEnd = end.userKey()
	}
	userCmp := v.vset.icmp.userComparator
	for i := 0; i < len(v.files[level]); {
		f := v.files[level][i]
		i++
		fileStart := f.smallest.userKey()
		fileLimit := f.largest.userKey()
		if begin != nil && userCmp.Compare(fileLimit, userBegin) < 0 {
		} else if end != nil && userCmp.Compare(fileStart, userEnd) > 0 {
		} else {
			*inputs = append(*inputs, f)
			if level == 0 {
				if begin != nil && userCmp.Compare(fileStart, userBegin) < 0 {
					userBegin = fileStart
					*inputs = make([]*fileMetaData, 0)
					i = 0
				} else if end != nil && userCmp.Compare(fileLimit, userEnd) > 0 {
					userEnd = fileLimit
					*inputs = make([]*fileMetaData, 0)
					i = 0
				}
			}
		}
	}
}

func (v *version) debugString() string {
	var b strings.Builder
	for level := 0; level < numLevels; level++ {
		b.WriteString("--- level ")
		util.AppendNumberTo(&b, uint64(level))
		b.WriteString(" ---\n")
		for _, f := range v.files[level] {
			b.WriteByte(' ')
			util.AppendNumberTo(&b, f.number)
			b.WriteByte(':')
			util.AppendNumberTo(&b, f.fileSize)
			b.WriteByte('[')
			b.WriteString(f.smallest.debugString())
			b.WriteString(" .. ")
			b.WriteString(f.largest.debugString())
			b.WriteString("]\n")
		}
	}
	return b.String()
}

type saverState int8

const (
	notFound saverState = iota
	found
	deleted
	corrupt
)

type saver struct {
	state   saverState
	ucmp    ssdb.Comparator
	userKey []byte
	value   []byte
}

func saveValue(arg interface{}, ikey, v []byte) error {
	s := arg.(*saver)
	parsedKey := new(parsedInternalKey)
	if !parseInternalKey(ikey, parsedKey) {
		s.state = corrupt
	} else {
		if s.ucmp.Compare(parsedKey.userKey, s.userKey) == 0 {
			if parsedKey.valueType == ssdb.TypeValue {
				s.state = found
			} else {
				s.state = deleted
			}
			if s.state == found {
				s.value = make([]byte, len(v))
				copy(s.value, v)
			}
		}
	}
	return nil
}

func newestFirst(a, b *fileMetaData) bool {
	return a.number > b.number
}

func findFile(icmp *internalKeyComparator, files []*fileMetaData, key []byte) int {
	left, right := 0, len(files)
	var (
		mid int
		f   *fileMetaData
	)
	for left < right {
		mid = (left + right) / 2
		f = files[mid]
		if icmp.Compare(f.largest.encode(), key) < 0 {
			left = mid + 1
		} else {
			right = mid
		}
	}
	return right
}

func afterFile(ucmp ssdb.Comparator, userKey []byte, f *fileMetaData) bool {
	return userKey != nil && ucmp.Compare(userKey, f.largest.userKey()) > 0
}

func beforeFile(ucmp ssdb.Comparator, userKey []byte, f *fileMetaData) bool {
	return userKey != nil && ucmp.Compare(userKey, f.smallest.userKey()) < 0
}

func someFileOverlapsRange(icmp *internalKeyComparator, disjointSortedFiles bool, files []*fileMetaData, smallestUserKey, largestUserKey []byte) bool {
	ucmp := icmp.userComparator
	if !disjointSortedFiles {
		for _, f := range files {
			if afterFile(ucmp, smallestUserKey, f) || beforeFile(ucmp, largestUserKey, f) {
			} else {
				return true
			}
		}
		return false
	}

	var index int
	if smallestUserKey != nil {
		smallKey := newInternalKey(smallestUserKey, maxSequenceNumber, valueTypeForSeek)
		index = findFile(icmp, files, smallKey.encode())
	}
	if index >= len(files) {
		return false
	}
	return !beforeFile(ucmp, largestUserKey, files[index])
}

// An internal iterator.  For a given version/level pair, yields
// information about the files in the level.  For a given entry, key()
// is the largest key that occurs in the file, and value() is an
// 16-byte value containing the file number and file size, both
// encoded using EncodeFixed64.
type levelFileNumIterator struct {
	table.CleanUpIterator
	icmp     *internalKeyComparator
	flist    []*fileMetaData
	index    int
	valueBuf [16]byte
}

func newLevelFileNumIterator(icmp *internalKeyComparator, flist []*fileMetaData) *levelFileNumIterator {
	return &levelFileNumIterator{
		icmp:  icmp,
		flist: flist,
		index: len(flist),
	}
}

func (i *levelFileNumIterator) Valid() bool {
	return i.index < len(i.flist)
}

func (i *levelFileNumIterator) SeekToFirst() {
	i.index = 0
}

func (i *levelFileNumIterator) SeekToLast() {
	if l := len(i.flist); l == 0 {
		i.index = 0
	} else {
		i.index = l - 1
	}
}

func (i *levelFileNumIterator) Seek(target []byte) {
	i.index = findFile(i.icmp, i.flist, target)
}

func (i *levelFileNumIterator) Next() {
	if !i.Valid() {
		panic("levelFileNumIterator: not valid")
	}
	i.index++
}

func (i *levelFileNumIterator) Prev() {
	if !i.Valid() {
		panic("levelFileNumIterator: not valid")
	}
	if i.index == 0 {
		i.index = len(i.flist)
	} else {
		i.index--
	}
}

func (i *levelFileNumIterator) Key() []byte {
	if !i.Valid() {
		panic("levelFileNumIterator: not valid")
	}
	return i.flist[i.index].largest.encode()
}

func (i *levelFileNumIterator) Value() []byte {
	if !i.Valid() {
		panic("levelFileNumIterator: not valid")
	}
	util.EncodeFixed64((*[8]byte)(unsafe.Pointer(&i.valueBuf)), i.flist[i.index].number)
	util.EncodeFixed64((*[8]byte)(unsafe.Pointer(uintptr(unsafe.Pointer(&i.valueBuf))+8*unsafe.Sizeof(byte(0)))), i.flist[i.index].fileSize)
	return i.valueBuf[:]
}

func (i *levelFileNumIterator) Status() error {
	return nil
}

func getFileIterator(arg interface{}, options *ssdb.ReadOptions, fileValue []byte) ssdb.Iterator {
	cache := arg.(*tableCache)
	if len(fileValue) != 16 {
		return table.NewErrorIterator(util.CorruptionError1("FileReader invoked with unexpected value"))
	} else {
		return cache.newIterator(options, util.DecodeFixed64(fileValue), util.DecodeFixed64(fileValue[8:]), nil)
	}
}

type getStats struct {
	seekFile      *fileMetaData
	seekFileLevel int
}

type versionSet struct {
	env                ssdb.Env
	dbName             string
	options            *ssdb.Options
	tableCache         *tableCache
	icmp               *internalKeyComparator
	nextFileNumber     uint64
	manifestFileNumber uint64
	lastSequence       sequenceNumber
	logNumber          uint64
	prevLogNumber      uint64
	descriptorFile     ssdb.WritableFile
	descriptorLog      *logWriter
	dummyVersions      version
	current            *version
	compactPointer     [numLevels][]byte
}

func newVersionSet(dbName string, options *ssdb.Options, tableCache *tableCache, cmp *internalKeyComparator) *versionSet {
	s := &versionSet{
		env:                options.Env,
		dbName:             dbName,
		options:            options,
		tableCache:         tableCache,
		icmp:               cmp,
		nextFileNumber:     2,
		manifestFileNumber: 0,
		lastSequence:       0,
		logNumber:          0,
		prevLogNumber:      0,
		descriptorFile:     nil,
		descriptorLog:      nil,
		current:            nil,
		compactPointer:     [numLevels][]byte{},
	}
	s.dummyVersions = *newVersion(s)
	s.appendVersion(newVersion(s))
	runtime.SetFinalizer(s, func(s *versionSet) {

	})
	return s
}

func (s *versionSet) finalize() {
	s.current.unref()
	if s.dummyVersions.next != &s.dummyVersions {
		panic("versionSet: dummyVersions.next != &dummyVersions")
	}
	s.descriptorLog = nil
	s.descriptorFile.Finalize()
}

func (s *versionSet) appendVersion(v *version) {
	if v.refs != 0 {
		panic("versionSet: v.refs != 0")
	}
	if v == s.current {
		panic("versionSet: v == current")
	}
	if s.current != nil {
		s.current.unref()
	}
	s.current = v
	v.ref()
	v.prev = s.dummyVersions.prev
	v.next = &s.dummyVersions
	v.prev.next = v
	v.next.prev = v
}

func (s *versionSet) logAndApply(edit *versionEdit, mu *sync.Mutex) (err error) {
	if edit.hasLogNumber {
		if edit.logNumber < s.logNumber {
			panic("versionSet: edit.logNumber < logNumber")
		}
		if edit.logNumber >= s.nextFileNumber {
			panic("versionSet: edit.logNumber >= nextFileNumber")
		}
	} else {
		edit.setLogNumber(s.logNumber)
	}
	if !edit.hasPrevLogNumber {
		edit.setPrevLogNumber(s.prevLogNumber)
	}
	edit.setNextFile(s.nextFileNumber)
	edit.setLastSequence(sequenceNumber(s.lastSequence))

	v := newVersion(s)
	builder := newVersionSetBuilder(s, s.current)
	builder.apply(edit)
	builder.saveTo(v)
	s.finalizeVersion(v)

	var newManifestFile string
	if s.descriptorLog == nil {
		if s.descriptorFile != nil {
			panic("versionSet: descriptorFile != nil")
		}
		newManifestFile = descriptorFileName(s.dbName, s.manifestFileNumber)
		edit.setNextFile(s.nextFileNumber)
		if s.descriptorFile, err = s.env.NewWritableFile(newManifestFile); err == nil {
			s.descriptorLog = newLogWriter(s.descriptorFile)
			err = s.writeSnapshot(s.descriptorLog)
		}
	}

	mu.Lock()
	if err == nil {
		record := make([]byte, 0)
		edit.encodeTo(&record)
		if err = s.descriptorLog.addRecord(record); err == nil {
			err = s.descriptorFile.Sync()
		}
		if err != nil {
			ssdb.Log(s.options.InfoLog, "MANIFEST write: %s.\n", err)
		}
	}
	if err == nil && len(newManifestFile) != 0 {
		err = setCurrentFile(s.env, s.dbName, s.manifestFileNumber)
	}
	mu.Unlock()

	if err == nil {
		s.appendVersion(v)
		s.logNumber = edit.logNumber
		s.prevLogNumber = edit.prevLogNumber
	} else {
		v.finalize()
		if len(newManifestFile) != 0 {
			s.descriptorFile.Finalize()
			s.descriptorLog = nil
			s.descriptorFile = nil
			_ = s.env.DeleteFile(newManifestFile)
		}
	}
	return
}

type versionSetLogReporter struct {
	err error
}

func (r *versionSetLogReporter) corruption(bytes int, err error) {
	if r.err == nil {
		r.err = err
	}
}

func (s *versionSet) recover() (saveManifest bool, err error) {
	var current []byte
	if current, err = ssdb.ReadFileToString(s.env, currentFileName(s.dbName)); err != nil {
		return
	}
	if len(current) == 0 || current[len(current)-1] == '\n' {
		err = util.CorruptionError1("CURRENT file does not end with newline")
		return
	}
	current = current[:len(current)-1]
	dscName := s.dbName + "/" + string(current)
	var file ssdb.SequentialFile
	if file, err = s.env.NewSequentialFile(dscName); err != nil {
		if ssdb.IsNotFound(err) {
			err = util.CorruptionError2("CURRENT points to a non-existent file", err.Error())
		}
		return
	}

	haveLogNumber := false
	havePreLogNumber := false
	haveNextFile := false
	haveLastSequence := false
	nextFile := uint64(0)
	lastSequence := sequenceNumber(0)
	logNumber := uint64(0)
	prevLogNumber := uint64(0)
	builder := newVersionSetBuilder(s, s.current)

	reporter := versionSetLogReporter{err: err}
	reader := newLogReader(file, &reporter, true, 0)
	var (
		record []byte
		ok     bool
	)
	for ok && err == nil {
		record, ok = reader.readRecord()
		edit := newVersionEdit()
		if err = edit.decodeFrom(record); err == nil {
			if edit.hasComparator && edit.comparator != s.icmp.userComparator.Name() {
				err = util.InvalidArgumentError2(edit.comparator+" does not match existing comparator ", s.icmp.userComparator.Name())
			}
		}
		if err == nil {
			builder.apply(edit)
		}
		if edit.hasLogNumber {
			logNumber = edit.logNumber
			haveLogNumber = true
		}
		if edit.hasPrevLogNumber {
			prevLogNumber = edit.prevLogNumber
			havePreLogNumber = true
		}
		if edit.hasNextFileNumber {
			nextFile = edit.nextFileNumber
			haveNextFile = true
		}
		if edit.hasLastSequence {
			lastSequence = edit.lastSequence
			haveLastSequence = true
		}
	}
	file.Finalize()
	file = nil
	if err == nil {
		if !haveNextFile {
			err = util.CorruptionError1("no meta-nextfile entry in descriptor")
		} else if !haveLogNumber {
			err = util.CorruptionError1("no meta-lognumber entry in descriptor")
		} else if !haveLastSequence {
			err = util.CorruptionError1("no last-sequence-number entry in descriptor")
		}

		if !havePreLogNumber {
			prevLogNumber = 0
		}
		s.markFileNumberUsed(prevLogNumber)
		s.markFileNumberUsed(logNumber)
	}

	if err == nil {
		v := newVersion(s)
		builder.saveTo(v)
		s.finalizeVersion(v)
		s.appendVersion(v)
		s.manifestFileNumber = nextFile
		s.nextFileNumber = nextFile + 1
		s.lastSequence = lastSequence
		s.logNumber = logNumber
		s.prevLogNumber = prevLogNumber
		if s.reuseManifest(dscName, string(current)) {
		} else {
			saveManifest = true
		}
	}
	return
}

func (s *versionSet) reuseManifest(dscName, dscBase string) bool {
	if !s.options.ReuseLogs {
		return false
	}
	var (
		manifestType   fileType
		manifestNumber uint64
		manifestSize   uint64
		err            error
	)
	if !parseFileName(dscBase, &manifestNumber, &manifestType) || manifestType != descriptorFile {
		return false
	}
	//if manifestSize, err = s.env.GetFileSize(dscName); err != nil || manifestSize >= targetFileSize(s.options) {
	//	return false
	//}
	if s.descriptorFile != nil {
		panic("versionSet: descriptorFile != nil")
	}
	if s.descriptorLog != nil {
		panic("versionSet: descriptorLog != nil")
	}
	if s.descriptorFile, err = s.env.NewAppendableFile(dscName); err != nil {
		if s.descriptorFile != nil {
			panic("versionSet: descriptorFile != nil")
		}
		return false
	}
	s.descriptorLog = newLogWriterWithLength(s.descriptorFile, manifestSize)
	s.manifestFileNumber = manifestNumber
	return true
}

func (s *versionSet) markFileNumberUsed(number uint64) {
	if s.nextFileNumber <= number {
		s.nextFileNumber = number + 1
	}
}

func (s *versionSet) finalizeVersion(v *version) {
	bestLevel := -1
	bestScore := float64(-1)

	var score float64
	for level := 0; level < numLevels; level++ {
		if level == 0 {
			// We treat level-0 specially by bounding the number of files
			// instead of number of bytes for two reasons:
			//
			// (1) With larger write-buffer sizes, it is nice not to do too
			// many level-0 compactions.
			//
			// (2) The files in level-0 are merged on every read and
			// therefore we wish to avoid too many files when the individual
			// file size is small (perhaps because of a small write-buffer
			// setting, or very high compression ratios, or lots of
			// overwrites/deletions).
			score = float64(len(v.files[level]) / l0CompactionTrigger)
		} else {
			levelBytes := totalFileSize(v.files[level])
			score = float64(levelBytes) / maxBytesForLevel(s.options, level)
		}
		if score > bestScore {
			bestLevel = level
			bestScore = score
		}
	}
	v.compactionLevel = bestLevel
	v.compactionScore = bestScore
}

func (s *versionSet) writeSnapshot(log *logWriter) error {
	edit := newVersionEdit()
	edit.setComparatorName(s.icmp.userComparator.Name())
	for level := 0; level < numLevels; level++ {
		if len(s.compactPointer[level]) > 0 {
			key := new(internalKey)
			key.decodeFrom(s.compactPointer[level])
			edit.setCompactPointer(level, key)
		}
	}

	for level := 0; level < numLevels; level++ {
		files := s.current.files[level]
		for _, f := range files {
			edit.addFile(level, f.number, f.fileSize, f.smallest, f.largest)
		}
	}

	record := make([]byte, 0)
	edit.encodeTo(&record)
	return log.addRecord(record)
}

func (s *versionSet) numLevelFiles(level int) int {
	if level < 0 {
		panic("versionSet: level < 0")
	}
	if level >= numLevels {
		panic("versionSet: level >= numLevels")
	}
	return len(s.current.files[level])
}

func (s *versionSet) levelSummary(scratch *levelSummaryStorage) [100]byte {
	buf := bytes.NewBuffer(scratch.buffer[:])
	fmt.Fprintf(buf, "files[ %d %d %d %d %d %d %d ]", len(s.current.files[0]), len(s.current.files[1]), len(s.current.files[2]),
		len(s.current.files[3]), len(s.current.files[4]), len(s.current.files[5]), len(s.current.files[6]))
	return scratch.buffer
}

func (s *versionSet) approximateOffsetOf(v *version, ikey *internalKey) (result uint64) {
	result = 0
	for level := 0; level < numLevels; level++ {
		files := v.files[level]
		for _, f := range files {
			if s.icmp.compare(&f.largest, ikey) <= 0 {
				result += f.fileSize
			} else if s.icmp.compare(&f.smallest, ikey) > 0 {
				if level > 0 {
					break
				}
			} else {
				var tablePtr ssdb.Table
				iter := s.tableCache.newIterator(ssdb.NewReadOptions(), f.number, f.fileSize, &tablePtr)
				if tablePtr != nil {
					result += tablePtr.ApproximateOffsetOf(ikey.encode())
				}
				iter.Finalize()
			}
		}
	}
	return
}

func (s *versionSet) addLiveFiles(live map[uint64]struct{}) {
	for v := s.dummyVersions.next; v != &s.dummyVersions; v = v.next {
		for level := 0; level < numLevels; level++ {
			files := v.files[level]
			for _, f := range files {
				live[f.number] = struct{}{}
			}
		}
	}
}

func (s *versionSet) numLevelBytes(level int) int64 {
	if level < 0 {
		panic("versionSet: level < 0")
	}
	if level >= numLevels {
		panic("versionSet: level > numLevels")
	}
	return totalFileSize(s.current.files[level])
}

func (s *versionSet) maxNextLevelOverlappingBytes() (result int64) {
	var sum int64
	var overlaps []*fileMetaData
	for level := 1; level < numLevels; level++ {
		for _, f := range s.current.files[level] {
			s.current.getOverlappingInputs(level+1, &f.smallest, &f.largest, &overlaps)
			sum = totalFileSize(overlaps)
			if sum > result {
				result = sum
			}
		}
	}
	return
}

func (s *versionSet) getRange(inputs []*fileMetaData, smallest, largest *internalKey) {
	if len(inputs) == 0 {
		panic("versionSet: len(inputs) == 0")
	}
	smallest.clear()
	largest.clear()
	for i, f := range inputs {
		if i == 0 {
			*smallest = f.smallest
			*largest = f.largest
		} else {
			if s.icmp.compare(&f.smallest, smallest) < 0 {
				*smallest = f.smallest
			}
			if s.icmp.compare(&f.largest, largest) > 0 {
				*largest = f.largest
			}
		}
	}
}

func (s *versionSet) getRange2(inputs1, inputs2 []*fileMetaData, smallest, largest *internalKey) {
	all := make([]*fileMetaData, 0, len(inputs1)+len(inputs2))
	all = append(all, inputs1...)
	all = append(all, inputs2...)
	s.getRange(all, smallest, largest)
}

func (s *versionSet) makeInputIterator(c *compaction) ssdb.Iterator {
	options := ssdb.NewReadOptions()
	options.VerifyChecksums = s.options.ParanoidChecks
	options.FillCache = false

	// Level-0 files have to be merged together.  For other levels,
	// we will make a concatenating iterator per level.
	// TODO(opt): use concatenating iterator for level-0 if there is no overlap
	var space int
	if c.level == 0 {
		space = len(c.inputs[0]) + 1
	} else {
		space = 2
	}
	list := make([]ssdb.Iterator, space)
	num := 0
	for which := 0; which < 2; which++ {
		if len(c.inputs[which]) != 0 {
			if c.level+which == 0 {
				files := c.inputs[which]
				for _, f := range files {
					list[num] = s.tableCache.newIterator(options, f.number, f.fileSize, nil)
					num++
				}
			} else {
				list[num] = table.NewTwoLevelIterator(newLevelFileNumIterator(s.icmp, c.inputs[which]), getFileIterator, s.tableCache, options)
				num++
			}
		}
	}
	if num > space {
		panic("versionSet: num > space")
	}
	result := table.NewMergingIterator(s.icmp, list)
	list = nil
	return result
}

func (s *versionSet) pickCompaction() *compaction {
	var (
		level int
		c     *compaction
	)
	sizeCompaction := s.current.compactionScore >= 1
	seekCompaction := s.current.fileToCompact != nil
	if sizeCompaction {
		level = s.current.compactionLevel
		if level < 0 {
			panic("versionSet: level < 0")
		}
		if level+1 >= numLevels {
			panic("versionSet: level + 1 >= numLevels")
		}
		c = newCompaction(s.options, level)
		for _, f := range s.current.files[level] {
			if len(s.compactPointer[level]) == 0 || s.icmp.Compare(f.largest.encode(), s.compactPointer[level]) > 0 {
				c.inputs[0] = append(c.inputs[0], f)
				break
			}
		}
		if len(c.inputs[0]) == 0 {
			c.inputs[0] = append(c.inputs[0], s.current.files[level][0])
		}
	} else if seekCompaction {
		level = s.current.fileToCompactLevel
		c = newCompaction(s.options, level)
		c.inputs[0] = append(c.inputs[0], s.current.fileToCompact)
	} else {
		return nil
	}
	c.inputVersion = s.current
	c.inputVersion.ref()
	if level == 0 {
		smallest, largest := new(internalKey), new(internalKey)
		s.getRange(c.inputs[0], smallest, largest)
		// Note that the next call will discard the file we placed in
		// c->inputs_[0] earlier and replace it with an overlapping set
		// which will include the picked file.
		s.current.getOverlappingInputs(0, smallest, largest, &c.inputs[0])
		if len(c.inputs[0]) == 0 {
			panic("versionSet: len(c.inputs[0]) == 0")
		}
	}
	s.setupOtherInputs(c)
	return c
}

func findLargestKey(icmp *internalKeyComparator, files []*fileMetaData) (largestKey *internalKey, ok bool) {
	if len(files) == 0 {
		ok = false
		return
	}
	largestKey = &files[0].largest
	for _, f := range files {
		if icmp.compare(&f.largest, largestKey) > 0 {
			largestKey = &f.largest
		}
	}
	ok = true
	return
}

func findSmallestBoundaryFile(icmp *internalKeyComparator, levelFiles []*fileMetaData, largestKey *internalKey) *fileMetaData {
	userCmp := icmp.userComparator
	var smallestBoundaryFile *fileMetaData
	for _, f := range levelFiles {
		if icmp.compare(&f.smallest, largestKey) > 0 && userCmp.Compare(f.smallest.userKey(), largestKey.userKey()) == 0 {
			if smallestBoundaryFile == nil || icmp.compare(&f.smallest, &smallestBoundaryFile.smallest) < 0 {
				smallestBoundaryFile = f
			}
		}
	}
	return smallestBoundaryFile
}

// Extracts the largest file b1 from |compaction_files| and then searches for a
// b2 in |level_files| for which user_key(u1) = user_key(l2). If it finds such a
// file b2 (known as a boundary file) it adds it to |compaction_files| and then
// searches again using this new upper bound.
//
// If there are two blocks, b1=(l1, u1) and b2=(l2, u2) and
// user_key(u1) = user_key(l2), and if we compact b1 but not b2 then a
// subsequent get operation will yield an incorrect result because it will
// return the record from b2 in level i rather than from b1 because it searches
// level by level for records matching the supplied user key.
//
// parameters:
//   in     level_files:      List of files to search for boundary files.
//   in/out compaction_files: List of files to extend by adding boundary files.
func addBoundaryInputs(icmp *internalKeyComparator, levelFiles []*fileMetaData, compactionFiles *[]*fileMetaData) {
	var (
		largestKey *internalKey
		ok         bool
	)
	if largestKey, ok = findLargestKey(icmp, *compactionFiles); !ok {
		return
	}
	continueSearching := true
	for continueSearching {
		smallestBoundaryFile := findSmallestBoundaryFile(icmp, levelFiles, largestKey)
		if smallestBoundaryFile != nil {
			*compactionFiles = append(*compactionFiles, smallestBoundaryFile)
			largestKey = &smallestBoundaryFile.largest
		} else {
			continueSearching = false
		}
	}
}

func (s *versionSet) newFileNumber() (n uint64) {
	n = s.nextFileNumber
	s.nextFileNumber++
	return
}

func (s *versionSet) reuseFileNumber(fileNumber uint64) {
	if s.nextFileNumber == fileNumber+1 {
		s.nextFileNumber = fileNumber
	}
}

func (s *versionSet) setLastSequence(seq sequenceNumber) {
	if seq < s.lastSequence {
		panic("versionSet: seq < lastSequence")
	}
	s.lastSequence = seq
}

func (s *versionSet) needsCompaction() bool {
	v := s.current
	return v.compactionScore >= 1 || v.fileToCompact != nil
}

func (s *versionSet) setupOtherInputs(c *compaction) {
	level := c.level
	smallest, largest := new(internalKey), new(internalKey)
	addBoundaryInputs(s.icmp, s.current.files[level], &c.inputs[0])
	s.getRange(c.inputs[0], smallest, largest)
	s.current.getOverlappingInputs(level+1, smallest, largest, &c.inputs[1])

	allStart, allLimit := new(internalKey), new(internalKey)
	s.getRange2(c.inputs[0], c.inputs[1], allStart, allLimit)

	if len(c.inputs[1]) != 0 {
		var expanded0 []*fileMetaData
		s.current.getOverlappingInputs(level, allStart, allLimit, &expanded0)
		input0Size := totalFileSize(c.inputs[0])
		input1Size := totalFileSize(c.inputs[1])
		expanded0Size := totalFileSize(expanded0)
		if expanded0Size > int64(len(c.inputs[0])) && input1Size+expanded0Size < expandedCompactionByteSizeLimit(s.options) {
			newStart, newLimit := new(internalKey), new(internalKey)
			s.getRange(expanded0, newStart, newLimit)
			var expanded1 []*fileMetaData
			s.current.getOverlappingInputs(level+1, newStart, newLimit, &expanded1)
			if len(expanded1) == len(c.inputs[1]) {
				s.options.InfoLog.Printf("Expanding@%d %d+%d (%d+%d bytes) to %d+%d (%d+%d bytes)\n",
					level, len(c.inputs[0]), len(c.inputs[1]), input0Size, input1Size, len(expanded0), len(expanded1), expanded0Size, input1Size)
				smallest = newStart
				largest = newLimit
				c.inputs[0] = expanded0
				c.inputs[1] = expanded1
				s.getRange2(c.inputs[0], c.inputs[1], allStart, allLimit)
			}
		}
	}

	// Compute the set of grandparent files that overlap this compaction
	// (parent == level+1; grandparent == level+2)
	if level+2 < numLevels {
		s.current.getOverlappingInputs(level+2, allStart, allLimit, &c.grandParents)
	}

	// Update the place where we will do the next compaction for this level.
	// We update this immediately instead of waiting for the VersionEdit
	// to be applied so that if the compaction fails, we will try a different
	// key range next time.
	s.compactPointer[level] = largest.encode()
	c.edit.setCompactPointer(level, largest)
}

func (s *versionSet) compactRange(level int, begin, end *internalKey) *compaction {
	var inputs []*fileMetaData
	s.current.getOverlappingInputs(level, begin, end, &inputs)
	if len(inputs) == 0 {
		return nil
	}

	// Avoid compacting too much in one shot in case the range is large.
	// But we cannot do this for level-0 since level-0 files can overlap
	// and we must not pick one file and drop another older file if the
	// two files overlap.
	if level > 0 {
		limit := maxFileSizeForLevel(s.options, level)
		total := uint64(0)
		for i, input := range inputs {
			total += input.fileSize
			if total >= limit {
				inputs = inputs[:i+1]
				break
			}
		}
	}

	c := newCompaction(s.options, level)
	c.inputVersion = s.current
	c.inputVersion.ref()
	c.inputs[0] = inputs
	s.setupOtherInputs(c)
	return c
}

type bySmallestKey struct {
	internalComparator *internalKeyComparator
}

func (k *bySmallestKey) less(f1, f2 *fileMetaData) bool {
	if r := k.internalComparator.compare(&f1.smallest, &f2.smallest); r != 0 {
		return r < 0
	} else {
		return f1.number < f2.number
	}
}

type fileSet map[*fileMetaData]struct{}

type levelState struct {
	deletedFiles map[uint64]struct{}
	addedFiles   fileSet
}

type versionSetBuilder struct {
	vset   *versionSet
	base   *version
	levels [numLevels]levelState
}

func newVersionSetBuilder(vset *versionSet, base *version) *versionSetBuilder {
	b := &versionSetBuilder{
		vset:   vset,
		base:   base,
		levels: [numLevels]levelState{},
	}
	b.base.unref()
	_ = bySmallestKey{internalComparator: vset.icmp}
	for _, level := range b.levels {
		level.addedFiles = make(map[*fileMetaData]struct{})
	}
	return b
}

func (b *versionSetBuilder) finalize() {
	for level := 0; level < numLevels; level++ {
		added := b.levels[level].addedFiles
		toUnref := make([]*fileMetaData, 0, len(added))
		for k := range added {
			toUnref = append(toUnref, k)
		}
		sort.Sort(fileMetaDataSlice(toUnref))
		added = nil
		for _, f := range toUnref {
			f.refs--
			if f.refs <= 0 {
				f = nil
			}
		}
	}
	b.base.unref()
}

func (b *versionSetBuilder) apply(edit *versionEdit) {
	for _, p := range edit.compactPointers {
		level := p.first
		b.vset.compactPointer[level] = p.second.encode()
	}
	del := edit.deletedFiles
	files := make(deletedFileSlice, len(del))
	for k := range del {
		files = append(files, k)
	}
	sort.Sort(files)
	for _, f := range files {
		b.levels[f.first].deletedFiles[f.second] = struct{}{}
	}

	for _, file := range edit.newFiles {
		level := file.first
		f := &fileMetaData{
			refs:         file.second.refs,
			allowedSeeks: file.second.allowedSeeks,
			number:       file.second.number,
			fileSize:     file.second.fileSize,
			smallest:     file.second.smallest,
			largest:      file.second.largest,
		}
		f.refs = 1

		// We arrange to automatically compact this file after
		// a certain number of seeks.  Let's assume:
		//   (1) One seek costs 10ms
		//   (2) Writing or reading 1MB costs 10ms (100MB/s)
		//   (3) A compaction of 1MB does 25MB of IO:
		//         1MB read from this level
		//         10-12MB read from next level (boundaries may be misaligned)
		//         10-12MB written to next level
		// This implies that 25 seeks cost the same as the compaction
		// of 1MB of data.  I.e., one seek costs approximately the
		// same as the compaction of 40KB of data.  We are a little
		// conservative and allow approximately one seek for every 16KB
		// of data before triggering a compaction.
		if f.allowedSeeks = int(f.fileSize / 16384); f.allowedSeeks < 100 {
			f.allowedSeeks = 100
		}
		delete(b.levels[level].deletedFiles, f.number)
		b.levels[level].addedFiles[f] = struct{}{}
	}
}

func (b *versionSetBuilder) saveTo(v *version) {
	cmp := bySmallestKey{internalComparator: b.vset.icmp}
	var (
		baseFiles []*fileMetaData
		added     fileSet
	)
	for level := 0; level < numLevels; level++ {
		baseFiles = b.base.files[level]
		baseIndex, baseEnd := 0, len(baseFiles)
		added = b.levels[level].addedFiles
		tmp := make([]*fileMetaData, 0, len(added))
		for k := range added {
			tmp = append(tmp, k)
		}
		sort.Sort(fileMetaDataSlice(tmp))
		for _, f := range tmp {
			b.maybeAddFile(v, level, f)
			pos := sort.Search(len(baseFiles[baseIndex:]), func(i int) bool {
				return cmp.less(f, baseFiles[baseIndex+i])
			})
			for ; baseIndex != pos; baseIndex++ {
				b.maybeAddFile(v, level, baseFiles[baseIndex])
			}
		}
		for ; baseIndex < baseEnd; baseIndex++ {
			b.maybeAddFile(v, level, baseFiles[baseIndex])
		}
	}
}

func (b *versionSetBuilder) maybeAddFile(v *version, level int, f *fileMetaData) {
	if len(b.levels[level].deletedFiles) > 0 {
	} else {
		files := v.files[level]
		if level > 0 && len(files) != 0 {
			if b.vset.icmp.compare(&(files[len(files)-1].largest), &(f.smallest)) >= 0 {
				panic("versionSetBuilder: overlap")
			}
		}
		f.refs++
		v.files[level] = append(v.files[level], f)
	}
}

type levelSummaryStorage struct {
	buffer [100]byte
}

type compaction struct {
	level             int
	maxOutputFileSize uint64
	inputVersion      *version
	edit              versionEdit
	inputs            [2][]*fileMetaData
	grandParents      []*fileMetaData
	grandParentIndex  int
	seenKey           bool
	overlappedBytes   int64
	levelPtrs         [numLevels]int
}

func newCompaction(options *ssdb.Options, level int) *compaction {
	c := &compaction{
		level:             level,
		maxOutputFileSize: maxFileSizeForLevel(options, level),
		inputVersion:      nil,
		grandParents:      nil,
		grandParentIndex:  0,
		seenKey:           false,
		overlappedBytes:   0,
		levelPtrs:         [numLevels]int{},
	}
	return c
}

func (c *compaction) finalize() {
	if c.inputVersion != nil {
		c.inputVersion.unref()
	}
}

func (c *compaction) numInputFiles(which int) int {
	return len(c.inputs[which])
}

func (c *compaction) input(which, i int) *fileMetaData {
	return c.inputs[which][i]
}

func (c *compaction) isTrivialMove() bool {
	vset := c.inputVersion.vset
	return c.numInputFiles(0) == 1 && c.numInputFiles(1) == 0 &&
		totalFileSize(c.grandParents) <= maxGrandParentOverlapBytes(vset.options)
}

func (c *compaction) addInputDeletions(edit *versionEdit) {
	for which := 0; which < 2; which++ {
		for _, i := range c.inputs[which] {
			edit.deletedFile(c.level+which, i.number)
		}
	}
}

func (c *compaction) isBaseLevelForKey(userKey []byte) bool {
	userCmp := c.inputVersion.vset.icmp.userComparator
	for lvl := c.level + 2; lvl < numLevels; lvl++ {
		files := c.inputVersion.files[lvl]
		for c.levelPtrs[lvl] < len(files) {
			f := files[c.levelPtrs[lvl]]
			if userCmp.Compare(userKey, f.largest.userKey()) <= 0 {
				if userCmp.Compare(userKey, f.smallest.userKey()) >= 0 {
					return false
				}
				break
			}
			c.levelPtrs[lvl]++
		}
	}
	return true
}

func (c *compaction) shouldStopBefore(internalKey []byte) bool {
	vset := c.inputVersion.vset
	icmp := vset.icmp
	for c.grandParentIndex < len(c.grandParents) && icmp.Compare(internalKey, c.grandParents[c.grandParentIndex].largest.encode()) > 0 {
		if c.seenKey {
			c.overlappedBytes += int64(c.grandParents[c.grandParentIndex].fileSize)
		}
		c.grandParentIndex++
	}
	c.seenKey = true
	if c.overlappedBytes > maxGrandParentOverlapBytes(vset.options) {
		c.overlappedBytes = 0
		return true
	}
	return false
}

func (c *compaction) releaseInputs() {
	if c.inputVersion != nil {
		c.inputVersion.unref()
		c.inputVersion = nil
	}
}
