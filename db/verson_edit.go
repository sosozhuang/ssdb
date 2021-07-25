package db

import (
	"sort"
	"ssdb/util"
	"strings"
)

const (
	tagComparator = 1 + iota
	tagLogNumber
	tagNexFileNumber
	tagLastSequence
	tagCompactPointer
	tagDeletedFile
	tagNewFile
	tagPrevLogNumber = 9
)

type fileMetaData struct {
	refs         int
	allowedSeeks int
	number       uint64
	fileSize     uint64
	smallest     internalKey
	largest      internalKey
}

func newFileMetaData() *fileMetaData {
	return &fileMetaData{
		refs:         0,
		allowedSeeks: 1 << 30,
		fileSize:     0,
	}
}

type intAndUint64 struct {
	first  int
	second uint64
}

type intAndInternalKey struct {
	first  int
	second internalKey
}

type intAndFileMetaData struct {
	first  int
	second fileMetaData
}

type deletedFileSet map[intAndUint64]struct{}
type deletedFileSlice []intAndUint64

func (s deletedFileSlice) Len() int {
	return len(s)
}

func (s deletedFileSlice) Less(i, j int) bool {
	return s[i].first < s[j].first || (s[i].first == s[j].first && s[i].second < s[j].second)
}

func (s deletedFileSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

type versionEdit struct {
	comparator        string
	logNumber         uint64
	prevLogNumber     uint64
	nextFileNumber    uint64
	lastSequence      sequenceNumber
	hasComparator     bool
	hasLogNumber      bool
	hasPrevLogNumber  bool
	hasNextFileNumber bool
	hasLastSequence   bool
	compactPointers   []intAndInternalKey
	deletedFiles      deletedFileSet
	newFiles          []intAndFileMetaData
}

func newVersionEdit() *versionEdit {
	e := new(versionEdit)
	e.clear()
	return e
}

func (e *versionEdit) clear() {
	e.comparator = ""
	e.logNumber = 0
	e.prevLogNumber = 0
	e.lastSequence = 0
	e.nextFileNumber = 0
	e.hasComparator = false
	e.hasLogNumber = false
	e.hasPrevLogNumber = false
	e.hasNextFileNumber = false
	e.hasLastSequence = false
	e.compactPointers = make([]intAndInternalKey, 0)
	e.deletedFiles = make(map[intAndUint64]struct{})
	e.newFiles = make([]intAndFileMetaData, 0)
}

func (e *versionEdit) setComparatorName(name string) {
	e.hasComparator = true
	e.comparator = name
}

func (e *versionEdit) setLogNumber(num uint64) {
	e.hasLogNumber = true
	e.logNumber = num
}

func (e *versionEdit) setPrevLogNumber(num uint64) {
	e.hasPrevLogNumber = true
	e.prevLogNumber = num
}

func (e *versionEdit) setNextFile(num uint64) {
	e.hasNextFileNumber = true
	e.nextFileNumber = num
}

func (e *versionEdit) setLastSequence(seq sequenceNumber) {
	e.hasLastSequence = true
	e.lastSequence = seq
}

func (e *versionEdit) setCompactPointer(level int, key *internalKey) {
	e.compactPointers = append(e.compactPointers, intAndInternalKey{level, *key})
}

// Add the specified file at the specified number.
// REQUIRES: This version has not been saved (see VersionSet::SaveTo)
// REQUIRES: "smallest" and "largest" are smallest and largest keys in file
func (e *versionEdit) addFile(level int, file uint64, fileSize uint64, smallest, largest internalKey) {
	f := newFileMetaData()
	f.number = file
	f.fileSize = fileSize
	f.smallest = smallest
	f.largest = largest
	e.newFiles = append(e.newFiles, intAndFileMetaData{level, *f})
}

func (e *versionEdit) deletedFile(level int, file uint64) {
	e.deletedFiles[intAndUint64{level, file}] = struct{}{}
}

func (e *versionEdit) encodeTo(dst *[]byte) {
	if e.hasComparator {
		util.PutVarInt32(dst, tagComparator)
		util.PutLengthPrefixedSlice(dst, []byte(e.comparator))
	}
	if e.hasLogNumber {
		util.PutVarInt32(dst, tagLogNumber)
		util.PutVarInt64(dst, e.logNumber)
	}
	if e.hasPrevLogNumber {
		util.PutVarInt32(dst, tagPrevLogNumber)
		util.PutVarInt64(dst, e.prevLogNumber)
	}
	if e.hasNextFileNumber {
		util.PutVarInt32(dst, tagNexFileNumber)
		util.PutVarInt64(dst, e.nextFileNumber)
	}
	if e.hasLastSequence {
		util.PutVarInt32(dst, tagLastSequence)
		util.PutVarInt64(dst, uint64(e.lastSequence))
	}
	for _, p := range e.compactPointers {
		util.PutVarInt32(dst, tagCompactPointer)
		util.PutVarInt32(dst, uint32(p.first))
		util.PutLengthPrefixedSlice(dst, p.second.encode())
	}

	files := make(deletedFileSlice, 0, len(e.deletedFiles))
	for k := range e.deletedFiles {
		files = append(files, k)
	}
	sort.Sort(files)
	for _, f := range files {
		util.PutVarInt32(dst, tagDeletedFile)
		util.PutVarInt32(dst, uint32(f.first))
		util.PutVarInt64(dst, f.second)
	}
	for _, file := range e.newFiles {
		f := file.second
		util.PutVarInt32(dst, tagNewFile)
		util.PutVarInt32(dst, uint32(file.first))
		util.PutVarInt64(dst, f.number)
		util.PutVarInt64(dst, f.fileSize)
		util.PutLengthPrefixedSlice(dst, f.smallest.encode())
		util.PutLengthPrefixedSlice(dst, f.largest.encode())
	}
}

func getInternalKey(input *[]byte, dst *internalKey) bool {
	var b []byte
	if util.GetLengthPrefixedSlice(input, &b) {
		dst.decodeFrom(b)
		return true
	}
	return false
}

func getLevel(input *[]byte, level *int) bool {
	var v uint32
	if util.GetVarInt32(input, &v) && v < numLevels {
		*level = int(v)
		return true
	}
	return false
}

func (e *versionEdit) decodeFrom(src []byte) error {
	e.clear()
	input := src
	var (
		msg    string
		tag    uint32
		level  int
		number uint64
		f      fileMetaData
		str    []byte
		key    internalKey
	)
	f = *newFileMetaData()
	for msg == "" && util.GetVarInt32(&input, &tag) {
		switch tag {
		case tagComparator:
			if util.GetLengthPrefixedSlice(&input, &str) {
				e.comparator = string(str)
				e.hasComparator = true
			} else {
				msg = "comparator name"
			}
		case tagLogNumber:
			if util.GetVarInt64(&input, &e.logNumber) {
				e.hasLogNumber = true
			} else {
				msg = "log number"
			}
		case tagPrevLogNumber:
			if util.GetVarInt64(&input, &e.prevLogNumber) {
				e.hasPrevLogNumber = true
			} else {
				msg = "previous log number"
			}
		case tagNexFileNumber:
			if util.GetVarInt64(&input, &e.nextFileNumber) {
				e.hasNextFileNumber = true
			} else {
				msg = "next file number"
			}
		case tagLastSequence:
			if util.GetVarInt64(&input, (*uint64)(&e.lastSequence)) {
				e.hasLastSequence = true
			} else {
				msg = "last sequence number"
			}
		case tagCompactPointer:
			if getLevel(&input, &level) && getInternalKey(&input, &key) {
				e.compactPointers = append(e.compactPointers, intAndInternalKey{level, key})
			} else {
				msg = "compaction pointer"
			}
		case tagDeletedFile:
			if getLevel(&input, &level) && util.GetVarInt64(&input, &number) {
				e.deletedFiles[intAndUint64{level, number}] = struct{}{}
			} else {
				msg = "deleted file"
			}
		case tagNewFile:
			if getLevel(&input, &level) && util.GetVarInt64(&input, &f.number) &&
				util.GetVarInt64(&input, &f.fileSize) && getInternalKey(&input, &f.smallest) &&
				getInternalKey(&input, &f.largest) {
				e.newFiles = append(e.newFiles, intAndFileMetaData{level, f})
			} else {
				msg = "new-file entry"
			}
		default:
			msg = "unknown tag"
		}
	}
	if msg == "" && len(input) > 0 {
		msg = "invalid tag"
	}
	if msg != "" {
		return util.CorruptionError2("VersionEdit", msg)
	}
	return nil
}

func (e *versionEdit) debugString() string {
	var builder strings.Builder
	builder.WriteString("VersionEdit {")
	if e.hasComparator {
		builder.WriteString("\n  Comparator: ")
		builder.WriteString(e.comparator)
	}
	if e.hasLogNumber {
		builder.WriteString("\n  LogNumber: ")
		util.AppendNumberTo(&builder, e.logNumber)
	}
	if e.hasPrevLogNumber {
		builder.WriteString("\n  PrevLogNumber: ")
		util.AppendNumberTo(&builder, e.prevLogNumber)
	}
	if e.hasNextFileNumber {
		builder.WriteString("\n  NextFile: ")
		util.AppendNumberTo(&builder, e.nextFileNumber)
	}
	if e.hasLastSequence {
		builder.WriteString("\n  LastSeq: ")
		util.AppendNumberTo(&builder, uint64(e.lastSequence))
	}
	for _, p := range e.compactPointers {
		builder.WriteString("\n  CompactPointer: ")
		util.AppendNumberTo(&builder, uint64(p.first))
		builder.WriteByte(' ')
		builder.WriteString(p.second.debugString())
	}
	for k := range e.deletedFiles {
		builder.WriteString("\n  DeleteFile: ")
		util.AppendNumberTo(&builder, uint64(k.first))
		builder.WriteByte(' ')
		util.AppendNumberTo(&builder, k.second)
	}
	for _, file := range e.newFiles {
		f := file.second
		builder.WriteString("\n  AddFile: ")
		util.AppendNumberTo(&builder, uint64(file.first))
		builder.WriteByte(' ')
		util.AppendNumberTo(&builder, f.number)
		builder.WriteByte(' ')
		util.AppendNumberTo(&builder, f.fileSize)
		builder.WriteByte(' ')
		builder.WriteString(f.smallest.debugString())
		builder.WriteString(" .. ")
		builder.WriteString(f.largest.debugString())

	}
	builder.WriteString("\n}\n")
	return builder.String()
}
