package db

import (
	"fmt"
	"ssdb"
	"ssdb/util"
)

const (
	_ = maxRecordType + iota
	eof
	badRecord
)

type logReader struct {
	file              ssdb.SequentialFile
	reporter          reporter
	checksum          bool
	buffer            []byte
	eof               bool
	lastRecordOffset  uint64
	endOfBufferOffset uint64
	initialOffset     uint64
	resyncing         bool
}

func (r *logReader) readRecord() (record []byte, ok bool) {
	if r.lastRecordOffset < r.initialOffset {
		if !r.skipToInitialBlock() {
			ok = false
			return
		}
	}

	inFragmentedRecord := false
	prospectiveRecordOffset := uint64(0)
	var fragment []byte
	var recordType recordType
	var physicalRecordOffset uint64
	for {
		fragment, recordType = r.readPhysicalRecord()
		physicalRecordOffset = r.endOfBufferOffset - uint64(len(r.buffer)) - headerSize - uint64(len(fragment))
		if r.resyncing {
			if recordType == middleType {
				continue
			} else if recordType == lastType {
				r.resyncing = false
				continue
			} else {
				r.resyncing = false
			}
		}

		switch recordType {
		case fullType:
			if inFragmentedRecord {
				// Handle bug in earlier versions of log::Writer where
				// it could emit an empty kFirstType record at the tail end
				// of a block followed by a kFullType or kFirstType record
				// at the beginning of the next block.
				if len(record) > 0 {
					r.reportCorruption(len(record), "partial record without end(1)")
				}
			}
			prospectiveRecordOffset = physicalRecordOffset
			record = fragment
			r.lastRecordOffset = prospectiveRecordOffset
			ok = true
			return
		case firstType:
			if inFragmentedRecord {
				// Handle bug in earlier versions of log::Writer where
				// it could emit an empty kFirstType record at the tail end
				// of a block followed by a kFullType or kFirstType record
				// at the beginning of the next block.
				if len(record) > 0 {
					r.reportCorruption(len(record), "partial record without end(2)")
				}
			}
			prospectiveRecordOffset = physicalRecordOffset
			record = fragment
			inFragmentedRecord = true
		case middleType:
			if !inFragmentedRecord {
				r.reportCorruption(len(fragment), "missing start of fragmented record(1)")
			} else {
				record = append(record, fragment...)
			}
		case lastType:
			if !inFragmentedRecord {
				r.reportCorruption(len(fragment), "missing start of fragmented record(2)")
			} else {
				record = append(record, fragment...)
				r.lastRecordOffset = prospectiveRecordOffset
				ok = true
				return
			}
		case eof:
			if inFragmentedRecord {
				// This can be caused by the writer dying immediately after
				// writing a physical record but before completing the next; don't
				// treat it as a corruption, just ignore the entire logical record.
				record = nil
			}
			ok = false
			return
		case badRecord:
			if inFragmentedRecord {
				r.reportCorruption(len(record), "error in middle of record")
				inFragmentedRecord = false
				record = nil
			}
		default:
			if inFragmentedRecord {
				r.reportCorruption(len(fragment)+len(record), fmt.Sprintf("unknown record type %d", recordType))
			} else {
				r.reportCorruption(len(fragment), fmt.Sprintf("unknown record type %d", recordType))
			}
			inFragmentedRecord = false
			record = nil
		}
	}
}

func (r *logReader) skipToInitialBlock() bool {
	offsetInBlock := r.initialOffset % blockSize
	blockStartLocation := r.initialOffset - offsetInBlock
	if offsetInBlock > blockSize-6 {
		blockStartLocation += blockSize
	}
	r.endOfBufferOffset = blockStartLocation
	if blockStartLocation > 0 {
		if err := r.file.Skip(blockStartLocation); err != nil {
			r.reportDrop(int(blockStartLocation), err)
			return false
		}
	}
	return true
}

func (r *logReader) readPhysicalRecord() (result []byte, rt recordType) {
	var (
		n   int
		err error
	)
	for {
		if len(r.buffer) < headerSize {
			if !r.eof {
				r.buffer = make([]byte, blockSize)
				if r.buffer, n, err = r.file.Read(r.buffer); err != nil {
					r.buffer = nil
					r.reportDrop(blockSize, err)
					r.eof = true
					rt = eof
					return
				} else if n < blockSize {
					r.buffer = r.buffer[:n]
					r.eof = true
				}
				r.endOfBufferOffset += uint64(n)
				continue
			} else {
				// Note that if buffer_ is non-empty, we have a truncated header at the
				// end of the file, which can be caused by the writer crashing in the
				// middle of writing the header. Instead of considering this an error,
				// just report EOF.
				r.buffer = nil
				rt = eof
				return
			}
		}
		a := uint32(r.buffer[4] & 0xff)
		b := uint32(r.buffer[5] & 0xff)
		rt = recordType(r.buffer[6])
		length := a | (b << 8)
		if headerSize+int(length) > len(r.buffer) {
			r.buffer = nil
			if !r.eof {
				r.reportCorruption(n, "bad record length")
				rt = badRecord
				return
			}
			// If the end of the file has been reached without reading |length| bytes
			// of payload, assume the writer died in the middle of writing the record.
			// Don't report a corruption.
			rt = eof
			return
		}

		if rt == zeroType && length == 0 {
			r.buffer = nil
			rt = badRecord
			return
		}

		if r.checksum {
			expected := util.UnmaskChecksum(util.DecodeFixed32(r.buffer))
			actual := util.ChecksumValue(r.buffer[6 : 6+1+length])
			if expected != actual {
				r.buffer = make([]byte, 0)
				r.reportCorruption(n, "checksum mismatch")
				rt = badRecord
				return
			}
		}

		result = r.buffer[headerSize : headerSize+length]
		r.buffer = r.buffer[headerSize+length:]
		if r.endOfBufferOffset-uint64(len(r.buffer))-uint64(headerSize)-uint64(length) < r.initialOffset {
			result = nil
			rt = badRecord
			return
		}
		return
	}
}

func (r *logReader) reportCorruption(bytes int, reason string) {
	r.reportDrop(bytes, util.CorruptionError1(reason))
}

func (r *logReader) reportDrop(bytes int, err error) {
	if r.reporter != nil && r.endOfBufferOffset-uint64(len(r.buffer))-uint64(bytes) >= r.initialOffset {
		r.reporter.corruption(bytes, err)
	}
}

func newLogReader(file ssdb.SequentialFile, reporter reporter, checksum bool, initialOffset uint64) *logReader {
	return &logReader{
		file:              file,
		reporter:          reporter,
		checksum:          checksum,
		buffer:            nil,
		eof:               false,
		lastRecordOffset:  0,
		endOfBufferOffset: 0,
		initialOffset:     initialOffset,
		resyncing:         initialOffset > 0,
	}
}

type reporter interface {
	corruption(bytes int, err error)
}
