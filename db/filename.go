package db

import (
	"bytes"
	"fmt"
	"ssdb"
	"ssdb/util"
	"strings"
)

type fileType uint8

const (
	logFile fileType = iota
	dbLockFile
	tableFile
	descriptorFile
	currentFile
	tempFile
	infoLogFile
)

func makeFileName(db string, number uint64, suffix string) string {
	buffer := bytes.NewBufferString("")
	fmt.Fprintf(buffer, "/%06d.%s", number, suffix)
	return db + buffer.String()
}

func logFileName(db string, number uint64) string {
	return makeFileName(db, number, "log")
}

func tableFileName(db string, number uint64) string {
	return makeFileName(db, number, "ssdb")
}

func sstTableFileName(db string, number uint64) string {
	return makeFileName(db, number, "sst")
}

func descriptorFileName(db string, number uint64) string {
	buffer := bytes.NewBufferString("")
	fmt.Fprintf(buffer, "/MANIFEST-%06d", number)
	return db + buffer.String()
}

func currentFileName(db string) string {
	return db + "/CURRENT"
}

func lockFileName(db string) string {
	return db + "/LOCK"
}

func tempFileName(db string, number uint64) string {
	return makeFileName(db, number, "dbtmp")
}

func infoLogFileName(db string) string {
	return db + "/LOG"
}

func oldInfoLogFileName(db string) string {
	return db + "/LOG.old"
}

func parseFileName(filename string, number *uint64, ft *fileType) (ok bool) {
	if filename == "CURRENT" {
		*number = 0
		*ft = currentFile
	} else if filename == "LOCK" {
		*number = 0
		*ft = dbLockFile
	} else if filename == "LOG" || filename == "LOG.old" {
		*number = 0
		*ft = infoLogFile
	} else if strings.HasPrefix(filename, "MANIFEST-") {
		filename = filename[len("MANIFEST-"):]
		if !util.ConsumeDecimalNumber(&filename, number) {
			ok = false
			return
		}
		if len(filename) != 0 {
			ok = false
			return
		}
		*ft = descriptorFile
	} else {
		if !util.ConsumeDecimalNumber(&filename, number) {
			ok = false
			return
		}
		suffix := filename
		if suffix == ".log" {
			*ft = logFile
		} else if suffix == ".sst" || suffix == ".ssdb" {
			*ft = tableFile
		} else if suffix == ".dbtmp" {
			*ft = tempFile
		} else {
			ok = false
			return
		}
	}
	ok = true
	return
}

func setCurrentFile(env ssdb.Env, db string, descriptorNumber uint64) error {
	manifest := descriptorFileName(db, descriptorNumber)
	if !strings.HasPrefix(manifest, db+"/") {
		panic("manifest not start with " + db + "/")
	}
	contents := manifest[len(db)+1:]
	tmp := tempFileName(db, descriptorNumber)
	var err error
	if err = ssdb.WriteStringToFileSync(env, []byte(contents+"\n"), tmp); err == nil {
		err = env.RenameFile(tmp, currentFileName(db))
	}
	if err != nil {
		_ = env.DeleteFile(tmp)
	}
	return err
}
