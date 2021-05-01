package ssdb

import (
	"os"
	"ssdb/util"
	"testing"
)

const (
	posixReadOnlyFileLimit = 4
	posixMMapLimit         = 4
)

func setPosixFileLimits(readOnlyFileLimit, mmapLimit int, t *testing.T) {
	setPosixReadOnlyFDLimit(readOnlyFileLimit, t)
	setPosixReadOnlyMMapLimit(mmapLimit, t)
}

func setPosixReadOnlyFDLimit(limit int, t *testing.T) {
	util.AssertTrue(e == nil, "e == nil", t)
	openReadOnlyFileLimit = limit
}

func setPosixReadOnlyMMapLimit(limit int, t *testing.T) {
	util.AssertTrue(e == nil, "e == nil", t)
	defaultMmapLimit = limit
}

func TestPosixOpenOnRead(t *testing.T) {
	setPosixFileLimits(posixReadOnlyFileLimit, posixMMapLimit, t)
	env := DefaultEnv()
	testDir, err := env.GetTestDirectory()
	if err != nil {
		t.Fatal(err)
	}
	testFile := testDir + "/open_on_read.txt"
	f, err := os.OpenFile(testFile, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		t.Fatal(err)
	}
	fileData := []byte("abcdefghijklmnopqrstuvwxyz")
	if _, err = f.Write(fileData); err != nil {
		t.Error(err)
	}
	_ = f.Close()

	numFiles := posixReadOnlyFileLimit + posixMMapLimit + 5
	files := make([]RandomAccessFile, numFiles)
	for i := range files {
		if files[i], err = env.NewRandomAccessFile(testFile); err != nil {
			t.Error(err)
		}
	}

	read := make([]byte, 1)
	var result []byte
	for i, file := range files {
		if result, _, err = file.Read(read, int64(i)); err != nil {
			t.Error(err)
		}
		util.AssertEqual(fileData[i], result[0], "read file", t)
	}
	files = nil
	if err = env.DeleteFile(testFile); err != nil {
		t.Error(err)
	}
}
