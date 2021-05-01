package db

import (
	"ssdb/util"
	"testing"
)

func TestParse(t *testing.T) {
	type ts struct {
		name   string
		number uint64
		ft     fileType
	}
	cases := []ts{
		{"100.log", 100, logFile},
		{"0.log", 0, logFile},
		{"0.sst", 0, tableFile},
		{"0.ldb", 0, tableFile},
		{"CURRENT", 0, currentFile},
		{"LOCK", 0, dbLockFile},
		{"MANIFEST-2", 2, descriptorFile},
		{"MANIFEST-7", 7, descriptorFile},
		{"LOG", 0, infoLogFile},
		{"LOG.old", 0, infoLogFile},
		{"18446744073709551615.log", 18446744073709551615, logFile},
	}

	var (
		ft     fileType
		number uint64
	)
	for i := 0; i < len(cases); i++ {
		if !parseFileName(cases[i].name, &number, &ft) {
			t.Errorf("parse file name [%s] not ok.\n", cases[i].name)
		}
		if cases[i].ft != ft {
			t.Errorf("fileType not match, expected: %d. actual: %d.\n", cases[i].ft, ft)
		}
		if cases[i].number != number {
			t.Errorf("number not match, expected: %d, actual: %d.\n", cases[i].number, number)
		}
	}

	errors := []string{
		"",
		"foo",
		"foo-dx-100.log",
		".log",
		"",
		"manifest",
		"CURREN",
		"CURRENTX",
		"MANIFES",
		"MANIFEST",
		"MANIFEST-",
		"XMANIFEST-3",
		"MANIFEST-3x",
		"LOC",
		"LOCKx",
		"LO",
		"LOGx",
		"18446744073709551616.log",
		"184467440737095516150.log",
		"100",
		"100.",
		"100.lop",
	}
	for _, e := range errors {
		if parseFileName(e, &number, &ft) {
			t.Errorf("parse file name [%s] not ok.\n", e)
		}
	}
}

func TestConstruction(t *testing.T) {
	var (
		number uint64
		ft     fileType
	)
	name := currentFileName("foo")
	util.AssertEqual("foo/", name[:4], "currentFileName", t)
	util.AssertTrue(parseFileName(name[4:], &number, &ft), "parseFileName", t)
	util.AssertEqual(uint64(0), number, "parseFileName number", t)
	util.AssertEqual(currentFile, ft, "parseFileName type", t)

	name = lockFileName("foo")
	util.AssertEqual("foo/", name[:4], "currentFileName", t)
	util.AssertTrue(parseFileName(name[4:], &number, &ft), "parseFileName", t)
	util.AssertEqual(uint64(0), number, "parseFileName number", t)
	util.AssertEqual(dbLockFile, ft, "parseFileName type", t)

	name = logFileName("foo", 192)
	util.AssertEqual("foo/", name[:4], "currentFileName", t)
	util.AssertTrue(parseFileName(name[4:], &number, &ft), "parseFileName", t)
	util.AssertEqual(uint64(192), number, "parseFileName number", t)
	util.AssertEqual(logFile, ft, "parseFileName type", t)

	name = tableFileName("bar", 200)
	util.AssertEqual("bar/", name[:4], "currentFileName", t)
	util.AssertTrue(parseFileName(name[4:], &number, &ft), "parseFileName", t)
	util.AssertEqual(uint64(200), number, "parseFileName number", t)
	util.AssertEqual(tableFile, ft, "parseFileName type", t)

	name = descriptorFileName("bar", 100)
	util.AssertEqual("bar/", name[:4], "currentFileName", t)
	util.AssertTrue(parseFileName(name[4:], &number, &ft), "parseFileName", t)
	util.AssertEqual(uint64(100), number, "parseFileName number", t)
	util.AssertEqual(descriptorFile, ft, "parseFileName type", t)

	name = tempFileName("tmp", 999)
	util.AssertEqual("tmp/", name[:4], "currentFileName", t)
	util.AssertTrue(parseFileName(name[4:], &number, &ft), "parseFileName", t)
	util.AssertEqual(uint64(999), number, "parseFileName number", t)
	util.AssertEqual(tempFile, ft, "parseFileName type", t)

	name = infoLogFileName("foo")
	util.AssertEqual("foo/", name[:4], "currentFileName", t)
	util.AssertTrue(parseFileName(name[4:], &number, &ft), "parseFileName", t)
	util.AssertEqual(uint64(0), number, "parseFileName number", t)
	util.AssertEqual(infoLogFile, ft, "parseFileName type", t)

	name = oldInfoLogFileName("foo")
	util.AssertEqual("foo/", name[:4], "currentFileName", t)
	util.AssertTrue(parseFileName(name[4:], &number, &ft), "parseFileName", t)
	util.AssertEqual(uint64(0), number, "parseFileName number", t)
	util.AssertEqual(infoLogFile, ft, "parseFileName type", t)
}
