# ssdb
`ssdb`(sorted string database) is a library that provides persistent key value for storing data on disk. It is an implementation of [LevelDB](https://github.com/google/leveldb) in Go, including every feature in LevelDB. The core storage architecture is a Log Structured Merge Tree.

## Getting Started
### Example Code
```go
package main

import (
	"fmt"
	"os"
	"ssdb"
	"ssdb/db"
)

func main() {
	options := ssdb.NewOptions()
	options.CreateIfMissing = true
	var (
		d ssdb.DB
		err error
	)
	if d, err = db.Open(options, "/tmp/test_db"); err != nil {
		fmt.Fprintf(os.Stderr, "Open db error: %v.\n", err)
		os.Exit(1)
	}
	defer d.Close()
	key := []byte("hello")
	if _, err = d.Get(ssdb.NewReadOptions(), key); !ssdb.IsNotFound(err) {
		fmt.Fprintln(os.Stderr, "Expect value not found.")
		os.Exit(1)
	}
	value := []byte("world")
	if err = d.Put(ssdb.NewWriteOptions(), key, value); err != nil {
		fmt.Fprintf(os.Stderr, "Put kv error: %v.\n", err)
		os.Exit(1)
	}
	var v []byte
	if v, err = d.Get(ssdb.NewReadOptions(), key); err != nil || string(v) != "world"{
		fmt.Fprintf(os.Stderr, "Get value error: %v.\n", err)
		os.Exit(1)
	}
}
```
More examples can be found [here](https://github.com/sosozhuang/ssdb/blob/master/examples.md).

## Performance
A benchmark program [dbbench](https://github.com/sosozhuang/ssdb/tree/master/cmd/dbbench) is written in order to test performance.

### Setup
    SSDB:       version 1.22
    Date:       2021-08-03T21:50:13
    CPU:        8
    CPUCache:   8192 KB
    Keys:       16 bytes each
    Values:     100 bytes each (50 bytes after compression)
    Entries:    1000000
    RawSize:    110.6 MB (estimated)
    FileSize:   62.9 MB (estimated)

### Write performance

    fillseq      :       5.922 micros/op;   18.7 MB/s
    fillsync     :     600.972 micros/op;    0.2 MB/s (10000 ops)
    fillrandom   :       7.661 micros/op;   14.4 MB/s
    overwrite    :       7.917 micros/op;   14.0 MB/s

### Read performance

    readrandom  :  8.105 micros/op;  (approximately 12,338 reads per second)
    readseq     :  0.548 micros/op;  111.3 MB/s
    readreverse :  0.803 micros/op;  76.0 MB/s

Read performance after compactions is better.

    readrandom  :  5.015 micros/op;  (approximately 19,940 reads per second)
    readseq     :  0.273 micros/op;  223.3 MB/s
    readreverse :  0.338 micros/op;  180.7 MB/s

Read performance improves if enough memory cache are provided, so the uncompressed block can hold in memory.

    readrandom  : 5.153 micros/op;  (approximately 19,406 reads per second before compaction)
    readrandom  : 2.838 micros/op;  (approximately 35,236 reads per second after compaction)
