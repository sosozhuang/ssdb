ssdb examples
=======

## Opening A Database
```go
    options := ssdb.NewOptions()
    options.CreateIfMissing = true
    d, err := db.Open(options, "/tmp/test_db")
```
## Closing A Database
```go
    //open db here...
    d.Close()
```
## Reads And Writes
```go
    err := d.Put(ssdb.NewWriteOptions(), key, value)
    value, err = d.Get(ssdb.NewReadOptions(), key)
    err = d.Delete(ssdb.NewWriteOptions(), key)
```
## Atomic Updates
```go
    batch := ssdb.NewWriteBatch()
    batch.Delete(key1)
    batch.Put(key2, value)
    err := d.Write(ssdb.NewWriteOptions(), batch)
```
## Synchronous Writes
```go
    options := ssdb.NewWriteOptions()
    options.Sync = true
    err = d.Put(options, key, value)
```
## Iteration
```go
    it := d.NewIterator(ssdb.NewReadOptions())
    for it.SeekToFirst(); it.Valid(); it.Next() {
        it.Key()
        it.Value()
    }
    err := it.Status()
    it.Close()
```
process entries in reverse order.
```go
    for it.SeekToLast(); it.Valid(); it.Prev() {
    }
```
## Snapshots
```go
    options := ssdb.NewReadOptions()
    options.Snapshot = d.GetSnapshot()
    //apply some updates
    iter := d.NewIterator(options)
    //read using iter to view the snapshot state
    iter.Close()
    d.ReleaseSnapshot(options.Snapshot)
```
## Comparators
```go
type twoPartComparator struct {
}

func (c *twoPartComparator) Compare(a, b []byte) int {
    var a1, a2, b1, b2 int
    ParseKey(a, &a1, &a2)
    ParseKey(b, &b1, &b2)
    if a1 < b1 {
        return -1
    }
    if a1 > b1 {
        return +1
    }
    if a2 < b2 {
        return -1
    }
    if a2 > b2 {
        return +1
    }
    return 0
}

func (c *twoPartComparator) Name() string {
    return "TwoPartComparator"
}

func (c *twoPartComparator) FindShortestSeparator(start *[]byte, limit []byte) {
}

func (c *twoPartComparator) FindShortSuccessor(key *[]byte) {
}
```
Create a db using the custom comparator.
```go
    cmp := new(twoPartComparator)
    options := ssdb.NewOptions()
    options.CreateIfMissing = true
    options.Comparator = cmp
    d, err := db.Open(options, "/tmp/test_db")
```
### Compression
```go
    options := ssdb.NewOptions()
    options.CompressionType = ssdb.NoCompression
    d, err := db.Open(options, "/tmp/test_db")
```
### Cache
```go
    options := ssdb.NewOptions()
    options.BlockCache = ssdb.NewLRUCache(100 * 1048576)
    d, err := db.Open(options, "/tmp/test_db")
    // use the db
    d.Close()
    options.BlockCache.Clear()
```
### Filters
```go
    options := ssdb.NewOptions()
    options.FilterPolicy = ssdb.NewBloomFilterPolicy(10)
    d, err = db.Open(options, "/tmp/test_db")
    //use the db
    d.Close()    
```
## Approximate Sizes
```go
    ranges := make([]ssdb.Range, 2)
    ranges[0] = ssdb.Range{
        Start: []byte("a"),
        Limit: []byte("c"),
    }
    ranges[1] = ssdb.Range{
        Start: []byte("x"),
        Limit: []byte("z"),
    }
    sizes := d.GetApproximateSizes(ranges)
```
## Environment
```go
    env := new(slowEnv)
    options := ssdb.NewOptions()
    options.Env = env
    err := db.Open(options, "/tmp/test_db")
```