package main

import (
	"bytes"
	"flag"
	"fmt"
	"github.com/golang/snappy"
	"os"
	"runtime"
	"runtime/pprof"
	"ssdb"
	"ssdb/db"
	"ssdb/util"
	"strings"
	"sync"
	"time"
)

var flagsBenchmarks = []string{
	"fillseq",
	"fillsync",
	"fillrandom",
	"overwrite",
	"readrandom",
	"readrandom", // Extra run to allow previous compactions to quiesce
	"readseq",
	"readreverse",
	"compact",
	"readrandom",
	"readseq",
	"readreverse",
	"fill100K",
	"crc32c",
	"snappycomp",
	"snappyuncomp",
}

var (
	flagsNum              = 1000000
	flagsReads            = -1
	flagsThreads          = 1
	flagsValueSize        = 100
	flagsCompressionRatio = 0.5
	flagsHistogram        = false
	flagsWriteBufferSize  = 0
	flagsMaxFileSize      = 0
	flagsBlockSize        = 0
	flagsCacheSize        = -1
	flagsOpenFiles        = 0
	flagsBloomBits        = -1
	flagsUseExistingDB    = false
	flagsReuseLogs        = false
	flagsDB               = ""
)

var env ssdb.Env

type randomGenerator struct {
	data []byte
	pos  int
}

func newRandomGenerator() *randomGenerator {
	g := &randomGenerator{
		data: make([]byte, 0, 1048576),
		pos:  0,
	}
	rnd := util.NewRandom(301)
	for len(g.data) < 1048576 {
		piece := util.CompressibleString(rnd, flagsCompressionRatio, 100)
		g.data = append(g.data, piece...)
	}
	return g
}

func (g *randomGenerator) generate(l int) []byte {
	if g.pos+l > len(g.data) {
		g.pos = 0
		if l >= len(g.data) {
			panic("randomGenerator: l > len(data)")
		}
	}
	g.pos += l
	return g.data[g.pos-l : g.pos]
}

func appendWithSpace(b *[]byte, msg []byte) {
	if len(msg) == 0 {
		return
	}
	if len(*b) != 0 {
		*b = append(*b, ' ')
	}
	*b = append(*b, msg...)
}

type stats struct {
	start        float64
	finish       float64
	seconds      float64
	done         int
	nextReport   int
	bytes        int64
	lastOpFinish float64
	hist         util.Histogram
	message      []byte
}

func newStats() *stats {
	s := new(stats)
	s.doStart()
	return s
}

func (s *stats) doStart() {
	s.nextReport = 100
	s.lastOpFinish = s.start
	s.hist.Clear()
	s.done = 0
	s.bytes = 0
	s.seconds = 0
	s.start = float64(env.NowMicros())
	s.finish = s.start
	s.message = []byte{}
}

func (s *stats) merge(other *stats) {
	s.hist.Merge(&other.hist)
	s.done += other.done
	s.bytes += other.bytes
	s.seconds += other.seconds
	if other.start < s.start {
		s.start = other.start
	}
	if other.finish > s.finish {
		s.finish = other.finish
	}
	if len(s.message) == 0 {
		s.message = other.message
	}
}

func (s *stats) doStop() {
	s.finish = float64(env.NowMicros())
	s.seconds = (s.finish - s.start) * 1e-6
}

func (s *stats) addMessage(msg []byte) {
	appendWithSpace(&s.message, msg)
}

func (s *stats) finishSingleOp() {
	if flagsHistogram {
		now := env.NowMicros()
		micros := float64(now) - s.lastOpFinish
		s.hist.Add(micros)
		if micros > 20000 {
			fmt.Fprintf(os.Stderr, "long op: %.1f micros%30s\r", micros, "")
		}
		s.lastOpFinish = float64(now)
	}
	s.done++
	if s.done >= s.nextReport {
		if s.nextReport < 1000 {
			s.nextReport += 100
		} else if s.nextReport < 5000 {
			s.nextReport += 500
		} else if s.nextReport < 10000 {
			s.nextReport += 1000
		} else if s.nextReport < 50000 {
			s.nextReport += 5000
		} else if s.nextReport < 100000 {
			s.nextReport += 10000
		} else if s.nextReport < 500000 {
			s.nextReport += 50000
		} else {
			s.nextReport += 100000
		}
		fmt.Fprintf(os.Stderr, "... finished %d ops%30s\r", s.done, "")
	}
}

func (s *stats) addBytes(n int64) {
	s.bytes += n
}

func (s *stats) report(name string) {
	if s.done < 1 {
		s.done = 1
	}
	buf := bytes.NewBufferString("")
	if s.bytes > 0 {
		elapsed := (s.finish - s.start) * 1e-6
		fmt.Fprintf(buf, "%6.1f MB/s", float64(s.bytes)/1048576.0/elapsed)
	}
	extra := buf.Bytes()
	appendWithSpace(&extra, s.message)
	var c string
	if len(extra) == 0 {
		c = ""
	} else {
		c = " "
	}
	fmt.Fprintf(os.Stdout, "%-12s : %11.3f micros/op;%s%s\n", name, s.seconds*1e6/float64(s.done), c, extra)
	if flagsHistogram {
		fmt.Fprintf(os.Stdout, "Microseconds per op:\n%s\n", s.hist.String())
	}
}

type sharedState struct {
	mu             sync.Mutex
	cond           *sync.Cond
	total          int
	numInitialized int
	numDone        int
	start          bool
}

func newSharedState(total int) *sharedState {
	s := &sharedState{
		total:          total,
		numInitialized: 0,
		numDone:        0,
		start:          false,
	}
	s.cond = sync.NewCond(&s.mu)
	return s
}

type threadState struct {
	tid    int
	rand   *util.Random
	stats  *stats
	shared *sharedState
}

func newThreadState(index int) *threadState {
	return &threadState{
		tid:    index,
		rand:   util.NewRandom(uint32(1000 + index)),
		stats:  newStats(),
		shared: nil,
	}
}

type benchmark struct {
	cache           ssdb.Cache
	filterPolicy    ssdb.FilterPolicy
	db              ssdb.DB
	num             int
	valueSize       int
	entriesPerBatch int
	writeOptions    *ssdb.WriteOptions
	reads           int
	heapCounter     int
}

func newBenchmark() *benchmark {
	b := &benchmark{
		cache:           nil,
		filterPolicy:    nil,
		db:              nil,
		num:             flagsNum,
		valueSize:       flagsValueSize,
		entriesPerBatch: 1,
		writeOptions:    ssdb.NewWriteOptions(),
		heapCounter:     0,
	}
	if flagsCacheSize >= 0 {
		b.cache = ssdb.NewLRUCache(flagsCacheSize)
	}
	if flagsBloomBits >= 0 {
		b.filterPolicy = ssdb.NewBloomFilterPolicy(flagsBloomBits)
	}
	if flagsReads < 0 {
		b.reads = flagsNum
	} else {
		b.reads = flagsReads
	}
	files, _ := env.GetChildren(flagsDB)
	for _, file := range files {
		if strings.HasPrefix(file, "heap-") {
			_ = env.DeleteFile(flagsDB + "/" + file)
		}
	}
	if !flagsUseExistingDB {
		_ = db.Destroy("", ssdb.NewOptions())
	}
	return b
}

func (b *benchmark) finish() {
	b.db.Close()
	b.cache.Clear()
}

func (b *benchmark) printHeader() {
	const keySize = 16
	b.printEnvironment()
	fmt.Fprintf(os.Stdout, "Keys:       %d bytes each\n", keySize)
	fmt.Fprintf(os.Stdout, "Values:     %d bytes each (%d bytes after compression)\n", flagsValueSize, int64(float64(flagsValueSize)*flagsCompressionRatio+0.5))
	fmt.Fprintf(os.Stdout, "Entries:    %d\n", b.num)
	fmt.Fprintf(os.Stdout, "RawSize:    %.1f MB (estimated)\n", float64(keySize+flagsValueSize)*float64(b.num)/1048576.0)
	fmt.Fprintf(os.Stdout, "FileSize:   %.1f MB (estimated)\n", (keySize+float64(flagsValueSize)*flagsCompressionRatio)*float64(b.num)/1048576.0)
	fmt.Fprintf(os.Stdout, "------------------------------------------------\n")
}

func (b *benchmark) printEnvironment() {
	fmt.Fprintf(os.Stderr, "SSDB:    version %d.%d\n", ssdb.MajorVersion, ssdb.MinorVersion)
	now := time.Now()
	fmt.Fprintf(os.Stderr, "Date:       %s\n", now.String())
	fmt.Fprintf(os.Stderr, "CPU:        %d\n", runtime.NumCPU())
}

func (b *benchmark) run() {
	b.printHeader()
	b.open()
	for _, name := range flagsBenchmarks {
		b.num = flagsNum
		if flagsReads < 0 {
			b.reads = flagsNum
		} else {
			b.reads = flagsReads
		}
		b.valueSize = flagsValueSize
		b.entriesPerBatch = 1
		b.writeOptions = ssdb.NewWriteOptions()

		var method func(*threadState)
		freshDB := false
		numThreads := flagsThreads
		switch name {
		case "open":
			method = b.openBench
			b.num /= 10000
			if b.num < 1 {
				b.num = 1
			}
		case "fillseq":
			freshDB = true
			method = b.writeSeq
		case "fillbatch":
			freshDB = true
			b.entriesPerBatch = 1000
			method = b.writeSeq
		case "fillrandom":
			freshDB = true
			method = b.writeRandom
		case "overwrite":
			freshDB = false
			method = b.writeRandom
		case "fillsync":
			freshDB = true
			b.num /= 1000
			b.writeOptions.Sync = true
			method = b.writeRandom
		case "fill100k":
			freshDB = true
			b.num /= 1000
			b.valueSize = 100 * 1000
			method = b.writeRandom
		case "readseq":
			method = b.readSequential
		case "readreverse":
			method = b.readReverse
		case "readrandom":
			method = b.readRandom
		case "readmissing":
			method = b.readMissing
		case "seekrandom":
			method = b.seekRandom
		case "readhot":
			method = b.readHot
		case "readrandomsmall":
			b.num /= 1000
			method = b.readRandom
		case "deleteseq":
			method = b.deleteSeq
		case "deleterandom":
			method = b.deleteRandom
		case "readwhilewriting":
			numThreads++
			method = b.readWhileWriting
		case "compact":
			method = b.compact
		case "crc32c":
			method = crc32
		case "snappycomp":
			method = snappyCompress
		case "snappyuncomp":
			method = snappyUncompress
		case "heapprofile":
			b.heapProfile()
		case "stats":
			b.printStats("ssdb.stats")
		case "sstables":
			b.printStats("ssdb.sstables")
		default:
			if len(name) != 0 {
				fmt.Fprintf(os.Stderr, "unknown benchmark '%s'\n", name)
			}
		}

		if freshDB {
			if flagsUseExistingDB {
				fmt.Fprintf(os.Stdout, "%-12s : skipped (--use_existing_db is true)\n", name)
				method = nil
			} else {
				b.db = nil
				_ = db.Destroy(flagsDB, ssdb.NewOptions())
				b.open()
			}
		}
		if method != nil {
			b.runBenchmark(numThreads, name, method)
		}
	}
}

func (b *benchmark) runBenchmark(n int, name string, method func(*threadState)) {
	shared := newSharedState(n)
	args := make([]threadArg, n)
	for i := range args {
		args[i] = threadArg{
			bm:     b,
			shared: shared,
			thread: newThreadState(i),
			method: method,
		}
		args[i].thread.shared = shared
		env.StartThread(threadBody, &args[i])
	}
	shared.mu.Lock()
	for shared.numInitialized < n {
		shared.cond.Wait()
	}
	shared.start = true
	shared.cond.Broadcast()
	for shared.numDone < n {
		shared.cond.Wait()
	}
	shared.mu.Unlock()
	for i := 1; i < n; i++ {
		args[0].thread.stats.merge(args[i].thread.stats)
	}
	args[0].thread.stats.report(name)
}

func crc32(thread *threadState) {
	const size = 4096
	label := "(4K per op)"
	data := bytes.Repeat([]byte{'x'}, size)
	bs := int64(0)
	crc := uint32(0)
	for bs < 500*1048476 {
		crc = util.ChecksumValue(data)
		thread.stats.finishSingleOp()
		bs += size
	}

	fmt.Fprintf(os.Stderr, "... crc=0x%x\r", crc)
	thread.stats.addBytes(bs)
	thread.stats.addMessage([]byte(label))
}

func snappyCompress(thread *threadState) {
	gen := newRandomGenerator()
	input := gen.generate(int(ssdb.NewOptions().BlockSize))
	bs := int64(0)
	produced := int64(0)
	var compressed []byte
	for bs < 1024*1048576 {
		compressed = snappy.Encode(nil, input)
		produced += int64(len(compressed))
		bs += int64(len(input))
		thread.stats.finishSingleOp()
	}
	buf := bytes.NewBufferString("")
	fmt.Fprintf(buf, "(output: %.1f%%)", float64(produced*100.0)/float64(bs))
	thread.stats.addMessage(buf.Bytes())
	thread.stats.addBytes(bs)
}

func snappyUncompress(thread *threadState) {
	gen := newRandomGenerator()
	input := gen.generate(int(ssdb.NewOptions().BlockSize))
	compressed := snappy.Encode(nil, input)
	var err error
	bs := int64(0)
	for err == nil && bs < 1024*1048576 {
		_, err = snappy.Decode(nil, compressed)
		bs += int64(len(input))
		thread.stats.finishSingleOp()
	}
	if err != nil {
		thread.stats.addMessage([]byte("(snappy failure)"))
	} else {
		thread.stats.addBytes(bs)
	}
}

func (b *benchmark) open() {
	options := ssdb.NewOptions()
	options.Env = env
	options.CreateIfMissing = !flagsUseExistingDB
	options.BlockCache = b.cache
	options.WriteBufferSize = flagsWriteBufferSize
	options.MaxFileSize = flagsMaxFileSize
	options.BlockSize = flagsBlockSize
	options.MaxOpenFiles = flagsOpenFiles
	options.FilterPolicy = b.filterPolicy
	options.ReuseLogs = flagsReuseLogs
	var err error
	if b.db, err = db.Open(options, flagsDB); err != nil {
		fmt.Fprintf(os.Stderr, "open error: %v\n", err)
		os.Exit(1)
	}
}

func (b *benchmark) openBench(thread *threadState) {
	for i := 0; i < b.num; i++ {
		b.open()
		thread.stats.finishSingleOp()
	}
}

func (b *benchmark) writeSeq(thread *threadState) {
	b.doWrite(thread, true)
}

func (b *benchmark) writeRandom(thread *threadState) {
	b.doWrite(thread, false)
}

func (b *benchmark) doWrite(thread *threadState, seq bool) {
	if b.num != flagsNum {
		thread.stats.addMessage([]byte(fmt.Sprintf("(%d ops)", b.num)))
	}
	gen := newRandomGenerator()
	batch := ssdb.NewWriteBatch()
	var err error
	bs := int64(0)
	for i := 0; i < flagsNum; i += b.entriesPerBatch {
		batch.Clear()
		for j := 0; j < b.entriesPerBatch; j++ {
			var k int
			if seq {
				k = i + j
			} else {
				k = int(thread.rand.Next()) % flagsNum
			}
			buf := bytes.NewBuffer(make([]byte, 0, 100))
			fmt.Fprintf(buf, "%016d", k)
			batch.Put(buf.Bytes(), gen.generate(b.valueSize))
			bs += int64(b.valueSize + buf.Len())
			thread.stats.finishSingleOp()
		}
		if err = b.db.Write(b.writeOptions, batch); err != nil {
			fmt.Fprintf(os.Stderr, "put error: %v\n", err)
			os.Exit(1)
		}
	}
	thread.stats.addBytes(bs)
}

func (b *benchmark) readSequential(thread *threadState) {
	iter := b.db.NewIterator(ssdb.NewReadOptions())
	i := 0
	bs := int64(0)
	for iter.SeekToFirst(); i < b.reads && iter.Valid(); iter.Next() {
		bs += int64(len(iter.Key()) + len(iter.Value()))
		thread.stats.finishSingleOp()
		i++
	}
	iter.Close()
	thread.stats.addBytes(bs)
}

func (b *benchmark) readReverse(thread *threadState) {
	iter := b.db.NewIterator(ssdb.NewReadOptions())
	i := 0
	bs := int64(0)
	for iter.SeekToLast(); i < b.reads && iter.Valid(); iter.Prev() {
		bs += int64(len(iter.Key()) + len(iter.Value()))
		thread.stats.finishSingleOp()
		i++
	}
	iter.Close()
	thread.stats.addBytes(bs)
}

func (b *benchmark) readRandom(thread *threadState) {
	options := ssdb.NewReadOptions()
	found := 0
	buf := bytes.NewBuffer(make([]byte, 0, 100))
	for i := 0; i < b.reads; i++ {
		k := int(thread.rand.Next()) % flagsNum
		buf.Truncate(0)
		fmt.Fprintf(buf, "%016d", k)
		if _, err := b.db.Get(options, buf.Bytes()); err == nil {
			found++
		}
		thread.stats.finishSingleOp()
	}
	thread.stats.addMessage([]byte(fmt.Sprintf("(%d of %d found)", found, b.num)))
}

func (b *benchmark) readMissing(thread *threadState) {
	options := ssdb.NewReadOptions()
	buf := bytes.NewBuffer(make([]byte, 0, 100))
	for i := 0; i < b.reads; i++ {
		k := int(thread.rand.Next()) % flagsNum
		buf.Truncate(0)
		fmt.Fprintf(buf, "%016d", k)
		_, _ = b.db.Get(options, buf.Bytes())
		thread.stats.finishSingleOp()
	}
}

func (b *benchmark) readHot(thread *threadState) {
	options := ssdb.NewReadOptions()
	r := (flagsNum + 99) / 100
	buf := bytes.NewBuffer(make([]byte, 0, 100))
	for i := 0; i < b.reads; i++ {
		k := int(thread.rand.Next()) % r
		buf.Truncate(0)
		fmt.Fprintf(buf, "%016d", k)
		_, _ = b.db.Get(options, buf.Bytes())
		thread.stats.finishSingleOp()
	}
}

func (b *benchmark) seekRandom(thread *threadState) {
	options := ssdb.NewReadOptions()
	found := 0
	buf := bytes.NewBuffer(make([]byte, 0, 100))
	for i := 0; i < b.reads; i++ {
		iter := b.db.NewIterator(options)
		k := int(thread.rand.Next()) % flagsNum
		buf.Truncate(0)
		fmt.Fprintf(buf, "%016d", k)
		key := buf.Bytes()
		iter.Seek(key)
		if iter.Valid() && bytes.Compare(iter.Key(), key) == 0 {
			found++
		}
		iter.Close()
		thread.stats.finishSingleOp()
	}
	thread.stats.addMessage([]byte(fmt.Sprintf("(%d of %d found)", found, b.num)))
}

func (b *benchmark) doDelete(thread *threadState, seq bool) {
	batch := ssdb.NewWriteBatch()
	var (
		err error
		k   int
	)
	buf := bytes.NewBuffer(make([]byte, 0, 100))
	for i := 0; i < b.num; i += b.entriesPerBatch {
		batch.Clear()
		for j := 0; j < b.entriesPerBatch; j++ {
			if seq {
				k = i + j
			} else {
				k = int(thread.rand.Next()) % flagsNum
			}
			buf.Truncate(0)
			fmt.Fprintf(buf, "%016d", k)
			batch.Delete(buf.Bytes())
			thread.stats.finishSingleOp()
		}
		if err = b.db.Write(b.writeOptions, batch); err != nil {
			fmt.Fprintf(os.Stderr, "del error: %v\n", err)
			os.Exit(1)
		}
	}
}

func (b *benchmark) deleteSeq(thread *threadState) {
	b.doDelete(thread, true)
}

func (b *benchmark) deleteRandom(thread *threadState) {
	b.doDelete(thread, false)
}

func (b *benchmark) readWhileWriting(thread *threadState) {
	if thread.tid > 0 {
		b.readRandom(thread)
	} else {
		gen := newRandomGenerator()
		buf := bytes.NewBuffer(make([]byte, 0, 100))
		for {
			thread.shared.mu.Lock()
			if thread.shared.numDone+1 >= thread.shared.numInitialized {
				break
			}
			thread.shared.mu.Unlock()

			k := int(thread.rand.Next()) % flagsNum
			buf.Truncate(0)
			fmt.Fprintf(buf, "%016d", k)
			if err := b.db.Put(b.writeOptions, buf.Bytes(), gen.generate(b.valueSize)); err != nil {
				fmt.Fprintf(os.Stderr, "put error: %v\n", err)
				os.Exit(1)
			}
		}
		thread.stats.doStart()
	}
}

func (b *benchmark) compact(thread *threadState) {
	b.db.CompactRange(nil, nil)
}

func (b *benchmark) printStats(key string) {
	stats, ok := b.db.GetProperty(key)
	if !ok {
		stats = "(failed)"
	}
	fmt.Fprintf(os.Stdout, "\n%s\n", stats)
}

func (b *benchmark) heapProfile() {
	b.heapCounter++
	fname := fmt.Sprintf("%s/heap-%04d", flagsDB, b.heapCounter)
	file, err := env.NewWritableFile(fname)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return
	}
	buf := bytes.NewBuffer(make([]byte, 0, 256))
	err = pprof.WriteHeapProfile(buf)
	if err == nil {
		err = file.Append(buf.Bytes())
	}
	file.Finalize()
	if err != nil {
		fmt.Fprintf(os.Stderr, "heap profiling not supported\n")
		_ = env.DeleteFile(fname)
	}
}

type threadArg struct {
	bm     *benchmark
	shared *sharedState
	thread *threadState
	method func(*threadState)
}

func threadBody(i interface{}) {
	arg := i.(*threadArg)
	shared, thread := arg.shared, arg.thread
	shared.mu.Lock()
	shared.numInitialized++
	if shared.numInitialized >= shared.total {
		shared.cond.Broadcast()
	}
	for !shared.start {
		shared.cond.Wait()
	}
	shared.mu.Unlock()

	thread.stats.doStart()
	arg.method(thread)
	thread.stats.doStop()

	shared.mu.Lock()
	defer shared.mu.Unlock()
	shared.numDone++
	if shared.numDone >= shared.total {
		shared.cond.Broadcast()
	}
}

func main() {
	options := ssdb.NewOptions()
	flagsWriteBufferSize = options.WriteBufferSize
	flagsMaxFileSize = options.MaxFileSize
	flagsBlockSize = options.BlockSize
	flagsOpenFiles = options.MaxOpenFiles
	var (
		d    float64
		n    int
		junk byte
		err  error
	)
	for _, arg := range os.Args {
		if strings.HasPrefix(arg, "--benchmarks=") {
			flagsBenchmarks = strings.Split(strings.TrimPrefix(arg, "--benchmarks="), ",")
		} else if _, err = fmt.Sscanf(arg, "--compression_ratio=%f%c", &d, &junk); err == nil {
			flagsCompressionRatio = d
		} else if _, err = fmt.Sscanf(arg, "--histogram=%d%c", &n, &junk); err == nil && (n == 0 || n == 1) {
			flagsHistogram = n == 1
		} else if _, err = fmt.Sscanf(arg, "--use_existing_db=%d%c", &n, &junk); err == nil && (n == 0 || n == 1) {
			flagsUseExistingDB = n == 1
		} else if _, err = fmt.Sscanf(arg, "--reuse_logs=%d%c", &n, &junk); err == nil && (n == 0 || n == 1) {
			flagsReuseLogs = n == 1
		} else if _, err = fmt.Sscanf(arg, "--num=%d%c", &n, &junk); err == nil {
			flagsNum = n
		} else if _, err = fmt.Sscanf(arg, "--reads=%d%c", &n, &junk); err == nil {
			flagsReads = n
		} else if _, err = fmt.Sscanf(arg, "--threads=%d%c", &n, &junk); err == nil {
			flagsThreads = n
		} else if _, err = fmt.Sscanf(arg, "--value_size=%d%c", &n, &junk); err == nil {
			flagsValueSize = n
		} else if _, err = fmt.Sscanf(arg, "--write_buffer_size=%d%c", &n, &junk); err == nil {
			flagsWriteBufferSize = n
		} else if _, err = fmt.Sscanf(arg, "--max_file_size=%d%c", &n, &junk); err == nil {
			flagsMaxFileSize = n
		} else if _, err = fmt.Sscanf(arg, "--max_file_size=%d%c", &n, &junk); err == nil {
			flagsMaxFileSize = n
		} else if _, err = fmt.Sscanf(arg, "--block_size=%d%c", &n, &junk); err == nil {
			flagsBlockSize = n
		} else if _, err = fmt.Sscanf(arg, "--cache_size=%d%c", &n, &junk); err == nil {
			flagsCacheSize = n
		} else if _, err = fmt.Sscanf(arg, "--bloom_bits=%d%c", &n, &junk); err == nil {
			flagsBloomBits = n
		} else if _, err = fmt.Sscanf(arg, "--open_files=%d%c", &n, &junk); err == nil {
			flagsOpenFiles = n
		} else if strings.HasPrefix(arg, "--db=") {
			flagsDB = strings.TrimPrefix(arg, "--db=")
		} else {
			fmt.Fprintf(os.Stderr, "Invalid flag '%s'\n", arg)
			os.Exit(1)
		}
	}
	env = ssdb.DefaultEnv()

	if flagsDB == "" {
		flagsDB, _ = env.GetTestDirectory()
		flagsDB += "/dbbench"
	}
	flag.Parse()
	benchmark := newBenchmark()
	defer benchmark.finish()
	benchmark.run()
}
