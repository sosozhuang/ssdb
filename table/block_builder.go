package table

import (
	"bytes"
	"ssdb"
	"ssdb/util"
)

type blockBuilder struct {
	options  *ssdb.Options
	buffer   []byte
	restarts []uint32
	counter  int
	finished bool
	lastKey  []byte
}

func (b *blockBuilder) reset() {
	b.buffer = make([]byte, 0)
	b.restarts = make([]uint32, 1)
	b.restarts[0] = 0
	b.counter = 0
	b.finished = false
	b.lastKey = make([]byte, 0)
}

func (b *blockBuilder) currentSizeEstimate() int {
	const n = int(uint32Size)
	return len(b.buffer) + len(b.restarts)*n + n
}

func (b *blockBuilder) empty() bool {
	return len(b.buffer) == 0
}

func (b *blockBuilder) finish() []byte {
	for _, restart := range b.restarts {
		util.PutFixed32(&b.buffer, restart)
	}
	util.PutFixed32(&b.buffer, uint32(len(b.restarts)))
	b.finished = true
	return b.buffer
}

func (b *blockBuilder) add(key, value []byte) {
	if b.finished {
		panic("blockBuilder: finish = true")
	}
	if b.counter > b.options.BlockRestartInterval {
		panic("blockBuilder: counter > options.BlockRestartInterval")
	}
	if len(b.buffer) > 0 && b.options.Comparator.Compare(key, b.lastKey) <= 0 {
		panic("blockBuilder: len(buffer) > 0 && options.Comparator.Compare(key, lastKey) <= 0")
	}
	shared := 0
	if b.counter < b.options.BlockRestartInterval {
		minLength := len(b.lastKey)
		if minLength > len(key) {
			minLength = len(key)
		}
		for shared < minLength && b.lastKey[shared] == key[shared] {
			shared++
		}
	} else {
		b.restarts = append(b.restarts, uint32(len(b.buffer)))
		b.counter = 0
	}
	nonShared := len(key) - shared

	// Add "<shared><non_shared><value_size>" to buffer_
	util.PutVarInt32(&b.buffer, uint32(shared))
	util.PutVarInt32(&b.buffer, uint32(nonShared))
	util.PutVarInt32(&b.buffer, uint32(len(value)))

	b.buffer = append(b.buffer, key[shared:]...)
	b.buffer = append(b.buffer, value...)
	b.lastKey = b.lastKey[:shared]
	b.lastKey = append(b.lastKey, key[shared:]...)
	if bytes.Compare(b.lastKey, key) != 0 {
		panic("blockBuilder: lastKey != key")
	}
	b.counter++
}

func newBlockBuilder(options *ssdb.Options) *blockBuilder {
	if options.BlockRestartInterval < 1 {
		panic("blockBuilder: options.BlockRestartInterval < 1")
	}
	b := &blockBuilder{
		options:  options,
		buffer:   make([]byte, 0),
		restarts: make([]uint32, 1),
		counter:  0,
		finished: false,
		lastKey:  make([]byte, 0),
	}
	b.restarts[0] = 0
	return b
}
