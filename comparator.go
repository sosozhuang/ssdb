package ssdb

import (
	"bytes"
)

type Comparator interface {
	Compare(a, b []byte) int
	Name() string
	FindShortestSeparator(start *[]byte, limit []byte)
	FindShortSuccessor(key *[]byte)
}

var BytewiseComparator bytewiseComparator

type bytewiseComparator struct{}

func (_ bytewiseComparator) Compare(a, b []byte) int {
	return bytes.Compare(a, b)
}

func (_ bytewiseComparator) Name() string {
	return "ssdb.BytewiseComparator"
}

func (b bytewiseComparator) FindShortestSeparator(start *[]byte, limit []byte) {
	minLen := len(*start)
	if minLen > len(limit) {
		minLen = len(limit)
	}
	diffIndex := 0
	for diffIndex < minLen && (*start)[diffIndex] == limit[diffIndex] {
		diffIndex++
	}
	if diffIndex >= minLen {
	} else {
		diffByte := (*start)[diffIndex]
		if diffByte < 0xff && diffByte+1 < limit[diffIndex] {
			(*start)[diffIndex]++
			*start = (*start)[:diffIndex+1]
			if b.Compare(*start, limit) >= 0 {
				panic("start >= limit")
			}
		}
	}
}

func (_ bytewiseComparator) FindShortSuccessor(key *[]byte) {
	for i, b := range *key {
		if b != 0xff {
			(*key)[i] = b + 1
			*key = (*key)[:i+1]
			return
		}
	}
}
