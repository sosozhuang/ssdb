package main

import (
	"fmt"
	"io"
	"os"
	"strings"
)

func printCPUInfo() {
	f, err := os.OpenFile("/proc/cpuinfo", os.O_RDONLY, 0)
	if err != nil {
		return
	}
	defer f.Close()
	var (
		builder strings.Builder
		n       int
		offset  int64
	)
	for {
		b := make([]byte, 1000)
		n, err = f.ReadAt(b, offset)
		if err != nil && err != io.EOF {
			return
		}
		if n == 0 {
			break
		}
		builder.Write(b[:n])
		if err == io.EOF {
			break
		}
		offset += int64(n)
	}
	var (
		numCPUs   int
		cpuType   string
		cacheSize string
		kv        []string
	)
	lines := strings.Split(builder.String(), "\n")
	for _, line := range lines {
		kv = strings.SplitN(line, ":", 2)
		if len(kv) < 2 {
			continue
		}
		key := strings.TrimSpace(kv[0])
		val := strings.TrimSpace(kv[1])
		if key == "model name" {
			numCPUs++
			cpuType = val
		} else if key == "cache size" {
			cacheSize = val
		}
	}
	if numCPUs > 0 {
		fmt.Fprintf(os.Stderr, "CPU:        %d * %s\n", numCPUs, cpuType)
	}
	if cacheSize != "" {
		fmt.Fprintf(os.Stderr, "CPUCache:   %s\n", cacheSize)
	}
}
