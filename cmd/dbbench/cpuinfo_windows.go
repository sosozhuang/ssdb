package main

import (
	"fmt"
	"os"
	"runtime"
)

func printCPUInfo() {
	fmt.Fprintf(os.Stderr, "CPU:        %d Core(s)\n", runtime.NumCPU())
}
