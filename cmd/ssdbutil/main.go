package main

import (
	"fmt"
	"os"
	"ssdb"
	"ssdb/db"
)

func main() {
	env := ssdb.DefaultEnv()
	args := os.Args
	ok := true
	if len(args) < 2 {
		usage()
		ok = false
	} else {
		command := args[1]
		if command == "dump" {
			ok = handleDumpCommand(env, args[2:])
		} else {
			usage()
			ok = false
		}
	}
	if ok {
		os.Exit(0)
	} else {
		os.Exit(1)
	}
}

func usage() {
	_, _ = fmt.Fprintln(os.Stderr, "Usage: ssdbutil command...")
	_, _ = fmt.Fprintln(os.Stderr, "   dump files...         -- dump contents of specified files")
}

func handleDumpCommand(env ssdb.Env, args []string) bool {
	printer := new(stdoutPrinter)
	ok := true
	var err error
	for _, arg := range args {
		if err = db.DumpFile(env, arg, printer); err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			ok = false
		}
	}
	return ok
}

type stdoutPrinter struct{}

func (p *stdoutPrinter) Append(data []byte) error {
	fmt.Fprint(os.Stdout, data)
	return nil
}

func (p *stdoutPrinter) Close() error {
	return nil
}

func (p *stdoutPrinter) Flush() error {
	return nil
}

func (p *stdoutPrinter) Sync() error {
	return nil
}

func (p *stdoutPrinter) Finalize() {
}
