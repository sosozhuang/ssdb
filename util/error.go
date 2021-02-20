package util

import (
	"bytes"
	"fmt"
)

type Code int8

const (
	OK Code = iota
	NotFound
	Corruption
	NotSupported
	InvalidArgument
	IOError
)

type DBError struct {
	code Code
	msg  string
	msg2 string
}

func NewError(code Code, msg string, msg2 string) error {
	if code == OK {
		panic("code cannot be OK")
	}
	return &DBError{
		code: code,
		msg:  msg,
		msg2: msg2,
	}
}

func NotFoundError1(msg string) error {
	return NotFoundError2(msg, "")
}

func NotFoundError2(msg string, msg2 string) error {
	return NewError(NotFound, msg, msg2)
}

func CorruptionError1(msg string) error {
	return CorruptionError2(msg, "")
}

func CorruptionError2(msg string, msg2 string) error {
	return NewError(Corruption, msg, msg2)
}

func NotSupportedError1(msg string) error {
	return NotSupportedError2(msg, "")
}

func NotSupportedError2(msg string, msg2 string) error {
	return NewError(NotSupported, msg, msg2)
}

func InvalidArgumentError1(msg string) error {
	return InvalidArgumentError2(msg, "")
}

func InvalidArgumentError2(msg string, msg2 string) error {
	return NewError(InvalidArgument, msg, msg2)
}

func IOError1(msg string) error {
	return IOError2(msg, "")
}

func IOError2(msg string, msg2 string) error {
	return NewError(IOError, msg, msg2)
}

func (e *DBError) Error() string {
	return e.String()
}

func (e *DBError) String() string {
	buf := bytes.NewBufferString("")
	switch e.code {
	case OK:
		fmt.Fprint(buf, "")
	case NotFound:
		fmt.Fprint(buf, "NotFound: ")
	case Corruption:
		fmt.Fprint(buf, "Corruption: ")
	case NotSupported:
		fmt.Fprint(buf, "Not implemented: ")
	case InvalidArgument:
		fmt.Fprint(buf, "Invalid argument: ")
	case IOError:
		fmt.Fprint(buf, "IO error: ")
	default:
		fmt.Fprintf(buf, "Unknown code(%d): ", e.code)
	}
	if e.msg2 != "" {
		fmt.Fprintf(buf, "%s: %s", e.msg, e.msg2)
	} else {
		fmt.Fprintf(buf, "%s", e.msg)
	}
	return buf.String()
}

func (e *DBError) Code() Code {
	return e.code
}
