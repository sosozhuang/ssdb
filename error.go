package ssdb

import "ssdb/util"

func IsNotFound(err error) bool {
	return errorCode(err) == util.NotFound
}

func IsCorruption(err error) bool {
	return errorCode(err) == util.Corruption
}

func IsIOError(err error) bool {
	return errorCode(err) == util.IOError
}

func IsNotSupportedError(err error) bool {
	return errorCode(err) == util.NotSupported
}

func IsInvalidArgument(err error) bool {
	return errorCode(err) == util.InvalidArgument
}

func errorCode(err error) util.Code {
	switch err := err.(type) {
	case *util.DBError:
		return err.Code()
	}
	return -1
}
