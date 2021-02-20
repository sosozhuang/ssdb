package util

import (
	"testing"
)

func TestErrorString(t *testing.T) {
	err := NotFoundError1("custom NotFound status message")
	TestEqual("NotFound: custom NotFound status message", err.(*DBError).String(), "error string", t)
}
