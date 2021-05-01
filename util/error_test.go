package util

import (
	"testing"
)

func TestErrorString(t *testing.T) {
	err := NotFoundError1("custom NotFound status message")
	AssertEqual("NotFound: custom NotFound status message", err.(*DBError).String(), "error string", t)
}
