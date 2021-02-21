package util

import (
	"bytes"
	"fmt"
	"math"
)

func AppendNumberTo(s *string, num uint64) {
	buf := bytes.NewBufferString(*s)
	fmt.Fprintf(buf, "%d", num)
	*s = buf.String()
}

func appendEscapedStringTo(str *string, value []byte) {
	buf := bytes.NewBufferString("")
	for _, v := range value {
		if v >= ' ' && v <= '~' {
			fmt.Fprint(buf, v)
		} else {
			fmt.Fprintf(buf, "\\x%02x", v&0xff)
		}
	}
	*str = buf.String()
}

func NumberToString(num uint64) string {
	var s string
	AppendNumberTo(&s, num)
	return s
}

func EscapeString(value []byte) string {
	var r string
	appendEscapedStringTo(&r, value)
	return r
}

func ConsumeDecimalNumber(in *[]byte, val *uint64) bool {
	const lastDigit = '0' + byte(math.MaxUint64%10)
	value := uint64(0)
	consumed := 0
	for _, b := range *in {
		if b < '0' || b > '9' {
			break
		}
		if value > math.MaxUint64/10 || (value == math.MaxUint64/10 && b > lastDigit) {
			return false
		}
		value = (value * 10) + uint64(b-'0')
		consumed++
	}
	*val = value
	*in = (*in)[consumed:]
	return consumed != 0
}
