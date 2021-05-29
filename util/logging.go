package util

import (
	"fmt"
	"io"
	"math"
	"strings"
)

func AppendNumberTo(w io.Writer, num uint64) {
	fmt.Fprintf(w, "%d", num)
}

func AppendEscapedStringTo(w io.Writer, value []byte) {
	for _, v := range value {
		if v >= ' ' && v <= '~' {
			fmt.Fprint(w, v)
		} else {
			fmt.Fprintf(w, "\\x%02x", v&0xff)
		}
	}
}

func NumberToString(num uint64) string {
	var builder strings.Builder
	AppendNumberTo(&builder, num)
	return builder.String()
}

func EscapeString(value []byte) string {
	var builder strings.Builder
	AppendEscapedStringTo(&builder, value)
	return builder.String()
}

func ConsumeDecimalNumber(in *string, val *uint64) bool {
	const lastDigit = '0' + byte(math.MaxUint64%10)
	value := uint64(0)
	consumed := 0
	for _, b := range *in {
		if b < '0' || b > '9' {
			break
		}
		if value > math.MaxUint64/10 || (value == math.MaxUint64/10 && byte(b) > lastDigit) {
			return false
		}
		value = (value * 10) + uint64(b-'0')
		consumed++
	}
	*val = value
	*in = (*in)[consumed:]
	return consumed != 0
}
