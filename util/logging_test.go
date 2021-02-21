package util

import (
	"math"
	"reflect"
	"testing"
	"unsafe"
)

func TestNumberToString(t *testing.T) {
	TestEqual("0", NumberToString(0), "NumberToString", t)
	TestEqual("1", NumberToString(1), "NumberToString", t)
	TestEqual("9", NumberToString(9), "NumberToString", t)

	TestEqual("10", NumberToString(10), "NumberToString", t)
	TestEqual("11", NumberToString(11), "NumberToString", t)
	TestEqual("19", NumberToString(19), "NumberToString", t)
	TestEqual("99", NumberToString(99), "NumberToString", t)

	TestEqual("100", NumberToString(100), "NumberToString", t)
	TestEqual("109", NumberToString(109), "NumberToString", t)
	TestEqual("190", NumberToString(190), "NumberToString", t)
	TestEqual("123", NumberToString(123), "NumberToString", t)
	TestEqual("12345678", NumberToString(12345678), "NumberToString", t)

	TestEqual("18446744073709551000", NumberToString(18446744073709551000), "NumberToString", t)
	TestEqual("18446744073709551600", NumberToString(18446744073709551600), "NumberToString", t)
	TestEqual("18446744073709551610", NumberToString(18446744073709551610), "NumberToString", t)
	TestEqual("18446744073709551614", NumberToString(18446744073709551614), "NumberToString", t)
	TestEqual("18446744073709551615", NumberToString(18446744073709551615), "NumberToString", t)
}

func consumeDecimalNumberRoundTrip(number uint64, t *testing.T) {
	consumeDecimalNumberRoundTripWithPadding(number, "", t)
}

func consumeDecimalNumberRoundTripWithPadding(number uint64, padding string, t *testing.T) {
	decimalNumber := NumberToString(number)
	input := []byte(decimalNumber + padding)
	output := input[:]
	var result uint64
	TestTrue(ConsumeDecimalNumber(&output, &result), "ConsumeDecimalNumber", t)
	TestEqual(number, result, "result", t)
	ish := (*reflect.SliceHeader)(unsafe.Pointer(&input))
	osh := (*reflect.SliceHeader)(unsafe.Pointer(&output))
	TestEqual(len(decimalNumber), int(osh.Data-ish.Data), "result", t)
	TestEqual(len(padding), len(output), "output length", t)
}

func TestConsumeDecimalNumberRoundTrip(t *testing.T) {
	consumeDecimalNumberRoundTrip(0, t)
	consumeDecimalNumberRoundTrip(1, t)
	consumeDecimalNumberRoundTrip(9, t)

	consumeDecimalNumberRoundTrip(10, t)
	consumeDecimalNumberRoundTrip(11, t)
	consumeDecimalNumberRoundTrip(19, t)
	consumeDecimalNumberRoundTrip(99, t)

	consumeDecimalNumberRoundTrip(100, t)
	consumeDecimalNumberRoundTrip(109, t)
	consumeDecimalNumberRoundTrip(190, t)
	consumeDecimalNumberRoundTrip(123, t)
	TestEqual("12345678", NumberToString(12345678), "NumberToString", t)

	var largeNumber uint64
	for i := uint64(0); i < 100; i++ {
		largeNumber = math.MaxUint64 - i
		consumeDecimalNumberRoundTrip(largeNumber, t)
	}
}

func TestConsumeDecimalNumberRoundTripWithPadding(t *testing.T) {
	consumeDecimalNumberRoundTripWithPadding(0, " ", t)
	consumeDecimalNumberRoundTripWithPadding(1, "abc", t)
	consumeDecimalNumberRoundTripWithPadding(9, "x", t)

	consumeDecimalNumberRoundTripWithPadding(10, "_", t)
	consumeDecimalNumberRoundTripWithPadding(11, "\000\000\000\000\000\000\000\000\000", t)
	consumeDecimalNumberRoundTripWithPadding(19, "abc", t)
	consumeDecimalNumberRoundTripWithPadding(99, "padding", t)

	consumeDecimalNumberRoundTripWithPadding(100, " ", t)

	var largeNumber uint64
	for i := uint64(0); i < 100; i++ {
		largeNumber = math.MaxUint64 - i
		consumeDecimalNumberRoundTripWithPadding(largeNumber, "pad", t)
	}
}

func consumeDecimalNumberOverflow(inputString string, t *testing.T) {
	input := []byte(inputString)
	output := input[:]
	var result uint64
	TestFalse(ConsumeDecimalNumber(&output, &result), "ConsumeDecimalNumber", t)
}

func TestConsumeDecimalNumberNoDigits(t *testing.T) {
	consumeDecimalNumberOverflow("18446744073709551616", t)
	consumeDecimalNumberOverflow("18446744073709551617", t)
	consumeDecimalNumberOverflow("18446744073709551618", t)
	consumeDecimalNumberOverflow("18446744073709551619", t)
	consumeDecimalNumberOverflow("18446744073709551620", t)
	consumeDecimalNumberOverflow("18446744073709551621", t)
	consumeDecimalNumberOverflow("18446744073709551622", t)
	consumeDecimalNumberOverflow("18446744073709551623", t)
	consumeDecimalNumberOverflow("18446744073709551624", t)
	consumeDecimalNumberOverflow("18446744073709551625", t)
	consumeDecimalNumberOverflow("18446744073709551626", t)
	consumeDecimalNumberOverflow("18446744073709551700", t)
	consumeDecimalNumberOverflow("99999999999999999999", t)
}
