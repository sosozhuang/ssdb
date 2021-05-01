package util

import (
	"os"
	"reflect"
	"strconv"
	"testing"
)

func AssertTrue(condition bool, s string, t *testing.T) {
	t.Helper()
	if !condition {
		t.Errorf("Test [%s] failed, condition is false.\n", s)
	}
}

func AssertFalse(condition bool, s string, t *testing.T) {
	t.Helper()
	if condition {
		t.Errorf("Test [%s] failed, condition is true.\n", s)
	}
}

func AssertEqual(expected, actual interface{}, s string, t *testing.T) {
	t.Helper()
	type1 := reflect.TypeOf(expected)
	type2 := reflect.TypeOf(actual)
	if type1.Kind() != type2.Kind() {
		t.Errorf("Test [%s] failed, expected type: [%s], actual: [%s].\n", s, type1.String(), type2.String())
		return
	}
	if type1.Kind() == reflect.Struct || type1.Kind() == reflect.Slice || type1.Kind() == reflect.Array {
		if !reflect.DeepEqual(expected, actual) {
			t.Errorf("Test [%s] failed, expected equal: [%v], actual: [%v].\n", s, expected, actual)
		}
	} else {
		if expected != actual {
			t.Errorf("Test [%s] failed, expected equal: [%v], actual: [%v].\n", s, expected, actual)
		}
	}
}

func AssertNotEqual(v1, v2 interface{}, s string, t *testing.T) {
	t.Helper()
	type1 := reflect.TypeOf(v1)
	type2 := reflect.TypeOf(v2)
	if type1.Kind() != type2.Kind() {
		t.Errorf("Test [%s] failed, expected type: [%s], actual: [%s].\n", s, type1.String(), type2.String())
	}
	if type1.Kind() == reflect.Struct || type1.Kind() == reflect.Slice || type1.Kind() == reflect.Array {
		if reflect.DeepEqual(v1, v2) {
			t.Errorf("Test [%s] failed, expected not equal: [%v].\n", s, v1)
		}
	} else {
		if v1 == v2 {
			t.Errorf("Test [%s] failed, expected not equal: [%v].\n", s, v1)
		}
	}
}

func AssertGreaterThan(v1, v2 interface{}, s string, t *testing.T) {
	t.Helper()
	type1 := reflect.TypeOf(v1)
	type2 := reflect.TypeOf(v2)
	if type1.Kind() != type2.Kind() {
		t.Errorf("Test [%s] failed, types are different: [%s], [%s].\n", s, type1.String(), type2.String())
	}
	if !(type1.Kind() >= reflect.Int && type1.Kind() <= reflect.Float64) {
		t.Errorf("Test [%s] failed, [%s] is not integer type", s, type1.String())
	}
	var b bool
	switch type1.Kind() {
	case reflect.Int:
		b = v1.(int) <= v2.(int)
	case reflect.Int8:
		b = v1.(int8) <= v2.(int8)
	case reflect.Int16:
		b = v1.(int16) <= v2.(int16)
	case reflect.Int32:
		b = v1.(int32) <= v2.(int32)
	case reflect.Int64:
		b = v1.(int64) <= v2.(int64)
	case reflect.Uint:
		b = v1.(uint) <= v2.(uint)
	case reflect.Uint8:
		b = v1.(uint8) <= v2.(uint8)
	case reflect.Uint16:
		b = v1.(uint16) <= v2.(uint16)
	case reflect.Uint32:
		b = v1.(uint32) <= v2.(uint32)
	case reflect.Uint64:
		b = v1.(uint64) <= v2.(uint64)
	case reflect.Uintptr:
		b = v1.(uintptr) <= v2.(uintptr)
	case reflect.Float32:
		b = v1.(float32) <= v2.(float32)
	case reflect.Float64:
		b = v1.(float64) <= v2.(float64)
	}
	if !b {
		t.Errorf("Test [%s] failed, expected: [%v] <= [%v].\n", s, v1, v2)
	}
}

func AssertLessThan(v1, v2 interface{}, s string, t *testing.T) {
	t.Helper()
	type1 := reflect.TypeOf(v1)
	type2 := reflect.TypeOf(v2)
	if type1.Kind() != type2.Kind() {
		t.Errorf("Test [%s] failed, types are different: [%s], [%s].\n", s, type1.String(), type2.String())
	}
	if !(type1.Kind() >= reflect.Int && type1.Kind() <= reflect.Float64) {
		t.Errorf("Test [%s] failed, [%s] is not integer type", s, type1.String())
	}
	var b bool
	switch type1.Kind() {
	case reflect.Int:
		b = v1.(int) < v2.(int)
	case reflect.Int8:
		b = v1.(int8) < v2.(int8)
	case reflect.Int16:
		b = v1.(int16) < v2.(int16)
	case reflect.Int32:
		b = v1.(int32) < v2.(int32)
	case reflect.Int64:
		b = v1.(int64) < v2.(int64)
	case reflect.Uint:
		b = v1.(uint) < v2.(uint)
	case reflect.Uint8:
		b = v1.(uint8) < v2.(uint8)
	case reflect.Uint16:
		b = v1.(uint16) < v2.(uint16)
	case reflect.Uint32:
		b = v1.(uint32) < v2.(uint32)
	case reflect.Uint64:
		b = v1.(uint64) < v2.(uint64)
	case reflect.Uintptr:
		b = v1.(uintptr) < v2.(uintptr)
	case reflect.Float32:
		b = v1.(float32) < v2.(float32)
	case reflect.Float64:
		b = v1.(float64) < v2.(float64)
	}
	if !b {
		t.Errorf("Test [%s] failed, expected: [%v] < [%v].\n", s, v1, v2)
	}
}

func AssertGreaterThanOrEqual(v1, v2 interface{}, s string, t *testing.T) {
	t.Helper()
	type1 := reflect.TypeOf(v1)
	type2 := reflect.TypeOf(v2)
	if type1.Kind() != type2.Kind() {
		t.Errorf("Test [%s] failed, types are different: [%s], [%s].\n", s, type1.String(), type2.String())
	}
	if !(type1.Kind() >= reflect.Int && type1.Kind() <= reflect.Float64) {
		t.Errorf("Test [%s] failed, [%s] is not integer type", s, type1.String())
	}
	var b bool
	switch type1.Kind() {
	case reflect.Int:
		b = v1.(int) >= v2.(int)
	case reflect.Int8:
		b = v1.(int8) >= v2.(int8)
	case reflect.Int16:
		b = v1.(int16) >= v2.(int16)
	case reflect.Int32:
		b = v1.(int32) >= v2.(int32)
	case reflect.Int64:
		b = v1.(int64) >= v2.(int64)
	case reflect.Uint:
		b = v1.(uint) >= v2.(uint)
	case reflect.Uint8:
		b = v1.(uint8) >= v2.(uint8)
	case reflect.Uint16:
		b = v1.(uint16) >= v2.(uint16)
	case reflect.Uint32:
		b = v1.(uint32) >= v2.(uint32)
	case reflect.Uint64:
		b = v1.(uint64) >= v2.(uint64)
	case reflect.Uintptr:
		b = v1.(uintptr) >= v2.(uintptr)
	case reflect.Float32:
		b = v1.(float32) >= v2.(float32)
	case reflect.Float64:
		b = v1.(float64) >= v2.(float64)
	}
	if !b {
		t.Errorf("Test [%s] failed, expected: [%v] >= [%v].\n", s, v1, v2)
	}
}

func AssertLessThanOrEqual(v1, v2 interface{}, s string, t *testing.T) {
	t.Helper()
	type1 := reflect.TypeOf(v1)
	type2 := reflect.TypeOf(v2)
	if type1.Kind() != type2.Kind() {
		t.Errorf("Test [%s] failed, types are different: [%s], [%s].\n", s, type1.String(), type2.String())
	}
	if !(type1.Kind() >= reflect.Int && type1.Kind() <= reflect.Float64) {
		t.Errorf("Test [%s] failed, [%s] is not integer type", s, type1.String())
	}
	var b bool
	switch type1.Kind() {
	case reflect.Int:
		b = v1.(int) <= v2.(int)
	case reflect.Int8:
		b = v1.(int8) <= v2.(int8)
	case reflect.Int16:
		b = v1.(int16) <= v2.(int16)
	case reflect.Int32:
		b = v1.(int32) <= v2.(int32)
	case reflect.Int64:
		b = v1.(int64) <= v2.(int64)
	case reflect.Uint:
		b = v1.(uint) <= v2.(uint)
	case reflect.Uint8:
		b = v1.(uint8) <= v2.(uint8)
	case reflect.Uint16:
		b = v1.(uint16) <= v2.(uint16)
	case reflect.Uint32:
		b = v1.(uint32) <= v2.(uint32)
	case reflect.Uint64:
		b = v1.(uint64) <= v2.(uint64)
	case reflect.Uintptr:
		b = v1.(uintptr) <= v2.(uintptr)
	case reflect.Float32:
		b = v1.(float32) <= v2.(float32)
	case reflect.Float64:
		b = v1.(float64) <= v2.(float64)
	}
	if !b {
		t.Errorf("Test [%s] failed, expected: [%v] <= [%v].\n", s, v1, v2)
	}
}

func AssertNotError(err error, s string, t *testing.T) {
	t.Helper()
	if err != nil {
		t.Errorf("Test [%s] failed, error is [%v].\n", s, err)
	}
}

func AssertError(err error, s string, t *testing.T) {
	t.Helper()
	if err == nil {
		t.Errorf("Test [%s] failed, error is nil.\n", s)
	}
}

func RandomSeed() (result int) {
	env, ok := os.LookupEnv("TEST_RANDOM_SEED")
	if ok && env != "" {
		var err error
		result, err = strconv.Atoi(env)
		if result < 0 || err != nil {
			result = 301
		}
	} else {
		result = 301
	}
	return
}
