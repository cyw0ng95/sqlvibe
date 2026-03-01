package DS

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb -lstdc++
#cgo CFLAGS: -I${SRCDIR}/../../src/core/DS
#include "value.h"
#include <stdlib.h>
*/
import "C"
import (
	"fmt"
	"unsafe"
)

// ValueType enumerates the supported column types.
type ValueType int

const (
	TypeNull   ValueType = iota
	TypeInt              // int64
	TypeFloat            // float64
	TypeString           // string
	TypeBytes            // []byte
	TypeBool             // bool
)

// Value holds a single typed datum.
type Value struct {
	Type  ValueType
	Int   int64
	Float float64
	Str   string
	Bytes []byte
}

// toGoValue converts a C svdb_value_t to Go Value
func toGoValue(cv C.svdb_value_t) Value {
	var v Value
	switch ValueType(cv.val_type) {
	case TypeNull:
		v.Type = TypeNull
	case TypeInt:
		v.Type = TypeInt
		v.Int = int64(cv.int_val)
	case TypeFloat:
		v.Type = TypeFloat
		v.Float = float64(cv.float_val)
	case TypeString:
		v.Type = TypeString
		if cv.str_data != nil && cv.str_len > 0 {
			v.Str = C.GoStringN(cv.str_data, C.int(cv.str_len))
		}
	case TypeBool:
		v.Type = TypeBool
		v.Int = int64(cv.int_val)
	case TypeBytes:
		v.Type = TypeBytes
		if cv.bytes_data != nil && cv.bytes_len > 0 {
			v.Bytes = C.GoBytes(unsafe.Pointer(cv.bytes_data), C.int(cv.bytes_len))
		}
	}
	return v
}

func NullValue() Value {
	var cv C.svdb_value_t
	C.svdb_value_init_null(&cv)
	return toGoValue(cv)
}

func IntValue(v int64) Value {
	var cv C.svdb_value_t
	C.svdb_value_init_int(&cv, C.int64_t(v))
	return toGoValue(cv)
}

func FloatValue(v float64) Value {
	var cv C.svdb_value_t
	C.svdb_value_init_float(&cv, C.double(v))
	return toGoValue(cv)
}

func StringValue(v string) Value {
	var cv C.svdb_value_t
	cs := C.CString(v)
	defer C.free(unsafe.Pointer(cs))
	C.svdb_value_init_string(&cv, cs, C.size_t(len(v)))
	return toGoValue(cv)
}

func BoolValue(v bool) Value {
	var cv C.svdb_value_t
	boolVal := 0
	if v {
		boolVal = 1
	}
	C.svdb_value_init_bool(&cv, C.int(boolVal))
	return toGoValue(cv)
}

func BytesValue(v []byte) Value {
	var cv C.svdb_value_t
	if len(v) > 0 {
		C.svdb_value_init_bytes(&cv, (*C.char)(unsafe.Pointer(&v[0])), C.size_t(len(v)))
	} else {
		C.svdb_value_init_bytes(&cv, nil, 0)
	}
	return toGoValue(cv)
}

// IsNull returns true if the value is NULL.
func (v Value) IsNull() bool {
	return v.Type == TypeNull
}

// String returns a human-readable representation.
func (v Value) String() string {
	return v.ToString()
}

// ToString returns a C++ computed string representation.
func (v Value) ToString() string {
	// For now, use Go implementation
	// Can be extended to use C++ ToString() via CGO
	switch v.Type {
	case TypeNull:
		return "NULL"
	case TypeInt:
		return fmt.Sprintf("%d", v.Int)
	case TypeFloat:
		return fmt.Sprintf("%g", v.Float)
	case TypeString:
		return v.Str
	case TypeBool:
		if v.Int != 0 {
			return "true"
		}
		return "false"
	case TypeBytes:
		return fmt.Sprintf("%x", v.Bytes)
	default:
		return "?"
	}
}

// Equal returns true when the two values are equal (NULL != NULL per SQL semantics).
func (v Value) Equal(other Value) bool {
	// Handle NULL semantics
	if v.Type == TypeNull || other.Type == TypeNull {
		return false
	}
	return Compare(v, other) == 0
}

// Compare compares two Values. Returns -1, 0, or 1.
// NULL is treated as less than everything else.
// When types differ the numeric types are coerced; otherwise type order is used.
func Compare(a, b Value) int {
	// NULL ordering
	if a.Type == TypeNull && b.Type == TypeNull {
		return 0
	}
	if a.Type == TypeNull {
		return -1
	}
	if b.Type == TypeNull {
		return 1
	}

	// For string and bytes types, use Go implementation directly
	// to avoid CGO overhead and memory management issues
	if a.Type == TypeString && b.Type == TypeString {
		return cmpString(a.Str, b.Str)
	}
	if a.Type == TypeBytes && b.Type == TypeBytes {
		return cmpBytes(a.Bytes, b.Bytes)
	}

	// For numeric types, use C++ implementation
	if (a.Type == TypeInt || a.Type == TypeFloat || a.Type == TypeBool) &&
		(b.Type == TypeInt || b.Type == TypeFloat || b.Type == TypeBool) {
		ca := a.cValueSimple()
		cb := b.cValueSimple()
		return int(C.svdb_value_compare(&ca, &cb))
	}

	// Mixed types - use C++ for type coercion
	ca := a.cValueSimple()
	cb := b.cValueSimple()
	return int(C.svdb_value_compare(&ca, &cb))
}

// cValueSimple converts a Go Value to C svdb_value_t without allocating strings.
// For string/bytes types, it sets null pointers - caller should handle separately.
func (v Value) cValueSimple() C.svdb_value_t {
	var cv C.svdb_value_t
	switch v.Type {
	case TypeNull:
		C.svdb_value_init_null(&cv)
	case TypeInt:
		C.svdb_value_init_int(&cv, C.int64_t(v.Int))
	case TypeFloat:
		C.svdb_value_init_float(&cv, C.double(v.Float))
	case TypeString:
		// Don't pass string pointer - handled in Go
		cv.val_type = C.int32_t(TypeString)
	case TypeBool:
		boolVal := 0
		if v.Int != 0 {
			boolVal = 1
		}
		C.svdb_value_init_bool(&cv, C.int(boolVal))
	case TypeBytes:
		// Don't pass bytes pointer - handled in Go
		cv.val_type = C.int32_t(TypeBytes)
	}
	return cv
}

func cmpString(a, b string) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func cmpBytes(a, b []byte) int {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return 0
}

// ParseValue parses a string (as produced by Value.String()) back to a Value.
// It tries int64, then float64, then falls back to string.
func ParseValue(s string) Value {
	cs := C.CString(s)
	defer C.free(unsafe.Pointer(cs))
	var cv C.svdb_value_t
	C.svdb_value_parse(&cv, cs, C.size_t(len(s)))
	return toGoValue(cv)
}
