
package VM

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb
#cgo CFLAGS: -I${SRCDIR}/../../src/core/VM
#include "type_conv.h"
#include <stdlib.h>
*/
import "C"

import "unsafe"

// ParseInt64Batch parses int64 values from strings (CGO).
func ParseInt64Batch(strs []string) ([]int64, []bool) {
	if len(strs) == 0 {
		return nil, nil
	}
	cstrs := make([]*C.char, len(strs))
	for i, s := range strs {
		cstrs[i] = C.CString(s)
	}
	defer func() {
		for _, cs := range cstrs {
			C.free(unsafe.Pointer(cs))
		}
	}()

	results := make([]int64, len(strs))
	cok := make([]C.int, len(strs))

	C.svdb_parse_int64_batch(
		(**C.char)(unsafe.Pointer(&cstrs[0])),
		(*C.int64_t)(unsafe.Pointer(&results[0])),
		&cok[0],
		C.size_t(len(strs)),
	)

	ok := make([]bool, len(strs))
	for i, v := range cok {
		ok[i] = v != 0
	}
	return results, ok
}

// ParseFloat64Batch parses float64 values from strings (CGO).
func ParseFloat64Batch(strs []string) ([]float64, []bool) {
	if len(strs) == 0 {
		return nil, nil
	}
	cstrs := make([]*C.char, len(strs))
	for i, s := range strs {
		cstrs[i] = C.CString(s)
	}
	defer func() {
		for _, cs := range cstrs {
			C.free(unsafe.Pointer(cs))
		}
	}()

	results := make([]float64, len(strs))
	cok := make([]C.int, len(strs))

	C.svdb_parse_float64_batch(
		(**C.char)(unsafe.Pointer(&cstrs[0])),
		(*C.double)(unsafe.Pointer(&results[0])),
		&cok[0],
		C.size_t(len(strs)),
	)

	ok := make([]bool, len(strs))
	for i, v := range cok {
		ok[i] = v != 0
	}
	return results, ok
}

// FormatInt64Batch formats int64 values to strings (CGO).
func FormatInt64Batch(values []int64) []string {
	if len(values) == 0 {
		return nil
	}
	// Each int64 fits in 32 bytes (sign + 19 digits + NUL)
	bufSize := len(values) * 32
	buf := make([]byte, bufSize)
	offsets := make([]C.size_t, len(values))

	C.svdb_format_int64_batch(
		(*C.int64_t)(unsafe.Pointer(&values[0])),
		(*C.char)(unsafe.Pointer(&buf[0])),
		&offsets[0],
		C.size_t(len(values)),
	)

	results := make([]string, len(values))
	for i, off := range offsets {
		start := int(off)
		end := start
		for end < len(buf) && buf[end] != 0 {
			end++
		}
		results[i] = string(buf[start:end])
	}
	return results
}

// FormatFloat64Batch formats float64 values to strings (CGO).
func FormatFloat64Batch(values []float64) []string {
	if len(values) == 0 {
		return nil
	}
	// Each float64 fits in 64 bytes
	bufSize := len(values) * 64
	buf := make([]byte, bufSize)
	offsets := make([]C.size_t, len(values))

	C.svdb_format_float64_batch(
		(*C.double)(unsafe.Pointer(&values[0])),
		(*C.char)(unsafe.Pointer(&buf[0])),
		&offsets[0],
		C.size_t(len(values)),
	)

	results := make([]string, len(values))
	for i, off := range offsets {
		start := int(off)
		end := start
		for end < len(buf) && buf[end] != 0 {
			end++
		}
		results[i] = string(buf[start:end])
	}
	return results
}
