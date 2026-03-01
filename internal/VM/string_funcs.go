
package VM

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb
#cgo CFLAGS: -I${SRCDIR}/../../src/core/VM
#include "string_funcs.h"
#include <stdlib.h>
*/
import "C"

import "unsafe"

// StrUpperBatch converts strings to uppercase (CGO).
func StrUpperBatch(strs []string) []string {
	if len(strs) == 0 {
		return nil
	}

	// Calculate total buffer size needed
	totalLen := 0
	for _, s := range strs {
		totalLen += len(s) + 1
	}
	buf := make([]byte, totalLen)
	cstrs := make([]*C.char, len(strs))
	clens := make([]C.size_t, len(strs))
	offsets := make([]C.size_t, len(strs))

	for i, s := range strs {
		cstrs[i] = C.CString(s)
		clens[i] = C.size_t(len(s))
	}
	defer func() {
		for _, cs := range cstrs {
			C.free(unsafe.Pointer(cs))
		}
	}()

	var outPtr *C.char
	if len(buf) > 0 {
		outPtr = (*C.char)(unsafe.Pointer(&buf[0]))
	}

	C.svdb_str_upper_batch(
		(**C.char)(unsafe.Pointer(&cstrs[0])),
		&clens[0],
		outPtr,
		&offsets[0],
		C.size_t(len(strs)),
	)

	results := make([]string, len(strs))
	for i, off := range offsets {
		start := int(off)
		n := int(clens[i])
		results[i] = string(buf[start : start+n])
	}
	return results
}

// StrLowerBatch converts strings to lowercase (CGO).
func StrLowerBatch(strs []string) []string {
	if len(strs) == 0 {
		return nil
	}

	totalLen := 0
	for _, s := range strs {
		totalLen += len(s) + 1
	}
	buf := make([]byte, totalLen)
	cstrs := make([]*C.char, len(strs))
	clens := make([]C.size_t, len(strs))
	offsets := make([]C.size_t, len(strs))

	for i, s := range strs {
		cstrs[i] = C.CString(s)
		clens[i] = C.size_t(len(s))
	}
	defer func() {
		for _, cs := range cstrs {
			C.free(unsafe.Pointer(cs))
		}
	}()

	var outPtr *C.char
	if len(buf) > 0 {
		outPtr = (*C.char)(unsafe.Pointer(&buf[0]))
	}

	C.svdb_str_lower_batch(
		(**C.char)(unsafe.Pointer(&cstrs[0])),
		&clens[0],
		outPtr,
		&offsets[0],
		C.size_t(len(strs)),
	)

	results := make([]string, len(strs))
	for i, off := range offsets {
		start := int(off)
		n := int(clens[i])
		results[i] = string(buf[start : start+n])
	}
	return results
}

// StrTrimBatch trims whitespace from both ends (CGO).
func StrTrimBatch(strs []string) []string {
	if len(strs) == 0 {
		return nil
	}
	results := make([]string, len(strs))
	for i, s := range strs {
		if len(s) == 0 {
			results[i] = s
			continue
		}
		cs := C.CString(s)
		var outLen C.size_t
		startOff := C.svdb_str_trim(cs, C.size_t(len(s)), 1, 1, &outLen)
		results[i] = s[int(startOff) : int(startOff)+int(outLen)]
		C.free(unsafe.Pointer(cs))
	}
	return results
}

// StrSubstrBatch extracts substrings (CGO).
// start is 1-based; length=-1 means to end.
func StrSubstrBatch(strs []string, starts, lengths []int64) []string {
	n := len(strs)
	if n == 0 {
		return nil
	}
	results := make([]string, n)
	// Reuse a small buffer for each substring
	buf := make([]byte, 4096)
	for i := 0; i < n; i++ {
		s := strs[i]
		start := int64(1)
		length := int64(-1)
		if i < len(starts) {
			start = starts[i]
		}
		if i < len(lengths) {
			length = lengths[i]
		}
		needed := len(s) + 1
		if needed > len(buf) {
			buf = make([]byte, needed)
		}
		cs := C.CString(s)
		nout := C.svdb_str_substr(
			cs,
			C.size_t(len(s)),
			C.int64_t(start),
			C.int64_t(length),
			(*C.char)(unsafe.Pointer(&buf[0])),
			C.size_t(len(buf)),
		)
		C.free(unsafe.Pointer(cs))
		results[i] = string(buf[:int(nout)])
	}
	return results
}
