
package VM

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb
#cgo CFLAGS: -I${SRCDIR}/../../src/core/VM
#include "datetime.h"
#include <stdlib.h>
*/
import "C"

import "unsafe"

// JuliandayFromString parses a date/datetime string into a Julian Day Number (CGO).
func JuliandayFromString(s string) (float64, bool) {
	cs := C.CString(s)
	defer C.free(unsafe.Pointer(cs))
	v := float64(C.svdb_julianday(cs, C.size_t(len(s))))
	return v, v != 0.0
}

// UnixepochFromString parses a date/datetime string into a Unix timestamp (CGO).
func UnixepochFromString(s string) (int64, bool) {
	cs := C.CString(s)
	defer C.free(unsafe.Pointer(cs))
	v := int64(C.svdb_unixepoch(cs, C.size_t(len(s))))
	// svdb_unixepoch returns 0 on error; accept 0 for 1970-01-01
	return v, len(s) >= 10
}

// JuliandayBatch converts date strings to Julian Day Numbers (CGO).
func JuliandayBatch(strs []string) ([]float64, []bool) {
	if len(strs) == 0 {
		return nil, nil
	}
	cstrs := make([]*C.char, len(strs))
	clens := make([]C.size_t, len(strs))
	for i, s := range strs {
		cstrs[i] = C.CString(s)
		clens[i] = C.size_t(len(s))
	}
	defer func() {
		for _, cs := range cstrs {
			C.free(unsafe.Pointer(cs))
		}
	}()

	results := make([]float64, len(strs))
	cok := make([]C.int, len(strs))

	C.svdb_julianday_batch(
		(**C.char)(unsafe.Pointer(&cstrs[0])),
		&clens[0],
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

// UnixepochBatch converts date strings to Unix timestamps (CGO).
func UnixepochBatch(strs []string) ([]int64, []bool) {
	if len(strs) == 0 {
		return nil, nil
	}
	cstrs := make([]*C.char, len(strs))
	clens := make([]C.size_t, len(strs))
	for i, s := range strs {
		cstrs[i] = C.CString(s)
		clens[i] = C.size_t(len(s))
	}
	defer func() {
		for _, cs := range cstrs {
			C.free(unsafe.Pointer(cs))
		}
	}()

	results := make([]int64, len(strs))
	cok := make([]C.int, len(strs))

	C.svdb_unixepoch_batch(
		(**C.char)(unsafe.Pointer(&cstrs[0])),
		&clens[0],
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
