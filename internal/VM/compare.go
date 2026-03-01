
package VM

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb
#cgo CFLAGS: -I${SRCDIR}/../../src/core/VM
#include "compare.h"
#include <stdlib.h>
*/
import "C"

import (
	"unsafe"
)

// Compare compares two byte slices
// Returns: -1 if a < b, 0 if a == b, 1 if a > b
func Compare(a, b []byte) int {
	var aPtr *C.uint8_t
	var bPtr *C.uint8_t
	var aLen, bLen C.size_t

	if len(a) > 0 {
		aPtr = (*C.uint8_t)(unsafe.Pointer(&a[0]))
		aLen = C.size_t(len(a))
	}
	if len(b) > 0 {
		bPtr = (*C.uint8_t)(unsafe.Pointer(&b[0]))
		bLen = C.size_t(len(b))
	}

	return int(C.svdb_compare(aPtr, aLen, bPtr, bLen))
}

// CompareBatch compares multiple pairs of byte slices
func CompareBatch(a, b [][]byte) []int {
	if len(a) != len(b) {
		return nil
	}
	if len(a) == 0 {
		return nil
	}

	aPtrs := make([]*C.uint8_t, len(a))
	aLens := make([]C.size_t, len(a))
	bPtrs := make([]*C.uint8_t, len(b))
	bLens := make([]C.size_t, len(b))
	results := make([]int, len(a))

	for i := range a {
		if len(a[i]) > 0 {
			aPtrs[i] = (*C.uint8_t)(unsafe.Pointer(&a[i][0]))
			aLens[i] = C.size_t(len(a[i]))
		}
		if len(b[i]) > 0 {
			bPtrs[i] = (*C.uint8_t)(unsafe.Pointer(&b[i][0]))
			bLens[i] = C.size_t(len(b[i]))
		}
	}

	C.svdb_compare_batch(
		&aPtrs[0], &aLens[0],
		&bPtrs[0], &bLens[0],
		(*C.int)(unsafe.Pointer(&results[0])),
		C.size_t(len(a)),
	)

	return results
}

// EqualBatch checks equality for multiple pairs
func EqualBatch(a, b [][]byte) []bool {
	if len(a) != len(b) {
		return nil
	}
	if len(a) == 0 {
		return nil
	}

	aPtrs := make([]*C.uint8_t, len(a))
	aLens := make([]C.size_t, len(a))
	bPtrs := make([]*C.uint8_t, len(b))
	bLens := make([]C.size_t, len(b))
	results := make([]uint8, len(a))

	for i := range a {
		if len(a[i]) > 0 {
			aPtrs[i] = (*C.uint8_t)(unsafe.Pointer(&a[i][0]))
			aLens[i] = C.size_t(len(a[i]))
		}
		if len(b[i]) > 0 {
			bPtrs[i] = (*C.uint8_t)(unsafe.Pointer(&b[i][0]))
			bLens[i] = C.size_t(len(b[i]))
		}
	}

	C.svdb_equal_batch(
		&aPtrs[0], &aLens[0],
		&bPtrs[0], &bLens[0],
		(*C.uint8_t)(unsafe.Pointer(&results[0])),
		C.size_t(len(a)),
	)

	boolResults := make([]bool, len(a))
	for i := range results {
		boolResults[i] = results[i] != 0
	}
	return boolResults
}

// HasPrefix checks if a starts with prefix
func HasPrefix(a, prefix []byte) bool {
	var aPtr *C.uint8_t
	var prefixPtr *C.uint8_t
	var aLen, prefixLen C.size_t

	if len(a) > 0 {
		aPtr = (*C.uint8_t)(unsafe.Pointer(&a[0]))
		aLen = C.size_t(len(a))
	}
	if len(prefix) > 0 {
		prefixPtr = (*C.uint8_t)(unsafe.Pointer(&prefix[0]))
		prefixLen = C.size_t(len(prefix))
	}

	return C.svdb_has_prefix(aPtr, aLen, prefixPtr, prefixLen) != 0
}
