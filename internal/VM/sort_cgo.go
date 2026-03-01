//go:build SVDB_ENABLE_CGO_VM
// +build SVDB_ENABLE_CGO_VM

package VM

/*
#cgo LDFLAGS: -L${SRCDIR}/cgo/../../../.build/cmake/lib -lsvdb_vm
#cgo CFLAGS: -I${SRCDIR}/cgo
#include "sort.h"
#include <stdlib.h>
*/
import "C"

import (
	"bytes"
	"sort"
	"unsafe"
)

// SortInt64 sorts int64 slice in place (CGO)
func SortInt64(data []int64) {
	if len(data) <= 1 {
		return
	}
	C.svdb_sort_int64((*C.int64_t)(unsafe.Pointer(&data[0])), C.size_t(len(data)))
}

// SortInt64WithIndices sorts int64 with index tracking
func SortInt64WithIndices(data []int64, indices []int64) {
	if len(data) <= 1 {
		return
	}
	var idxPtr *C.int64_t
	if len(indices) > 0 {
		idxPtr = (*C.int64_t)(unsafe.Pointer(&indices[0]))
	}
	C.svdb_sort_int64_with_indices(
		(*C.int64_t)(unsafe.Pointer(&data[0])),
		idxPtr,
		C.size_t(len(data)),
	)
}

// SortStrings sorts string slice (CGO)
func SortStrings(data []string) {
	if len(data) <= 1 {
		return
	}

	// Convert to C strings
	cStrings := make([]*C.char, len(data))
	for i, s := range data {
		cStrings[i] = C.CString(s)
	}
	defer func() {
		for _, cs := range cStrings {
			C.free(unsafe.Pointer(cs))
		}
	}()

	C.svdb_sort_strings(&cStrings[0], C.size_t(len(data)))

	// Convert back to Go strings
	for i, cs := range cStrings {
		data[i] = C.GoString(cs)
	}
}

// SortStringsWithIndices sorts strings with index tracking
func SortStringsWithIndices(data []string, indices []int64) {
	if len(data) <= 1 {
		return
	}

	cStrings := make([]*C.char, len(data))
	for i, s := range data {
		cStrings[i] = C.CString(s)
	}
	defer func() {
		for _, cs := range cStrings {
			C.free(unsafe.Pointer(cs))
		}
	}()

	var idxPtr *C.int64_t
	if len(indices) > 0 {
		idxPtr = (*C.int64_t)(unsafe.Pointer(&indices[0]))
	}

	C.svdb_sort_strings_with_indices(&cStrings[0], idxPtr, C.size_t(len(data)))

	for i, cs := range cStrings {
		data[i] = C.GoString(cs)
	}
}

// SortBytes sorts byte slices by content
func SortBytes(data [][]byte) {
	if len(data) <= 1 {
		return
	}

	// For simplicity, use Go sort for now
	// CGO implementation would require tracking original indices
	sort.Slice(data, func(i, j int) bool {
		return bytes.Compare(data[i], data[j]) < 0
	})
}
