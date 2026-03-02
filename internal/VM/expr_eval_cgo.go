package VM

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb -lstdc++
#cgo CFLAGS: -I${SRCDIR}/../../src/core/VM
#include "expr_eval.h"
#include <stdlib.h>
*/
import "C"
import (
	"runtime"
	"unsafe"
)

// CompareInt64Batch performs a batch comparison of two int64 slices.
// Each element of results is set to -1, 0, or 1.
// len(a) must equal len(b) and len(results) must be >= len(a).
func CompareInt64Batch(a, b []int64, results []int32) {
	n := len(a)
	if n == 0 || len(b) < n || len(results) < n {
		return
	}
	var pinner runtime.Pinner
	pinner.Pin(&a[0])
	pinner.Pin(&b[0])
	pinner.Pin(&results[0])
	defer pinner.Unpin()
	C.svdb_compare_int64_batch(
		(*C.int64_t)(unsafe.Pointer(&a[0])),
		(*C.int64_t)(unsafe.Pointer(&b[0])),
		(*C.int)(unsafe.Pointer(&results[0])),
		C.size_t(n),
	)
}

// CompareFloat64Batch performs a batch comparison of two float64 slices.
// Each element of results is set to -1, 0, or 1.
func CompareFloat64Batch(a, b []float64, results []int32) {
	n := len(a)
	if n == 0 || len(b) < n || len(results) < n {
		return
	}
	var pinner runtime.Pinner
	pinner.Pin(&a[0])
	pinner.Pin(&b[0])
	pinner.Pin(&results[0])
	defer pinner.Unpin()
	C.svdb_compare_float64_batch(
		(*C.double)(unsafe.Pointer(&a[0])),
		(*C.double)(unsafe.Pointer(&b[0])),
		(*C.int)(unsafe.Pointer(&results[0])),
		C.size_t(n),
	)
}

// AddInt64Batch adds two int64 slices element-wise into results.
func AddInt64Batch(a, b, results []int64) {
	n := len(a)
	if n == 0 || len(b) < n || len(results) < n {
		return
	}
	var pinner runtime.Pinner
	pinner.Pin(&a[0])
	pinner.Pin(&b[0])
	pinner.Pin(&results[0])
	defer pinner.Unpin()
	C.svdb_add_int64_batch(
		(*C.int64_t)(unsafe.Pointer(&a[0])),
		(*C.int64_t)(unsafe.Pointer(&b[0])),
		(*C.int64_t)(unsafe.Pointer(&results[0])),
		C.size_t(n),
	)
}

// AddFloat64Batch adds two float64 slices element-wise into results.
func AddFloat64Batch(a, b, results []float64) {
	n := len(a)
	if n == 0 || len(b) < n || len(results) < n {
		return
	}
	var pinner runtime.Pinner
	pinner.Pin(&a[0])
	pinner.Pin(&b[0])
	pinner.Pin(&results[0])
	defer pinner.Unpin()
	C.svdb_add_float64_batch(
		(*C.double)(unsafe.Pointer(&a[0])),
		(*C.double)(unsafe.Pointer(&b[0])),
		(*C.double)(unsafe.Pointer(&results[0])),
		C.size_t(n),
	)
}

// SubInt64Batch subtracts b from a element-wise into results.
func SubInt64Batch(a, b, results []int64) {
	n := len(a)
	if n == 0 || len(b) < n || len(results) < n {
		return
	}
	var pinner runtime.Pinner
	pinner.Pin(&a[0])
	pinner.Pin(&b[0])
	pinner.Pin(&results[0])
	defer pinner.Unpin()
	C.svdb_sub_int64_batch(
		(*C.int64_t)(unsafe.Pointer(&a[0])),
		(*C.int64_t)(unsafe.Pointer(&b[0])),
		(*C.int64_t)(unsafe.Pointer(&results[0])),
		C.size_t(n),
	)
}

// SubFloat64Batch subtracts b from a element-wise into results.
func SubFloat64Batch(a, b, results []float64) {
	n := len(a)
	if n == 0 || len(b) < n || len(results) < n {
		return
	}
	var pinner runtime.Pinner
	pinner.Pin(&a[0])
	pinner.Pin(&b[0])
	pinner.Pin(&results[0])
	defer pinner.Unpin()
	C.svdb_sub_float64_batch(
		(*C.double)(unsafe.Pointer(&a[0])),
		(*C.double)(unsafe.Pointer(&b[0])),
		(*C.double)(unsafe.Pointer(&results[0])),
		C.size_t(n),
	)
}

// MulInt64Batch multiplies two int64 slices element-wise into results.
func MulInt64Batch(a, b, results []int64) {
	n := len(a)
	if n == 0 || len(b) < n || len(results) < n {
		return
	}
	var pinner runtime.Pinner
	pinner.Pin(&a[0])
	pinner.Pin(&b[0])
	pinner.Pin(&results[0])
	defer pinner.Unpin()
	C.svdb_mul_int64_batch(
		(*C.int64_t)(unsafe.Pointer(&a[0])),
		(*C.int64_t)(unsafe.Pointer(&b[0])),
		(*C.int64_t)(unsafe.Pointer(&results[0])),
		C.size_t(n),
	)
}

// MulFloat64Batch multiplies two float64 slices element-wise into results.
func MulFloat64Batch(a, b, results []float64) {
	n := len(a)
	if n == 0 || len(b) < n || len(results) < n {
		return
	}
	var pinner runtime.Pinner
	pinner.Pin(&a[0])
	pinner.Pin(&b[0])
	pinner.Pin(&results[0])
	defer pinner.Unpin()
	C.svdb_mul_float64_batch(
		(*C.double)(unsafe.Pointer(&a[0])),
		(*C.double)(unsafe.Pointer(&b[0])),
		(*C.double)(unsafe.Pointer(&results[0])),
		C.size_t(n),
	)
}

// FilterMask applies a boolean mask to an index array.
// mask[i] != 0 means indices[i] is selected.
// Returns the filtered indices as a new slice.
func FilterMask(mask []int8, indices []int64) []int64 {
	n := len(mask)
	if n == 0 || len(indices) < n {
		return nil
	}
	out := make([]int64, n) // worst case: all selected
	var pinner runtime.Pinner
	pinner.Pin(&mask[0])
	pinner.Pin(&indices[0])
	pinner.Pin(&out[0])
	defer pinner.Unpin()
	kept := C.svdb_filter_mask(
		(*C.int8_t)(unsafe.Pointer(&mask[0])),
		(*C.int64_t)(unsafe.Pointer(&indices[0])),
		(*C.int64_t)(unsafe.Pointer(&out[0])),
		C.size_t(n),
	)
	return out[:int(kept)]
}
