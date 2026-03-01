//go:build SVDB_ENABLE_CGO_VM
// +build SVDB_ENABLE_CGO_VM

package VM

/*
#cgo LDFLAGS: -L${SRCDIR}/cgo/../../../.build/cmake/lib -lsvdb_vm
#cgo CFLAGS: -I${SRCDIR}/cgo
#include "expr_eval.h"
#include <stdlib.h>
*/
import "C"

import "unsafe"

// CompareInt64Batch compares pairs of int64 values (CGO).
func CompareInt64Batch(a, b []int64) []int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	if n == 0 {
		return nil
	}
	cresults := make([]C.int, n)
	C.svdb_compare_int64_batch(
		(*C.int64_t)(unsafe.Pointer(&a[0])),
		(*C.int64_t)(unsafe.Pointer(&b[0])),
		&cresults[0],
		C.size_t(n),
	)
	results := make([]int, n)
	for i, v := range cresults {
		results[i] = int(v)
	}
	return results
}

// CompareFloat64Batch compares pairs of float64 values (CGO).
func CompareFloat64Batch(a, b []float64) []int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	if n == 0 {
		return nil
	}
	cresults := make([]C.int, n)
	C.svdb_compare_float64_batch(
		(*C.double)(unsafe.Pointer(&a[0])),
		(*C.double)(unsafe.Pointer(&b[0])),
		&cresults[0],
		C.size_t(n),
	)
	results := make([]int, n)
	for i, v := range cresults {
		results[i] = int(v)
	}
	return results
}

// AddInt64Batch adds pairs of int64 values (CGO).
func AddInt64Batch(a, b []int64) []int64 {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	if n == 0 {
		return nil
	}
	results := make([]int64, n)
	C.svdb_add_int64_batch(
		(*C.int64_t)(unsafe.Pointer(&a[0])),
		(*C.int64_t)(unsafe.Pointer(&b[0])),
		(*C.int64_t)(unsafe.Pointer(&results[0])),
		C.size_t(n),
	)
	return results
}

// AddFloat64Batch adds pairs of float64 values (CGO).
func AddFloat64Batch(a, b []float64) []float64 {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	if n == 0 {
		return nil
	}
	results := make([]float64, n)
	C.svdb_add_float64_batch(
		(*C.double)(unsafe.Pointer(&a[0])),
		(*C.double)(unsafe.Pointer(&b[0])),
		(*C.double)(unsafe.Pointer(&results[0])),
		C.size_t(n),
	)
	return results
}

// SubInt64Batch subtracts pairs of int64 values (CGO).
func SubInt64Batch(a, b []int64) []int64 {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	if n == 0 {
		return nil
	}
	results := make([]int64, n)
	C.svdb_sub_int64_batch(
		(*C.int64_t)(unsafe.Pointer(&a[0])),
		(*C.int64_t)(unsafe.Pointer(&b[0])),
		(*C.int64_t)(unsafe.Pointer(&results[0])),
		C.size_t(n),
	)
	return results
}

// SubFloat64Batch subtracts pairs of float64 values (CGO).
func SubFloat64Batch(a, b []float64) []float64 {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	if n == 0 {
		return nil
	}
	results := make([]float64, n)
	C.svdb_sub_float64_batch(
		(*C.double)(unsafe.Pointer(&a[0])),
		(*C.double)(unsafe.Pointer(&b[0])),
		(*C.double)(unsafe.Pointer(&results[0])),
		C.size_t(n),
	)
	return results
}

// MulInt64Batch multiplies pairs of int64 values (CGO).
func MulInt64Batch(a, b []int64) []int64 {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	if n == 0 {
		return nil
	}
	results := make([]int64, n)
	C.svdb_mul_int64_batch(
		(*C.int64_t)(unsafe.Pointer(&a[0])),
		(*C.int64_t)(unsafe.Pointer(&b[0])),
		(*C.int64_t)(unsafe.Pointer(&results[0])),
		C.size_t(n),
	)
	return results
}

// MulFloat64Batch multiplies pairs of float64 values (CGO).
func MulFloat64Batch(a, b []float64) []float64 {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	if n == 0 {
		return nil
	}
	results := make([]float64, n)
	C.svdb_mul_float64_batch(
		(*C.double)(unsafe.Pointer(&a[0])),
		(*C.double)(unsafe.Pointer(&b[0])),
		(*C.double)(unsafe.Pointer(&results[0])),
		C.size_t(n),
	)
	return results
}

// FilterMask returns indices where mask[i] is true (CGO).
func FilterMask(mask []bool, indices []int64) []int64 {
	n := len(mask)
	if len(indices) < n {
		n = len(indices)
	}
	if n == 0 {
		return nil
	}
	// Convert []bool to []int8 for C
	cmask := make([]int8, n)
	for i := 0; i < n; i++ {
		if mask[i] {
			cmask[i] = 1
		}
	}
	out := make([]int64, n)
	cnt := C.svdb_filter_mask(
		(*C.int8_t)(unsafe.Pointer(&cmask[0])),
		(*C.int64_t)(unsafe.Pointer(&indices[0])),
		(*C.int64_t)(unsafe.Pointer(&out[0])),
		C.size_t(n),
	)
	return out[:cnt]
}
