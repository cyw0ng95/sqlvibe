//go:build SVDB_ENABLE_CGO_VM
// +build SVDB_ENABLE_CGO_VM

package VM

/*
#cgo LDFLAGS: -L${SRCDIR}/cgo/../../../.build/cmake/lib -lsvdb_vm
#cgo CFLAGS: -I${SRCDIR}/cgo
#include "vm_dispatch.h"
#include <stdlib.h>
*/
import "C"

import "unsafe"

// DispatchSIMDLevel returns the SIMD capability level of the C++ dispatch engine.
func DispatchSIMDLevel() int {
	return int(C.svdb_dispatch_simd_level())
}

// DispatchIsDirectThreaded returns true when the C++ dispatch is compiled in.
func DispatchIsDirectThreaded() bool {
	return C.svdb_dispatch_is_direct_threaded() != 0
}

// ArithInt64Batch applies an arithmetic operation to slices of int64 values (CGO).
// op: 0=add, 1=sub, 2=mul, 3=div, 4=rem.
func ArithInt64Batch(op int, a, b []int64) ([]int64, string) {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	if n == 0 {
		return nil, ""
	}
	results := make([]int64, n)
	rc := C.svdb_dispatch_arith_int64(
		C.int(op),
		(*C.int64_t)(unsafe.Pointer(&a[0])),
		(*C.int64_t)(unsafe.Pointer(&b[0])),
		(*C.int64_t)(unsafe.Pointer(&results[0])),
		C.size_t(n),
	)
	// svdb_dispatch_arith_int64 returns:
	//   0  = success
	//  -1  = division by zero
	//  -2  = unsupported operation
	if rc == -1 {
		return nil, "division by zero"
	}
	if rc != 0 {
		return nil, "unsupported operation"
	}
	return results, ""
}

// ArithFloat64Batch applies an arithmetic operation to slices of float64 values (CGO).
func ArithFloat64Batch(op int, a, b []float64) []float64 {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	if n == 0 {
		return nil
	}
	results := make([]float64, n)
	C.svdb_dispatch_arith_float64(
		C.int(op),
		(*C.double)(unsafe.Pointer(&a[0])),
		(*C.double)(unsafe.Pointer(&b[0])),
		(*C.double)(unsafe.Pointer(&results[0])),
		C.size_t(n),
	)
	return results
}
