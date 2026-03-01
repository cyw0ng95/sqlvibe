//go:build SVDB_ENABLE_CGO_VM
// +build SVDB_ENABLE_CGO_VM

package VM

/*
#cgo LDFLAGS: -L${SRCDIR}/cgo/../../../.build/cmake/lib -lsvdb_vm
#cgo CFLAGS: -I${SRCDIR}/cgo
#include "aggregate.h"
#include <stdlib.h>
*/
import "C"

import "unsafe"

// AggSumInt64 sums int64 values, skipping nulls (CGO).
func AggSumInt64(values []int64, nullMask []bool) (int64, bool) {
	if len(values) == 0 {
		return 0, false
	}
	var cmask []int8
	var cmaskPtr *C.int8_t
	if nullMask != nil {
		cmask = make([]int8, len(values))
		for i, v := range nullMask {
			if i < len(values) && v {
				cmask[i] = 1
			}
		}
		cmaskPtr = (*C.int8_t)(unsafe.Pointer(&cmask[0]))
	}
	var cok C.int
	v := C.svdb_agg_sum_int64(
		(*C.int64_t)(unsafe.Pointer(&values[0])),
		cmaskPtr,
		C.size_t(len(values)),
		&cok,
	)
	return int64(v), cok != 0
}

// AggSumFloat64 sums float64 values (CGO).
func AggSumFloat64(values []float64, nullMask []bool) (float64, bool) {
	if len(values) == 0 {
		return 0, false
	}
	var cmask []int8
	var cmaskPtr *C.int8_t
	if nullMask != nil {
		cmask = make([]int8, len(values))
		for i, v := range nullMask {
			if i < len(values) && v {
				cmask[i] = 1
			}
		}
		cmaskPtr = (*C.int8_t)(unsafe.Pointer(&cmask[0]))
	}
	var cok C.int
	v := C.svdb_agg_sum_float64(
		(*C.double)(unsafe.Pointer(&values[0])),
		cmaskPtr,
		C.size_t(len(values)),
		&cok,
	)
	return float64(v), cok != 0
}

// AggMinInt64 finds the minimum int64 value (CGO).
func AggMinInt64(values []int64, nullMask []bool) (int64, bool) {
	if len(values) == 0 {
		return 0, false
	}
	var cmask []int8
	var cmaskPtr *C.int8_t
	if nullMask != nil {
		cmask = make([]int8, len(values))
		for i, v := range nullMask {
			if i < len(values) && v {
				cmask[i] = 1
			}
		}
		cmaskPtr = (*C.int8_t)(unsafe.Pointer(&cmask[0]))
	}
	var cok C.int
	v := C.svdb_agg_min_int64(
		(*C.int64_t)(unsafe.Pointer(&values[0])),
		cmaskPtr,
		C.size_t(len(values)),
		&cok,
	)
	return int64(v), cok != 0
}

// AggMaxInt64 finds the maximum int64 value (CGO).
func AggMaxInt64(values []int64, nullMask []bool) (int64, bool) {
	if len(values) == 0 {
		return 0, false
	}
	var cmask []int8
	var cmaskPtr *C.int8_t
	if nullMask != nil {
		cmask = make([]int8, len(values))
		for i, v := range nullMask {
			if i < len(values) && v {
				cmask[i] = 1
			}
		}
		cmaskPtr = (*C.int8_t)(unsafe.Pointer(&cmask[0]))
	}
	var cok C.int
	v := C.svdb_agg_max_int64(
		(*C.int64_t)(unsafe.Pointer(&values[0])),
		cmaskPtr,
		C.size_t(len(values)),
		&cok,
	)
	return int64(v), cok != 0
}

// AggMinFloat64 finds the minimum float64 value (CGO).
func AggMinFloat64(values []float64, nullMask []bool) (float64, bool) {
	if len(values) == 0 {
		return 0, false
	}
	var cmask []int8
	var cmaskPtr *C.int8_t
	if nullMask != nil {
		cmask = make([]int8, len(values))
		for i, v := range nullMask {
			if i < len(values) && v {
				cmask[i] = 1
			}
		}
		cmaskPtr = (*C.int8_t)(unsafe.Pointer(&cmask[0]))
	}
	var cok C.int
	v := C.svdb_agg_min_float64(
		(*C.double)(unsafe.Pointer(&values[0])),
		cmaskPtr,
		C.size_t(len(values)),
		&cok,
	)
	return float64(v), cok != 0
}

// AggMaxFloat64 finds the maximum float64 value (CGO).
func AggMaxFloat64(values []float64, nullMask []bool) (float64, bool) {
	if len(values) == 0 {
		return 0, false
	}
	var cmask []int8
	var cmaskPtr *C.int8_t
	if nullMask != nil {
		cmask = make([]int8, len(values))
		for i, v := range nullMask {
			if i < len(values) && v {
				cmask[i] = 1
			}
		}
		cmaskPtr = (*C.int8_t)(unsafe.Pointer(&cmask[0]))
	}
	var cok C.int
	v := C.svdb_agg_max_float64(
		(*C.double)(unsafe.Pointer(&values[0])),
		cmaskPtr,
		C.size_t(len(values)),
		&cok,
	)
	return float64(v), cok != 0
}

// AggCountNotNull counts non-null values (CGO).
func AggCountNotNull(nullMask []bool, total int) int64 {
	if total == 0 {
		return 0
	}
	if nullMask == nil {
		return int64(total)
	}
	cmask := make([]int8, total)
	for i := 0; i < total && i < len(nullMask); i++ {
		if nullMask[i] {
			cmask[i] = 1
		}
	}
	return int64(C.svdb_agg_count_notnull(
		(*C.int8_t)(unsafe.Pointer(&cmask[0])),
		C.size_t(total),
	))
}
