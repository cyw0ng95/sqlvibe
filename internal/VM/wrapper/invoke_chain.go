// Package wrapper provides Phase 4 invoke chain CGO bindings.
// Each exported function sequences multiple C-level subsystem calls
// inside a single CGO boundary crossing, reducing Go↔CGO overhead.
package wrapper

/*
#cgo LDFLAGS: -L${SRCDIR}/cgo/../../../../.build/cmake/lib -lsvdb
#cgo CFLAGS: -I${SRCDIR}/cgo/../../../../src/core/wrapper
#include "invoke_chain_wrapper.h"
#include <stdlib.h>
*/
import "C"

import (
	"runtime"
	"unsafe"
)

// ─── Phase 4.2: Invoke chain wrapper ─────────────────────────────────────────

// PipelineHashFilter hashes keys and returns indices whose hash falls in the
// target bucket (hash(key) % bucketCount == targetBucket).
// All three operations (hash, modulo, filter) happen inside one CGO call.
func PipelineHashFilter(keys [][]byte, seed, bucketCount, targetBucket uint64) []int {
	if len(keys) == 0 || bucketCount == 0 {
		return nil
	}

	var pinner runtime.Pinner
	defer pinner.Unpin()

	keyPtrs := make([]*C.uint8_t, len(keys))
	keyLens := make([]C.size_t, len(keys))
	for i, k := range keys {
		if len(k) > 0 {
			pinner.Pin(&k[0])
			keyPtrs[i] = (*C.uint8_t)(unsafe.Pointer(&k[0]))
		}
		keyLens[i] = C.size_t(len(k))
	}
	pinner.Pin(&keyPtrs[0])
	pinner.Pin(&keyLens[0])

	outIndices := make([]C.size_t, len(keys))
	n := C.svdb_pipeline_hash_filter(
		&keyPtrs[0],
		&keyLens[0],
		C.size_t(len(keys)),
		C.uint64_t(seed),
		C.uint64_t(bucketCount),
		C.uint64_t(targetBucket),
		&outIndices[0],
	)

	count := int(n)
	result := make([]int, count)
	for i := range result {
		result[i] = int(outIndices[i])
	}
	return result
}

// ─── Phase 4.3: Expression batch wrapper ─────────────────────────────────────

// BatchEvalCompareInt64 compares pairs (a[i], b[i]) with the given operator
// and returns a bitmask (1=pass, 0=fail) plus the count of passing rows.
// All comparisons happen inside one CGO call.
func BatchEvalCompareInt64(a, b []int64, op CompareOp) (mask []byte, passCount int) {
	n := len(a)
	if n == 0 || len(b) < n {
		return nil, 0
	}
	outMask := make([]byte, n)
	cnt := C.svdb_batch_eval_compare_int64(
		(*C.int64_t)(unsafe.Pointer(&a[0])),
		(*C.int64_t)(unsafe.Pointer(&b[0])),
		C.size_t(n),
		C.int(op),
		(*C.uint8_t)(unsafe.Pointer(&outMask[0])),
	)
	return outMask, int(cnt)
}

// BatchEvalCompareFloat64 is like BatchEvalCompareInt64 but for float64 values.
func BatchEvalCompareFloat64(a, b []float64, op CompareOp) (mask []byte, passCount int) {
	n := len(a)
	if n == 0 || len(b) < n {
		return nil, 0
	}
	outMask := make([]byte, n)
	cnt := C.svdb_batch_eval_compare_float64(
		(*C.double)(unsafe.Pointer(&a[0])),
		(*C.double)(unsafe.Pointer(&b[0])),
		C.size_t(n),
		C.int(op),
		(*C.uint8_t)(unsafe.Pointer(&outMask[0])),
	)
	return outMask, int(cnt)
}

// BatchArithAndCompareInt64 computes tmp[i] = arithOp(a[i], b[i]) then
// evaluates tmp[i] cmpOp threshold in a single CGO call.
// Returns bitmask and count of passing rows.
func BatchArithAndCompareInt64(a, b []int64, arith ArithOp, threshold int64, cmp CompareOp) (mask []byte, passCount int) {
	n := len(a)
	if n == 0 || len(b) < n {
		return nil, 0
	}
	outMask := make([]byte, n)
	cnt := C.svdb_batch_arith_and_compare_int64(
		(*C.int64_t)(unsafe.Pointer(&a[0])),
		(*C.int64_t)(unsafe.Pointer(&b[0])),
		C.size_t(n),
		C.int(arith),
		C.int64_t(threshold),
		C.int(cmp),
		(*C.uint8_t)(unsafe.Pointer(&outMask[0])),
	)
	return outMask, int(cnt)
}

// ─── Phase 4.4: Storage access wrapper ───────────────────────────────────────

// ScanFilterInt64 scans a column, applies a comparison filter, and returns
// the indices of matching rows — all in a single CGO call.
func ScanFilterInt64(column []int64, op CompareOp, threshold int64) []int {
	if len(column) == 0 {
		return nil
	}
	outIdx := make([]C.size_t, len(column))
	n := C.svdb_scan_filter_int64(
		(*C.int64_t)(unsafe.Pointer(&column[0])),
		C.size_t(len(column)),
		C.int(op),
		C.int64_t(threshold),
		&outIdx[0],
	)
	count := int(n)
	result := make([]int, count)
	for i := range result {
		result[i] = int(outIdx[i])
	}
	return result
}

// ScanFilterFloat64 is like ScanFilterInt64 for float64 columns.
func ScanFilterFloat64(column []float64, op CompareOp, threshold float64) []int {
	if len(column) == 0 {
		return nil
	}
	outIdx := make([]C.size_t, len(column))
	n := C.svdb_scan_filter_float64(
		(*C.double)(unsafe.Pointer(&column[0])),
		C.size_t(len(column)),
		C.int(op),
		C.double(threshold),
		&outIdx[0],
	)
	count := int(n)
	result := make([]int, count)
	for i := range result {
		result[i] = int(outIdx[i])
	}
	return result
}

// ScanAggregateInt64 scans a column, optionally filters it, then computes an
// aggregate (sum/min/max/count) — all in one CGO call.
// Pass filterOp < 0 to disable filtering.
func ScanAggregateInt64(column []int64, filterOp CompareOp, filterThreshold int64, hasFilter bool, agg AggOp) (aggValue int64, matchCount int) {
	if len(column) == 0 {
		return 0, 0
	}
	op := C.int(filterOp)
	if !hasFilter {
		op = C.int(-1)
	}
	var outAgg C.int64_t
	n := C.svdb_scan_aggregate_int64(
		(*C.int64_t)(unsafe.Pointer(&column[0])),
		C.size_t(len(column)),
		op,
		C.int64_t(filterThreshold),
		C.int(agg),
		&outAgg,
	)
	return int64(outAgg), int(n)
}
