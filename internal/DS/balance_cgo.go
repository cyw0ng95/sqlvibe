package DS

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb -lstdc++
#cgo CFLAGS: -I${SRCDIR}/../../src/core/DS
#include "balance.h"
*/
import "C"
import "unsafe"

// CBalanceIsOverfull returns true if the page is more than 90% used.
func CBalanceIsOverfull(data []byte) bool {
	if len(data) == 0 {
		return false
	}
	return C.svdb_balance_is_overfull((*C.uint8_t)(unsafe.Pointer(&data[0])), C.size_t(len(data))) != 0
}

// CBalanceIsUnderfull returns true if the page is less than 33% used.
func CBalanceIsUnderfull(data []byte) bool {
	if len(data) == 0 {
		return false
	}
	return C.svdb_balance_is_underfull((*C.uint8_t)(unsafe.Pointer(&data[0])), C.size_t(len(data))) != 0
}

// CBalanceSplitLeaf splits a leaf page at splitPoint.
// leftData is modified in-place (retains cells [0..splitPoint)).
// rightData receives cells [splitPoint..n). Both slices must be the same length (page_size).
// Returns the divider key bytes and ok=true on success.
func CBalanceSplitLeaf(leftData []byte, rightData []byte, splitPoint int) (dividerKey []byte, ok bool) {
	if len(leftData) == 0 || len(rightData) == 0 {
		return nil, false
	}
	buf := make([]byte, len(leftData))
	var outLen C.size_t
	rc := C.svdb_balance_split_leaf(
		(*C.uint8_t)(unsafe.Pointer(&leftData[0])),
		C.size_t(len(leftData)),
		(*C.uint8_t)(unsafe.Pointer(&rightData[0])),
		C.int(splitPoint),
		(*C.uint8_t)(unsafe.Pointer(&buf[0])),
		&outLen,
	)
	if rc == 0 {
		return nil, false
	}
	return buf[:int(outLen)], true
}

// CBalanceMergePages merges all cells from rightData into leftData.
// Both slices must be the same length (page_size); the C layer uses len(leftData) as page_size for both.
func CBalanceMergePages(leftData []byte, rightData []byte) bool {
	if len(leftData) == 0 || len(rightData) == 0 {
		return false
	}
	return C.svdb_balance_merge_pages(
		(*C.uint8_t)(unsafe.Pointer(&leftData[0])),
		C.size_t(len(leftData)),
		(*C.uint8_t)(unsafe.Pointer(&rightData[0])),
	) == 1
}

// CBalanceRedistribute moves moveCount cells from srcData to dstData.
// Both slices must be the same length (page_size).
func CBalanceRedistribute(srcData []byte, dstData []byte, moveCount int) bool {
	if len(srcData) == 0 || len(dstData) == 0 {
		return false
	}
	return C.svdb_balance_redistribute(
		(*C.uint8_t)(unsafe.Pointer(&srcData[0])),
		C.size_t(len(srcData)),
		(*C.uint8_t)(unsafe.Pointer(&dstData[0])),
		C.int(moveCount),
	) == 1
}

// CBalanceCalculateSplitPoint returns the optimal split point index, or -1 on error.
func CBalanceCalculateSplitPoint(data []byte) int {
	if len(data) == 0 {
		return -1
	}
	return int(C.svdb_balance_calculate_split_point((*C.uint8_t)(unsafe.Pointer(&data[0])), C.size_t(len(data))))
}
