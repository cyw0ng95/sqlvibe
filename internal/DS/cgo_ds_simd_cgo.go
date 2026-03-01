//go:build SVDB_ENABLE_CGO_DS
// +build SVDB_ENABLE_CGO_DS

package DS

/*
#cgo LDFLAGS: -L${SRCDIR}/cgo/../../../.build/cmake/lib -lsvdb_ds
#cgo CFLAGS: -I${SRCDIR}/cgo
#include "simd.h"
#include "roaring.h"
#include <stdlib.h>
#include <string.h>
*/
import "C"

import (
	"unsafe"
)

// SIMD vector operations

// VectorSumDouble computes sum of float64 slice using SIMD
func VectorSumDouble(a []float64) float64 {
	if len(a) == 0 {
		return 0
	}
	return float64(C.svdb_vector_sum_double((*C.double)(unsafe.Pointer(&a[0])), C.size_t(len(a))))
}

// VectorSumInt64 computes sum of int64 slice using SIMD
func VectorSumInt64(a []int64) int64 {
	if len(a) == 0 {
		return 0
	}
	return int64(C.svdb_vector_sum_int64((*C.int64_t)(unsafe.Pointer(&a[0])), C.size_t(len(a))))
}

// VectorAddInt64 computes element-wise addition using SIMD: out = a + b
func VectorAddInt64(a, b, out []int64) {
	n := len(a)
	if n == 0 || len(b) < n || len(out) < n {
		return
	}
	C.svdb_vector_add_int64(
		(*C.int64_t)(unsafe.Pointer(&a[0])),
		(*C.int64_t)(unsafe.Pointer(&b[0])),
		(*C.int64_t)(unsafe.Pointer(&out[0])),
		C.size_t(n),
	)
}

// VectorEqInt64 counts equal elements using SIMD
func VectorEqInt64(a, b []int64) int {
	n := len(a)
	if n == 0 || len(b) < n {
		return 0
	}
	return int(C.svdb_vector_eq_int64(
		(*C.int64_t)(unsafe.Pointer(&a[0])),
		(*C.int64_t)(unsafe.Pointer(&b[0])),
		C.size_t(n),
	))
}

// VectorGTInt64 counts elements where a[i] > b[i] using SIMD
func VectorGTInt64(a, b []int64) int {
	n := len(a)
	if n == 0 || len(b) < n {
		return 0
	}
	return int(C.svdb_vector_gt_int64(
		(*C.int64_t)(unsafe.Pointer(&a[0])),
		(*C.int64_t)(unsafe.Pointer(&b[0])),
		C.size_t(n),
	))
}

// VectorMinInt64 finds minimum using SIMD
func VectorMinInt64(a []int64) int64 {
	if len(a) == 0 {
		return 0
	}
	return int64(C.svdb_vector_min_int64((*C.int64_t)(unsafe.Pointer(&a[0])), C.size_t(len(a))))
}

// VectorMaxInt64 finds maximum using SIMD
func VectorMaxInt64(a []int64) int64 {
	if len(a) == 0 {
		return 0
	}
	return int64(C.svdb_vector_max_int64((*C.int64_t)(unsafe.Pointer(&a[0])), C.size_t(len(a))))
}

// Bitmap operations

// BitmapAND computes a = a & b using SIMD
func BitmapAND(a, b []uint64) {
	n := len(a)
	if n == 0 || len(b) < n {
		return
	}
	C.svdb_bitmap_and(
		(*C.uint64_t)(unsafe.Pointer(&a[0])),
		(*C.uint64_t)(unsafe.Pointer(&b[0])),
		C.size_t(n),
	)
}

// BitmapOR computes a = a | b using SIMD
func BitmapOR(a, b []uint64) {
	n := len(a)
	if n == 0 || len(b) < n {
		return
	}
	C.svdb_bitmap_or(
		(*C.uint64_t)(unsafe.Pointer(&a[0])),
		(*C.uint64_t)(unsafe.Pointer(&b[0])),
		C.size_t(n),
	)
}

// BitmapPopcount counts set bits using SIMD
func BitmapPopcount(a []uint64) int {
	if len(a) == 0 {
		return 0
	}
	return int(C.svdb_bitmap_popcount((*C.uint64_t)(unsafe.Pointer(&a[0])), C.size_t(len(a))))
}

// RoaringBitmap CGO wrapper

// RoaringBitmapCGO is a CGO-accelerated roaring bitmap
type RoaringBitmapCGO struct {
	handle *C.svdb_roaring_bitmap_t
}

// NewRoaringBitmapCGO creates a new roaring bitmap
func NewRoaringBitmapCGO() *RoaringBitmapCGO {
	return &RoaringBitmapCGO{
		handle: C.svdb_roaring_create(),
	}
}

// Close frees the roaring bitmap
func (rb *RoaringBitmapCGO) Close() {
	if rb.handle != nil {
		C.svdb_roaring_free(rb.handle)
		rb.handle = nil
	}
}

// Add adds a value to the bitmap
func (rb *RoaringBitmapCGO) Add(x uint32) {
	if rb.handle != nil {
		C.svdb_roaring_add(rb.handle, C.uint32_t(x))
	}
}

// Remove removes a value from the bitmap
func (rb *RoaringBitmapCGO) Remove(x uint32) {
	if rb.handle != nil {
		C.svdb_roaring_remove(rb.handle, C.uint32_t(x))
	}
}

// Contains checks if a value is in the bitmap
func (rb *RoaringBitmapCGO) Contains(x uint32) bool {
	if rb.handle == nil {
		return false
	}
	return C.svdb_roaring_contains(rb.handle, C.uint32_t(x)) != 0
}

// Cardinality returns the number of set bits
func (rb *RoaringBitmapCGO) Cardinality() int {
	if rb.handle == nil {
		return 0
	}
	return int(C.svdb_roaring_cardinality(rb.handle))
}

// IsEmpty returns true if the bitmap is empty
func (rb *RoaringBitmapCGO) IsEmpty() bool {
	if rb.handle == nil {
		return true
	}
	return C.svdb_roaring_is_empty(rb.handle) != 0
}

// And computes intersection: rb = rb & other
func (rb *RoaringBitmapCGO) And(other *RoaringBitmapCGO) {
	if rb.handle != nil && other != nil && other.handle != nil {
		C.svdb_roaring_and(rb.handle, other.handle)
	}
}

// Or computes union: rb = rb | other
func (rb *RoaringBitmapCGO) Or(other *RoaringBitmapCGO) {
	if rb.handle != nil && other != nil && other.handle != nil {
		C.svdb_roaring_or(rb.handle, other.handle)
	}
}

// ToArray returns all values as a sorted slice
func (rb *RoaringBitmapCGO) ToArray() []uint32 {
	if rb.handle == nil {
		return nil
	}
	
	var count C.size_t
	cArray := C.svdb_roaring_to_array(rb.handle, &count)
	if cArray == nil || count == 0 {
		return nil
	}
	defer C.free(unsafe.Pointer(cArray))
	
	result := make([]uint32, count)
	copy(result, (*[1 << 30]uint32)(unsafe.Pointer(cArray))[:count])
	return result
}

// FromArray creates a bitmap from a sorted slice
func FromArrayCGO(values []uint32) *RoaringBitmapCGO {
	if len(values) == 0 {
		return nil
	}
	rb := &RoaringBitmapCGO{
		handle: C.svdb_roaring_from_array((*C.uint32_t)(unsafe.Pointer(&values[0])), C.size_t(len(values))),
	}
	return rb
}

// Min returns the minimum value
func (rb *RoaringBitmapCGO) Min() uint32 {
	if rb.handle == nil {
		return 0
	}
	return uint32(C.svdb_roaring_min(rb.handle))
}

// Max returns the maximum value
func (rb *RoaringBitmapCGO) Max() uint32 {
	if rb.handle == nil {
		return 0
	}
	return uint32(C.svdb_roaring_max(rb.handle))
}

// Rank returns count of values <= x
func (rb *RoaringBitmapCGO) Rank(x uint32) int {
	if rb.handle == nil {
		return 0
	}
	return int(C.svdb_roaring_rank(rb.handle, C.uint32_t(x)))
}

// Select returns the n-th smallest value (0-indexed)
func (rb *RoaringBitmapCGO) Select(n int) uint32 {
	if rb.handle == nil {
		return 0
	}
	return uint32(C.svdb_roaring_select(rb.handle, C.size_t(n)))
}
