//go:build !SVDB_ENABLE_CGO_DS
// +build !SVDB_ENABLE_CGO_DS

package DS

import "unsafe"

// Pure Go fallback for SIMD operations

// VectorSumDouble computes sum of float64 slice (pure Go)
func VectorSumDouble(a []float64) float64 {
	var sum float64
	for _, v := range a {
		sum += v
	}
	return sum
}

// VectorSumInt64 computes sum of int64 slice (pure Go)
func VectorSumInt64(a []int64) int64 {
	var sum int64
	for _, v := range a {
		sum += v
	}
	return sum
}

// VectorAddInt64 computes element-wise addition (pure Go)
func VectorAddInt64(a, b, out []int64) {
	for i := range a {
		out[i] = a[i] + b[i]
	}
}

// VectorEqInt64 counts equal elements (pure Go)
func VectorEqInt64(a, b []int64) int {
	count := 0
	for i := range a {
		if a[i] == b[i] {
			count++
		}
	}
	return count
}

// VectorGTInt64 counts elements where a[i] > b[i] (pure Go)
func VectorGTInt64(a, b []int64) int {
	count := 0
	for i := range a {
		if a[i] > b[i] {
			count++
		}
	}
	return count
}

// VectorMinInt64 finds minimum (pure Go)
func VectorMinInt64(a []int64) int64 {
	if len(a) == 0 {
		return 0
	}
	min := a[0]
	for _, v := range a[1:] {
		if v < min {
			min = v
		}
	}
	return min
}

// VectorMaxInt64 finds maximum (pure Go)
func VectorMaxInt64(a []int64) int64 {
	if len(a) == 0 {
		return 0
	}
	max := a[0]
	for _, v := range a[1:] {
		if v > max {
			max = v
		}
	}
	return max
}

// BitmapAND computes a = a & b (pure Go)
func BitmapAND(a, b []uint64) {
	for i := range a {
		a[i] &= b[i]
	}
}

// BitmapOR computes a = a | b (pure Go)
func BitmapOR(a, b []uint64) {
	for i := range a {
		a[i] |= b[i]
	}
}

// BitmapPopcount counts set bits (pure Go)
func BitmapPopcount(a []uint64) int {
	count := 0
	for _, v := range a {
		count += popcount64(v)
	}
	return count
}

func popcount64(x uint64) int {
	count := 0
	for x != 0 {
		x &= x - 1
		count++
	}
	return count
}

// RoaringBitmapCGO is a stub type for pure Go builds
type RoaringBitmapCGO struct {
	handle unsafe.Pointer
}

// NewRoaringBitmapCGO returns nil in pure Go builds
func NewRoaringBitmapCGO() *RoaringBitmapCGO {
	return nil
}

// Close is a no-op in pure Go builds
func (rb *RoaringBitmapCGO) Close() {}

// Add is a no-op in pure Go builds
func (rb *RoaringBitmapCGO) Add(x uint32) {}

// Remove is a no-op in pure Go builds
func (rb *RoaringBitmapCGO) Remove(x uint32) {}

// Contains returns false in pure Go builds
func (rb *RoaringBitmapCGO) Contains(x uint32) bool {
	return false
}

// Cardinality returns 0 in pure Go builds
func (rb *RoaringBitmapCGO) Cardinality() int {
	return 0
}

// IsEmpty returns true in pure Go builds
func (rb *RoaringBitmapCGO) IsEmpty() bool {
	return true
}

// And is a no-op in pure Go builds
func (rb *RoaringBitmapCGO) And(other *RoaringBitmapCGO) {}

// Or is a no-op in pure Go builds
func (rb *RoaringBitmapCGO) Or(other *RoaringBitmapCGO) {}

// ToArray returns nil in pure Go builds
func (rb *RoaringBitmapCGO) ToArray() []uint32 {
	return nil
}

// FromArray returns nil in pure Go builds
func FromArrayCGO(values []uint32) *RoaringBitmapCGO {
	return nil
}

// Min returns 0 in pure Go builds
func (rb *RoaringBitmapCGO) Min() uint32 {
	return 0
}

// Max returns 0 in pure Go builds
func (rb *RoaringBitmapCGO) Max() uint32 {
	return 0
}

// Rank returns 0 in pure Go builds
func (rb *RoaringBitmapCGO) Rank(x uint32) int {
	return 0
}

// Select returns 0 in pure Go builds
func (rb *RoaringBitmapCGO) Select(n int) uint32 {
	return 0
}
