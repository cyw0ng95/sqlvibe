//go:build !SVDB_ENABLE_CGO_VM
// +build !SVDB_ENABLE_CGO_VM

package VM

// CompareInt64Batch compares pairs of int64 values (pure Go fallback).
// results[i] = -1/0/1.
func CompareInt64Batch(a, b []int64) []int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	results := make([]int, n)
	for i := 0; i < n; i++ {
		if a[i] < b[i] {
			results[i] = -1
		} else if a[i] > b[i] {
			results[i] = 1
		} else {
			results[i] = 0
		}
	}
	return results
}

// CompareFloat64Batch compares pairs of float64 values (pure Go fallback).
func CompareFloat64Batch(a, b []float64) []int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	results := make([]int, n)
	for i := 0; i < n; i++ {
		if a[i] < b[i] {
			results[i] = -1
		} else if a[i] > b[i] {
			results[i] = 1
		} else {
			results[i] = 0
		}
	}
	return results
}

// AddInt64Batch adds pairs of int64 values (pure Go fallback).
func AddInt64Batch(a, b []int64) []int64 {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	results := make([]int64, n)
	for i := 0; i < n; i++ {
		results[i] = a[i] + b[i]
	}
	return results
}

// AddFloat64Batch adds pairs of float64 values (pure Go fallback).
func AddFloat64Batch(a, b []float64) []float64 {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	results := make([]float64, n)
	for i := 0; i < n; i++ {
		results[i] = a[i] + b[i]
	}
	return results
}

// SubInt64Batch subtracts pairs of int64 values (pure Go fallback).
func SubInt64Batch(a, b []int64) []int64 {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	results := make([]int64, n)
	for i := 0; i < n; i++ {
		results[i] = a[i] - b[i]
	}
	return results
}

// SubFloat64Batch subtracts pairs of float64 values (pure Go fallback).
func SubFloat64Batch(a, b []float64) []float64 {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	results := make([]float64, n)
	for i := 0; i < n; i++ {
		results[i] = a[i] - b[i]
	}
	return results
}

// MulInt64Batch multiplies pairs of int64 values (pure Go fallback).
func MulInt64Batch(a, b []int64) []int64 {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	results := make([]int64, n)
	for i := 0; i < n; i++ {
		results[i] = a[i] * b[i]
	}
	return results
}

// MulFloat64Batch multiplies pairs of float64 values (pure Go fallback).
func MulFloat64Batch(a, b []float64) []float64 {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	results := make([]float64, n)
	for i := 0; i < n; i++ {
		results[i] = a[i] * b[i]
	}
	return results
}

// FilterMask returns indices where mask[i] is true (pure Go fallback).
func FilterMask(mask []bool, indices []int64) []int64 {
	out := make([]int64, 0, len(indices))
	n := len(mask)
	if len(indices) < n {
		n = len(indices)
	}
	for i := 0; i < n; i++ {
		if mask[i] {
			out = append(out, indices[i])
		}
	}
	return out
}
