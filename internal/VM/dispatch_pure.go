//go:build !SVDB_ENABLE_CGO_VM
// +build !SVDB_ENABLE_CGO_VM

package VM

// DispatchSIMDLevel returns 0 (pure Go has no SIMD dispatch).
func DispatchSIMDLevel() int { return 0 }

// DispatchIsDirectThreaded returns false for pure Go dispatch.
func DispatchIsDirectThreaded() bool { return false }

// ArithInt64Batch applies an arithmetic operation to slices of int64 values.
// op: 0=add, 1=sub, 2=mul, 3=div, 4=rem.
// Returns error string on divide-by-zero, "" otherwise.
func ArithInt64Batch(op int, a, b []int64) ([]int64, string) {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	results := make([]int64, n)
	for i := 0; i < n; i++ {
		switch op {
		case 0:
			results[i] = a[i] + b[i]
		case 1:
			results[i] = a[i] - b[i]
		case 2:
			results[i] = a[i] * b[i]
		case 3:
			if b[i] == 0 {
				return nil, "division by zero"
			}
			results[i] = a[i] / b[i]
		case 4:
			if b[i] == 0 {
				return nil, "division by zero"
			}
			results[i] = a[i] % b[i]
		}
	}
	return results, ""
}

// ArithFloat64Batch applies an arithmetic operation to slices of float64 values.
func ArithFloat64Batch(op int, a, b []float64) []float64 {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	results := make([]float64, n)
	for i := 0; i < n; i++ {
		switch op {
		case 0:
			results[i] = a[i] + b[i]
		case 1:
			results[i] = a[i] - b[i]
		case 2:
			results[i] = a[i] * b[i]
		case 3:
			if b[i] != 0 {
				results[i] = a[i] / b[i]
			}
		}
	}
	return results
}
