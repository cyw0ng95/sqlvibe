//go:build !SVDB_ENABLE_CGO_VM
// +build !SVDB_ENABLE_CGO_VM

package VM

import "bytes"

// Compare compares two byte slices (pure Go fallback)
// Returns: -1 if a < b, 0 if a == b, 1 if a > b
func Compare(a, b []byte) int {
	return bytes.Compare(a, b)
}

// CompareBatch compares multiple pairs of byte slices (pure Go fallback)
func CompareBatch(a, b [][]byte) []int {
	if len(a) != len(b) {
		return nil
	}
	results := make([]int, len(a))
	for i := range a {
		results[i] = bytes.Compare(a[i], b[i])
	}
	return results
}

// EqualBatch checks equality for multiple pairs (pure Go fallback)
func EqualBatch(a, b [][]byte) []bool {
	if len(a) != len(b) {
		return nil
	}
	results := make([]bool, len(a))
	for i := range a {
		results[i] = bytes.Equal(a[i], b[i])
	}
	return results
}

// HasPrefix checks if a starts with prefix (pure Go fallback)
func HasPrefix(a, prefix []byte) bool {
	return bytes.HasPrefix(a, prefix)
}
