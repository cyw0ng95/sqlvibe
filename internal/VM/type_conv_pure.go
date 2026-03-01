//go:build !SVDB_ENABLE_CGO_VM
// +build !SVDB_ENABLE_CGO_VM

package VM

import (
	"fmt"
	"strconv"
)

// ParseInt64Batch parses int64 values from strings (pure Go fallback).
// ok[i] is true when parsing succeeded.
func ParseInt64Batch(strs []string) ([]int64, []bool) {
	results := make([]int64, len(strs))
	ok := make([]bool, len(strs))
	for i, s := range strs {
		if v, err := strconv.ParseInt(s, 10, 64); err == nil {
			results[i] = v
			ok[i] = true
		}
	}
	return results, ok
}

// ParseFloat64Batch parses float64 values from strings (pure Go fallback).
func ParseFloat64Batch(strs []string) ([]float64, []bool) {
	results := make([]float64, len(strs))
	ok := make([]bool, len(strs))
	for i, s := range strs {
		if v, err := strconv.ParseFloat(s, 64); err == nil {
			results[i] = v
			ok[i] = true
		}
	}
	return results, ok
}

// FormatInt64Batch formats int64 values to strings (pure Go fallback).
func FormatInt64Batch(values []int64) []string {
	results := make([]string, len(values))
	for i, v := range values {
		results[i] = strconv.FormatInt(v, 10)
	}
	return results
}

// FormatFloat64Batch formats float64 values to strings (pure Go fallback).
func FormatFloat64Batch(values []float64) []string {
	results := make([]string, len(values))
	for i, v := range values {
		results[i] = fmt.Sprintf("%g", v)
	}
	return results
}
