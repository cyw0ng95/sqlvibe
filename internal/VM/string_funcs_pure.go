//go:build !SVDB_ENABLE_CGO_VM
// +build !SVDB_ENABLE_CGO_VM

package VM

import "strings"

// StrUpperBatch converts strings to uppercase (pure Go fallback).
func StrUpperBatch(strs []string) []string {
	results := make([]string, len(strs))
	for i, s := range strs {
		results[i] = strings.ToUpper(s)
	}
	return results
}

// StrLowerBatch converts strings to lowercase (pure Go fallback).
func StrLowerBatch(strs []string) []string {
	results := make([]string, len(strs))
	for i, s := range strs {
		results[i] = strings.ToLower(s)
	}
	return results
}

// StrTrimBatch trims whitespace from both ends (pure Go fallback).
func StrTrimBatch(strs []string) []string {
	results := make([]string, len(strs))
	for i, s := range strs {
		results[i] = strings.TrimSpace(s)
	}
	return results
}

// StrSubstrBatch extracts substrings (pure Go fallback).
// start is 1-based; length=-1 means to end.
func StrSubstrBatch(strs []string, starts, lengths []int64) []string {
	n := len(strs)
	results := make([]string, n)
	for i := 0; i < n; i++ {
		s := strs[i]
		start := int64(1)
		length := int64(-1)
		if i < len(starts) {
			start = starts[i]
		}
		if i < len(lengths) {
			length = lengths[i]
		}
		idx := start - 1
		if idx < 0 {
			idx = int64(len(s)) + idx
			if idx < 0 {
				idx = 0
			}
		}
		if idx >= int64(len(s)) {
			results[i] = ""
			continue
		}
		if length < 0 {
			results[i] = s[idx:]
		} else {
			end := idx + length
			if end > int64(len(s)) {
				end = int64(len(s))
			}
			results[i] = s[idx:end]
		}
	}
	return results
}
