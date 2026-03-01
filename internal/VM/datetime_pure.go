//go:build !SVDB_ENABLE_CGO_VM
// +build !SVDB_ENABLE_CGO_VM

package VM

import (
	"time"
)

// JuliandayFromString parses a date/datetime string into a Julian Day Number
// (pure Go fallback). Returns 0, false on parse failure.
func JuliandayFromString(s string) (float64, bool) {
	layouts := []string{
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
		"2006-01-02",
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, s); err == nil {
			// Julian Day of 2000-01-01 12:00:00 UTC = 2451545.0
			const j2000 = 2451545.0
			const secondsPerDay = 86400.0
			ref, _ := time.Parse("2006-01-02 15:04:05", "2000-01-01 12:00:00")
			jd := j2000 + t.Sub(ref).Seconds()/secondsPerDay
			return jd, true
		}
	}
	return 0, false
}

// UnixepochFromString parses a date/datetime string into a Unix timestamp
// (pure Go fallback). Returns 0, false on parse failure.
func UnixepochFromString(s string) (int64, bool) {
	layouts := []string{
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
		"2006-01-02",
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC().Unix(), true
		}
	}
	return 0, false
}

// JuliandayBatch converts date strings to Julian Day Numbers (pure Go fallback).
func JuliandayBatch(strs []string) ([]float64, []bool) {
	results := make([]float64, len(strs))
	ok := make([]bool, len(strs))
	for i, s := range strs {
		results[i], ok[i] = JuliandayFromString(s)
	}
	return results, ok
}

// UnixepochBatch converts date strings to Unix timestamps (pure Go fallback).
func UnixepochBatch(strs []string) ([]int64, []bool) {
	results := make([]int64, len(strs))
	ok := make([]bool, len(strs))
	for i, s := range strs {
		results[i], ok[i] = UnixepochFromString(s)
	}
	return results, ok
}