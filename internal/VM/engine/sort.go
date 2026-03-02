package engine

import "sort"

/*
#cgo LDFLAGS: -L${SRCDIR}/../../../.build/cmake/lib -lsvdb -lstdc++
#cgo CFLAGS: -I${SRCDIR}/../../../src/core/VM/engine
#include "engine_api.h"
*/
import "C"

// SortOrder controls whether rows are sorted in ascending or descending order.
type SortOrder int

const (
	Ascending  SortOrder = 0
	Descending SortOrder = 1
)

// NullOrder controls where NULL values appear in sorted output.
type NullOrder int

const (
	NullsFirst NullOrder = 0
	NullsLast  NullOrder = 1
)

// SortKey defines a single sort criterion: a column name, direction, and null
// positioning.
type SortKey struct {
	ColName   string
	Order     SortOrder
	NullOrder NullOrder
}

// SortRowsByKeys sorts rows stably by the ordered list of sort keys.
// The cmp function compares two non-nil column values and must return a
// negative number if a < b, zero if a == b, or a positive number if a > b.
// Uses C++ implementation by default for better performance.
func SortRowsByKeys(rows []Row, keys []SortKey, cmp func(a, b interface{}) int) []Row {
	if len(keys) == 0 {
		return rows
	}
	// Convert Go SortKey to CSortKey
	cKeys := make([]CSortKey, len(keys))
	for i, k := range keys {
		cKeys[i] = CSortKey{
			ColName:   k.ColName,
			Order:     int32(k.Order),
			NullOrder: int32(k.NullOrder),
		}
	}
	return CSortRows(rows, cKeys)
}

// goSortRowsByKeys is the pure Go implementation of SortRowsByKeys (fallback).
func goSortRowsByKeys(rows []Row, keys []SortKey, cmp func(a, b interface{}) int) []Row {
	if len(keys) == 0 {
		return rows
	}
	out := make([]Row, len(rows))
	copy(out, rows)
	sort.SliceStable(out, func(i, j int) bool {
		ri, rj := out[i], out[j]
		for _, k := range keys {
			ai := ri[k.ColName]
			bi := rj[k.ColName]
			// Handle NULLs
			if ai == nil && bi == nil {
				continue
			}
			if ai == nil {
				return k.NullOrder == NullsFirst
			}
			if bi == nil {
				return k.NullOrder != NullsFirst
			}
			c := cmp(ai, bi)
			if c == 0 {
				continue
			}
			if k.Order == Descending {
				return c > 0
			}
			return c < 0
		}
		return false
	})
	return out
}

// TopKRows sorts rows and returns only the first k.  If k <= 0, all rows are
// returned.  This is equivalent to SortRowsByKeys followed by ApplyLimitOffset
// with limit=k, but avoids sorting more than needed for large k.
func TopKRows(rows []Row, k int, keys []SortKey, cmp func(a, b interface{}) int) []Row {
	sorted := SortRowsByKeys(rows, keys, cmp)
	if k > 0 && k < len(sorted) {
		return sorted[:k]
	}
	return sorted
}

// ReverseRows reverses a slice of rows.
// Uses C++ implementation by default for better performance.
func ReverseRows(rows []Row) []Row {
	return CReverseRows(rows)
}

// goReverseRows is the pure Go implementation of ReverseRows (fallback).
func goReverseRows(rows []Row) []Row {
	for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
		rows[i], rows[j] = rows[j], rows[i]
	}
	return rows
}
