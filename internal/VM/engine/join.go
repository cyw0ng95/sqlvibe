package engine

/*
#cgo LDFLAGS: -L${SRCDIR}/../../../.build/cmake/lib -lsvdb -lstdc++
#cgo CFLAGS: -I${SRCDIR}/../../../src/core/VM/engine
#include "engine_api.h"
*/
import "C"

// MergeRows merges two Row maps into a new Row.  Values from b overwrite
// values from a when both share the same key.
// Uses C++ implementation by default for better performance.
func MergeRows(a, b Row) Row {
	return CMergeRows(a, b)
}

// goMergeRows is the pure Go implementation of MergeRows (fallback).
func goMergeRows(a, b Row) Row {
	out := make(Row, len(a)+len(b))
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		out[k] = v
	}
	return out
}

// MergeRowsWithAlias merges two rows, adding alias-qualified keys for each
// source.  For example, with aliasA="e" and aliasB="d", column "id" from a
// is stored as both "id" and "e.id" in the result.  Values from b take
// precedence over a for unqualified keys.
// Uses C++ implementation by default for better performance.
func MergeRowsWithAlias(a Row, aliasA string, b Row, aliasB string) Row {
	return CMergeRowsWithAlias(a, aliasA, b, aliasB)
}

// goMergeRowsWithAlias is the pure Go implementation of MergeRowsWithAlias (fallback).
func goMergeRowsWithAlias(a Row, aliasA string, b Row, aliasB string) Row {
	out := make(Row, len(a)+len(b)+4)
	for k, v := range a {
		out[k] = v
		if aliasA != "" {
			out[aliasA+"."+k] = v
		}
	}
	for k, v := range b {
		out[k] = v
		if aliasB != "" {
			out[aliasB+"."+k] = v
		}
	}
	return out
}

// CrossJoin returns the Cartesian product of left and right.
// Uses C++ implementation by default for better performance.
func CrossJoin(left, right []Row) []Row {
	return CCrossJoin(left, right)
}

// goCrossJoin is the pure Go implementation of CrossJoin (fallback).
func goCrossJoin(left, right []Row) []Row {
	if len(left) == 0 || len(right) == 0 {
		return nil
	}
	out := make([]Row, 0, len(left)*len(right))
	for _, l := range left {
		for _, r := range right {
			out = append(out, goMergeRows(l, r))
		}
	}
	return out
}

// InnerJoin returns merged rows from left × right where pred(merged) is true.
// Uses C++ implementation by default for better performance.
func InnerJoin(left, right []Row, pred func(Row) bool) []Row {
	return CInnerJoin(left, right, pred)
}

// goInnerJoin is the pure Go implementation of InnerJoin (fallback).
func goInnerJoin(left, right []Row, pred func(Row) bool) []Row {
	out := make([]Row, 0, len(left))
	for _, l := range left {
		for _, r := range right {
			merged := goMergeRows(l, r)
			if pred == nil || pred(merged) {
				out = append(out, merged)
			}
		}
	}
	return out
}

// LeftOuterJoin returns all rows from left joined with matching rows from
// right.  When no right row matches, right columns are set to nil.
// rightCols lists the column names present in right rows.
// Uses C++ implementation by default for better performance.
func LeftOuterJoin(left, right []Row, pred func(Row) bool, rightCols []string) []Row {
	return CLeftOuterJoin(left, right, pred, rightCols)
}

// goLeftOuterJoin is the pure Go implementation of LeftOuterJoin (fallback).
func goLeftOuterJoin(left, right []Row, pred func(Row) bool, rightCols []string) []Row {
	out := make([]Row, 0, len(left))
	for _, l := range left {
		matched := false
		for _, r := range right {
			merged := goMergeRows(l, r)
			if pred == nil || pred(merged) {
				out = append(out, merged)
				matched = true
			}
		}
		if !matched {
			// Emit the left row with right columns set to nil.
			merged := make(Row, len(l)+len(rightCols))
			for k, v := range l {
				merged[k] = v
			}
			for _, col := range rightCols {
				merged[col] = nil
			}
			out = append(out, merged)
		}
	}
	return out
}
