package engine

/*
#cgo LDFLAGS: -L${SRCDIR}/../../../.build/cmake/lib -lsvdb -lstdc++
#cgo CFLAGS: -I${SRCDIR}/../../../src/core/VM/engine
#include "engine_api.h"
*/
import "C"

// MergeRows merges two Row maps into a new Row.  Values from b overwrite
// values from a when both share the same key.
func MergeRows(a, b Row) Row {
	return CMergeRows(a, b)
}

// MergeRowsWithAlias merges two rows, adding alias-qualified keys for each
// source.  For example, with aliasA="e" and aliasB="d", column "id" from a
// is stored as both "id" and "e.id" in the result.  Values from b take
// precedence over a for unqualified keys.
func MergeRowsWithAlias(a Row, aliasA string, b Row, aliasB string) Row {
	return CMergeRowsWithAlias(a, aliasA, b, aliasB)
}

// CrossJoin returns the Cartesian product of left and right.
func CrossJoin(left, right []Row) []Row {
	return CCrossJoin(left, right)
}

// InnerJoin returns merged rows from left × right where pred(merged) is true.
func InnerJoin(left, right []Row, pred func(Row) bool) []Row {
	return CInnerJoin(left, right, pred)
}

// LeftOuterJoin returns all rows from left joined with matching rows from
// right.  When no right row matches, right columns are set to nil.
// rightCols lists the column names present in right rows.
func LeftOuterJoin(left, right []Row, pred func(Row) bool, rightCols []string) []Row {
	return CLeftOuterJoin(left, right, pred, rightCols)
}
