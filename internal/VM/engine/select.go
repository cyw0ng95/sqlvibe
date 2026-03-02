// Package engine provides standalone SQL query engine utilities.
// Functions in this package operate on Row data without depending on the full
// QueryEngine struct, making them easy to compose and test independently.
package engine

/*
#cgo LDFLAGS: -L${SRCDIR}/../../../.build/cmake/lib -lsvdb -lstdc++
#cgo CFLAGS: -I${SRCDIR}/../../../src/core/VM/engine
#include "engine_api.h"
*/
import "C"

// Row is a single database row represented as a column-name → value map.
type Row = map[string]interface{}

// FilterRows returns only those rows for which pred returns true.
// A nil predicate returns all rows unchanged.
func FilterRows(rows []Row, pred func(Row) bool) []Row {
	if pred == nil {
		return rows
	}
	return CFilterRows(rows, pred)
}

// ProjectRow applies a set of projection functions to a single row and returns
// the resulting row. projections maps output column name → function that
// computes the value from the input row.
func ProjectRow(row Row, projections map[string]func(Row) interface{}) Row {
	out := make(Row, len(projections))
	for col, fn := range projections {
		out[col] = fn(row)
	}
	return out
}

// ProjectRows applies ProjectRow to every row in rows.
func ProjectRows(rows []Row, projections map[string]func(Row) interface{}) []Row {
	out := make([]Row, len(rows))
	for i, r := range rows {
		out[i] = ProjectRow(r, projections)
	}
	return out
}

// ApplyDistinct removes duplicate rows using keyFn to compute a deduplication
// key for each row. The first occurrence of each key is retained.
func ApplyDistinct(rows []Row, keyFn func(Row) string) []Row {
	return CApplyDistinct(rows, keyFn)
}

// ApplyLimitOffset returns a sub-slice of rows after skipping offset rows and
// returning at most limit rows.  A limit of ≤ 0 means no upper bound.
func ApplyLimitOffset(rows []Row, limit, offset int) []Row {
	return CApplyLimitOffset(rows, limit, offset)
}

// ColNames returns a deduplicated, ordered list of all column names present in
// any row of the result set.
func ColNames(rows []Row) []string {
	return CColNames(rows)
}
