package engine

/*
#cgo LDFLAGS: -L${SRCDIR}/../../../.build/cmake/lib -lsvdb -lstdc++
#cgo CFLAGS: -I${SRCDIR}/../../../src/core/VM/engine
#include "engine_api.h"
*/
import "C"

// GroupRows partitions rows into groups using keyFn.  All rows that produce the
// same key are placed in the same group.  Order within each group is preserved.
func GroupRows(rows []Row, keyFn func(Row) string) map[string][]Row {
	groups := make(map[string][]Row)
	for _, r := range rows {
		k := keyFn(r)
		groups[k] = append(groups[k], r)
	}
	return groups
}

// CountRows counts the number of non-nil values in col across all rows.
// If col is empty, every row is counted regardless of content.
func CountRows(rows []Row, col string) int64 {
	return CCountRows(rows, col)
}

// SumRows returns the sum of numeric values in col.  Non-numeric and nil
// values are ignored.  Returns nil if there are no non-nil values.
func SumRows(rows []Row, col string) interface{} {
	return CSumRows(rows, col)
}

// AvgRows returns the arithmetic mean of numeric values in col.
// Returns nil if there are no non-nil values.
func AvgRows(rows []Row, col string) interface{} {
	return CAvgRows(rows, col)
}

// MinRows returns the minimum value in col.
// Returns nil if there are no non-nil values.
func MinRows(rows []Row, col string, cmp func(a, b interface{}) int) interface{} {
	return CMinRows(rows, col)
}

// MaxRows returns the maximum value in col.
// Returns nil if there are no non-nil values.
func MaxRows(rows []Row, col string, cmp func(a, b interface{}) int) interface{} {
	return CMaxRows(rows, col)
}

// GroupByAndAggregate groups rows by keyFn, then applies aggFn to each group,
// returning one result row per group.  aggFn is called with the group key and
// all rows in that group; it returns the output row for the group.
func GroupByAndAggregate(rows []Row, keyFn func(Row) string, aggFn func(key string, group []Row) Row) []Row {
	// Preserve insertion order for deterministic output.
	type entry struct {
		key  string
		rows []Row
	}
	var order []string
	groups := make(map[string]*entry)
	for _, r := range rows {
		k := keyFn(r)
		if e, ok := groups[k]; ok {
			e.rows = append(e.rows, r)
		} else {
			order = append(order, k)
			groups[k] = &entry{key: k, rows: []Row{r}}
		}
	}
	out := make([]Row, 0, len(order))
	for _, k := range order {
		out = append(out, aggFn(k, groups[k].rows))
	}
	return out
}
