package vm

import (
	"github.com/cyw0ng95/sqlvibe/internal/QP"
)

// ColumnSet is a set of column names for O(1) membership tests.
type ColumnSet map[string]struct{}

// NewColumnSet creates a ColumnSet from a slice of column names.
func NewColumnSet(cols []string) ColumnSet {
	cs := make(ColumnSet, len(cols))
	for _, c := range cols {
		cs[c] = struct{}{}
	}
	return cs
}

// Contains reports whether col is in the set.
func (cs ColumnSet) Contains(col string) bool {
	_, ok := cs[col]
	return ok
}

// PruneColumns returns only the columns from available that are referenced by
// stmt (WHERE predicates + SELECT expressions + ORDER BY). An empty result
// means all columns are needed (no pruning possible).
// Column pruning reduces the data transferred from storage to the VM.
func PruneColumns(stmt *QP.SelectStmt, available []string) []string {
	if stmt == nil || len(available) == 0 {
		return nil
	}

	needed := make(ColumnSet, len(available))

	// Columns referenced in SELECT list
	for _, col := range stmt.Columns {
		for _, ref := range CollectColumnRefs(col) {
			needed[ref] = struct{}{}
		}
		// SELECT * — all columns needed
		if cr, ok := col.(*QP.ColumnRef); ok && cr.Name == "*" {
			return nil
		}
	}

	// Columns referenced in WHERE
	for _, ref := range CollectColumnRefs(stmt.Where) {
		needed[ref] = struct{}{}
	}

	// Columns referenced in ORDER BY
	for _, ob := range stmt.OrderBy {
		for _, ref := range CollectColumnRefs(ob.Expr) {
			needed[ref] = struct{}{}
		}
	}

	// Columns referenced in GROUP BY
	for _, g := range stmt.GroupBy {
		for _, ref := range CollectColumnRefs(g) {
			needed[ref] = struct{}{}
		}
	}

	// Columns referenced in HAVING
	for _, ref := range CollectColumnRefs(stmt.Having) {
		needed[ref] = struct{}{}
	}

	if len(needed) == 0 || len(needed) >= len(available) {
		return nil // no pruning benefit
	}

	// Preserve original column order
	pruned := make([]string, 0, len(needed))
	for _, c := range available {
		if needed.Contains(c) {
			pruned = append(pruned, c)
		}
	}
	if len(pruned) >= len(available) {
		return nil
	}
	return pruned
}

// CanPushdownWhere reports whether the WHERE clause can be partially or fully
// evaluated at the storage layer (before VM execution).
func CanPushdownWhere(where QP.Expr) bool {
	if where == nil {
		return false
	}
	pushable, _ := QP.SplitPushdownPredicates(where)
	return len(pushable) > 0
}

// SplitPredicates splits a WHERE clause into predicates that can be evaluated
// at the storage layer (pushable) and those that must be evaluated by the VM
// (remaining). This is a thin wrapper around QP.SplitPushdownPredicates to
// expose the functionality through the vm package.
func SplitPredicates(where QP.Expr) (pushable []QP.Expr, remaining QP.Expr) {
	return QP.SplitPushdownPredicates(where)
}
