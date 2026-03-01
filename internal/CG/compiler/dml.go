package compiler

import (
	QP "github.com/cyw0ng95/sqlvibe/internal/QP"
)

// InsertColumnNames returns the explicit column list from an INSERT statement.
// Returns nil when no column list was provided (INSERT INTO t VALUES ...).
func InsertColumnNames(stmt *QP.InsertStmt) []string {
	if stmt == nil || len(stmt.Columns) == 0 {
		return nil
	}
	names := make([]string, len(stmt.Columns))
	copy(names, stmt.Columns)
	return names
}

// InsertValueRowCount returns the number of value rows in an INSERT statement.
func InsertValueRowCount(stmt *QP.InsertStmt) int {
	if stmt == nil {
		return 0
	}
	return len(stmt.Values)
}

// UpdateColumnList returns the list of column names being updated.
func UpdateColumnList(stmt *QP.UpdateStmt) []string {
	if stmt == nil {
		return nil
	}
	names := make([]string, 0, len(stmt.Set))
	for _, s := range stmt.Set {
		if col, ok := s.Column.(*QP.ColumnRef); ok {
			names = append(names, col.Name)
		}
	}
	return names
}

// HasReturning reports whether the INSERT statement has a RETURNING clause.
func HasReturning(stmt *QP.InsertStmt) bool {
	return stmt != nil && len(stmt.Returning) > 0
}

// IsInsertWithSelect reports whether the INSERT uses a SELECT as its source.
func IsInsertWithSelect(stmt *QP.InsertStmt) bool {
	return stmt != nil && stmt.SelectQuery != nil
}

// DeleteHasWhere reports whether the DELETE statement has a WHERE clause.
func DeleteHasWhere(stmt *QP.DeleteStmt) bool {
	return stmt != nil && stmt.Where != nil
}

// UpdateHasWhere reports whether the UPDATE statement has a WHERE clause.
func UpdateHasWhere(stmt *QP.UpdateStmt) bool {
	return stmt != nil && stmt.Where != nil
}
