// Package compiler provides pure helper functions for SQL compilation.
// These helpers are extracted from the CG package to improve modularity
// and testability without requiring access to the Compiler struct.
package compiler

import (
	"fmt"
	"strings"

	QP "github.com/cyw0ng95/sqlvibe/internal/QP"
)

// HasAggregates reports whether stmt contains any aggregate functions or a GROUP BY clause.
func HasAggregates(stmt *QP.SelectStmt) bool {
	if stmt == nil {
		return false
	}
	for _, col := range stmt.Columns {
		if ExprHasAggregate(col) {
			return true
		}
	}
	return stmt.GroupBy != nil
}

// HasWindowFunctions reports whether stmt contains any window function calls.
func HasWindowFunctions(stmt *QP.SelectStmt) bool {
	if stmt == nil {
		return false
	}
	for _, col := range stmt.Columns {
		if exprHasWindowFunc(col) {
			return true
		}
	}
	return false
}

// ShouldUseColumnar returns true when the query would benefit from columnar execution.
func ShouldUseColumnar(stmt *QP.SelectStmt) bool {
	if stmt == nil {
		return false
	}
	if stmt.From != nil && stmt.From.Join != nil {
		return false
	}
	if HasAggregates(stmt) {
		return true
	}
	return stmt.Where == nil
}

// GetSelectColumnNames returns the output column names for a SELECT statement.
// For aliased expressions the alias is used; for column references the column name;
// otherwise an empty string is returned.
func GetSelectColumnNames(stmt *QP.SelectStmt) []string {
	if stmt == nil {
		return nil
	}
	names := make([]string, len(stmt.Columns))
	for i, col := range stmt.Columns {
		names[i] = ExprOutputName(col, i)
	}
	return names
}

// ExprOutputName returns the display name for a column expression.
// idx is the 0-based column position used as fallback.
func ExprOutputName(expr QP.Expr, idx int) string {
	if expr == nil {
		return ""
	}
	switch e := expr.(type) {
	case *QP.AliasExpr:
		return e.Alias
	case *QP.ColumnRef:
		return e.Name
	case *QP.FuncCall:
		return strings.ToUpper(e.Name)
	default:
		if idx >= 0 {
			return fmt.Sprintf("col_%d", idx)
		}
	}
	return ""
}

// IsStarSelect reports whether stmt selects all columns (SELECT *).
func IsStarSelect(stmt *QP.SelectStmt) bool {
	if stmt == nil || len(stmt.Columns) != 1 {
		return false
	}
	col, ok := stmt.Columns[0].(*QP.ColumnRef)
	return ok && col.Name == "*"
}

// HasJoin reports whether the FROM clause contains any JOIN.
func HasJoin(stmt *QP.SelectStmt) bool {
	return stmt != nil && stmt.From != nil && stmt.From.Join != nil
}

// exprHasWindowFunc recursively checks whether expr contains a window function.
func exprHasWindowFunc(expr QP.Expr) bool {
	if expr == nil {
		return false
	}
	switch e := expr.(type) {
	case *QP.WindowFuncExpr:
		return e != nil
	case *QP.BinaryExpr:
		return exprHasWindowFunc(e.Left) || exprHasWindowFunc(e.Right)
	case *QP.UnaryExpr:
		return exprHasWindowFunc(e.Expr)
	case *QP.AliasExpr:
		return exprHasWindowFunc(e.Expr)
	case *QP.CastExpr:
		return exprHasWindowFunc(e.Expr)
	}
	return false
}
