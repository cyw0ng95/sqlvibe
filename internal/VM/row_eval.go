package VM

import (
	"github.com/cyw0ng95/sqlvibe/internal/QP"
)

// CompareVals is a package-level wrapper around the compareVals function.
func CompareVals(a, b interface{}) int {
	return compareVals(a, b)
}

// EvalExprRow evaluates an AST expression against a row map without cursor state.
func EvalExprRow(row map[string]interface{}, expr QP.Expr) interface{} {
	return EvalExprRowWithData(row, expr, nil)
}

// EvalExprRowWithData evaluates an AST expression against a row map,
// with an optional table data map for subquery support.
func EvalExprRowWithData(row map[string]interface{}, expr QP.Expr, data map[string][]map[string]interface{}) interface{} {
	if expr == nil {
		return nil
	}
	if data == nil {
		data = make(map[string][]map[string]interface{})
	}
	qe := &QueryEngine{data: data}
	return qe.evalValue(row, expr)
}

// EvalExprRowErr evaluates an AST expression against a row map and also returns
// any evaluation error (e.g. "no such function").
func EvalExprRowErr(row map[string]interface{}, expr QP.Expr) (interface{}, error) {
	return EvalExprRowErrWithData(row, expr, nil)
}

// EvalExprRowErrWithData evaluates an AST expression with data map and returns any error.
func EvalExprRowErrWithData(row map[string]interface{}, expr QP.Expr, data map[string][]map[string]interface{}) (interface{}, error) {
	if expr == nil {
		return nil, nil
	}
	if data == nil {
		data = make(map[string][]map[string]interface{})
	}
	qe := &QueryEngine{data: data}
	val := qe.evalValue(row, expr)
	return val, qe.LastError()
}

// EvalBoolRow evaluates an AST boolean expression against a row map without cursor state.
func EvalBoolRow(row map[string]interface{}, expr QP.Expr) bool {
	return EvalBoolRowWithData(row, expr, nil)
}

// EvalBoolRowWithData evaluates a boolean expression with an optional data map for subqueries.
func EvalBoolRowWithData(row map[string]interface{}, expr QP.Expr, data map[string][]map[string]interface{}) bool {
	if expr == nil {
		return true
	}
	if data == nil {
		data = make(map[string][]map[string]interface{})
	}
	qe := &QueryEngine{data: data}
	return qe.evalExpr(row, expr)
}

// SortRows sorts result data based on ORDER BY expressions (standalone, no QueryEngine needed).
func SortRows(data [][]interface{}, orderBy []QP.OrderBy, cols []string) [][]interface{} {
	qe := &QueryEngine{data: make(map[string][]map[string]interface{})}
	return qe.SortRows(data, orderBy, cols)
}

// SortRowsTopK sorts data and returns the first topK rows (standalone, no QueryEngine needed).
func SortRowsTopK(data [][]interface{}, orderBy []QP.OrderBy, cols []string, topK int) [][]interface{} {
	qe := &QueryEngine{data: make(map[string][]map[string]interface{})}
	return qe.SortRowsTopK(data, orderBy, cols, topK)
}

// ApplyLimit applies LIMIT and OFFSET to result data (standalone, no QueryEngine needed).
func ApplyLimit(data [][]interface{}, limit, offset int) [][]interface{} {
	qe := &QueryEngine{data: make(map[string][]map[string]interface{})}
	return qe.ApplyLimit(data, limit, offset)
}
