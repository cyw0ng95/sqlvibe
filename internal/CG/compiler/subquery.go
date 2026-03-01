package compiler

import (
	QP "github.com/cyw0ng95/sqlvibe/internal/QP"
)

// HasSubquery reports whether expr contains any subquery expression.
func HasSubquery(expr QP.Expr) bool {
	if expr == nil {
		return false
	}
	switch e := expr.(type) {
	case *QP.SubqueryExpr:
		return e != nil
	case *QP.BinaryExpr:
		return HasSubquery(e.Left) || HasSubquery(e.Right)
	case *QP.UnaryExpr:
		return HasSubquery(e.Expr)
	case *QP.AliasExpr:
		return HasSubquery(e.Expr)
	case *QP.CastExpr:
		return HasSubquery(e.Expr)
	case *QP.FuncCall:
		for _, arg := range e.Args {
			if HasSubquery(arg) {
				return true
			}
		}
	case *QP.AnyAllExpr:
		return true // AnyAllExpr wraps a subquery
	case *QP.CaseExpr:
		if HasSubquery(e.Operand) {
			return true
		}
		for _, when := range e.Whens {
			if HasSubquery(when.Condition) || HasSubquery(when.Result) {
				return true
			}
		}
		return HasSubquery(e.Else)
	}
	return false
}

// HasWhereSubquery reports whether the WHERE clause of stmt contains a subquery.
func HasWhereSubquery(stmt *QP.SelectStmt) bool {
	return stmt != nil && HasSubquery(stmt.Where)
}

// HasColumnSubquery reports whether any SELECT column of stmt contains a subquery.
func HasColumnSubquery(stmt *QP.SelectStmt) bool {
	if stmt == nil {
		return false
	}
	for _, col := range stmt.Columns {
		if HasSubquery(col) {
			return true
		}
	}
	return false
}

// ExtractSubqueries collects all SubqueryExpr nodes from an expression tree.
func ExtractSubqueries(expr QP.Expr) []*QP.SubqueryExpr {
	var out []*QP.SubqueryExpr
	collectSubqueries(expr, &out)
	return out
}

func collectSubqueries(expr QP.Expr, out *[]*QP.SubqueryExpr) {
	if expr == nil {
		return
	}
	switch e := expr.(type) {
	case *QP.SubqueryExpr:
		*out = append(*out, e)
	case *QP.BinaryExpr:
		collectSubqueries(e.Left, out)
		collectSubqueries(e.Right, out)
	case *QP.UnaryExpr:
		collectSubqueries(e.Expr, out)
	case *QP.AliasExpr:
		collectSubqueries(e.Expr, out)
	case *QP.CastExpr:
		collectSubqueries(e.Expr, out)
	case *QP.FuncCall:
		for _, arg := range e.Args {
			collectSubqueries(arg, out)
		}
	case *QP.CaseExpr:
		collectSubqueries(e.Operand, out)
		for _, when := range e.Whens {
			collectSubqueries(when.Condition, out)
			collectSubqueries(when.Result, out)
		}
		collectSubqueries(e.Else, out)
	}
}
