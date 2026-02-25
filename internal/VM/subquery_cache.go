package VM

import (
	"fmt"
	"strings"

	QP "github.com/cyw0ng95/sqlvibe/internal/QP"
)

// subqueryResultCache caches non-correlated subquery results for a single VM execution.
// This prevents re-executing the same subquery for every outer row when the subquery
// result is independent of the outer row (i.e., the subquery is non-correlated).
type subqueryResultCache struct {
	hashSets map[*QP.SelectStmt]map[string]bool // for IN / NOT IN subqueries
	scalars  map[*QP.SelectStmt]interface{}     // for scalar subqueries
	exists   map[*QP.SelectStmt]bool            // for EXISTS / NOT EXISTS subqueries
}

func newSubqueryResultCache() *subqueryResultCache {
	return &subqueryResultCache{
		hashSets: make(map[*QP.SelectStmt]map[string]bool),
		scalars:  make(map[*QP.SelectStmt]interface{}),
		exists:   make(map[*QP.SelectStmt]bool),
	}
}

// subqueryHashKey converts a value to a string suitable as a hash-set key.
func subqueryHashKey(v interface{}) string {
	if v == nil {
		return "\x00nil"
	}
	return fmt.Sprintf("%T\x00%v", v, v)
}

// isSubqueryCorrelated reports whether stmt references columns from an outer query.
// It detects table-qualified column references where the table qualifier does not
// match the subquery's own FROM table or alias.
// If the subquery has no WHERE clause or only references its own table, it is
// considered non-correlated and safe to cache.
func isSubqueryCorrelated(stmt *QP.SelectStmt) bool {
	if stmt == nil || stmt.From == nil {
		return false
	}
	innerTable := strings.ToLower(stmt.From.Name)
	innerAlias := strings.ToLower(stmt.From.Alias)
	return exprIsCorrelated(stmt.Where, innerTable, innerAlias)
}

// exprIsCorrelated walks expr checking for table-qualified ColumnRef nodes whose
// table qualifier differs from innerTable/innerAlias (indicating a reference to
// an outer query's table).
func exprIsCorrelated(expr QP.Expr, innerTable, innerAlias string) bool {
	if expr == nil {
		return false
	}
	switch e := expr.(type) {
	case *QP.ColumnRef:
		if e.Table != "" {
			tbl := strings.ToLower(e.Table)
			if innerAlias != "" {
				return tbl != innerAlias
			}
			return tbl != innerTable
		}
	case *QP.BinaryExpr:
		if exprIsCorrelated(e.Left, innerTable, innerAlias) ||
			exprIsCorrelated(e.Right, innerTable, innerAlias) {
			return true
		}
		if leftCol, ok := e.Left.(*QP.ColumnRef); ok {
			if rightCol, ok := e.Right.(*QP.ColumnRef); ok {
				leftIsInner := (leftCol.Table != "" && strings.ToLower(leftCol.Table) == innerTable) ||
					(leftCol.Table != "" && innerAlias != "" && strings.ToLower(leftCol.Table) == innerAlias)
				rightIsInner := (rightCol.Table != "" && strings.ToLower(rightCol.Table) == innerTable) ||
					(rightCol.Table != "" && innerAlias != "" && strings.ToLower(rightCol.Table) == innerAlias)
				if leftIsInner && rightCol.Table == "" {
					return true
				}
				if rightIsInner && leftCol.Table == "" {
					return true
				}
			}
		}
	case *QP.UnaryExpr:
		return exprIsCorrelated(e.Expr, innerTable, innerAlias)
	case *QP.FuncCall:
		for _, arg := range e.Args {
			if exprIsCorrelated(arg, innerTable, innerAlias) {
				return true
			}
		}
	case *QP.AliasExpr:
		return exprIsCorrelated(e.Expr, innerTable, innerAlias)
	case *QP.CaseExpr:
		if exprIsCorrelated(e.Operand, innerTable, innerAlias) {
			return true
		}
		for _, when := range e.Whens {
			if exprIsCorrelated(when.Condition, innerTable, innerAlias) ||
				exprIsCorrelated(when.Result, innerTable, innerAlias) {
				return true
			}
		}
		return exprIsCorrelated(e.Else, innerTable, innerAlias)
	}
	return false
}
