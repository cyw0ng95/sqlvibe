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
// It also checks for unqualified column references in the WHERE clause, which may
// reference outer table columns when they don't exist in the inner table's scope.
func exprIsCorrelated(expr QP.Expr, innerTable, innerAlias string) bool {
	if expr == nil {
		return false
	}
	switch e := expr.(type) {
	case *QP.ColumnRef:
		if e.Table != "" {
			tbl := strings.ToLower(e.Table)
			// When the inner table has an alias, it must be referenced via that alias.
			// Any table qualifier that differs from the alias is a reference to an outer table.
			// When the inner table has no alias, references to its table name are internal.
			if innerAlias != "" {
				return tbl != innerAlias
			}
			return tbl != innerTable
		}
		// Unqualified column reference in WHERE clause - conservatively treat as
		// potentially correlated since it may reference an outer table's column.
		// This is safe because it just prevents caching of non-existent results.
		return true
	case *QP.BinaryExpr:
		return exprIsCorrelated(e.Left, innerTable, innerAlias) ||
			exprIsCorrelated(e.Right, innerTable, innerAlias)
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
