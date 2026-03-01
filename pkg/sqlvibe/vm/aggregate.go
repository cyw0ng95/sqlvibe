package vm

import (
	"strings"

	"github.com/cyw0ng95/sqlvibe/internal/DS"
	"github.com/cyw0ng95/sqlvibe/internal/QP"
)

// FilterInfo holds the decomposed parts of a simple WHERE col OP literal clause.
type FilterInfo struct {
	ColName string
	Op      string
	Value   DS.Value
}

// IsSimpleAggregate reports whether stmt is a simple single-aggregate query
// (COUNT(*), SUM(col), MIN(col), MAX(col)) with no JOIN, GROUP BY, HAVING,
// DISTINCT, or subquery in FROM. Returns the aggregate name and column name.
func IsSimpleAggregate(stmt *QP.SelectStmt) (funcName string, colName string, ok bool) {
	if stmt == nil || stmt.From == nil {
		return
	}
	if stmt.From.Join != nil || stmt.From.Subquery != nil {
		return
	}
	if stmt.GroupBy != nil || stmt.Having != nil || stmt.Distinct {
		return
	}
	if stmt.Where != nil {
		return
	}
	if len(stmt.Columns) != 1 {
		return
	}
	fc, isFc := stmt.Columns[0].(*QP.FuncCall)
	if !isFc || fc.Distinct {
		return
	}
	upper := strings.ToUpper(fc.Name)
	switch upper {
	case "COUNT":
		// Only fast-path COUNT(*) — COUNT(col) must skip NULLs, handled by VM.
		if len(fc.Args) == 0 {
			return "COUNT", "*", true
		}
		if len(fc.Args) == 1 {
			if cr, ok2 := fc.Args[0].(*QP.ColumnRef); ok2 && cr.Name == "*" {
				return "COUNT", "*", true
			}
		}
	case "SUM", "MIN", "MAX":
		if len(fc.Args) == 1 {
			if cr, ok2 := fc.Args[0].(*QP.ColumnRef); ok2 {
				return upper, cr.Name, true
			}
		}
	}
	return
}

// IsVectorizedFilterEligible reports whether a SELECT statement can use the
// vectorized filter fast path: single table, simple col OP literal WHERE clause,
// no GROUP BY / HAVING / DISTINCT, and only plain column references in SELECT.
func IsVectorizedFilterEligible(stmt *QP.SelectStmt) bool {
	if stmt == nil || stmt.From == nil {
		return false
	}
	if stmt.From.Join != nil || stmt.From.Subquery != nil {
		return false
	}
	if stmt.Where == nil {
		return false
	}
	if stmt.GroupBy != nil || stmt.Having != nil || stmt.Distinct {
		return false
	}

	// Check that SELECT columns are simple: either * or column references only.
	for _, col := range stmt.Columns {
		switch col.(type) {
		case *QP.ColumnRef:
			// OK
		case *QP.AliasExpr:
			if _, ok := col.(*QP.AliasExpr).Expr.(*QP.ColumnRef); !ok {
				return false
			}
		default:
			return false
		}
	}

	bin, ok := stmt.Where.(*QP.BinaryExpr)
	if !ok {
		return false
	}

	switch bin.Op {
	case QP.TokenEq, QP.TokenNe, QP.TokenLt, QP.TokenLe, QP.TokenGt, QP.TokenGe:
	default:
		return false
	}

	if _, ok := bin.Left.(*QP.ColumnRef); !ok {
		return false
	}
	if _, ok := bin.Right.(*QP.Literal); !ok {
		return false
	}

	return true
}

// ExtractFilterInfo extracts column name, operator, and value from a simple
// WHERE clause. Panics if the WHERE is not a valid simple BinaryExpr — callers
// must check IsVectorizedFilterEligible first.
func ExtractFilterInfo(stmt *QP.SelectStmt) *FilterInfo {
	bin := stmt.Where.(*QP.BinaryExpr)
	col := bin.Left.(*QP.ColumnRef)
	lit := bin.Right.(*QP.Literal)

	var op string
	switch bin.Op {
	case QP.TokenEq:
		op = "="
	case QP.TokenNe:
		op = "!="
	case QP.TokenLt:
		op = "<"
	case QP.TokenLe:
		op = "<="
	case QP.TokenGt:
		op = ">"
	case QP.TokenGe:
		op = ">="
	}

	var val DS.Value
	switch v := lit.Value.(type) {
	case int64:
		val = DS.IntValue(v)
	case float64:
		val = DS.FloatValue(v)
	case string:
		val = DS.StringValue(v)
	case bool:
		if v {
			val = DS.IntValue(1)
		} else {
			val = DS.IntValue(0)
		}
	default:
		return nil
	}

	return &FilterInfo{
		ColName: col.Name,
		Op:      op,
		Value:   val,
	}
}
