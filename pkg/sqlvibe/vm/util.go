package vm

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/cyw0ng95/sqlvibe/internal/QP"
)

// CollectColumnRefs recursively collects all column names referenced by expr.
func CollectColumnRefs(expr QP.Expr) []string {
	if expr == nil {
		return nil
	}
	var refs []string
	switch e := expr.(type) {
	case *QP.ColumnRef:
		if e.Name != "" && e.Name != "*" {
			refs = append(refs, e.Name)
		}
	case *QP.BinaryExpr:
		refs = append(refs, CollectColumnRefs(e.Left)...)
		refs = append(refs, CollectColumnRefs(e.Right)...)
	case *QP.UnaryExpr:
		refs = append(refs, CollectColumnRefs(e.Expr)...)
	case *QP.FuncCall:
		for _, arg := range e.Args {
			refs = append(refs, CollectColumnRefs(arg)...)
		}
	case *QP.CaseExpr:
		refs = append(refs, CollectColumnRefs(e.Operand)...)
		for _, when := range e.Whens {
			refs = append(refs, CollectColumnRefs(when.Condition)...)
			refs = append(refs, CollectColumnRefs(when.Result)...)
		}
		refs = append(refs, CollectColumnRefs(e.Else)...)
	case *QP.AliasExpr:
		refs = append(refs, CollectColumnRefs(e.Expr)...)
	}
	return refs
}

// ExprToSQL converts a QP expression back to a SQL string representation.
func ExprToSQL(expr QP.Expr) string {
	if expr == nil {
		return "NULL"
	}
	switch e := expr.(type) {
	case *QP.Literal:
		if e.Value == nil {
			return "NULL"
		}
		return LiteralToString(e.Value)
	case *QP.ColumnRef:
		if e.Table != "" {
			return e.Table + "." + e.Name
		}
		return e.Name
	case *QP.BinaryExpr:
		var op string
		switch e.Op {
		case QP.TokenPlus:
			op = "+"
		case QP.TokenMinus:
			op = "-"
		case QP.TokenAsterisk:
			op = "*"
		case QP.TokenSlash:
			op = "/"
		case QP.TokenPercent:
			op = "%"
		default:
			op = "+"
		}
		return "(" + ExprToSQL(e.Left) + " " + op + " " + ExprToSQL(e.Right) + ")"
	case *QP.UnaryExpr:
		if e.Op == QP.TokenMinus {
			return "-" + ExprToSQL(e.Expr)
		}
		return ExprToSQL(e.Expr)
	case *QP.FuncCall:
		args := make([]string, len(e.Args))
		for i, arg := range e.Args {
			args[i] = ExprToSQL(arg)
		}
		return e.Name + "(" + strings.Join(args, ", ") + ")"
	default:
		return "NULL"
	}
}

// LiteralToString converts a literal value (or *QP.Literal wrapper) to a
// SQL literal string suitable for embedding in a query.
func LiteralToString(val interface{}) string {
	// Handle QP.Literal wrapper
	if lit, ok := val.(*QP.Literal); ok {
		val = lit.Value
	}
	// Handle QP.Expr (e.g., DEFAULT (1+1)) - serialize back to SQL
	if expr, ok := val.(QP.Expr); ok {
		return ExprToSQL(expr)
	}
	switch v := val.(type) {
	case int64:
		return fmt.Sprintf("%d", v)
	case float64:
		return fmt.Sprintf("%v", v)
	case string:
		return "'" + strings.ReplaceAll(v, "'", "''") + "'"
	case []byte:
		return "X'" + hex.EncodeToString(v) + "'"
	case bool:
		if v {
			return "1"
		}
		return "0"
	default:
		return "NULL"
	}
}
