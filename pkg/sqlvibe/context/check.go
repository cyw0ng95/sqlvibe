package context

import (
	"strings"

	"github.com/cyw0ng95/sqlvibe/internal/QP"
	"github.com/cyw0ng95/sqlvibe/internal/VM"
)

// EvalCheckExpr evaluates a SQL expression against a row map and returns the raw value.
// Column references are resolved from the row map. This is used for CHECK constraint evaluation.
func EvalCheckExpr(expr QP.Expr, row map[string]interface{}) interface{} {
	if expr == nil {
		return nil
	}
	switch e := expr.(type) {
	case *QP.Literal:
		return e.Value

	case *QP.ColumnRef:
		// Try qualified name first
		if e.Table != "" {
			if val, ok := row[e.Table+"."+e.Name]; ok {
				return val
			}
		}
		if val, ok := row[e.Name]; ok {
			return val
		}
		// Case-insensitive fallback
		lower := strings.ToLower(e.Name)
		for k, v := range row {
			if strings.ToLower(k) == lower {
				return v
			}
		}
		return nil

	case *QP.BinaryExpr:
		left := EvalCheckExpr(e.Left, row)
		right := EvalCheckExpr(e.Right, row)

		// NULL propagation for comparisons
		if left == nil || right == nil {
			switch e.Op {
			case QP.TokenAnd:
				if left == nil && right == nil {
					return nil
				}
				if left == nil {
					if !IsTruthy(right) {
						return false
					}
					return nil
				}
				if !IsTruthy(left) {
					return false
				}
				return nil
			case QP.TokenOr:
				if left != nil && IsTruthy(left) {
					return true
				}
				if right != nil && IsTruthy(right) {
					return true
				}
				return nil
			}
			return nil
		}

		switch e.Op {
		case QP.TokenEq:
			return VM.CompareVals(left, right) == 0
		case QP.TokenNe:
			return VM.CompareVals(left, right) != 0
		case QP.TokenLt:
			return VM.CompareVals(left, right) < 0
		case QP.TokenLe:
			return VM.CompareVals(left, right) <= 0
		case QP.TokenGt:
			return VM.CompareVals(left, right) > 0
		case QP.TokenGe:
			return VM.CompareVals(left, right) >= 0
		case QP.TokenAnd:
			return IsTruthy(left) && IsTruthy(right)
		case QP.TokenOr:
			return IsTruthy(left) || IsTruthy(right)
		case QP.TokenPlus:
			return AddVals(left, right)
		case QP.TokenMinus:
			return SubVals(left, right)
		case QP.TokenAsterisk:
			return MulVals(left, right)
		case QP.TokenSlash:
			return DivVals(left, right)
		}

	case *QP.UnaryExpr:
		val := EvalCheckExpr(e.Expr, row)
		if e.Op == QP.TokenMinus {
			if iv, ok := val.(int64); ok {
				return -iv
			}
			if fv, ok := val.(float64); ok {
				return -fv
			}
		}
		return val
	}

	return nil
}

// CheckPasses evaluates a CHECK constraint expression and returns whether it passes.
// A CHECK constraint passes when the expression evaluates to a truthy non-NULL value.
func CheckPasses(expr QP.Expr, row map[string]interface{}) bool {
	result := EvalCheckExpr(expr, row)
	return IsTruthy(result)
}
