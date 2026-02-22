package QP

import "strings"

// ColumnType represents the SQL column type for type inference.
type ColumnType int

const (
	TypeAny ColumnType = iota
	TypeInt
	TypeFloat
	TypeText
	TypeBlob
	TypeBool
	TypeNull
)

// InferExprType infers the result type of an expression given a schema map.
func InferExprType(expr Expr, schema map[string]ColumnType) ColumnType {
	if expr == nil {
		return TypeNull
	}
	switch e := expr.(type) {
	case *ColumnRef:
		if t, ok := schema[e.Name]; ok {
			return t
		}
		return TypeAny
	case *Literal:
		return inferFromValue(e.Value)
	case *BinaryExpr:
		left := InferExprType(e.Left, schema)
		right := InferExprType(e.Right, schema)
		return promoteTypes(left, right)
	case *FuncCall:
		return getFuncReturnType(e.Name)
	case *UnaryExpr:
		return InferExprType(e.Expr, schema)
	}
	return TypeAny
}

func inferFromValue(v interface{}) ColumnType {
	switch v.(type) {
	case int64, int:
		return TypeInt
	case float64:
		return TypeFloat
	case string:
		return TypeText
	case []byte:
		return TypeBlob
	case bool:
		return TypeBool
	case nil:
		return TypeNull
	}
	return TypeAny
}

func promoteTypes(a, b ColumnType) ColumnType {
	if a == b {
		return a
	}
	if a == TypeNull {
		return b
	}
	if b == TypeNull {
		return a
	}
	if a == TypeFloat || b == TypeFloat {
		return TypeFloat
	}
	if a == TypeInt || b == TypeInt {
		return TypeInt
	}
	return TypeAny
}

func getFuncReturnType(funcName string) ColumnType {
	switch strings.ToUpper(funcName) {
	case "COUNT", "LENGTH", "INSTR":
		return TypeInt
	case "AVG", "ROUND", "ABS", "CEIL", "FLOOR":
		return TypeFloat
	case "UPPER", "LOWER", "TRIM", "SUBSTR", "REPLACE", "COALESCE":
		return TypeText
	case "MAX", "MIN", "SUM":
		return TypeAny
	}
	return TypeAny
}
