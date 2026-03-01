package CG

import (
	"testing"

	QP "github.com/cyw0ng95/sqlvibe/internal/QP"
)

func TestCompileExpr_Literal(t *testing.T) {
	colIndices := map[string]int{"id": 0, "name": 1}

	// Test integer literal
	expr := &QP.Literal{Value: int64(42)}
	bytecode := CompileExpr(expr, colIndices)
	if bytecode == nil {
		t.Fatal("CompileExpr should not return nil")
	}

	result := bytecode.Eval(nil)
	if result != int64(42) {
		t.Errorf("Expected 42, got %v", result)
	}
}

func TestCompileExpr_LiteralString(t *testing.T) {
	colIndices := map[string]int{"id": 0, "name": 1}

	expr := &QP.Literal{Value: "hello"}
	bytecode := CompileExpr(expr, colIndices)
	if bytecode == nil {
		t.Fatal("CompileExpr should not return nil")
	}

	result := bytecode.Eval(nil)
	if result != "hello" {
		t.Errorf("Expected 'hello', got %v", result)
	}
}

func TestCompileExpr_LiteralFloat(t *testing.T) {
	colIndices := map[string]int{"id": 0}

	expr := &QP.Literal{Value: float64(3.14)}
	bytecode := CompileExpr(expr, colIndices)
	if bytecode == nil {
		t.Fatal("CompileExpr should not return nil")
	}

	result := bytecode.Eval(nil)
	if result != float64(3.14) {
		t.Errorf("Expected 3.14, got %v", result)
	}
}

func TestCompileExpr_LiteralNull(t *testing.T) {
	colIndices := map[string]int{"id": 0}

	expr := &QP.Literal{Value: nil}
	bytecode := CompileExpr(expr, colIndices)
	if bytecode == nil {
		t.Fatal("CompileExpr should not return nil")
	}

	result := bytecode.Eval(nil)
	if result != nil {
		t.Errorf("Expected nil, got %v", result)
	}
}

func TestCompileExpr_ColumnRef(t *testing.T) {
	colIndices := map[string]int{"id": 0, "name": 1, "age": 2}

	expr := &QP.ColumnRef{Name: "name"}
	bytecode := CompileExpr(expr, colIndices)
	if bytecode == nil {
		t.Fatal("CompileExpr should not return nil")
	}

	row := []interface{}{int64(1), "Alice", int64(30)}
	result := bytecode.Eval(row)
	if result != "Alice" {
		t.Errorf("Expected 'Alice', got %v", result)
	}
}

func TestCompileExpr_ColumnRefNotFound(t *testing.T) {
	colIndices := map[string]int{"id": 0}

	// Column not in schema - should handle gracefully
	expr := &QP.ColumnRef{Name: "unknown_col"}
	bytecode := CompileExpr(expr, colIndices)
	if bytecode == nil {
		t.Fatal("CompileExpr should not return nil")
	}

	// Should not panic on eval
	result := bytecode.Eval([]interface{}{int64(1)})
	_ = result // Just verify no panic
}

func TestCompileExpr_BinaryExpr_Add(t *testing.T) {
	colIndices := map[string]int{"a": 0, "b": 1}

	expr := &QP.BinaryExpr{
		Op:    QP.TokenPlus,
		Left:  &QP.Literal{Value: int64(10)},
		Right: &QP.Literal{Value: int64(5)},
	}
	bytecode := CompileExpr(expr, colIndices)
	if bytecode == nil {
		t.Fatal("CompileExpr should not return nil")
	}

	result := bytecode.Eval(nil)
	if result != int64(15) {
		t.Errorf("Expected 15, got %v", result)
	}
}

func TestCompileExpr_BinaryExpr_Sub(t *testing.T) {
	colIndices := map[string]int{"a": 0, "b": 1}

	expr := &QP.BinaryExpr{
		Op:    QP.TokenMinus,
		Left:  &QP.Literal{Value: int64(10)},
		Right: &QP.Literal{Value: int64(3)},
	}
	bytecode := CompileExpr(expr, colIndices)

	result := bytecode.Eval(nil)
	if result != int64(7) {
		t.Errorf("Expected 7, got %v", result)
	}
}

func TestCompileExpr_BinaryExpr_Mul(t *testing.T) {
	colIndices := map[string]int{"a": 0, "b": 1}

	expr := &QP.BinaryExpr{
		Op:    QP.TokenAsterisk,
		Left:  &QP.Literal{Value: int64(6)},
		Right: &QP.Literal{Value: int64(7)},
	}
	bytecode := CompileExpr(expr, colIndices)

	result := bytecode.Eval(nil)
	if result != int64(42) {
		t.Errorf("Expected 42, got %v", result)
	}
}

func TestCompileExpr_BinaryExpr_Div(t *testing.T) {
	colIndices := map[string]int{"a": 0, "b": 1}

	expr := &QP.BinaryExpr{
		Op:    QP.TokenSlash,
		Left:  &QP.Literal{Value: int64(100)},
		Right: &QP.Literal{Value: int64(4)},
	}
	bytecode := CompileExpr(expr, colIndices)

	result := bytecode.Eval(nil)
	_ = result // Just verify no panic
}

func TestCompileExpr_BinaryExpr_Mod(t *testing.T) {
	colIndices := map[string]int{"a": 0, "b": 1}

	expr := &QP.BinaryExpr{
		Op:    QP.TokenPercent,
		Left:  &QP.Literal{Value: int64(10)},
		Right: &QP.Literal{Value: int64(3)},
	}
	bytecode := CompileExpr(expr, colIndices)

	result := bytecode.Eval(nil)
	if result != int64(1) {
		t.Errorf("Expected 1, got %v", result)
	}
}

func TestCompileExpr_BinaryExpr_Comparison(t *testing.T) {
	tests := []struct {
		name   string
		op     QP.TokenType
		left   interface{}
		right  interface{}
		expect int64
	}{
		{"Eq_true", QP.TokenEq, int64(5), int64(5), int64(1)},
		{"Eq_false", QP.TokenEq, int64(5), int64(3), int64(0)},
		{"Ne_true", QP.TokenNe, int64(5), int64(3), int64(1)},
		{"Ne_false", QP.TokenNe, int64(5), int64(5), int64(0)},
		{"Lt_true", QP.TokenLt, int64(3), int64(5), int64(1)},
		{"Lt_false", QP.TokenLt, int64(5), int64(3), int64(0)},
		{"Le_true", QP.TokenLe, int64(5), int64(5), int64(1)},
		{"Gt_true", QP.TokenGt, int64(5), int64(3), int64(1)},
		{"Ge_true", QP.TokenGe, int64(5), int64(5), int64(1)},
	}

	colIndices := map[string]int{"a": 0, "b": 1}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr := &QP.BinaryExpr{
				Op:    tt.op,
				Left:  &QP.Literal{Value: tt.left},
				Right: &QP.Literal{Value: tt.right},
			}
			bytecode := CompileExpr(expr, colIndices)

			result := bytecode.Eval(nil)
			if result != tt.expect {
				t.Errorf("Expected %v, got %v", tt.expect, result)
			}
		})
	}
}

func TestCompileExpr_BinaryExpr_And(t *testing.T) {
	colIndices := map[string]int{"a": 0, "b": 1}

	expr := &QP.BinaryExpr{
		Op:    QP.TokenAnd,
		Left:  &QP.Literal{Value: int64(1)},
		Right: &QP.Literal{Value: int64(1)},
	}
	bytecode := CompileExpr(expr, colIndices)

	result := bytecode.Eval(nil)
	_ = result // Just verify no panic
}

func TestCompileExpr_BinaryExpr_Or(t *testing.T) {
	colIndices := map[string]int{"a": 0, "b": 1}

	expr := &QP.BinaryExpr{
		Op:    QP.TokenOr,
		Left:  &QP.Literal{Value: int64(1)},
		Right: &QP.Literal{Value: int64(0)},
	}
	bytecode := CompileExpr(expr, colIndices)

	result := bytecode.Eval(nil)
	_ = result // Just verify no panic
}

func TestCompileExpr_BinaryExpr_InvalidOp(t *testing.T) {
	colIndices := map[string]int{"a": 0}

	expr := &QP.BinaryExpr{
		Op:    -1, // Invalid operator
		Left:  &QP.Literal{Value: int64(1)},
		Right: &QP.Literal{Value: int64(2)},
	}
	bytecode := CompileExpr(expr, colIndices)

	// Should not panic, should return nil
	result := bytecode.Eval(nil)
	if result != nil {
		t.Errorf("Expected nil for invalid operator, got %v", result)
	}
}

func TestCompileExpr_UnaryExpr_Negate(t *testing.T) {
	colIndices := map[string]int{"a": 0}

	expr := &QP.UnaryExpr{
		Op:   QP.TokenMinus,
		Expr: &QP.Literal{Value: int64(42)},
	}
	bytecode := CompileExpr(expr, colIndices)

	result := bytecode.Eval(nil)
	if result != int64(-42) {
		t.Errorf("Expected -42, got %v", result)
	}
}

func TestCompileExpr_UnaryExpr_Not(t *testing.T) {
	colIndices := map[string]int{"a": 0}

	expr := &QP.UnaryExpr{
		Op:   QP.TokenNot,
		Expr: &QP.Literal{Value: int64(0)},
	}
	bytecode := CompileExpr(expr, colIndices)

	result := bytecode.Eval(nil)
	_ = result // Just verify no panic
}

func TestCompileExpr_AliasExpr(t *testing.T) {
	colIndices := map[string]int{"a": 0}

	inner := &QP.Literal{Value: int64(42)}
	expr := &QP.AliasExpr{
		Expr:  inner,
		Alias: "result",
	}
	bytecode := CompileExpr(expr, colIndices)

	result := bytecode.Eval(nil)
	if result != int64(42) {
		t.Errorf("Expected 42, got %v", result)
	}
}

func TestCompileExpr_Nil(t *testing.T) {
	colIndices := map[string]int{"a": 0}

	bytecode := CompileExpr(nil, colIndices)
	if bytecode == nil {
		t.Error("CompileExpr(nil) should return empty bytecode, not nil")
	}
}

func TestCompileExpr_ColumnWithColumns(t *testing.T) {
	colIndices := map[string]int{"id": 0, "name": 1, "email": 2}

	// Test accessing different columns
	expr := &QP.ColumnRef{Name: "email"}
	bytecode := CompileExpr(expr, colIndices)

	row := []interface{}{int64(1), "Alice", "alice @example.com"}
	result := bytecode.Eval(row)
	if result != "alice @example.com" {
		t.Errorf("Expected 'alice @example.com', got %v", result)
	}
}

func TestCompileExpr_ComplexExpression(t *testing.T) {
	colIndices := map[string]int{"a": 0, "b": 1, "c": 2}

	// (a + b) * c
	expr := &QP.BinaryExpr{
		Op: QP.TokenAsterisk,
		Left: &QP.BinaryExpr{
			Op:    QP.TokenPlus,
			Left:  &QP.ColumnRef{Name: "a"},
			Right: &QP.ColumnRef{Name: "b"},
		},
		Right: &QP.ColumnRef{Name: "c"},
	}
	bytecode := CompileExpr(expr, colIndices)

	row := []interface{}{int64(2), int64(3), int64(4)}
	result := bytecode.Eval(row)
	if result != int64(20) {
		t.Errorf("Expected (2+3)*4=20, got %v", result)
	}
}

func TestCompileExpr_BooleanLiteral(t *testing.T) {
	colIndices := map[string]int{"a": 0}

	expr := &QP.Literal{Value: true}
	bytecode := CompileExpr(expr, colIndices)

	result := bytecode.Eval(nil)
	if result != true {
		t.Errorf("Expected true, got %v", result)
	}
}
